package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/evstack/apex/pkg/s3"
	"github.com/evstack/apex/pkg/types"
)

type ObjectStore struct {
	writer *sql.DB
	reader *sql.DB
	ns     types.Namespace
}

func NewObjectStore(db *SQLiteStore, namespace types.Namespace) *ObjectStore {
	return &ObjectStore{
		writer: db.writer,
		reader: db.reader,
		ns:     namespace,
	}
}

func (s *ObjectStore) PutBucket(ctx context.Context, name string) error {
	now := time.Now().UnixNano()
	_, err := s.writer.ExecContext(ctx,
		`INSERT INTO s3_buckets (name, created_at, updated_at) VALUES (?, ?, ?)`,
		name, now, now)
	if err != nil {
		if isSQLiteUniqueConstraint(err) {
			return s3.ErrBucketAlreadyExists
		}
		return fmt.Errorf("insert bucket: %w", err)
	}
	return nil
}

func (s *ObjectStore) GetBucket(ctx context.Context, name string) (*s3.Bucket, error) {
	var b s3.Bucket
	var createdAt, updatedAt int64
	err := s.reader.QueryRowContext(ctx,
		`SELECT name, created_at, updated_at FROM s3_buckets WHERE name = ?`, name).
		Scan(&b.Name, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, s3.ErrBucketNotFound
		}
		return nil, fmt.Errorf("query bucket: %w", err)
	}
	b.CreatedAt = time.Unix(0, createdAt)
	b.LastModified = time.Unix(0, updatedAt)
	return &b, nil
}

func (s *ObjectStore) DeleteBucket(ctx context.Context, name string) error {
	var count int
	err := s.reader.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM s3_objects WHERE bucket = ?`, name).Scan(&count)
	if err != nil {
		return fmt.Errorf("count objects: %w", err)
	}
	if count > 0 {
		return s3.ErrBucketNotEmpty
	}

	result, err := s.writer.ExecContext(ctx,
		`DELETE FROM s3_buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return s3.ErrBucketNotFound
	}
	return nil
}

func (s *ObjectStore) ListBuckets(ctx context.Context) ([]s3.Bucket, error) {
	rows, err := s.reader.QueryContext(ctx,
		`SELECT name, created_at, updated_at FROM s3_buckets ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("query buckets: %w", err)
	}
	defer rows.Close()

	var buckets []s3.Bucket
	for rows.Next() {
		var b s3.Bucket
		var createdAt, updatedAt int64
		if err := rows.Scan(&b.Name, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		b.CreatedAt = time.Unix(0, createdAt)
		b.LastModified = time.Unix(0, updatedAt)
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}

func (s *ObjectStore) PutObject(ctx context.Context, bucket, key string, data []byte, contentType string) (*s3.Object, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}

	etag := computeETag(data)
	now := time.Now().UnixNano()
	commitmentsJSON, _ := json.Marshal([]string{})

	result, err := s.writer.ExecContext(ctx,
		`INSERT INTO s3_objects (bucket, key, size, etag, content_type, last_modified, height, namespace, blob_count, commitments, data)
		 VALUES (?, ?, ?, ?, ?, ?, 0, ?, 0, ?, ?)
		 ON CONFLICT(bucket, key) DO UPDATE SET
		   size = excluded.size,
		   etag = excluded.etag,
		   content_type = excluded.content_type,
		   last_modified = excluded.last_modified,
		   data = excluded.data`,
		bucket, key, len(data), etag, contentType, now, s.ns.String(), commitmentsJSON, data)
	if err != nil {
		return nil, fmt.Errorf("insert object: %w", err)
	}

	obj := &s3.Object{
		Key:          key,
		Bucket:       bucket,
		Size:         int64(len(data)),
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Unix(0, now),
		Namespace:    s.ns.String(),
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		return obj, nil
	}
	return obj, nil
}

func (s *ObjectStore) GetObject(ctx context.Context, bucket, key string) (*s3.Object, []byte, error) {
	obj, err := s.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}

	var data []byte
	var height uint64
	var namespace string
	var commitmentsJSON string
	err = s.reader.QueryRowContext(ctx,
		`SELECT data, height, namespace, commitments FROM s3_objects WHERE bucket = ? AND key = ?`,
		bucket, key).Scan(&data, &height, &namespace, &commitmentsJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("query object data: %w", err)
	}

	obj.Height = height
	obj.Namespace = namespace
	if commitmentsJSON != "" && commitmentsJSON != "null" {
		_ = json.Unmarshal([]byte(commitmentsJSON), &obj.Commitments)
	}

	return obj, data, nil
}

func (s *ObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	result, err := s.writer.ExecContext(ctx,
		`DELETE FROM s3_objects WHERE bucket = ? AND key = ?`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return s3.ErrObjectNotFound
	}
	return nil
}

func (s *ObjectStore) ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*s3.ListObjectsResult, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}

	query := `SELECT key, last_modified, etag, size FROM s3_objects WHERE bucket = ?`
	args := []any{bucket}

	if prefix != "" {
		query += ` AND key LIKE ?`
		args = append(args, prefix+"%")
	}
	if marker != "" {
		query += ` AND key > ?`
		args = append(args, marker)
	}

	query += ` ORDER BY key LIMIT ?`
	args = append(args, maxKeys+1)

	rows, err := s.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query objects: %w", err)
	}
	defer rows.Close()

	result := &s3.ListObjectsResult{
		Bucket:    bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	prefixes := make(map[string]bool)

	count := 0
	for rows.Next() {
		if count >= maxKeys {
			result.IsTruncated = true
			break
		}

		var key string
		var lastModified int64
		var etag string
		var size int64
		if err := rows.Scan(&key, &lastModified, &etag, &size); err != nil {
			return nil, fmt.Errorf("scan object: %w", err)
		}

		if delimiter != "" {
			afterPrefix := strings.TrimPrefix(key, prefix)
			if idx := strings.Index(afterPrefix, delimiter); idx >= 0 {
				commonPrefix := prefix + afterPrefix[:idx+1]
				if !prefixes[commonPrefix] {
					prefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
				continue
			}
		}

		result.Objects = append(result.Objects, s3.ObjectInfo{
			Key:          key,
			LastModified: time.Unix(0, lastModified),
			ETag:         etag,
			Size:         size,
			StorageClass: "STANDARD",
		})
		count++
	}

	return result, rows.Err()
}

func (s *ObjectStore) HeadObject(ctx context.Context, bucket, key string) (*s3.Object, error) {
	var obj s3.Object
	var lastModified int64
	err := s.reader.QueryRowContext(ctx,
		`SELECT key, bucket, size, etag, content_type, last_modified FROM s3_objects WHERE bucket = ? AND key = ?`,
		bucket, key).Scan(&obj.Key, &obj.Bucket, &obj.Size, &obj.ETag, &obj.ContentType, &lastModified)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, s3.ErrObjectNotFound
		}
		return nil, fmt.Errorf("query object: %w", err)
	}
	obj.LastModified = time.Unix(0, lastModified)
	return &obj, nil
}

func (s *ObjectStore) UpdateObjectWithBlobs(ctx context.Context, bucket, key string, height uint64, commitments []string) error {
	commitmentsJSON, _ := json.Marshal(commitments)
	_, err := s.writer.ExecContext(ctx,
		`UPDATE s3_objects SET height = ?, blob_count = ?, commitments = ? WHERE bucket = ? AND key = ?`,
		height, len(commitments), string(commitmentsJSON), bucket, key)
	return err
}

func isSQLiteUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func computeETag(data []byte) string {
	return fmt.Sprintf("%x", len(data))
}
