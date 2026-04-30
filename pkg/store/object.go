package store

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 required by S3 protocol for ETag
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/evstack/apex/pkg/s3"
	"github.com/evstack/apex/pkg/types"
)

// ObjectStore implements s3.ObjectStore using SQLite.
type ObjectStore struct {
	writer *sql.DB
	reader *sql.DB
	ns     types.Namespace
}

// NewObjectStore creates an ObjectStore backed by the given SQLiteStore.
func NewObjectStore(db *SQLiteStore, namespace types.Namespace) *ObjectStore {
	return &ObjectStore{
		writer: db.writer,
		reader: db.reader,
		ns:     namespace,
	}
}

func (o *ObjectStore) PutBucket(ctx context.Context, name string) error {
	now := time.Now().UnixNano()
	_, err := o.writer.ExecContext(ctx,
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

func (o *ObjectStore) GetBucket(ctx context.Context, name string) (*s3.Bucket, error) {
	var b s3.Bucket
	var createdAt, updatedAt int64
	err := o.reader.QueryRowContext(ctx,
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

func (o *ObjectStore) DeleteBucket(ctx context.Context, name string) error {
	tx, err := o.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var count int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM s3_objects WHERE bucket = ?`, name).Scan(&count); err != nil {
		return fmt.Errorf("count objects: %w", err)
	}
	if count > 0 {
		return s3.ErrBucketNotEmpty
	}

	result, err := tx.ExecContext(ctx, `DELETE FROM s3_buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return s3.ErrBucketNotFound
	}
	return tx.Commit()
}

func (o *ObjectStore) ListBuckets(ctx context.Context) ([]s3.Bucket, error) {
	rows, err := o.reader.QueryContext(ctx,
		`SELECT name, created_at, updated_at FROM s3_buckets ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("query buckets: %w", err)
	}
	defer func() { _ = rows.Close() }()

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

func (o *ObjectStore) PutObject(ctx context.Context, bucket, key string, data []byte, contentType string, sha256 string, height uint64, commitments []string) (*s3.Object, error) {
	etag := computeETag(data)
	now := time.Now().UnixNano()
	if commitments == nil {
		commitments = []string{}
	}
	commitmentsJSON, _ := json.Marshal(commitments)

	_, err := o.writer.ExecContext(ctx,
		`INSERT INTO s3_objects (bucket, key, size, etag, content_type, last_modified, height, namespace, commitments, sha256, data)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(bucket, key) DO UPDATE SET
		   size = excluded.size,
		   etag = excluded.etag,
		   content_type = excluded.content_type,
		   last_modified = excluded.last_modified,
		   height = excluded.height,
		   commitments = excluded.commitments,
		   sha256 = excluded.sha256,
		   data = excluded.data`,
		bucket, key, len(data), etag, contentType, now, height, o.ns.String(), string(commitmentsJSON), sha256, data)
	if err != nil {
		if isSQLiteFKConstraint(err) {
			return nil, s3.ErrBucketNotFound
		}
		return nil, fmt.Errorf("insert object: %w", err)
	}

	return &s3.Object{
		Key:          key,
		Bucket:       bucket,
		Size:         int64(len(data)),
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Unix(0, now),
		SHA256:       sha256,
		Height:       height,
		Namespace:    o.ns.String(),
		Commitments:  commitments,
	}, nil
}

func (o *ObjectStore) GetObject(ctx context.Context, bucket, key string) (*s3.Object, []byte, error) {
	if err := o.ensureBucketExists(ctx, bucket); err != nil {
		return nil, nil, err
	}

	var obj s3.Object
	var lastModified int64
	var data []byte
	var commitmentsJSON string

	err := o.reader.QueryRowContext(ctx,
		`SELECT key, bucket, size, etag, content_type, last_modified, height, commitments, sha256, data
		 FROM s3_objects WHERE bucket = ? AND key = ?`,
		bucket, key).Scan(&obj.Key, &obj.Bucket, &obj.Size, &obj.ETag, &obj.ContentType,
		&lastModified, &obj.Height, &commitmentsJSON, &obj.SHA256, &data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, s3.ErrObjectNotFound
		}
		return nil, nil, fmt.Errorf("query object: %w", err)
	}
	obj.LastModified = time.Unix(0, lastModified)
	obj.Namespace = o.ns.String()
	if commitmentsJSON != "" && commitmentsJSON != "null" {
		_ = json.Unmarshal([]byte(commitmentsJSON), &obj.Commitments)
	}

	return &obj, data, nil
}

func (o *ObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := o.ensureBucketExists(ctx, bucket); err != nil {
		return err
	}

	result, err := o.writer.ExecContext(ctx,
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

func (o *ObjectStore) ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*s3.ListObjectsResult, error) {
	if _, err := o.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}

	query := `SELECT key, last_modified, etag, size FROM s3_objects WHERE bucket = ?`
	args := []any{bucket}

	if prefix != "" {
		query += ` AND key LIKE ? ESCAPE '\'`
		args = append(args, escapeLIKE(prefix)+"%")
	}
	if marker != "" {
		query += ` AND key > ?`
		args = append(args, marker)
	}

	query += ` ORDER BY key`

	rows, err := o.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query objects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := &s3.ListObjectsResult{
		Bucket:    bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	lastEntry := ""
	for rows.Next() {
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
				if commonPrefix <= marker || commonPrefix == lastEntry {
					continue
				}
				if len(result.Objects)+len(result.CommonPrefixes) >= maxKeys {
					result.IsTruncated = true
					result.NextMarker = lastEntry
					break
				}
				result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				lastEntry = commonPrefix
				continue
			}
		}

		if key <= marker {
			continue
		}
		if len(result.Objects)+len(result.CommonPrefixes) >= maxKeys {
			result.IsTruncated = true
			result.NextMarker = lastEntry
			break
		}
		result.Objects = append(result.Objects, s3.ObjectInfo{
			Key:          key,
			LastModified: time.Unix(0, lastModified),
			ETag:         etag,
			Size:         size,
			StorageClass: "STANDARD",
		})
		lastEntry = key
	}

	return result, rows.Err()
}

func (o *ObjectStore) HeadObject(ctx context.Context, bucket, key string) (*s3.Object, error) {
	if err := o.ensureBucketExists(ctx, bucket); err != nil {
		return nil, err
	}

	var obj s3.Object
	var lastModified int64
	err := o.reader.QueryRowContext(ctx,
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

func isSQLiteUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func isSQLiteFKConstraint(err error) bool {
	return err != nil && strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}

// computeETag returns the MD5 hex digest of data, matching S3's ETag spec.
func computeETag(data []byte) string {
	h := md5.Sum(data) //nolint:gosec // MD5 required by S3 protocol
	return hex.EncodeToString(h[:])
}

// escapeLIKE escapes SQLite LIKE wildcard characters in s so the value is
// treated as a literal prefix rather than a pattern.
func escapeLIKE(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

func (o *ObjectStore) ensureBucketExists(ctx context.Context, bucket string) error {
	_, err := o.GetBucket(ctx, bucket)
	return err
}
