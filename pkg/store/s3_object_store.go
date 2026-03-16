package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	apexs3 "github.com/evstack/apex/pkg/s3"
)

type S3ObjectStore struct {
	client *s3.Client
}

func NewS3ObjectStore(client *s3.Client) *S3ObjectStore {
	return &S3ObjectStore{
		client: client,
	}
}

func (s *S3ObjectStore) PutBucket(ctx context.Context, name string) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		var aerr *s3types.BucketAlreadyExists
		if errors.As(err, &aerr) {
			return apexs3.ErrBucketAlreadyExists
		}
		var oerr *s3types.BucketAlreadyOwnedByYou
		if errors.As(err, &oerr) {
			return apexs3.ErrBucketAlreadyExists
		}
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

func (s *S3ObjectStore) GetBucket(ctx context.Context, name string) (*apexs3.Bucket, error) {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		var nfe *s3types.NotFound
		if errors.As(err, &nfe) {
			return nil, apexs3.ErrBucketNotFound
		}
		if strings.Contains(err.Error(), "NotFound") {
			return nil, apexs3.ErrBucketNotFound
		}
		return nil, fmt.Errorf("head bucket: %w", err)
	}

	return &apexs3.Bucket{
		Name: name,
		// S3 HeadBucket doesn't return creation date, but we can return a dummy or fetch it differently
		CreatedAt:    time.Now(),
		LastModified: time.Now(),
	}, nil
}

func (s *S3ObjectStore) DeleteBucket(ctx context.Context, name string) error {
	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		if strings.Contains(err.Error(), "BucketNotEmpty") {
			return apexs3.ErrBucketNotEmpty
		}
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return apexs3.ErrBucketNotFound
		}
		return fmt.Errorf("delete bucket: %w", err)
	}
	return nil
}

func (s *S3ObjectStore) ListBuckets(ctx context.Context) ([]apexs3.Bucket, error) {
	out, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	var buckets []apexs3.Bucket
	for _, b := range out.Buckets {
		var createdAt time.Time
		if b.CreationDate != nil {
			createdAt = *b.CreationDate
		}
		buckets = append(buckets, apexs3.Bucket{
			Name:      aws.ToString(b.Name),
			CreatedAt: createdAt,
		})
	}
	return buckets, nil
}

func (s *S3ObjectStore) PutObject(ctx context.Context, bucket, key string, data []byte, contentType string) (*apexs3.Object, error) {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, apexs3.ErrBucketNotFound
		}
		return nil, fmt.Errorf("put object: %w", err)
	}

	// Calculate ETag locally or just head the object
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("head object after put: %w", err)
	}

	var lastModified time.Time
	if out.LastModified != nil {
		lastModified = *out.LastModified
	}

	return &apexs3.Object{
		Key:          key,
		Bucket:       bucket,
		Size:         int64(len(data)),
		ETag:         strings.Trim(aws.ToString(out.ETag), "\""),
		ContentType:  contentType,
		LastModified: lastModified,
	}, nil
}

func (s *S3ObjectStore) GetObject(ctx context.Context, bucket, key string) (*apexs3.Object, []byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, nil, apexs3.ErrObjectNotFound
		}
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, nil, apexs3.ErrBucketNotFound
		}
		return nil, nil, fmt.Errorf("get object: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read object body: %w", err)
	}

	var lastModified time.Time
	if out.LastModified != nil {
		lastModified = *out.LastModified
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	} else {
		size = int64(len(data))
	}

	// Extract custom metadata for Celestia
	var height uint64
	var namespace string
	var commitments []string
	if out.Metadata != nil {
		if h, ok := out.Metadata["celestia-height"]; ok {
			_, _ = fmt.Sscanf(h, "%d", &height)
		}
		if n, ok := out.Metadata["celestia-namespace"]; ok {
			namespace = n
		}
		if c, ok := out.Metadata["celestia-commitments"]; ok {
			commitments = strings.Split(c, ",")
		}
	}

	obj := &apexs3.Object{
		Key:          key,
		Bucket:       bucket,
		Size:         size,
		ETag:         strings.Trim(aws.ToString(out.ETag), "\""),
		ContentType:  aws.ToString(out.ContentType),
		LastModified: lastModified,
		Height:       height,
		Namespace:    namespace,
		Commitments:  commitments,
	}

	return obj, data, nil
}

func (s *S3ObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *S3ObjectStore) ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*apexs3.ListObjectsResult, error) {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(int32(maxKeys)),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	if marker != "" {
		input.Marker = aws.String(marker)
	}

	out, err := s.client.ListObjects(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, apexs3.ErrBucketNotFound
		}
		return nil, fmt.Errorf("list objects: %w", err)
	}

	res := &apexs3.ListObjectsResult{
		Bucket:      bucket,
		Prefix:      prefix,
		Delimiter:   delimiter,
		IsTruncated: aws.ToBool(out.IsTruncated),
	}

	for _, cp := range out.CommonPrefixes {
		res.CommonPrefixes = append(res.CommonPrefixes, aws.ToString(cp.Prefix))
	}

	for _, obj := range out.Contents {
		var lastModified time.Time
		if obj.LastModified != nil {
			lastModified = *obj.LastModified
		}

		var size int64
		if obj.Size != nil {
			size = *obj.Size
		}

		var storageClass string
		if obj.StorageClass != "" {
			storageClass = string(obj.StorageClass)
		} else {
			storageClass = "STANDARD"
		}

		res.Objects = append(res.Objects, apexs3.ObjectInfo{
			Key:          aws.ToString(obj.Key),
			LastModified: lastModified,
			ETag:         strings.Trim(aws.ToString(obj.ETag), "\""),
			Size:         size,
			StorageClass: storageClass,
		})
	}

	return res, nil
}

func (s *S3ObjectStore) HeadObject(ctx context.Context, bucket, key string) (*apexs3.Object, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nfe *s3types.NotFound
		if errors.As(err, &nfe) || strings.Contains(err.Error(), "NotFound") {
			return nil, apexs3.ErrObjectNotFound
		}
		return nil, fmt.Errorf("head object: %w", err)
	}

	var lastModified time.Time
	if out.LastModified != nil {
		lastModified = *out.LastModified
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	// Extract custom metadata for Celestia
	var height uint64
	var namespace string
	var commitments []string
	if out.Metadata != nil {
		if h, ok := out.Metadata["celestia-height"]; ok {
			_, _ = fmt.Sscanf(h, "%d", &height)
		}
		if n, ok := out.Metadata["celestia-namespace"]; ok {
			namespace = n
		}
		if c, ok := out.Metadata["celestia-commitments"]; ok {
			commitments = strings.Split(c, ",")
		}
	}

	return &apexs3.Object{
		Key:          key,
		Bucket:       bucket,
		Size:         size,
		ETag:         strings.Trim(aws.ToString(out.ETag), "\""),
		ContentType:  aws.ToString(out.ContentType),
		LastModified: lastModified,
		Height:       height,
		Namespace:    namespace,
		Commitments:  commitments,
	}, nil
}

func (s *S3ObjectStore) UpdateObjectWithBlobs(ctx context.Context, bucket, key string, height uint64, commitments []string) error {
	// To update metadata in S3, we have to copy the object to itself with new metadata.

	// First head the object to get its current content type and existing metadata
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("head object for update: %w", err)
	}

	metadata := head.Metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}

	metadata["celestia-height"] = strconv.FormatUint(height, 10)
	metadata["celestia-commitments"] = strings.Join(commitments, ",")

	source := fmt.Sprintf("%s/%s", bucket, key)
	_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(source),
		MetadataDirective: s3types.MetadataDirectiveReplace,
		Metadata:          metadata,
		ContentType:       head.ContentType,
	})
	if err != nil {
		return fmt.Errorf("update object metadata: %w", err)
	}
	return nil
}
