package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/evstack/apex/pkg/types"
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrObjectNotFound      = errors.New("object not found")
	ErrInvalidBucketName   = errors.New("invalid bucket name")
	ErrInvalidObjectKey    = errors.New("invalid object key")
	ErrObjectTooLarge      = errors.New("object too large")
)

type BlobSubmitter interface {
	SubmitBlob(ctx context.Context, namespace types.Namespace, data []byte) (*types.Blob, error)
}

type ObjectStore interface {
	PutBucket(ctx context.Context, name string) error
	GetBucket(ctx context.Context, name string) (*Bucket, error)
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) ([]Bucket, error)

	PutObject(ctx context.Context, bucket, key string, data []byte, contentType string) (*Object, error)
	GetObject(ctx context.Context, bucket, key string) (*Object, []byte, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*ListObjectsResult, error)
	HeadObject(ctx context.Context, bucket, key string) (*Object, error)
}

type Service struct {
	store     ObjectStore
	submitter BlobSubmitter
	namespace types.Namespace
}

func NewService(store ObjectStore, submitter BlobSubmitter, namespace types.Namespace) *Service {
	return &Service{
		store:     store,
		submitter: submitter,
		namespace: namespace,
	}
}

func (s *Service) CreateBucket(ctx context.Context, name string) error {
	return s.store.PutBucket(ctx, name)
}

func (s *Service) DeleteBucket(ctx context.Context, name string) error {
	return s.store.DeleteBucket(ctx, name)
}

func (s *Service) ListBuckets(ctx context.Context) ([]Bucket, error) {
	return s.store.ListBuckets(ctx)
}

func (s *Service) HeadBucket(ctx context.Context, name string) (*Bucket, error) {
	return s.store.GetBucket(ctx, name)
}

func (s *Service) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}
	if len(data) > maxObjectSize {
		return nil, ErrObjectTooLarge
	}

	obj, err := s.store.PutObject(ctx, bucket, key, data, contentType)
	if err != nil {
		return nil, err
	}

	if s.submitter != nil {
		blob, err := s.submitter.SubmitBlob(ctx, s.namespace, data)
		if err != nil {
			return nil, fmt.Errorf("submit to celestia: %w", err)
		}

		if updater, ok := s.store.(interface {
			UpdateObjectWithBlobs(ctx context.Context, bucket, key string, height uint64, commitments []string) error
		}); ok {
			_ = updater.UpdateObjectWithBlobs(ctx, bucket, key, blob.Height, []string{hex.EncodeToString(blob.Commitment)})
		}
	}

	return obj, nil
}

func (s *Service) GetObject(ctx context.Context, bucket, key string) (*Object, []byte, error) {
	return s.store.GetObject(ctx, bucket, key)
}

func (s *Service) DeleteObject(ctx context.Context, bucket, key string) error {
	return s.store.DeleteObject(ctx, bucket, key)
}

func (s *Service) ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*ListObjectsResult, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	return s.store.ListObjects(ctx, bucket, prefix, delimiter, marker, maxKeys)
}

func (s *Service) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	return s.store.HeadObject(ctx, bucket, key)
}
