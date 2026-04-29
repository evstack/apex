package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/evstack/apex/pkg/submit"
	"github.com/evstack/apex/pkg/types"
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrObjectNotFound      = errors.New("object not found")
	ErrObjectTooLarge      = errors.New("object too large")
	ErrReadOnly            = errors.New("S3 API is read-only: submission is not configured")
)

// ObjectStore is the persistence interface for S3 buckets and objects.
type ObjectStore interface {
	PutBucket(ctx context.Context, name string) error
	GetBucket(ctx context.Context, name string) (*Bucket, error)
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) ([]Bucket, error)

	PutObject(ctx context.Context, bucket, key string, data []byte, contentType string, height uint64, commitments []string) (*Object, error)
	GetObject(ctx context.Context, bucket, key string) (*Object, []byte, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*ListObjectsResult, error)
	HeadObject(ctx context.Context, bucket, key string) (*Object, error)
}

// Service implements S3 API business logic.
type Service struct {
	store     ObjectStore
	submitter submit.Submitter
	namespace types.Namespace
}

// NewService creates a new S3 service.
func NewService(store ObjectStore, submitter submit.Submitter, namespace types.Namespace) *Service {
	return &Service{
		store:     store,
		submitter: submitter,
		namespace: namespace,
	}
}

func (s *Service) CreateBucket(ctx context.Context, name string) error {
	if s.submitter == nil {
		return ErrReadOnly
	}
	return s.store.PutBucket(ctx, name)
}

func (s *Service) DeleteBucket(ctx context.Context, name string) error {
	if s.submitter == nil {
		return ErrReadOnly
	}
	return s.store.DeleteBucket(ctx, name)
}

func (s *Service) ListBuckets(ctx context.Context) ([]Bucket, error) {
	return s.store.ListBuckets(ctx)
}

func (s *Service) HeadBucket(ctx context.Context, name string) (*Bucket, error) {
	return s.store.GetBucket(ctx, name)
}

// PutObject submits blob to Celestia then stores object in SQLite.
// Returns ErrReadOnly if no submitter is configured.
func (s *Service) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	if s.submitter == nil {
		return nil, ErrReadOnly
	}

	data, err := io.ReadAll(r)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, ErrObjectTooLarge
		}
		return nil, fmt.Errorf("read object data: %w", err)
	}
	if len(data) > maxObjectSize {
		return nil, ErrObjectTooLarge
	}

	var height uint64
	var commitments []string

	if len(data) > 0 {
		blob, err := submit.BuildBlob(s.namespace, data, 0, nil)
		if err != nil {
			return nil, fmt.Errorf("build blob: %w", err)
		}
		result, submitErr := s.submitter.Submit(ctx, &submit.Request{
			Blobs: []submit.Blob{blob},
		})
		if submitErr != nil {
			return nil, fmt.Errorf("submit to celestia: %w", submitErr)
		}
		height = result.Height
		commitments = []string{hex.EncodeToString(blob.Commitment)}
	}

	// Write to store only after successful Celestia submission.
	obj, err := s.store.PutObject(ctx, bucket, key, data, contentType, height, commitments)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (s *Service) GetObject(ctx context.Context, bucket, key string) (*Object, []byte, error) {
	return s.store.GetObject(ctx, bucket, key)
}

func (s *Service) DeleteObject(ctx context.Context, bucket, key string) error {
	if s.submitter == nil {
		return ErrReadOnly
	}
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
