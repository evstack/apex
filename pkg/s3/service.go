package s3

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 required by S3 protocol for ETag
	sha256pkg "crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/evstack/apex/pkg/submit"
	"github.com/evstack/apex/pkg/types"
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrInvalidBucketName   = errors.New("bucket name is invalid: must be 3-63 lowercase alphanumeric characters or hyphens, start and end with a letter or number, and not be an IP address")
	ErrEmptyObject         = errors.New("empty objects are not supported")
	ErrObjectNotFound      = errors.New("object not found")
	ErrObjectTooLarge      = errors.New("object too large")
	ErrKeyTooLong          = errors.New("object key exceeds maximum length of 1024 bytes")
	ErrReadOnly            = errors.New("S3 API is read-only: submission is not configured")
)

// ObjectStore is the persistence interface for S3 buckets and objects.
type ObjectStore interface {
	PutBucket(ctx context.Context, name string) error
	GetBucket(ctx context.Context, name string) (*Bucket, error)
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) ([]Bucket, error)

	PutObject(ctx context.Context, bucket, key string, data []byte, contentType, etag, sha256 string) (*Object, error)
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
	if err := validateBucketName(name); err != nil {
		return err
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

// PutObject stores an object after submitting its commitment envelope to
// Celestia. Empty objects are rejected. Returns ErrReadOnly if no submitter is
// configured.
func (s *Service) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	if s.submitter == nil {
		return nil, ErrReadOnly
	}
	if len(key) > maxKeyLength {
		return nil, ErrKeyTooLong
	}
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		return nil, err
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
	if len(data) == 0 {
		return nil, ErrEmptyObject
	}

	sum := sha256pkg.Sum256(data)
	sha256Hex := hex.EncodeToString(sum[:])

	md5sum := md5.Sum(data) //nolint:gosec // MD5 required by S3 protocol for ETag
	etag := hex.EncodeToString(md5sum[:])

	envelope := CommitmentEnvelope{
		Version:     1,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		Size:        int64(len(data)),
		SHA256:      sha256Hex,
		ETag:        etag,
	}
	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal commitment envelope: %w", err)
	}

	blob, err := submit.BuildBlob(s.namespace, envelopeBytes, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("build blob: %w", err)
	}
	if _, submitErr := s.submitter.Submit(ctx, &submit.Request{
		Blobs: []submit.Blob{blob},
	}); submitErr != nil {
		return nil, fmt.Errorf("submit to celestia: %w", submitErr)
	}

	// Write to store only after successful Celestia submission.
	obj, err := s.store.PutObject(ctx, bucket, key, data, contentType, etag, sha256Hex)
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

// validateBucketName enforces S3 bucket naming rules:
// 3-63 lowercase alphanumeric characters or hyphens, starting and ending
// with a letter or number, and not formatted as an IP address.
func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return ErrInvalidBucketName
	}
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return ErrInvalidBucketName
		}
	}
	if name[0] == '-' || name[len(name)-1] == '-' {
		return ErrInvalidBucketName
	}
	if net.ParseIP(name) != nil {
		return ErrInvalidBucketName
	}
	return nil
}
