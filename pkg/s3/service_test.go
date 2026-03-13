package s3

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/evstack/apex/pkg/types"
)

type mockStore struct {
	buckets map[string]*Bucket
	objects map[string]map[string]*storedObject
}

type storedObject struct {
	obj  *Object
	data []byte
}

func newMockStore() *mockStore {
	return &mockStore{
		buckets: make(map[string]*Bucket),
		objects: make(map[string]map[string]*storedObject),
	}
}

func (m *mockStore) PutBucket(ctx context.Context, name string) error {
	if _, exists := m.buckets[name]; exists {
		return ErrBucketAlreadyExists
	}
	now := time.Now()
	m.buckets[name] = &Bucket{Name: name, CreatedAt: now, LastModified: now}
	m.objects[name] = make(map[string]*storedObject)
	return nil
}

func (m *mockStore) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	b, ok := m.buckets[name]
	if !ok {
		return nil, ErrBucketNotFound
	}
	return b, nil
}

func (m *mockStore) DeleteBucket(ctx context.Context, name string) error {
	if _, ok := m.buckets[name]; !ok {
		return ErrBucketNotFound
	}
	if len(m.objects[name]) > 0 {
		return ErrBucketNotEmpty
	}
	delete(m.buckets, name)
	delete(m.objects, name)
	return nil
}

func (m *mockStore) ListBuckets(ctx context.Context) ([]Bucket, error) {
	var result []Bucket
	for _, b := range m.buckets {
		result = append(result, *b)
	}
	return result, nil
}

func (m *mockStore) PutObject(ctx context.Context, bucket, key string, data []byte, contentType string) (*Object, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	now := time.Now()
	obj := &Object{
		Key:          key,
		Bucket:       bucket,
		Size:         int64(len(data)),
		ETag:         "etag-" + key,
		ContentType:  contentType,
		LastModified: now,
	}
	m.objects[bucket][key] = &storedObject{obj: obj, data: data}
	return obj, nil
}

func (m *mockStore) GetObject(ctx context.Context, bucket, key string) (*Object, []byte, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, nil, ErrBucketNotFound
	}
	stored, ok := m.objects[bucket][key]
	if !ok {
		return nil, nil, ErrObjectNotFound
	}
	return stored.obj, stored.data, nil
}

func (m *mockStore) DeleteObject(ctx context.Context, bucket, key string) error {
	if _, ok := m.buckets[bucket]; !ok {
		return ErrBucketNotFound
	}
	if _, ok := m.objects[bucket][key]; !ok {
		return ErrObjectNotFound
	}
	delete(m.objects[bucket], key)
	return nil
}

func (m *mockStore) ListObjects(ctx context.Context, bucket, prefix, delimiter, marker string, maxKeys int) (*ListObjectsResult, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	result := &ListObjectsResult{Bucket: bucket, Prefix: prefix, Delimiter: delimiter}
	for key, stored := range m.objects[bucket] {
		result.Objects = append(result.Objects, ObjectInfo{
			Key:          key,
			LastModified: stored.obj.LastModified,
			ETag:         stored.obj.ETag,
			Size:         stored.obj.Size,
			StorageClass: "STANDARD",
		})
	}
	return result, nil
}

func (m *mockStore) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	stored, ok := m.objects[bucket][key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return stored.obj, nil
}

func TestService_CreateBucket(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, nil, types.Namespace{})

	ctx := context.Background()
	err := svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	err = svc.CreateBucket(ctx, "test-bucket")
	if err != ErrBucketAlreadyExists {
		t.Fatalf("expected ErrBucketAlreadyExists, got: %v", err)
	}
}

func TestService_PutGetObject(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, nil, types.Namespace{})

	ctx := context.Background()
	err := svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	data := []byte("hello world")
	obj, err := svc.PutObject(ctx, "test-bucket", "test-key", bytes.NewReader(data), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if obj.Size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), obj.Size)
	}

	gotObj, gotData, err := svc.GetObject(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	if !bytes.Equal(gotData, data) {
		t.Errorf("expected data %q, got %q", data, gotData)
	}
	if gotObj.Key != "test-key" {
		t.Errorf("expected key test-key, got %s", gotObj.Key)
	}
}

func TestService_DeleteObject(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, nil, types.Namespace{})

	ctx := context.Background()
	err := svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	data := []byte("hello world")
	_, err = svc.PutObject(ctx, "test-bucket", "test-key", bytes.NewReader(data), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	err = svc.DeleteObject(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	_, _, err = svc.GetObject(ctx, "test-bucket", "test-key")
	if err != ErrObjectNotFound {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}

func TestService_ListBuckets(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, nil, types.Namespace{})

	ctx := context.Background()
	buckets, err := svc.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(buckets))
	}

	err = svc.CreateBucket(ctx, "bucket-a")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	err = svc.CreateBucket(ctx, "bucket-b")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	buckets, err = svc.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(buckets))
	}
}

func TestService_HeadObject(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, nil, types.Namespace{})

	ctx := context.Background()
	err := svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = svc.HeadObject(ctx, "test-bucket", "nonexistent")
	if err != ErrObjectNotFound {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	data := []byte("hello world")
	_, err = svc.PutObject(ctx, "test-bucket", "test-key", bytes.NewReader(data), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	obj, err := svc.HeadObject(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if obj.Key != "test-key" {
		t.Errorf("expected key test-key, got %s", obj.Key)
	}
}
