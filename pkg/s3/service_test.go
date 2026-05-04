package s3

import (
	"bytes"
	"context"
	sha256pkg "crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	gsquare "github.com/celestiaorg/go-square/v3/share"
	"github.com/evstack/apex/pkg/submit"
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

func (m *mockStore) PutBucket(_ context.Context, name string) error {
	if _, exists := m.buckets[name]; exists {
		return ErrBucketAlreadyExists
	}
	now := time.Now()
	m.buckets[name] = &Bucket{Name: name, CreatedAt: now, LastModified: now}
	m.objects[name] = make(map[string]*storedObject)
	return nil
}

func (m *mockStore) GetBucket(_ context.Context, name string) (*Bucket, error) {
	b, ok := m.buckets[name]
	if !ok {
		return nil, ErrBucketNotFound
	}
	return b, nil
}

func (m *mockStore) DeleteBucket(_ context.Context, name string) error {
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

func (m *mockStore) ListBuckets(_ context.Context) ([]Bucket, error) {
	result := make([]Bucket, 0, len(m.buckets))
	for _, b := range m.buckets {
		result = append(result, *b)
	}
	return result, nil
}

func (m *mockStore) PutObject(_ context.Context, bucket, key string, data []byte, contentType, etag, sha256 string) (*Object, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	now := time.Now()
	if etag == "" {
		etag = "etag-" + key
	}
	obj := &Object{
		Key:          key,
		Bucket:       bucket,
		Size:         int64(len(data)),
		ETag:         etag,
		ContentType:  contentType,
		LastModified: now,
		SHA256:       sha256,
	}
	m.objects[bucket][key] = &storedObject{obj: obj, data: data}
	return obj, nil
}

func (m *mockStore) GetObject(_ context.Context, bucket, key string) (*Object, []byte, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, nil, ErrBucketNotFound
	}
	stored, ok := m.objects[bucket][key]
	if !ok {
		return nil, nil, ErrObjectNotFound
	}
	return stored.obj, stored.data, nil
}

func (m *mockStore) DeleteObject(_ context.Context, bucket, key string) error {
	if _, ok := m.buckets[bucket]; !ok {
		return ErrBucketNotFound
	}
	if _, ok := m.objects[bucket][key]; !ok {
		return ErrObjectNotFound
	}
	delete(m.objects[bucket], key)
	return nil
}

func (m *mockStore) ListObjects(_ context.Context, bucket, prefix, delimiter, _ string, _ int) (*ListObjectsResult, error) {
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

func (m *mockStore) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	stored, ok := m.objects[bucket][key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return stored.obj, nil
}

type mockSubmitter struct {
	calls   int
	err     error
	lastReq *submit.Request
}

func (m *mockSubmitter) Submit(_ context.Context, req *submit.Request) (*submit.Result, error) {
	m.calls++
	m.lastReq = req
	if m.err != nil {
		return nil, m.err
	}
	return &submit.Result{Height: uint64(100 + m.calls)}, nil
}

func testNamespace() types.Namespace {
	namespace := gsquare.MustNewV0Namespace([]byte("apexs3x"))
	var ns types.Namespace
	copy(ns[:], namespace.Bytes())
	return ns
}

func TestService_ReadOnly(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, nil, types.Namespace{})
	ctx := context.Background()

	if err := svc.CreateBucket(ctx, "b"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("CreateBucket: expected ErrReadOnly, got %v", err)
	}
	if err := svc.DeleteBucket(ctx, "b"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("DeleteBucket: expected ErrReadOnly, got %v", err)
	}
	if _, err := svc.PutObject(ctx, "b", "k", bytes.NewReader([]byte("x")), "text/plain"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("PutObject: expected ErrReadOnly, got %v", err)
	}
	if err := svc.DeleteObject(ctx, "b", "k"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("DeleteObject: expected ErrReadOnly, got %v", err)
	}
}

func TestService_ValidateBucketName(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"my-bucket", true},
		{"abc", true},
		{"bucket123", true},
		{"123bucket", true},
		{"a", false},                     // too short
		{"ab", false},                    // too short
		{"-bucket", false},               // starts with hyphen
		{"bucket-", false},               // ends with hyphen
		{"Bucket", false},                // uppercase
		{"bucket.name", false},           // dot not allowed
		{"192.168.1.1", false},           // IP address
		{strings.Repeat("a", 64), false}, // too long
	}
	sub := &mockSubmitter{}
	for _, tc := range cases {
		store := newMockStore()
		svc := NewService(store, sub, types.Namespace{})
		err := svc.CreateBucket(context.Background(), tc.name)
		if tc.valid && errors.Is(err, ErrInvalidBucketName) {
			t.Errorf("bucket %q: expected valid, got ErrInvalidBucketName", tc.name)
		}
		if !tc.valid && !errors.Is(err, ErrInvalidBucketName) {
			t.Errorf("bucket %q: expected ErrInvalidBucketName, got %v", tc.name, err)
		}
	}
}

func TestService_PutObject_KeyTooLong(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, testNamespace())
	_ = svc.CreateBucket(context.Background(), "test-bucket")

	longKey := strings.Repeat("k", maxKeyLength+1)
	_, err := svc.PutObject(context.Background(), "test-bucket", longKey, bytes.NewReader([]byte("data")), "text/plain")
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestService_CreateBucket(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, types.Namespace{})

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	err := svc.CreateBucket(ctx, "test-bucket")
	if !errors.Is(err, ErrBucketAlreadyExists) {
		t.Fatalf("expected ErrBucketAlreadyExists, got: %v", err)
	}
}

func TestService_PutGetObject(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, testNamespace())

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
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

func TestService_PutObject_WithSubmitter(t *testing.T) {
	store := newMockStore()
	sub := &mockSubmitter{}
	svc := NewService(store, sub, testNamespace())

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	data := []byte("celestia blob data")
	if _, err := svc.PutObject(ctx, "test-bucket", "key1", bytes.NewReader(data), "application/octet-stream"); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if sub.calls != 1 {
		t.Errorf("expected 1 submit call, got %d", sub.calls)
	}
	if sub.lastReq == nil || len(sub.lastReq.Blobs) != 1 {
		t.Fatalf("expected a single submitted blob, got %#v", sub.lastReq)
	}
	// Submitted blob must be a CommitmentEnvelope, not raw data.
	var env CommitmentEnvelope
	if err := json.Unmarshal(sub.lastReq.Blobs[0].Data, &env); err != nil {
		t.Fatalf("submitted blob is not a CommitmentEnvelope: %v", err)
	}
	if env.Bucket != "test-bucket" || env.Key != "key1" || env.Size != int64(len(data)) {
		t.Errorf("envelope mismatch: bucket=%s key=%s size=%d", env.Bucket, env.Key, env.Size)
	}
	wantSHA256 := hex.EncodeToString(func() []byte { s := sha256pkg.Sum256(data); return s[:] }())
	if env.SHA256 != wantSHA256 {
		t.Errorf("envelope SHA256 = %s, want %s", env.SHA256, wantSHA256)
	}
}

func TestService_PutObject_MissingBucketDoesNotSubmit(t *testing.T) {
	store := newMockStore()
	sub := &mockSubmitter{}
	svc := NewService(store, sub, testNamespace())

	_, err := svc.PutObject(context.Background(), "missing-bucket", "key1", bytes.NewReader([]byte("data")), "text/plain")
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got: %v", err)
	}
	if sub.calls != 0 {
		t.Fatalf("expected no submit call for missing bucket, got %d", sub.calls)
	}
}

func TestService_PutObject_SHA256Verification(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, testNamespace())

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	data := []byte("verify me")
	obj, err := svc.PutObject(ctx, "test-bucket", "key", bytes.NewReader(data), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	sum := sha256pkg.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if obj.SHA256 != want {
		t.Errorf("obj.SHA256 = %s, want %s", obj.SHA256, want)
	}
}

func TestService_PutObject_SubmitterFails(t *testing.T) {
	store := newMockStore()
	failSub := &failingSubmitter{}
	svc := NewService(store, failSub, types.Namespace{})

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err := svc.PutObject(ctx, "test-bucket", "key1", bytes.NewReader([]byte("data")), "text/plain")
	if err == nil {
		t.Fatal("expected error when submitter fails")
	}

	// Object should NOT be in store (rollback behavior).
	_, _, getErr := svc.GetObject(ctx, "test-bucket", "key1")
	if !errors.Is(getErr, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound after failed submission, got: %v", getErr)
	}
}

type failingSubmitter struct{}

func (f *failingSubmitter) Submit(context.Context, *submit.Request) (*submit.Result, error) {
	return nil, errors.New("celestia unavailable")
}

func TestService_PutObject_EmptySkipsSubmission(t *testing.T) {
	store := newMockStore()
	sub := &mockSubmitter{}
	svc := NewService(store, sub, testNamespace())

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err := svc.PutObject(ctx, "test-bucket", "empty", bytes.NewReader([]byte{}), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if sub.calls != 0 {
		t.Errorf("expected 0 submit calls for empty object, got %d", sub.calls)
	}
}

func TestService_PutObject_TooLarge(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, types.Namespace{})
	if err := svc.CreateBucket(context.Background(), "any-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	bigData := make([]byte, maxObjectSize+1)
	_, err := svc.PutObject(context.Background(), "any-bucket", "big", bytes.NewReader(bigData), "application/octet-stream")
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("expected ErrObjectTooLarge, got: %v", err)
	}
}

func TestService_DeleteObject(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, testNamespace())

	ctx := context.Background()
	if err := svc.CreateBucket(ctx, "test-bucket"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	if _, err := svc.PutObject(ctx, "test-bucket", "key", bytes.NewReader([]byte("data")), "text/plain"); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if err := svc.DeleteObject(ctx, "test-bucket", "key"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	_, _, err := svc.GetObject(ctx, "test-bucket", "key")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}

func TestService_ListBuckets(t *testing.T) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, types.Namespace{})

	ctx := context.Background()
	buckets, err := svc.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(buckets))
	}

	_ = svc.CreateBucket(ctx, "bucket-a")
	_ = svc.CreateBucket(ctx, "bucket-b")

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
	svc := NewService(store, &mockSubmitter{}, testNamespace())

	ctx := context.Background()
	_ = svc.CreateBucket(ctx, "test-bucket")

	_, err := svc.HeadObject(ctx, "test-bucket", "nonexistent")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	_, _ = svc.PutObject(ctx, "test-bucket", "key", bytes.NewReader([]byte("data")), "text/plain")

	obj, err := svc.HeadObject(ctx, "test-bucket", "key")
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if obj.Key != "key" {
		t.Errorf("expected key 'key', got %s", obj.Key)
	}
}
