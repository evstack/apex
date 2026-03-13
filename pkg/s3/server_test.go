package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/evstack/apex/pkg/types"
	"github.com/rs/zerolog"
)

type httpMockStore struct {
	buckets map[string]*Bucket
	objects map[string]map[string]*httpStoredObject
}

type httpStoredObject struct {
	obj  *Object
	data []byte
}

func newHttpMockStore() *httpMockStore {
	return &httpMockStore{
		buckets: make(map[string]*Bucket),
		objects: make(map[string]map[string]*httpStoredObject),
	}
}

func (m *httpMockStore) PutBucket(_ context.Context, name string) error {
	if _, exists := m.buckets[name]; exists {
		return ErrBucketAlreadyExists
	}
	now := time.Now()
	m.buckets[name] = &Bucket{Name: name, CreatedAt: now, LastModified: now}
	m.objects[name] = make(map[string]*httpStoredObject)
	return nil
}

func (m *httpMockStore) GetBucket(_ context.Context, name string) (*Bucket, error) {
	b, ok := m.buckets[name]
	if !ok {
		return nil, ErrBucketNotFound
	}
	return b, nil
}

func (m *httpMockStore) DeleteBucket(_ context.Context, name string) error {
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

func (m *httpMockStore) ListBuckets(_ context.Context) ([]Bucket, error) {
	var result []Bucket
	for _, b := range m.buckets {
		result = append(result, *b)
	}
	return result, nil
}

func (m *httpMockStore) PutObject(_ context.Context, bucket, key string, data []byte, contentType string) (*Object, error) {
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
	m.objects[bucket][key] = &httpStoredObject{obj: obj, data: data}
	return obj, nil
}

func (m *httpMockStore) GetObject(_ context.Context, bucket, key string) (*Object, []byte, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, nil, ErrBucketNotFound
	}
	stored, ok := m.objects[bucket][key]
	if !ok {
		return nil, nil, ErrObjectNotFound
	}
	return stored.obj, stored.data, nil
}

func (m *httpMockStore) DeleteObject(_ context.Context, bucket, key string) error {
	if _, ok := m.buckets[bucket]; !ok {
		return ErrBucketNotFound
	}
	if _, ok := m.objects[bucket][key]; !ok {
		return ErrObjectNotFound
	}
	delete(m.objects[bucket], key)
	return nil
}

func (m *httpMockStore) ListObjects(_ context.Context, bucket, prefix, delimiter, _ string, _ int) (*ListObjectsResult, error) {
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

func (m *httpMockStore) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	stored, ok := m.objects[bucket][key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return stored.obj, nil
}

func setupHttpTestServer(t *testing.T) (*Server, *httpMockStore) {
	store := newHttpMockStore()
	svc := NewService(store, nil, types.Namespace{})
	log := zerolog.New(io.Discard)
	return NewServer(svc, "us-east-1", log), store
}

func TestServer_ListBuckets(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "bucket1")
	store.PutBucket(context.Background(), "bucket2")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "bucket1") || !strings.Contains(body, "bucket2") {
		t.Errorf("expected buckets in response, got: %s", body)
	}
	if !strings.Contains(body, "ListAllMyBucketsResult") {
		t.Errorf("expected ListAllMyBucketsResult in response, got: %s", body)
	}
}

func TestServer_CreateBucket(t *testing.T) {
	server, _ := setupHttpTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if rec.Header().Get("Location") != "/test-bucket" {
		t.Errorf("expected Location header, got: %s", rec.Header().Get("Location"))
	}
}

func TestServer_CreateBucket_AlreadyExists(t *testing.T) {
	server, _ := setupHttpTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	req2 := httptest.NewRequest(http.MethodPut, "/test-bucket", nil)
	rec2 := httptest.NewRecorder()
	server.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", rec2.Code)
	}
}

func TestServer_DeleteBucket(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodDelete, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", rec.Code)
	}
}

func TestServer_DeleteBucket_NotEmpty(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")
	store.PutObject(context.Background(), "test-bucket", "key", []byte("data"), "text/plain")

	req := httptest.NewRequest(http.MethodDelete, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", rec.Code)
	}
}

func TestServer_PutGetObject(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")

	body := []byte("hello world")
	req := httptest.NewRequest(http.MethodPut, "/test-bucket/hello.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if rec.Header().Get("ETag") == "" {
		t.Errorf("expected ETag header")
	}

	req2 := httptest.NewRequest(http.MethodGet, "/test-bucket/hello.txt", nil)
	rec2 := httptest.NewRecorder()
	server.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec2.Code)
	}
	if rec2.Body.String() != "hello world" {
		t.Errorf("expected body 'hello world', got: %s", rec2.Body.String())
	}
	if rec2.Header().Get("Content-Type") != "text/plain" {
		t.Errorf("expected Content-Type text/plain, got: %s", rec2.Header().Get("Content-Type"))
	}
}

func TestServer_GetObject_NotFound(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestServer_HeadObject(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")
	store.PutObject(context.Background(), "test-bucket", "test.txt", []byte("content"), "text/plain")

	req := httptest.NewRequest(http.MethodHead, "/test-bucket/test.txt", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Length") != "7" {
		t.Errorf("expected Content-Length 7, got: %s", rec.Header().Get("Content-Length"))
	}
	if rec.Body.Len() != 0 {
		t.Errorf("expected empty body for HEAD request")
	}
}

func TestServer_DeleteObject(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")
	store.PutObject(context.Background(), "test-bucket", "test.txt", []byte("content"), "text/plain")

	req := httptest.NewRequest(http.MethodDelete, "/test-bucket/test.txt", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/test-bucket/test.txt", nil)
	rec2 := httptest.NewRecorder()
	server.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusNotFound {
		t.Errorf("expected status 404 after delete, got %d", rec2.Code)
	}
}

func TestServer_ListObjects(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")
	store.PutObject(context.Background(), "test-bucket", "file1.txt", []byte("a"), "text/plain")
	store.PutObject(context.Background(), "test-bucket", "file2.txt", []byte("bb"), "text/plain")

	req := httptest.NewRequest(http.MethodGet, "/test-bucket?list-type=2", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "ListBucketResult") {
		t.Errorf("expected ListBucketResult in response, got: %s", body)
	}
	if !strings.Contains(body, "file1.txt") || !strings.Contains(body, "file2.txt") {
		t.Errorf("expected objects in response, got: %s", body)
	}
}

func TestServer_HeadBucket(t *testing.T) {
	server, store := setupHttpTestServer(t)
	store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodHead, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestServer_HeadBucket_NotFound(t *testing.T) {
	server, _ := setupHttpTestServer(t)

	req := httptest.NewRequest(http.MethodHead, "/nonexistent-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestServer_ErrorFormat(t *testing.T) {
	server, _ := setupHttpTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent-bucket/key", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "<Code>NoSuchBucket</Code>") {
		t.Errorf("expected error code NoSuchBucket in response, got: %s", body)
	}
	if !strings.Contains(body, "<RequestId>apex</RequestId>") {
		t.Errorf("expected RequestId apex in response, got: %s", body)
	}
}
