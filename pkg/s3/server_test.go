package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/rs/zerolog"
)

func setupHTTPTestServer() (*Server, *mockStore) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, testNamespace())
	log := zerolog.New(io.Discard)
	return NewServer(svc, "us-east-1", "", "", log), store
}

func setupHTTPTestServerWithAuth() (*Server, *mockStore) {
	store := newMockStore()
	svc := NewService(store, &mockSubmitter{}, testNamespace())
	log := zerolog.New(io.Discard)
	return NewServer(svc, "us-east-1", "app-key", "app-secret", log), store
}

func TestServer_ListBuckets(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "bucket1")
	_ = store.PutBucket(context.Background(), "bucket2")

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
	server, _ := setupHTTPTestServer()

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
	server, _ := setupHTTPTestServer()

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
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodDelete, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", rec.Code)
	}
}

func TestServer_DeleteBucket_NotEmpty(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")
	_, _ = store.PutObject(context.Background(), "test-bucket", "key", []byte("data"), "text/plain", "", "")

	req := httptest.NewRequest(http.MethodDelete, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", rec.Code)
	}
}

func TestServer_PutGetObject(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")

	body := []byte("hello world")
	req := httptest.NewRequest(http.MethodPut, "/test-bucket/hello.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if rec.Header().Get("ETag") == "" {
		t.Error("expected ETag header")
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

func TestServer_PutObject_EmptyRejected(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/empty.txt", bytes.NewReader(nil))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidRequest</Code>") {
		t.Fatalf("expected InvalidRequest error, got: %s", rec.Body.String())
	}
}

func TestServer_GetObject_NotFound(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/nonexistent.txt", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestServer_HeadObject(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")
	_, _ = store.PutObject(context.Background(), "test-bucket", "test.txt", []byte("content"), "text/plain", "", "")

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
		t.Error("expected empty body for HEAD request")
	}
}

func TestServer_DeleteObject(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")
	_, _ = store.PutObject(context.Background(), "test-bucket", "test.txt", []byte("content"), "text/plain", "", "")

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
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")
	_, _ = store.PutObject(context.Background(), "test-bucket", "file1.txt", []byte("a"), "text/plain", "", "")
	_, _ = store.PutObject(context.Background(), "test-bucket", "file2.txt", []byte("bb"), "text/plain", "", "")

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
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")

	req := httptest.NewRequest(http.MethodHead, "/test-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestServer_HeadBucket_NotFound(t *testing.T) {
	server, _ := setupHTTPTestServer()

	req := httptest.NewRequest(http.MethodHead, "/nonexistent-bucket", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestServer_ErrorFormat(t *testing.T) {
	server, _ := setupHTTPTestServer()

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

func TestServer_URLDecoding(t *testing.T) {
	server, store := setupHTTPTestServer()
	_ = store.PutBucket(context.Background(), "test-bucket")

	// PUT with URL-encoded key
	req := httptest.NewRequest(http.MethodPut, "/test-bucket/my%20file.txt", bytes.NewReader([]byte("data")))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// GET with same URL-encoded key
	req2 := httptest.NewRequest(http.MethodGet, "/test-bucket/my%20file.txt", nil)
	rec2 := httptest.NewRecorder()
	server.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec2.Code)
	}
	if rec2.Body.String() != "data" {
		t.Errorf("expected body 'data', got: %s", rec2.Body.String())
	}
}

func TestServer_AuthRejectsMissingAuthorization(t *testing.T) {
	server, _ := setupHTTPTestServerWithAuth()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>AccessDenied</Code>") {
		t.Fatalf("expected AccessDenied error, got: %s", rec.Body.String())
	}
}

func TestServer_AuthAcceptsSignedRequest(t *testing.T) {
	server, store := setupHTTPTestServerWithAuth()
	_ = store.PutBucket(context.Background(), "bucket1")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	signRequest(t, req, nil, "app-key", "app-secret", "us-east-1")

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServer_AuthRejectsWrongSecret(t *testing.T) {
	server, store := setupHTTPTestServerWithAuth()
	_ = store.PutBucket(context.Background(), "bucket1")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	signRequest(t, req, nil, "app-key", "wrong-secret", "us-east-1")

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>SignatureDoesNotMatch</Code>") {
		t.Fatalf("expected SignatureDoesNotMatch error, got: %s", rec.Body.String())
	}
}

func TestServer_HandlePutObjectRejectsPayloadHashMismatch(t *testing.T) {
	server, store := setupHTTPTestServerWithAuth()
	_ = store.PutBucket(context.Background(), "bucket1")

	signedBody := []byte("hello world")
	tamperedBody := []byte("jello world")
	req := httptest.NewRequest(http.MethodPut, "/bucket1/hello.txt", bytes.NewReader(tamperedBody))
	req.Header.Set("Content-Type", "text/plain")
	sum := sha256.Sum256(signedBody)
	req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))

	rec := httptest.NewRecorder()
	server.handlePutObject(req, rec, "bucket1", "hello.txt")

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>XAmzContentSHA256Mismatch</Code>") {
		t.Fatalf("expected XAmzContentSHA256Mismatch error, got: %s", rec.Body.String())
	}
}

func signRequest(t *testing.T, req *http.Request, body []byte, accessKeyID, secretAccessKey, region string) {
	t.Helper()

	if body != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}

	payloadHash := emptyPayloadSHA256
	if body != nil {
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	}

	signer := v4.NewSigner()
	err := signer.SignHTTP(
		context.Background(),
		aws.Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		},
		req,
		payloadHash,
		"s3",
		region,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	if body != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
}
