package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/evstack/apex/pkg/s3"
)

func openTestObjectStore(t *testing.T) *ObjectStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	sqliteStore, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	return NewObjectStore(sqliteStore)
}

func TestObjectStore_BucketCRUD(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	// Create bucket.
	if err := store.PutBucket(ctx, "my-bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}

	// Get bucket.
	b, err := store.GetBucket(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if b.Name != "my-bucket" {
		t.Errorf("expected name 'my-bucket', got %s", b.Name)
	}

	// Duplicate bucket.
	err = store.PutBucket(ctx, "my-bucket")
	if !errors.Is(err, s3.ErrBucketAlreadyExists) {
		t.Fatalf("expected ErrBucketAlreadyExists, got: %v", err)
	}

	// List buckets.
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}

	// Delete bucket.
	if err := store.DeleteBucket(ctx, "my-bucket"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	_, err = store.GetBucket(ctx, "my-bucket")
	if !errors.Is(err, s3.ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got: %v", err)
	}
}

func TestObjectStore_ObjectCRUD(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if err := store.PutBucket(ctx, "bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}

	// Put object.
	data := []byte("hello celestia")
	obj, err := store.PutObject(ctx, "bucket", "greeting.txt", data, "text/plain", "etag-greeting", "sha256-greeting")
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if obj.Size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), obj.Size)
	}

	// Get object.
	gotObj, gotData, err := store.GetObject(ctx, "bucket", "greeting.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(gotData) != "hello celestia" {
		t.Errorf("expected data 'hello celestia', got %q", gotData)
	}
	if gotObj.ETag == "" {
		t.Error("expected non-empty ETag")
	}
	if gotObj.SHA256 != "sha256-greeting" {
		t.Errorf("expected SHA256 'sha256-greeting', got %q", gotObj.SHA256)
	}

	// Head object.
	headObj, err := store.HeadObject(ctx, "bucket", "greeting.txt")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if headObj.Key != "greeting.txt" {
		t.Errorf("expected key greeting.txt, got %s", headObj.Key)
	}

	// Delete object.
	if err := store.DeleteObject(ctx, "bucket", "greeting.txt"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	_, _, err = store.GetObject(ctx, "bucket", "greeting.txt")
	if !errors.Is(err, s3.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}

func TestObjectStore_ObjectUpsert(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if err := store.PutBucket(ctx, "bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}

	_, err := store.PutObject(ctx, "bucket", "key", []byte("v1"), "text/plain", "", "")
	if err != nil {
		t.Fatalf("PutObject v1: %v", err)
	}

	_, err = store.PutObject(ctx, "bucket", "key", []byte("v2-updated"), "text/plain", "", "")
	if err != nil {
		t.Fatalf("PutObject v2: %v", err)
	}

	_, data, err := store.GetObject(ctx, "bucket", "key")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(data) != "v2-updated" {
		t.Errorf("expected data 'v2-updated', got %q", data)
	}
}

func TestObjectStore_ListObjects_PrefixAndPagination(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if err := store.PutBucket(ctx, "bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "docs/a.txt", []byte("a"), "text/plain", "", ""); err != nil {
		t.Fatalf("PutObject docs/a.txt: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "docs/b.txt", []byte("b"), "text/plain", "", ""); err != nil {
		t.Fatalf("PutObject docs/b.txt: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "images/cat.png", []byte("c"), "image/png", "", ""); err != nil {
		t.Fatalf("PutObject images/cat.png: %v", err)
	}

	// List with prefix.
	result, err := store.ListObjects(ctx, "bucket", "docs/", "", "", 10)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 2 {
		t.Errorf("expected 2 objects with prefix 'docs/', got %d", len(result.Objects))
	}

	// List with pagination (maxKeys=1).
	result, err = store.ListObjects(ctx, "bucket", "", "", "", 1)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(result.Objects))
	}
	if !result.IsTruncated {
		t.Error("expected IsTruncated=true")
	}
	if result.NextMarker == "" {
		t.Error("expected non-empty NextMarker")
	}
}

func TestObjectStore_ListObjects_Delimiter(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if err := store.PutBucket(ctx, "bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "photos/2024/jan.jpg", []byte("j"), "image/jpeg", "", ""); err != nil {
		t.Fatalf("PutObject photos/2024/jan.jpg: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "photos/2024/feb.jpg", []byte("f"), "image/jpeg", "", ""); err != nil {
		t.Fatalf("PutObject photos/2024/feb.jpg: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "photos/2025/mar.jpg", []byte("m"), "image/jpeg", "", ""); err != nil {
		t.Fatalf("PutObject photos/2025/mar.jpg: %v", err)
	}

	result, err := store.ListObjects(ctx, "bucket", "photos/", "/", "", 100)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}

	// Should group into common prefixes "photos/2024/" and "photos/2025/".
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("expected 2 common prefixes, got %d: %v", len(result.CommonPrefixes), result.CommonPrefixes)
	}
	if len(result.Objects) != 0 {
		t.Errorf("expected 0 direct objects, got %d", len(result.Objects))
	}
}

func TestObjectStore_DeleteBucket_NotEmpty(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if err := store.PutBucket(ctx, "bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}
	if _, err := store.PutObject(ctx, "bucket", "key", []byte("data"), "text/plain", "", ""); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	err := store.DeleteBucket(ctx, "bucket")
	if !errors.Is(err, s3.ErrBucketNotEmpty) {
		t.Fatalf("expected ErrBucketNotEmpty, got: %v", err)
	}
}

func TestObjectStore_ObjectOpsMissingBucket(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if _, _, err := store.GetObject(ctx, "missing", "key"); !errors.Is(err, s3.ErrBucketNotFound) {
		t.Fatalf("GetObject expected ErrBucketNotFound, got: %v", err)
	}
	if err := store.DeleteObject(ctx, "missing", "key"); !errors.Is(err, s3.ErrBucketNotFound) {
		t.Fatalf("DeleteObject expected ErrBucketNotFound, got: %v", err)
	}
	if _, err := store.HeadObject(ctx, "missing", "key"); !errors.Is(err, s3.ErrBucketNotFound) {
		t.Fatalf("HeadObject expected ErrBucketNotFound, got: %v", err)
	}
}

func TestObjectStore_ListObjects_DelimiterPagination(t *testing.T) {
	store := openTestObjectStore(t)
	ctx := context.Background()

	if err := store.PutBucket(ctx, "bucket"); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}
	_, _ = store.PutObject(ctx, "bucket", "photos/2024/jan.jpg", []byte("j"), "image/jpeg", "", "")
	_, _ = store.PutObject(ctx, "bucket", "photos/2024/feb.jpg", []byte("f"), "image/jpeg", "", "")
	_, _ = store.PutObject(ctx, "bucket", "photos/2025/mar.jpg", []byte("m"), "image/jpeg", "", "")

	page1, err := store.ListObjects(ctx, "bucket", "photos/", "/", "", 1)
	if err != nil {
		t.Fatalf("ListObjects page1: %v", err)
	}
	if !page1.IsTruncated {
		t.Fatal("expected page1 to be truncated")
	}
	if len(page1.CommonPrefixes) != 1 || page1.CommonPrefixes[0] != "photos/2024/" {
		t.Fatalf("page1 common prefixes = %v, want [photos/2024/]", page1.CommonPrefixes)
	}
	if page1.NextMarker != "photos/2024/" {
		t.Fatalf("page1 NextMarker = %q, want photos/2024/", page1.NextMarker)
	}

	page2, err := store.ListObjects(ctx, "bucket", "photos/", "/", page1.NextMarker, 1)
	if err != nil {
		t.Fatalf("ListObjects page2: %v", err)
	}
	if len(page2.CommonPrefixes) != 1 || page2.CommonPrefixes[0] != "photos/2025/" {
		t.Fatalf("page2 common prefixes = %v, want [photos/2025/]", page2.CommonPrefixes)
	}
	if page2.IsTruncated {
		t.Fatal("expected page2 not to be truncated")
	}
}
