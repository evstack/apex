package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/evstack/apex/pkg/types"
)

// mockS3Client is an in-memory S3 client for testing.
type mockS3Client struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{objects: make(map[string][]byte)}
}

func (m *mockS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = data
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) objectCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objects)
}

func newTestS3Store(t *testing.T) (*S3Store, *mockS3Client) {
	t.Helper()
	mock := newMockS3Client()
	store := newS3Store(mock, "test-bucket", "test", 4) // small chunk size for easier testing
	return store, mock
}

func TestS3Store_PutBlobsAndGetBlob(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)
	ns := testNamespace(1)

	blobs := []types.Blob{
		{Height: 1, Namespace: ns, Data: []byte("data1"), Commitment: []byte("c1"), Index: 0},
		{Height: 1, Namespace: ns, Data: []byte("data2"), Commitment: []byte("c2"), Index: 1},
		{Height: 2, Namespace: ns, Data: []byte("data3"), Commitment: []byte("c3"), Index: 0},
	}

	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// Before flush: read from buffer.
	b, err := s.GetBlob(ctx, ns, 1, 0)
	if err != nil {
		t.Fatalf("GetBlob from buffer: %v", err)
	}
	if !bytes.Equal(b.Data, []byte("data1")) {
		t.Errorf("got data %q, want %q", b.Data, "data1")
	}

	// Flush via SetSyncState.
	if err := s.SetSyncState(ctx, types.SyncStatus{State: types.Backfilling, LatestHeight: 2}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	// After flush: read from S3.
	b, err = s.GetBlob(ctx, ns, 2, 0)
	if err != nil {
		t.Fatalf("GetBlob from S3: %v", err)
	}
	if !bytes.Equal(b.Data, []byte("data3")) {
		t.Errorf("got data %q, want %q", b.Data, "data3")
	}

	// Not found.
	_, err = s.GetBlob(ctx, ns, 99, 0)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestS3Store_GetBlobs(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)
	ns := testNamespace(1)

	// Heights 0-7 span two chunks (0-3, 4-7) with chunkSize=4.
	var blobs []types.Blob
	for h := uint64(0); h < 8; h++ {
		blobs = append(blobs, types.Blob{
			Height:     h,
			Namespace:  ns,
			Data:       []byte{byte(h)},
			Commitment: []byte{byte(h)},
			Index:      0,
		})
	}

	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 7}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	got, err := s.GetBlobs(ctx, ns, 2, 6, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d blobs, want 5", len(got))
	}
	for i, b := range got {
		wantH := uint64(2 + i)
		if b.Height != wantH {
			t.Errorf("blob[%d].Height = %d, want %d", i, b.Height, wantH)
		}
	}

	// With limit and offset.
	got, err = s.GetBlobs(ctx, ns, 0, 7, 2, 3)
	if err != nil {
		t.Fatalf("GetBlobs with limit/offset: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d blobs, want 2", len(got))
	}
	if got[0].Height != 3 || got[1].Height != 4 {
		t.Errorf("unexpected heights: %d, %d", got[0].Height, got[1].Height)
	}
}

func TestS3Store_GetBlobByCommitment(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)
	ns := testNamespace(1)

	commit := []byte("unique-commitment")
	blobs := []types.Blob{
		{Height: 5, Namespace: ns, Data: []byte("target"), Commitment: commit, Index: 0},
	}

	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// From buffer (before flush).
	b, err := s.GetBlobByCommitment(ctx, commit)
	if err != nil {
		t.Fatalf("GetBlobByCommitment from buffer: %v", err)
	}
	if !bytes.Equal(b.Data, []byte("target")) {
		t.Errorf("got data %q, want %q", b.Data, "target")
	}

	// Flush and re-read from S3.
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 5}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	b, err = s.GetBlobByCommitment(ctx, commit)
	if err != nil {
		t.Fatalf("GetBlobByCommitment from S3: %v", err)
	}
	if !bytes.Equal(b.Data, []byte("target")) {
		t.Errorf("got data %q, want %q", b.Data, "target")
	}

	// Not found.
	_, err = s.GetBlobByCommitment(ctx, []byte("nonexistent"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestS3Store_PutHeaderAndGetHeader(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)

	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	hdr := &types.Header{
		Height:    10,
		Hash:      []byte("hash10"),
		DataHash:  []byte("datahash10"),
		Time:      now,
		RawHeader: []byte("raw10"),
	}

	if err := s.PutHeader(ctx, hdr); err != nil {
		t.Fatalf("PutHeader: %v", err)
	}

	// Read from buffer.
	got, err := s.GetHeader(ctx, 10)
	if err != nil {
		t.Fatalf("GetHeader from buffer: %v", err)
	}
	if !bytes.Equal(got.Hash, []byte("hash10")) {
		t.Errorf("got hash %q, want %q", got.Hash, "hash10")
	}

	// Flush and re-read.
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 10}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	got, err = s.GetHeader(ctx, 10)
	if err != nil {
		t.Fatalf("GetHeader from S3: %v", err)
	}
	if !bytes.Equal(got.Hash, []byte("hash10")) {
		t.Errorf("got hash %q, want %q", got.Hash, "hash10")
	}
	if !got.Time.Equal(now) {
		t.Errorf("got time %v, want %v", got.Time, now)
	}

	// Not found.
	_, err = s.GetHeader(ctx, 999)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestS3Store_SyncState(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)

	// Initially not found.
	_, err := s.GetSyncState(ctx)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	status := types.SyncStatus{
		State:         types.Backfilling,
		LatestHeight:  100,
		NetworkHeight: 200,
	}
	if err := s.SetSyncState(ctx, status); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	got, err := s.GetSyncState(ctx)
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if got.State != types.Backfilling || got.LatestHeight != 100 || got.NetworkHeight != 200 {
		t.Errorf("got %+v, want %+v", got, status)
	}

	// Update.
	status.State = types.Streaming
	status.LatestHeight = 200
	if err := s.SetSyncState(ctx, status); err != nil {
		t.Fatalf("SetSyncState update: %v", err)
	}
	got, err = s.GetSyncState(ctx)
	if err != nil {
		t.Fatalf("GetSyncState after update: %v", err)
	}
	if got.State != types.Streaming || got.LatestHeight != 200 {
		t.Errorf("got %+v, want state=streaming, latest=200", got)
	}
}

func TestS3Store_Namespaces(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)

	ns1 := testNamespace(1)
	ns2 := testNamespace(2)

	// Empty initially.
	got, err := s.GetNamespaces(ctx)
	if err != nil {
		t.Fatalf("GetNamespaces empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 namespaces, got %d", len(got))
	}

	// Add two namespaces.
	if err := s.PutNamespace(ctx, ns1); err != nil {
		t.Fatalf("PutNamespace ns1: %v", err)
	}
	if err := s.PutNamespace(ctx, ns2); err != nil {
		t.Fatalf("PutNamespace ns2: %v", err)
	}

	got, err = s.GetNamespaces(ctx)
	if err != nil {
		t.Fatalf("GetNamespaces: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(got))
	}

	// Idempotent add.
	if err := s.PutNamespace(ctx, ns1); err != nil {
		t.Fatalf("PutNamespace idempotent: %v", err)
	}
	got, err = s.GetNamespaces(ctx)
	if err != nil {
		t.Fatalf("GetNamespaces after idempotent: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 namespaces after idempotent add, got %d", len(got))
	}
}

func TestS3Store_FlushOnClose(t *testing.T) {
	ctx := context.Background()
	s, mock := newTestS3Store(t)
	ns := testNamespace(1)

	blobs := []types.Blob{
		{Height: 0, Namespace: ns, Data: []byte("close-data"), Commitment: []byte("cc"), Index: 0},
	}
	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	hdr := &types.Header{Height: 0, Hash: []byte("h0")}
	if err := s.PutHeader(ctx, hdr); err != nil {
		t.Fatalf("PutHeader: %v", err)
	}

	// No S3 objects yet (only in buffer).
	if mock.objectCount() != 0 {
		t.Fatalf("expected 0 S3 objects before close, got %d", mock.objectCount())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After close, objects should be flushed.
	if mock.objectCount() == 0 {
		t.Fatal("expected S3 objects after close, got 0")
	}

	// Re-create store pointing to same mock to verify data persisted.
	s2 := newS3Store(mock, "test-bucket", "test", 4)
	b, err := s2.GetBlob(ctx, ns, 0, 0)
	if err != nil {
		t.Fatalf("GetBlob after close: %v", err)
	}
	if !bytes.Equal(b.Data, []byte("close-data")) {
		t.Errorf("got data %q, want %q", b.Data, "close-data")
	}
}

func TestS3Store_IdempotentPut(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)
	ns := testNamespace(1)

	blob := types.Blob{Height: 3, Namespace: ns, Data: []byte("d"), Commitment: []byte("c"), Index: 0}

	// Put same blob twice.
	if err := s.PutBlobs(ctx, []types.Blob{blob}); err != nil {
		t.Fatalf("PutBlobs 1: %v", err)
	}
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 3}); err != nil {
		t.Fatalf("SetSyncState 1: %v", err)
	}

	if err := s.PutBlobs(ctx, []types.Blob{blob}); err != nil {
		t.Fatalf("PutBlobs 2: %v", err)
	}
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 3}); err != nil {
		t.Fatalf("SetSyncState 2: %v", err)
	}

	// Should still return exactly one blob.
	got, err := s.GetBlobs(ctx, ns, 3, 3, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 blob after idempotent put, got %d", len(got))
	}
}

func TestS3Store_ChunkBoundary(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t) // chunkSize=4
	ns := testNamespace(1)

	// Height 3 -> chunk 0, Height 4 -> chunk 4.
	blobs := []types.Blob{
		{Height: 3, Namespace: ns, Data: []byte("in-chunk-0"), Commitment: []byte("c3"), Index: 0},
		{Height: 4, Namespace: ns, Data: []byte("in-chunk-4"), Commitment: []byte("c4"), Index: 0},
	}
	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 4}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	b3, err := s.GetBlob(ctx, ns, 3, 0)
	if err != nil {
		t.Fatalf("GetBlob height=3: %v", err)
	}
	if !bytes.Equal(b3.Data, []byte("in-chunk-0")) {
		t.Errorf("height=3 data %q, want %q", b3.Data, "in-chunk-0")
	}

	b4, err := s.GetBlob(ctx, ns, 4, 0)
	if err != nil {
		t.Fatalf("GetBlob height=4: %v", err)
	}
	if !bytes.Equal(b4.Data, []byte("in-chunk-4")) {
		t.Errorf("height=4 data %q, want %q", b4.Data, "in-chunk-4")
	}
}

func TestS3Store_MultipleNamespaces(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)
	ns1 := testNamespace(1)
	ns2 := testNamespace(2)

	blobs := []types.Blob{
		{Height: 1, Namespace: ns1, Data: []byte("ns1"), Commitment: []byte("c1"), Index: 0},
		{Height: 1, Namespace: ns2, Data: []byte("ns2"), Commitment: []byte("c2"), Index: 0},
	}
	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}
	if err := s.SetSyncState(ctx, types.SyncStatus{LatestHeight: 1}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	got1, err := s.GetBlobs(ctx, ns1, 1, 1, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs ns1: %v", err)
	}
	if len(got1) != 1 || !bytes.Equal(got1[0].Data, []byte("ns1")) {
		t.Errorf("ns1 blobs: got %d, want 1 with data 'ns1'", len(got1))
	}

	got2, err := s.GetBlobs(ctx, ns2, 1, 1, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs ns2: %v", err)
	}
	if len(got2) != 1 || !bytes.Equal(got2[0].Data, []byte("ns2")) {
		t.Errorf("ns2 blobs: got %d, want 1 with data 'ns2'", len(got2))
	}
}

func TestS3Store_BufferReadThrough(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)
	ns := testNamespace(1)

	// Write header, don't flush.
	hdr := &types.Header{Height: 7, Hash: []byte("h7"), Time: time.Now().UTC().Truncate(time.Second)}
	if err := s.PutHeader(ctx, hdr); err != nil {
		t.Fatalf("PutHeader: %v", err)
	}

	// Write blob, don't flush.
	blob := types.Blob{Height: 7, Namespace: ns, Data: []byte("buf"), Commitment: []byte("cb"), Index: 0}
	if err := s.PutBlobs(ctx, []types.Blob{blob}); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// Read should hit the buffer.
	gotH, err := s.GetHeader(ctx, 7)
	if err != nil {
		t.Fatalf("GetHeader from buffer: %v", err)
	}
	if !bytes.Equal(gotH.Hash, []byte("h7")) {
		t.Errorf("header hash %q, want %q", gotH.Hash, "h7")
	}

	gotB, err := s.GetBlob(ctx, ns, 7, 0)
	if err != nil {
		t.Fatalf("GetBlob from buffer: %v", err)
	}
	if !bytes.Equal(gotB.Data, []byte("buf")) {
		t.Errorf("blob data %q, want %q", gotB.Data, "buf")
	}
}

func TestS3Store_PrefixAndKeyFormat(t *testing.T) {
	mock := newMockS3Client()

	// With prefix.
	s := newS3Store(mock, "b", "myprefix", 64)
	key := s.key("headers", chunkFileName(0))
	want := "myprefix/headers/chunk_0000000000000000.json"
	if key != want {
		t.Errorf("key with prefix = %q, want %q", key, want)
	}

	// Without prefix.
	s2 := newS3Store(mock, "b", "", 64)
	key2 := s2.key("headers", chunkFileName(64))
	want2 := "headers/chunk_0000000000000064.json"
	if key2 != want2 {
		t.Errorf("key without prefix = %q, want %q", key2, want2)
	}

	// Commitment index key.
	commitHex := hex.EncodeToString([]byte("test"))
	key3 := s.key("index", "commitments", commitHex+".json")
	want3 := "myprefix/index/commitments/" + commitHex + ".json"
	if key3 != want3 {
		t.Errorf("commitment key = %q, want %q", key3, want3)
	}
}

func TestS3Store_EmptyPutBlobs(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestS3Store(t)

	if err := s.PutBlobs(ctx, nil); err != nil {
		t.Fatalf("PutBlobs nil: %v", err)
	}
	if err := s.PutBlobs(ctx, []types.Blob{}); err != nil {
		t.Fatalf("PutBlobs empty: %v", err)
	}
}
