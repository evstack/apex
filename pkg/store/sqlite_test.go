package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/evstack/apex/pkg/types"
)

func openTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}

func TestPutGetHeader(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	want := &types.Header{
		Height:    42,
		Hash:      []byte("hash42"),
		DataHash:  []byte("datahash42"),
		Time:      time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC),
		RawHeader: []byte("raw42"),
	}

	if err := s.PutHeader(ctx, want); err != nil {
		t.Fatalf("PutHeader: %v", err)
	}

	got, err := s.GetHeader(ctx, 42)
	if err != nil {
		t.Fatalf("GetHeader: %v", err)
	}

	if got.Height != want.Height {
		t.Errorf("Height = %d, want %d", got.Height, want.Height)
	}
	if string(got.Hash) != string(want.Hash) {
		t.Errorf("Hash = %q, want %q", got.Hash, want.Hash)
	}
	if string(got.DataHash) != string(want.DataHash) {
		t.Errorf("DataHash = %q, want %q", got.DataHash, want.DataHash)
	}
	if !got.Time.Equal(want.Time) {
		t.Errorf("Time = %v, want %v", got.Time, want.Time)
	}
	if string(got.RawHeader) != string(want.RawHeader) {
		t.Errorf("RawHeader = %q, want %q", got.RawHeader, want.RawHeader)
	}
}

func TestGetHeaderNotFound(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	_, err := s.GetHeader(ctx, 999)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPutGetBlobs(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	ns := testNamespace(1)

	blobs := []types.Blob{
		{Height: 10, Namespace: ns, Commitment: []byte("c1"), Data: []byte("d1"), ShareVersion: 0, Index: 0},
		{Height: 10, Namespace: ns, Commitment: []byte("c2"), Data: []byte("d2"), ShareVersion: 0, Index: 1},
		{Height: 11, Namespace: ns, Commitment: []byte("c3"), Data: []byte("d3"), ShareVersion: 0, Index: 0},
	}
	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	// GetBlob single
	got, err := s.GetBlob(ctx, ns, 10, 0)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if string(got.Data) != "d1" {
		t.Errorf("GetBlob data = %q, want %q", got.Data, "d1")
	}
	if got.Height != 10 {
		t.Errorf("GetBlob height = %d, want 10", got.Height)
	}

	// GetBlobs range
	all, err := s.GetBlobs(ctx, ns, 10, 11, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("GetBlobs returned %d blobs, want 3", len(all))
	}
	// Verify ordering
	if all[0].Index != 0 || all[0].Height != 10 {
		t.Errorf("first blob: height=%d index=%d", all[0].Height, all[0].Index)
	}
	if all[1].Index != 1 || all[1].Height != 10 {
		t.Errorf("second blob: height=%d index=%d", all[1].Height, all[1].Index)
	}
	if all[2].Index != 0 || all[2].Height != 11 {
		t.Errorf("third blob: height=%d index=%d", all[2].Height, all[2].Index)
	}
}

func TestGetBlobNotFound(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	_, err := s.GetBlob(ctx, testNamespace(1), 999, 0)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetBlobByCommitment(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	ns := testNamespace(1)

	blobs := []types.Blob{
		{Height: 10, Namespace: ns, Commitment: []byte("c1"), Data: []byte("d1"), ShareVersion: 0, Index: 0},
		{Height: 10, Namespace: ns, Commitment: []byte("c2"), Data: []byte("d2"), ShareVersion: 0, Index: 1},
	}
	if err := s.PutBlobs(ctx, blobs); err != nil {
		t.Fatalf("PutBlobs: %v", err)
	}

	got, err := s.GetBlobByCommitment(ctx, []byte("c2"))
	if err != nil {
		t.Fatalf("GetBlobByCommitment: %v", err)
	}
	if string(got.Data) != "d2" {
		t.Errorf("Data = %q, want %q", got.Data, "d2")
	}
	if got.Height != 10 {
		t.Errorf("Height = %d, want 10", got.Height)
	}
	if got.Index != 1 {
		t.Errorf("Index = %d, want 1", got.Index)
	}
}

func TestGetBlobByCommitmentNotFound(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	_, err := s.GetBlobByCommitment(ctx, []byte("nonexistent"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPutBlobsIdempotent(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	ns := testNamespace(1)

	blob := types.Blob{
		Height: 10, Namespace: ns, Commitment: []byte("c1"),
		Data: []byte("d1"), ShareVersion: 0, Index: 0,
	}

	// Insert twice — second should be a no-op.
	if err := s.PutBlobs(ctx, []types.Blob{blob}); err != nil {
		t.Fatalf("PutBlobs (first): %v", err)
	}
	if err := s.PutBlobs(ctx, []types.Blob{blob}); err != nil {
		t.Fatalf("PutBlobs (second): %v", err)
	}

	all, err := s.GetBlobs(ctx, ns, 10, 10, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 blob after idempotent insert, got %d", len(all))
	}
}

func TestPutHeaderIdempotent(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	hdr := &types.Header{
		Height: 5, Hash: []byte("h"), DataHash: []byte("dh"),
		Time: time.Now(), RawHeader: []byte("r"),
	}
	if err := s.PutHeader(ctx, hdr); err != nil {
		t.Fatalf("PutHeader (first): %v", err)
	}
	if err := s.PutHeader(ctx, hdr); err != nil {
		t.Fatalf("PutHeader (second): %v", err)
	}
}

func TestPutGetNamespaces(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	ns1 := testNamespace(1)
	ns2 := testNamespace(2)

	if err := s.PutNamespace(ctx, ns1); err != nil {
		t.Fatalf("PutNamespace: %v", err)
	}
	if err := s.PutNamespace(ctx, ns2); err != nil {
		t.Fatalf("PutNamespace: %v", err)
	}
	// Idempotent
	if err := s.PutNamespace(ctx, ns1); err != nil {
		t.Fatalf("PutNamespace (dup): %v", err)
	}

	nss, err := s.GetNamespaces(ctx)
	if err != nil {
		t.Fatalf("GetNamespaces: %v", err)
	}
	if len(nss) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(nss))
	}
}

func TestSyncStateUpsertAndRead(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Fresh DB returns ErrNotFound.
	_, err := s.GetSyncState(ctx)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on fresh DB, got %v", err)
	}

	// Set initial state.
	want := types.SyncStatus{State: types.Backfilling, LatestHeight: 100, NetworkHeight: 200}
	if err := s.SetSyncState(ctx, want); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	got, err := s.GetSyncState(ctx)
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if *got != want {
		t.Errorf("SyncState = %+v, want %+v", *got, want)
	}

	// Update (upsert).
	want2 := types.SyncStatus{State: types.Streaming, LatestHeight: 200, NetworkHeight: 300}
	if err := s.SetSyncState(ctx, want2); err != nil {
		t.Fatalf("SetSyncState (update): %v", err)
	}
	got2, err := s.GetSyncState(ctx)
	if err != nil {
		t.Fatalf("GetSyncState (after update): %v", err)
	}
	if *got2 != want2 {
		t.Errorf("SyncState = %+v, want %+v", *got2, want2)
	}
}

func TestMigrationsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open (first): %v", err)
	}
	_ = s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (second): %v", err)
	}
	_ = s2.Close()
}

func TestPutBlobsEmpty(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	if err := s.PutBlobs(ctx, nil); err != nil {
		t.Fatalf("PutBlobs(nil): %v", err)
	}
	if err := s.PutBlobs(ctx, []types.Blob{}); err != nil {
		t.Fatalf("PutBlobs([]): %v", err)
	}
}

func TestGetBlobsEmptyRange(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	blobs, err := s.GetBlobs(ctx, testNamespace(1), 1, 100, 0, 0)
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(blobs) != 0 {
		t.Fatalf("expected 0 blobs, got %d", len(blobs))
	}
}
