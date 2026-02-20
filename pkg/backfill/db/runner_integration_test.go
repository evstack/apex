package db

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/backfill"
	"github.com/evstack/apex/pkg/metrics"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// memStore is a minimal in-memory store for integration testing.
type memStore struct {
	mu         sync.Mutex
	headers    map[uint64]*types.Header
	blobs      []types.Blob
	namespaces []types.Namespace
	syncState  *types.SyncStatus
}

func newMemStore(namespaces []types.Namespace) *memStore {
	return &memStore{
		headers:    make(map[uint64]*types.Header),
		namespaces: namespaces,
	}
}

func (m *memStore) PutHeader(_ context.Context, h *types.Header) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headers[h.Height] = h
	return nil
}

func (m *memStore) PutBlobs(_ context.Context, blobs []types.Blob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs = append(m.blobs, blobs...)
	return nil
}

func (m *memStore) GetNamespaces(_ context.Context) ([]types.Namespace, error) {
	return m.namespaces, nil
}

func (m *memStore) SetSyncState(_ context.Context, s types.SyncStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncState = &s
	return nil
}

func (m *memStore) GetSyncState(_ context.Context) (*types.SyncStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.syncState == nil {
		return nil, store.ErrNotFound
	}
	return m.syncState, nil
}

func (m *memStore) GetBlob(context.Context, types.Namespace, uint64, int) (*types.Blob, error) {
	return nil, store.ErrNotFound
}

func (m *memStore) GetBlobs(context.Context, types.Namespace, uint64, uint64, int, int) ([]types.Blob, error) {
	return nil, nil
}

func (m *memStore) GetBlobByCommitment(context.Context, []byte) (*types.Blob, error) {
	return nil, store.ErrNotFound
}

func (m *memStore) GetHeader(_ context.Context, h uint64) (*types.Header, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hdr, ok := m.headers[h]; ok {
		return hdr, nil
	}
	return nil, store.ErrNotFound
}

func (m *memStore) PutNamespace(context.Context, types.Namespace) error { return nil }
func (m *memStore) Close() error                                        { return nil }

func TestRunnerWithDBSource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		backend string
		layout  string
	}{
		{name: "pebble-v1", backend: "pebble", layout: layoutV1},
		{name: "pebble-v2", backend: "pebble", layout: layoutV2},
		{name: "leveldb-v1", backend: "leveldb", layout: layoutV1},
		{name: "leveldb-v2", backend: "leveldb", layout: layoutV2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const numHeights = 20
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "blockstore.db")

			baseTime := time.Unix(1_700_000_000, 0).UTC()

			// Write test blocks for heights 1..numHeights.
			for h := uint64(1); h <= numHeights; h++ {
				ts := baseTime.Add(time.Duration(h) * time.Second)
				hash := []byte{byte(h)}
				dataHash := []byte{byte(h), 0xff}
				if err := writeTestBlock(dbPath, tc.backend, tc.layout, h, hash, dataHash, ts, nil, 0); err != nil {
					t.Fatalf("writeTestBlock height %d: %v", h, err)
				}
			}

			src, err := NewSource(Config{
				Path:    dbPath,
				Backend: tc.backend,
				Layout:  tc.layout,
			}, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewSource: %v", err)
			}
			defer src.Close() //nolint:errcheck

			ms := newMemStore(nil) // no namespaces = headers only

			var observedHeights []uint64
			var obsMu sync.Mutex

			runner := backfill.Runner{
				Store:       ms,
				Source:      src,
				BatchSize:   7, // not a divisor of 20, tests remainder batch
				Concurrency: 3, // multiple concurrent readers
				Observer: func(height uint64, _ *types.Header, _ []types.Blob) {
					obsMu.Lock()
					observedHeights = append(observedHeights, height)
					obsMu.Unlock()
				},
				Metrics: metrics.Nop(),
				Log:     zerolog.Nop(),
			}

			if err := runner.Run(context.Background(), 1, numHeights); err != nil {
				t.Fatalf("Runner.Run: %v", err)
			}

			// Verify all headers were stored.
			for h := uint64(1); h <= numHeights; h++ {
				hdr, err := ms.GetHeader(context.Background(), h)
				if err != nil {
					t.Fatalf("missing header at height %d: %v", h, err)
				}
				if hdr.Height != h {
					t.Fatalf("header height mismatch: got %d want %d", hdr.Height, h)
				}
				expectedTime := baseTime.Add(time.Duration(h) * time.Second)
				if !hdr.Time.Equal(expectedTime) {
					t.Fatalf("header time at height %d: got %s want %s", h, hdr.Time, expectedTime)
				}
			}

			// Verify observer was called for every height.
			if len(observedHeights) != numHeights {
				t.Fatalf("observer called %d times, want %d", len(observedHeights), numHeights)
			}

			// Verify sync state was checkpointed.
			ss, err := ms.GetSyncState(context.Background())
			if err != nil {
				t.Fatalf("GetSyncState: %v", err)
			}
			if ss.LatestHeight != numHeights {
				t.Fatalf("sync state latest_height = %d, want %d", ss.LatestHeight, numHeights)
			}
		})
	}
}

func TestRunnerFromGreaterThanTo(t *testing.T) {
	t.Parallel()

	ms := newMemStore(nil)
	runner := backfill.Runner{
		Store:       ms,
		Source:      nil, // should never be called
		BatchSize:   10,
		Concurrency: 1,
		Metrics:     metrics.Nop(),
		Log:         zerolog.Nop(),
	}

	if err := runner.Run(context.Background(), 100, 50); err != nil {
		t.Fatalf("expected nil error for from > to, got: %v", err)
	}
}
