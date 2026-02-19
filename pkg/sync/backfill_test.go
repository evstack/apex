package syncer

import (
	"context"
	"fmt"
	"testing"

	"github.com/evstack/apex/pkg/types"
)

func TestBackfillerBatchProcessing(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(10)

	ns := types.Namespace{0: 1}
	if err := st.PutNamespace(context.Background(), ns); err != nil {
		t.Fatalf("PutNamespace: %v", err)
	}

	for h := uint64(1); h <= 10; h++ {
		ft.addHeader(makeHeader(h))
		ft.addBlobs(h, []types.Blob{
			{Height: h, Namespace: ns, Data: []byte("data"), Commitment: []byte(fmt.Sprintf("c%d", h)), Index: 0},
		})
	}

	bf := &Backfiller{
		store:       st,
		fetcher:     ft,
		batchSize:   3,
		concurrency: 2,
	}

	if err := bf.Run(context.Background(), 1, 10); err != nil {
		t.Fatalf("Backfiller.Run: %v", err)
	}

	// Verify all headers stored.
	for h := uint64(1); h <= 10; h++ {
		if _, err := st.GetHeader(context.Background(), h); err != nil {
			t.Errorf("header %d not stored: %v", h, err)
		}
	}

	// Verify blobs stored.
	st.mu.Lock()
	blobCount := len(st.blobs)
	st.mu.Unlock()
	if blobCount != 10 {
		t.Errorf("stored %d blobs, want 10", blobCount)
	}

	// Verify checkpoint was set.
	ss, err := st.GetSyncState(context.Background())
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if ss.LatestHeight != 10 {
		t.Errorf("checkpoint LatestHeight = %d, want 10", ss.LatestHeight)
	}
}

func TestBackfillerContextCancellation(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(100)

	for h := uint64(1); h <= 100; h++ {
		ft.addHeader(makeHeader(h))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	bf := &Backfiller{
		store:       st,
		fetcher:     ft,
		batchSize:   10,
		concurrency: 4,
	}

	err := bf.Run(ctx, 1, 100)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestBackfillerFetchError(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(5)

	// Only add headers 1-3, so 4 and 5 will fail.
	for h := uint64(1); h <= 3; h++ {
		ft.addHeader(makeHeader(h))
	}

	bf := &Backfiller{
		store:       st,
		fetcher:     ft,
		batchSize:   5,
		concurrency: 1,
	}

	err := bf.Run(context.Background(), 1, 5)
	if err == nil {
		t.Fatal("expected error when fetcher returns not found")
	}
}

func TestBackfillerSingleHeight(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(1)
	ft.addHeader(makeHeader(1))

	bf := &Backfiller{
		store:       st,
		fetcher:     ft,
		batchSize:   10,
		concurrency: 4,
	}

	if err := bf.Run(context.Background(), 1, 1); err != nil {
		t.Fatalf("Backfiller.Run: %v", err)
	}

	if _, err := st.GetHeader(context.Background(), 1); err != nil {
		t.Errorf("header 1 not stored: %v", err)
	}
}
