package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/evstack/apex/pkg/types"
)

func makeHeader(height uint64) *types.Header {
	return &types.Header{
		Height:    height,
		Hash:      []byte("hash"),
		DataHash:  []byte("datahash"),
		Time:      time.Now(),
		RawHeader: []byte("raw"),
	}
}

func TestCoordinatorFullCycle(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(5)

	// Populate fetcher with headers 1..5.
	for h := uint64(1); h <= 5; h++ {
		ft.addHeader(makeHeader(h))
	}

	coord := New(st, ft,
		WithStartHeight(1),
		WithBatchSize(3),
		WithConcurrency(2),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Set up subscription channel that sends headers 6..8 then triggers cancel.
	subCh := make(chan *types.Header, 10)
	ft.mu.Lock()
	ft.subCh = subCh
	ft.mu.Unlock()

	go func() {
		// Wait for backfill to finish before sending subscription headers.
		for {
			time.Sleep(10 * time.Millisecond)
			s := coord.Status()
			if s.State == types.Streaming {
				break
			}
		}
		for h := uint64(6); h <= 8; h++ {
			ft.addHeader(makeHeader(h))
			subCh <- makeHeader(h)
		}
		// Give time for processing then cancel.
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := coord.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify headers were stored.
	for h := uint64(1); h <= 8; h++ {
		if _, err := st.GetHeader(context.Background(), h); err != nil {
			t.Errorf("header %d not stored: %v", h, err)
		}
	}

	// Verify sync state was checkpointed.
	ss, err := st.GetSyncState(context.Background())
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if ss.LatestHeight != 8 {
		t.Errorf("LatestHeight = %d, want 8", ss.LatestHeight)
	}
}

func TestCoordinatorResumeFromCheckpoint(t *testing.T) {
	st := newMockStore()

	// Simulate prior run that synced to height 5.
	if err := st.SetSyncState(context.Background(), types.SyncStatus{
		State:        types.Streaming,
		LatestHeight: 5,
	}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	ft := newMockFetcher(8)
	for h := uint64(6); h <= 8; h++ {
		ft.addHeader(makeHeader(h))
	}

	ctx, cancel := context.WithCancel(context.Background())

	subCh := make(chan *types.Header, 10)
	ft.mu.Lock()
	ft.subCh = subCh
	ft.mu.Unlock()

	coord := New(st, ft, WithBatchSize(10), WithConcurrency(2))

	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			s := coord.Status()
			if s.State == types.Streaming {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := coord.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Should have backfilled 6..8.
	for h := uint64(6); h <= 8; h++ {
		if _, err := st.GetHeader(context.Background(), h); err != nil {
			t.Errorf("header %d not stored after resume: %v", h, err)
		}
	}
}

func TestCoordinatorGapTriggersRebackfill(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(3)

	for h := uint64(1); h <= 3; h++ {
		ft.addHeader(makeHeader(h))
	}

	// After first streaming, fetcher will report head=6 on re-init.
	callCount := 0
	origGetNetworkHead := ft.headH

	ctx, cancel := context.WithCancel(context.Background())
	coord := New(st, ft, WithStartHeight(1), WithBatchSize(10), WithConcurrency(2))

	subCh := make(chan *types.Header, 10)
	ft.mu.Lock()
	ft.subCh = subCh
	ft.mu.Unlock()

	go func() {
		// Wait for streaming mode.
		for {
			time.Sleep(10 * time.Millisecond)
			if coord.Status().State == types.Streaming {
				break
			}
		}

		if callCount == 0 {
			callCount++
			// Send header with gap (skip 4, send 5).
			ft.addHeader(makeHeader(4))
			ft.addHeader(makeHeader(5))
			ft.addHeader(makeHeader(6))
			ft.mu.Lock()
			ft.headH = 6
			ft.mu.Unlock()
			subCh <- makeHeader(5) // gap: expected 4, got 5

			// Wait for re-backfill to complete and new streaming.
			for {
				time.Sleep(10 * time.Millisecond)
				ss, err := st.GetSyncState(context.Background())
				if err == nil && ss.LatestHeight >= 6 {
					break
				}
			}

			// Create new sub channel for re-subscribe.
			newSubCh := make(chan *types.Header, 10)
			ft.mu.Lock()
			ft.subCh = newSubCh
			ft.mu.Unlock()

			// Wait for streaming again, then cancel.
			for {
				time.Sleep(10 * time.Millisecond)
				if coord.Status().State == types.Streaming {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			cancel()
		}
	}()

	_ = origGetNetworkHead
	err := coord.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Headers 4-6 should have been backfilled after gap.
	for h := uint64(4); h <= 6; h++ {
		if _, err := st.GetHeader(context.Background(), h); err != nil {
			t.Errorf("header %d not stored after gap recovery: %v", h, err)
		}
	}
}

func TestCoordinatorContextCancellation(t *testing.T) {
	st := newMockStore()
	ft := newMockFetcher(0) // head=0, so fromHeight > networkHeight, skip backfill

	ctx, cancel := context.WithCancel(context.Background())

	subCh := make(chan *types.Header, 10)
	ft.mu.Lock()
	ft.subCh = subCh
	ft.mu.Unlock()

	done := make(chan error, 1)
	coord := New(st, ft, WithStartHeight(1))
	go func() {
		done <- coord.Run(ctx)
	}()

	// Wait for streaming, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
