package syncer

import (
	"context"
	"fmt"
	"testing"

	"github.com/evstack/apex/pkg/types"
)

func TestBackfiller(t *testing.T) {
	t.Run("BatchProcessing", func(t *testing.T) {
		t.Parallel()
		st := newMockStore()
		ft := newMockFetcher(10)

		ns := types.Namespace{0: 1}
		if err := st.PutNamespace(context.Background(), ns); err != nil {
			t.Fatalf("PutNamespace: %v", err)
		}

		for h := uint64(1); h <= 10; h++ {
			ft.addHeader(makeHeader(h))
			ft.addBlobs(h, []types.Blob{
				{Height: h, Namespace: ns, Data: []byte("data"), Commitment: fmt.Appendf(nil, "c%d", h), Index: 0},
			})
		}

		bf := &Backfiller{
			store:       st,
			fetcher:     ft,
			batchSize:   3,
			concurrency: 2,
		}

		if err := bf.Run(context.Background(), 1, 10); err != nil {
			t.Fatalf("Run: %v", err)
		}

		for h := uint64(1); h <= 10; h++ {
			if _, err := st.GetHeader(context.Background(), h); err != nil {
				t.Errorf("header %d not stored: %v", h, err)
			}
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		t.Parallel()
		st := newMockStore()
		ft := newMockFetcher(100)

		for h := uint64(1); h <= 100; h++ {
			ft.addHeader(makeHeader(h))
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		bf := &Backfiller{
			store:       st,
			fetcher:     ft,
			batchSize:   10,
			concurrency: 4,
		}

		if err := bf.Run(ctx, 1, 100); err == nil {
			t.Fatal("expected error from cancelled context")
		}
	})

	t.Run("FetchError", func(t *testing.T) {
		t.Parallel()
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

		if err := bf.Run(context.Background(), 1, 5); err == nil {
			t.Fatal("expected error when fetcher returns not found")
		}
	})
}
