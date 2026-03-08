package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

func TestSubscriptionManagerUpdatesNetworkHeightFromStream(t *testing.T) {
	st := newMockStore()
	ns := types.Namespace{0: 1}
	if err := st.PutNamespace(context.Background(), ns); err != nil {
		t.Fatalf("PutNamespace: %v", err)
	}
	if err := st.SetSyncState(context.Background(), types.SyncStatus{
		State:         types.Streaming,
		LatestHeight:  5,
		NetworkHeight: 5,
	}); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	ft := newMockFetcher(5)
	subCh := make(chan *types.Header, 1)
	ft.mu.Lock()
	ft.subCh = subCh
	ft.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	sm := &SubscriptionManager{
		store:   st,
		fetcher: ft,
		log:     zerolog.Nop(),
	}
	go func() {
		done <- sm.Run(ctx)
	}()

	subCh <- makeHeader(6)

	deadline := time.After(2 * time.Second)
	for {
		ss, err := st.GetSyncState(context.Background())
		if err == nil && ss.LatestHeight == 6 {
			if ss.NetworkHeight != 6 {
				t.Fatalf("NetworkHeight = %d, want 6", ss.NetworkHeight)
			}
			cancel()
			break
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for sync state update")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
