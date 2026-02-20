package api

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}

func makeEvent(height uint64, namespaces ...types.Namespace) HeightEvent {
	blobs := make([]types.Blob, len(namespaces))
	for i, ns := range namespaces {
		blobs[i] = types.Blob{
			Height:    height,
			Namespace: ns,
			Data:      []byte("data"),
			Index:     i,
		}
	}
	return HeightEvent{
		Height: height,
		Header: &types.Header{Height: height},
		Blobs:  blobs,
	}
}

func TestNotifierSubscribePublish(t *testing.T) {
	n := NewNotifier(16, 1024, zerolog.Nop())
	sub, err := n.Subscribe(nil) // all namespaces
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer n.Unsubscribe(sub)

	event := makeEvent(1, testNamespace(1), testNamespace(2))
	n.Publish(event)

	select {
	case got := <-sub.Events():
		if got.Height != 1 {
			t.Errorf("Height = %d, want 1", got.Height)
		}
		if len(got.Blobs) != 2 {
			t.Errorf("Blobs = %d, want 2", len(got.Blobs))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestNotifierNamespaceFilter(t *testing.T) {
	n := NewNotifier(16, 1024, zerolog.Nop())
	ns1 := testNamespace(1)
	ns2 := testNamespace(2)

	sub, err := n.Subscribe([]types.Namespace{ns1})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer n.Unsubscribe(sub)

	event := makeEvent(1, ns1, ns2)
	n.Publish(event)

	select {
	case got := <-sub.Events():
		if len(got.Blobs) != 1 {
			t.Fatalf("Blobs = %d, want 1", len(got.Blobs))
		}
		if got.Blobs[0].Namespace != ns1 {
			t.Errorf("Blob namespace = %s, want %s", got.Blobs[0].Namespace, ns1)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestNotifierMultipleSubscribers(t *testing.T) {
	n := NewNotifier(16, 1024, zerolog.Nop())
	sub1, err1 := n.Subscribe(nil)
	sub2, err2 := n.Subscribe(nil)
	if err1 != nil || err2 != nil {
		t.Fatalf("Subscribe failed: %v, %v", err1, err2)
	}
	defer n.Unsubscribe(sub1)
	defer n.Unsubscribe(sub2)

	n.Publish(makeEvent(1))

	for i, sub := range []*Subscription{sub1, sub2} {
		select {
		case got := <-sub.Events():
			if got.Height != 1 {
				t.Errorf("sub%d: Height = %d, want 1", i, got.Height)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub%d: timed out", i)
		}
	}
}

func TestNotifierBufferOverflow(t *testing.T) {
	n := NewNotifier(2, 1024, zerolog.Nop())
	sub, err := n.Subscribe(nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer n.Unsubscribe(sub)

	// Fill buffer.
	n.Publish(makeEvent(1))
	n.Publish(makeEvent(2))
	// This should be dropped (non-blocking).
	n.Publish(makeEvent(3))

	// Drain and verify we got 1 and 2.
	got1 := <-sub.Events()
	got2 := <-sub.Events()
	if got1.Height != 1 || got2.Height != 2 {
		t.Errorf("got heights %d, %d; want 1, 2", got1.Height, got2.Height)
	}

	// Channel should be empty now.
	select {
	case ev := <-sub.Events():
		t.Fatalf("unexpected event: height %d", ev.Height)
	default:
		// expected
	}
}

func TestNotifierUnsubscribe(t *testing.T) {
	n := NewNotifier(16, 1024, zerolog.Nop())
	sub, err := n.Subscribe(nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	n.Unsubscribe(sub)

	// Channel should be closed.
	_, ok := <-sub.Events()
	if ok {
		t.Fatal("expected channel to be closed after unsubscribe")
	}

	// Double unsubscribe should not panic.
	n.Unsubscribe(sub)
}

func TestNotifierContiguityTracking(t *testing.T) {
	// Verify that after a buffer overflow, lastHeight is reset.
	// Next delivery should succeed without panic.
	n := NewNotifier(1, 1024, zerolog.Nop())
	sub, err := n.Subscribe(nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer n.Unsubscribe(sub)

	n.Publish(makeEvent(1))
	// Buffer full, event 2 dropped.
	n.Publish(makeEvent(2))
	// Drain.
	<-sub.Events()
	// Event 3 should deliver fine even though 2 was dropped.
	n.Publish(makeEvent(3))

	select {
	case got := <-sub.Events():
		if got.Height != 3 {
			t.Errorf("Height = %d, want 3", got.Height)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestNotifierEmptyNamespaceSetDeliversAll(t *testing.T) {
	n := NewNotifier(16, 1024, zerolog.Nop())
	sub, err := n.Subscribe([]types.Namespace{}) // explicit empty slice
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer n.Unsubscribe(sub)

	ns1 := testNamespace(1)
	ns2 := testNamespace(2)
	n.Publish(makeEvent(1, ns1, ns2))

	select {
	case got := <-sub.Events():
		if len(got.Blobs) != 2 {
			t.Errorf("Blobs = %d, want 2 (empty namespace set = all)", len(got.Blobs))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestNotifierMaxSubscribers(t *testing.T) {
	n := NewNotifier(1, 2, zerolog.Nop())

	sub1, err1 := n.Subscribe(nil)
	sub2, err2 := n.Subscribe(nil)
	_, err3 := n.Subscribe(nil)

	if err1 != nil {
		t.Fatalf("first subscribe failed: %v", err1)
	}
	defer n.Unsubscribe(sub1)
	if err2 != nil {
		t.Fatalf("second subscribe failed: %v", err2)
	}
	defer n.Unsubscribe(sub2)
	if err3 == nil {
		t.Error("third subscribe should have failed")
	}
}
