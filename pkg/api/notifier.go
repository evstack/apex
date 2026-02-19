package api

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

// HeightEvent is published when a new height is processed.
type HeightEvent struct {
	Height uint64
	Header *types.Header
	Blobs  []types.Blob
}

// Subscription receives height events from the Notifier.
type Subscription struct {
	id         uint64
	ch         chan HeightEvent
	namespaces map[types.Namespace]struct{} // empty = all namespaces
	lastHeight uint64
}

// Events returns the channel on which events are delivered.
func (s *Subscription) Events() <-chan HeightEvent {
	return s.ch
}

// Notifier fans out height events to subscribed API clients.
type Notifier struct {
	mu          sync.RWMutex
	subscribers map[uint64]*Subscription
	nextID      atomic.Uint64
	bufferSize  int
	log         zerolog.Logger
}

// NewNotifier creates a Notifier with the given per-subscriber buffer size.
func NewNotifier(bufferSize int, log zerolog.Logger) *Notifier {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	return &Notifier{
		subscribers: make(map[uint64]*Subscription),
		bufferSize:  bufferSize,
		log:         log.With().Str("component", "notifier").Logger(),
	}
}

// Subscribe creates a new subscription. If namespaces is empty, all blobs are
// delivered. The returned Subscription must be cleaned up via Unsubscribe.
func (n *Notifier) Subscribe(namespaces []types.Namespace) *Subscription {
	id := n.nextID.Add(1)
	nsSet := make(map[types.Namespace]struct{}, len(namespaces))
	for _, ns := range namespaces {
		nsSet[ns] = struct{}{}
	}

	sub := &Subscription{
		id:         id,
		ch:         make(chan HeightEvent, n.bufferSize),
		namespaces: nsSet,
	}

	n.mu.Lock()
	n.subscribers[id] = sub
	n.mu.Unlock()

	n.log.Debug().Uint64("sub_id", id).Int("namespaces", len(namespaces)).Msg("new subscription")
	return sub
}

// Unsubscribe removes a subscription and closes its channel.
func (n *Notifier) Unsubscribe(sub *Subscription) {
	n.mu.Lock()
	if _, ok := n.subscribers[sub.id]; ok {
		delete(n.subscribers, sub.id)
		close(sub.ch)
	}
	n.mu.Unlock()
}

// SubscriberCount returns the current number of active subscribers.
func (n *Notifier) SubscriberCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.subscribers)
}

// Publish sends an event to all matching subscribers. Non-blocking: if a
// subscriber's buffer is full the event is dropped and a gap is marked.
func (n *Notifier) Publish(event HeightEvent) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, sub := range n.subscribers {
		filtered := n.filterEvent(event, sub)

		// Warn at 75% buffer capacity.
		usage := len(sub.ch)
		threshold := n.bufferSize * 3 / 4
		if usage >= threshold {
			n.log.Warn().
				Uint64("sub_id", sub.id).
				Int("buffered", usage).
				Int("capacity", n.bufferSize).
				Msg("subscription buffer near capacity")
		}

		// Check contiguity: if lastHeight is set and this isn't the next height,
		// log a gap warning.
		if sub.lastHeight > 0 && event.Height != sub.lastHeight+1 {
			n.log.Warn().
				Uint64("sub_id", sub.id).
				Uint64("expected", sub.lastHeight+1).
				Uint64("got", event.Height).
				Msg("non-contiguous event delivery")
		}

		// Non-blocking send.
		select {
		case sub.ch <- filtered:
			sub.lastHeight = event.Height
		default:
			n.log.Warn().
				Uint64("sub_id", sub.id).
				Uint64("height", event.Height).
				Msg("subscriber buffer full, event dropped")
			// Reset lastHeight so next delivery triggers a gap warning.
			sub.lastHeight = 0
		}
	}
}

// filterEvent returns an event with blobs filtered to the subscriber's
// namespace set. If the subscriber watches all namespaces, the event is
// returned as-is.
func (n *Notifier) filterEvent(event HeightEvent, sub *Subscription) HeightEvent {
	if len(sub.namespaces) == 0 {
		return event
	}

	filtered := make([]types.Blob, 0, len(event.Blobs))
	for i := range event.Blobs {
		if _, ok := sub.namespaces[event.Blobs[i].Namespace]; ok {
			filtered = append(filtered, event.Blobs[i])
		}
	}

	return HeightEvent{
		Height: event.Height,
		Header: event.Header,
		Blobs:  filtered,
	}
}
