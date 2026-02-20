package jsonrpc

import (
	"context"
	"encoding/json"

	"github.com/evstack/apex/pkg/api"
)

// HeaderHandler implements the celestia-node header JSON-RPC namespace.
type HeaderHandler struct {
	svc *api.Service
}

// GetByHeight returns the raw header JSON at the given height.
func (h *HeaderHandler) GetByHeight(ctx context.Context, height uint64) (json.RawMessage, error) {
	return h.svc.HeaderGetByHeight(ctx, height)
}

// LocalHead returns the header at the latest synced height.
func (h *HeaderHandler) LocalHead(ctx context.Context) (json.RawMessage, error) {
	return h.svc.HeaderLocalHead(ctx)
}

// NetworkHead returns the current network head from the upstream node.
func (h *HeaderHandler) NetworkHead(ctx context.Context) (json.RawMessage, error) {
	return h.svc.HeaderNetworkHead(ctx)
}

// Subscribe returns a channel of header events.
// Only available over WebSocket.
func (h *HeaderHandler) Subscribe(ctx context.Context) (<-chan json.RawMessage, error) {
	sub, err := h.svc.HeaderSubscribe()
	if err != nil {
		return nil, err
	}
	out := make(chan json.RawMessage, cap(sub.Events()))

	go func() {
		defer close(out)
		defer h.svc.Notifier().Unsubscribe(sub)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-sub.Events():
				if !ok {
					return
				}
				if ev.Header != nil && ev.Header.RawHeader != nil {
					select {
					case out <- json.RawMessage(ev.Header.RawHeader):
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out, nil
}
