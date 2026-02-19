package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/evstack/apex/pkg/api"
	"github.com/evstack/apex/pkg/types"
)

// BlobHandler implements the celestia-node blob JSON-RPC namespace.
type BlobHandler struct {
	svc *api.Service
}

// Get returns a single blob matching the namespace and commitment at the given height.
func (h *BlobHandler) Get(ctx context.Context, height uint64, namespace []byte, commitment []byte) (json.RawMessage, error) {
	ns, err := bytesToNamespace(namespace)
	if err != nil {
		return nil, err
	}
	return h.svc.BlobGet(ctx, height, ns, commitment)
}

// GetAll returns all blobs for the given namespaces at the given height.
func (h *BlobHandler) GetAll(ctx context.Context, height uint64, namespaces [][]byte) (json.RawMessage, error) {
	nsList := make([]types.Namespace, len(namespaces))
	for i, nsBytes := range namespaces {
		ns, err := bytesToNamespace(nsBytes)
		if err != nil {
			return nil, err
		}
		nsList[i] = ns
	}
	return h.svc.BlobGetAll(ctx, height, nsList, 0, 0)
}

// Subscribe returns a channel of blob events for the given namespace.
// Only available over WebSocket.
func (h *BlobHandler) Subscribe(ctx context.Context, namespace []byte) (<-chan json.RawMessage, error) {
	ns, err := bytesToNamespace(namespace)
	if err != nil {
		return nil, err
	}

	sub := h.svc.BlobSubscribe(ns)
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
				for i := range ev.Blobs {
					raw := api.MarshalBlob(&ev.Blobs[i])
					select {
					case out <- raw:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out, nil
}

// GetProof forwards a proof request to the upstream Celestia node.
func (h *BlobHandler) GetProof(ctx context.Context, height uint64, namespace, commitment []byte) (json.RawMessage, error) {
	return h.svc.BlobGetProof(ctx, height, namespace, commitment)
}

// Included forwards an inclusion check to the upstream Celestia node.
func (h *BlobHandler) Included(ctx context.Context, height uint64, namespace []byte, proof json.RawMessage, commitment []byte) (bool, error) {
	return h.svc.BlobIncluded(ctx, height, namespace, proof, commitment)
}

// GetCommitmentProof is not supported by the indexer.
func (h *BlobHandler) GetCommitmentProof(_ context.Context, _ uint64, _ []byte, _ []byte) (json.RawMessage, error) {
	return nil, errNotSupported
}

// Submit is not supported — apex is read-only.
func (h *BlobHandler) Submit(_ context.Context, _ json.RawMessage, _ json.RawMessage) (json.RawMessage, error) {
	return nil, errReadOnly
}

func bytesToNamespace(b []byte) (types.Namespace, error) {
	if len(b) != types.NamespaceSize {
		return types.Namespace{}, fmt.Errorf("invalid namespace size: got %d, want %d", len(b), types.NamespaceSize)
	}
	var ns types.Namespace
	copy(ns[:], b)
	return ns, nil
}
