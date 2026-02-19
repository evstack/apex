package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/types"
)

// Service is the shared business logic layer used by both JSON-RPC and gRPC
// handlers. It reads from the store, forwards proofs upstream, and manages
// subscriptions via the Notifier.
type Service struct {
	store    store.Store
	fetcher  fetch.DataFetcher
	proof    fetch.ProofForwarder
	notifier *Notifier
	log      zerolog.Logger
}

// NewService creates a new API service. proof may be nil if upstream proof
// forwarding is not available.
func NewService(s store.Store, f fetch.DataFetcher, proof fetch.ProofForwarder, n *Notifier, log zerolog.Logger) *Service {
	return &Service{
		store:    s,
		fetcher:  f,
		proof:    proof,
		notifier: n,
		log:      log.With().Str("component", "api-service").Logger(),
	}
}

// BlobGet returns a single blob matching the namespace and commitment at the
// given height. Returns the blob as celestia-node compatible JSON.
func (s *Service) BlobGet(ctx context.Context, height uint64, namespace types.Namespace, commitment []byte) (json.RawMessage, error) {
	blobs, err := s.store.GetBlobs(ctx, namespace, height, height)
	if err != nil {
		return nil, fmt.Errorf("get blobs: %w", err)
	}

	for i := range blobs {
		if bytes.Equal(blobs[i].Commitment, commitment) {
			return MarshalBlob(&blobs[i]), nil
		}
	}

	return nil, store.ErrNotFound
}

// BlobGetAll returns all blobs for the given namespaces at the given height.
func (s *Service) BlobGetAll(ctx context.Context, height uint64, namespaces []types.Namespace) (json.RawMessage, error) {
	var allBlobs []types.Blob
	for _, ns := range namespaces {
		blobs, err := s.store.GetBlobs(ctx, ns, height, height)
		if err != nil {
			return nil, fmt.Errorf("get blobs for namespace %s: %w", ns, err)
		}
		allBlobs = append(allBlobs, blobs...)
	}

	if len(allBlobs) == 0 {
		return json.RawMessage("null"), nil
	}

	result := make([]json.RawMessage, len(allBlobs))
	for i := range allBlobs {
		result[i] = MarshalBlob(&allBlobs[i])
	}

	out, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal blobs: %w", err)
	}
	return out, nil
}

// BlobGetProof forwards a proof request to the upstream Celestia node.
func (s *Service) BlobGetProof(ctx context.Context, height uint64, namespace, commitment []byte) (json.RawMessage, error) {
	if s.proof == nil {
		return nil, fmt.Errorf("proof forwarding not available")
	}
	return s.proof.GetProof(ctx, height, namespace, commitment)
}

// BlobIncluded forwards an inclusion check to the upstream Celestia node.
func (s *Service) BlobIncluded(ctx context.Context, height uint64, namespace []byte, proof json.RawMessage, commitment []byte) (bool, error) {
	if s.proof == nil {
		return false, fmt.Errorf("proof forwarding not available")
	}
	return s.proof.Included(ctx, height, namespace, proof, commitment)
}

// BlobSubscribe creates a subscription for blobs in the given namespace.
func (s *Service) BlobSubscribe(namespace types.Namespace) *Subscription {
	return s.notifier.Subscribe([]types.Namespace{namespace})
}

// HeaderGetByHeight returns the raw header JSON at the given height.
func (s *Service) HeaderGetByHeight(ctx context.Context, height uint64) (json.RawMessage, error) {
	hdr, err := s.store.GetHeader(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("get header: %w", err)
	}
	return hdr.RawHeader, nil
}

// HeaderLocalHead returns the header at the latest synced height.
func (s *Service) HeaderLocalHead(ctx context.Context) (json.RawMessage, error) {
	ss, err := s.store.GetSyncState(ctx)
	if err != nil {
		return nil, fmt.Errorf("get sync state: %w", err)
	}
	hdr, err := s.store.GetHeader(ctx, ss.LatestHeight)
	if err != nil {
		return nil, fmt.Errorf("get header at height %d: %w", ss.LatestHeight, err)
	}
	return hdr.RawHeader, nil
}

// HeaderNetworkHead returns the current network head from the upstream node.
func (s *Service) HeaderNetworkHead(ctx context.Context) (json.RawMessage, error) {
	hdr, err := s.fetcher.GetNetworkHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("get network head: %w", err)
	}
	return hdr.RawHeader, nil
}

// HeaderSubscribe creates a subscription for all new headers.
func (s *Service) HeaderSubscribe() *Subscription {
	return s.notifier.Subscribe(nil)
}

// Notifier returns the service's notifier for direct access.
func (s *Service) Notifier() *Notifier {
	return s.notifier
}

// Store returns the underlying store for direct access.
func (s *Service) Store() store.Store {
	return s.store
}

// Fetcher returns the underlying fetcher for direct access.
func (s *Service) Fetcher() fetch.DataFetcher {
	return s.fetcher
}

// MarshalBlob converts a stored blob into celestia-node compatible JSON.
func MarshalBlob(b *types.Blob) json.RawMessage {
	m := map[string]any{
		"namespace":     b.Namespace[:],
		"data":          b.Data,
		"share_version": b.ShareVersion,
		"commitment":    b.Commitment,
		"index":         b.Index,
	}
	raw, _ := json.Marshal(m) //nolint:errcheck
	return raw
}
