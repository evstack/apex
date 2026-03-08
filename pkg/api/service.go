package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	b, err := s.GetBlob(ctx, height, namespace, commitment)
	if err != nil {
		return nil, err
	}
	return MarshalBlob(b), nil
}

// GetBlob returns a single blob matching the namespace and commitment.
func (s *Service) GetBlob(ctx context.Context, height uint64, namespace types.Namespace, commitment []byte) (*types.Blob, error) {
	blobs, err := s.store.GetBlobs(ctx, namespace, height, height, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("get blobs: %w", err)
	}

	for i := range blobs {
		if bytes.Equal(blobs[i].Commitment, commitment) {
			return &blobs[i], nil
		}
	}

	return nil, store.ErrNotFound
}

// BlobGetByCommitment returns a blob matching the given commitment as JSON.
// No height or namespace required — commitment is cryptographically unique.
func (s *Service) BlobGetByCommitment(ctx context.Context, commitment []byte) (json.RawMessage, error) {
	b, err := s.GetBlobByCommitment(ctx, commitment)
	if err != nil {
		return nil, err
	}
	return MarshalBlob(b), nil
}

// GetBlobByCommitment returns a blob matching the given commitment.
func (s *Service) GetBlobByCommitment(ctx context.Context, commitment []byte) (*types.Blob, error) {
	if len(commitment) == 0 {
		return nil, errors.New("commitment is required")
	}
	b, err := s.store.GetBlobByCommitment(ctx, commitment)
	if err != nil {
		return nil, fmt.Errorf("get blob by commitment: %w", err)
	}
	return b, nil
}

// BlobGetAll returns all blobs for the given namespaces at the given height.
// limit=0 means no limit; offset=0 means no offset.
// Pagination is applied to the aggregate result across all namespaces.
func (s *Service) BlobGetAll(ctx context.Context, height uint64, namespaces []types.Namespace, limit, offset int) (json.RawMessage, error) {
	allBlobs, err := s.GetAllBlobs(ctx, height, namespaces, limit, offset)
	if err != nil {
		return nil, err
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

// GetAllBlobs returns all blobs for the given namespaces at the given height.
// Pagination is applied to the aggregate result across all namespaces.
func (s *Service) GetAllBlobs(ctx context.Context, height uint64, namespaces []types.Namespace, limit, offset int) ([]types.Blob, error) {
	allBlobs := make([]types.Blob, 0, len(namespaces)*8) // preallocate for typical workload
	for _, ns := range namespaces {
		blobs, err := s.store.GetBlobs(ctx, ns, height, height, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("get blobs for namespace %s: %w", ns, err)
		}
		allBlobs = append(allBlobs, blobs...)
	}

	// Apply pagination to the aggregate result.
	if offset > 0 {
		if offset >= len(allBlobs) {
			allBlobs = nil
		} else {
			allBlobs = allBlobs[offset:]
		}
	}
	if limit > 0 && limit < len(allBlobs) {
		allBlobs = allBlobs[:limit]
	}
	return allBlobs, nil
}

// BlobGetProof forwards a proof request to the upstream Celestia node.
func (s *Service) BlobGetProof(ctx context.Context, height uint64, namespace, commitment []byte) (json.RawMessage, error) {
	if s.proof == nil {
		return nil, errors.New("proof forwarding not available")
	}
	return s.proof.GetProof(ctx, height, namespace, commitment)
}

// BlobIncluded forwards an inclusion check to the upstream Celestia node.
func (s *Service) BlobIncluded(ctx context.Context, height uint64, namespace []byte, proof json.RawMessage, commitment []byte) (bool, error) {
	if s.proof == nil {
		return false, errors.New("proof forwarding not available")
	}
	return s.proof.Included(ctx, height, namespace, proof, commitment)
}

// BlobSubscribe creates a subscription for blobs in the given namespace.
func (s *Service) BlobSubscribe(namespace types.Namespace) (*Subscription, error) {
	return s.notifier.Subscribe([]types.Namespace{namespace})
}

// HeaderGetByHeight returns the raw header JSON at the given height.
func (s *Service) HeaderGetByHeight(ctx context.Context, height uint64) (json.RawMessage, error) {
	hdr, err := s.GetHeaderByHeight(ctx, height)
	if err != nil {
		return nil, err
	}
	return hdr.RawHeader, nil
}

// GetHeaderByHeight returns the stored header at the given height.
func (s *Service) GetHeaderByHeight(ctx context.Context, height uint64) (*types.Header, error) {
	hdr, err := s.store.GetHeader(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("get header: %w", err)
	}
	return hdr, nil
}

// HeaderLocalHead returns the header at the latest synced height.
func (s *Service) HeaderLocalHead(ctx context.Context) (json.RawMessage, error) {
	hdr, err := s.GetLocalHead(ctx)
	if err != nil {
		return nil, err
	}
	return hdr.RawHeader, nil
}

// GetLocalHead returns the locally indexed head header.
func (s *Service) GetLocalHead(ctx context.Context) (*types.Header, error) {
	ss, err := s.store.GetSyncState(ctx)
	if err != nil {
		return nil, fmt.Errorf("get sync state: %w", err)
	}
	hdr, err := s.store.GetHeader(ctx, ss.LatestHeight)
	if err != nil {
		return nil, fmt.Errorf("get header at height %d: %w", ss.LatestHeight, err)
	}
	return hdr, nil
}

// HeaderNetworkHead returns the current network head from the upstream node.
func (s *Service) HeaderNetworkHead(ctx context.Context) (json.RawMessage, error) {
	hdr, err := s.GetNetworkHead(ctx)
	if err != nil {
		return nil, err
	}
	return hdr.RawHeader, nil
}

// GetNetworkHead returns the current network head from the upstream node.
func (s *Service) GetNetworkHead(ctx context.Context) (*types.Header, error) {
	hdr, err := s.fetcher.GetNetworkHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("get network head: %w", err)
	}
	return hdr, nil
}

// HeaderSubscribe creates a subscription for all new headers.
func (s *Service) HeaderSubscribe() (*Subscription, error) {
	return s.notifier.Subscribe(nil)
}

// Notifier returns the service's notifier for direct access.
func (s *Service) Notifier() *Notifier {
	return s.notifier
}

// blobJSON is a struct-based representation for celestia-node compatible JSON.
// Using a struct avoids the per-call map[string]any allocation that json.Marshal
// requires for maps.
type blobJSON struct {
	Namespace    []byte `json:"namespace"`
	Data         []byte `json:"data"`
	ShareVersion uint32 `json:"share_version"`
	Commitment   []byte `json:"commitment"`
	Index        int    `json:"index"`
}

// MarshalBlob converts a stored blob into celestia-node compatible JSON.
func MarshalBlob(b *types.Blob) json.RawMessage {
	raw, _ := json.Marshal(blobJSON{ //nolint:errcheck
		Namespace:    b.Namespace[:],
		Data:         b.Data,
		ShareVersion: b.ShareVersion,
		Commitment:   b.Commitment,
		Index:        b.Index,
	})
	return raw
}
