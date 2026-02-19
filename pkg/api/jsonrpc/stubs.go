package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
)

var (
	errNotSupported = fmt.Errorf("method not supported by apex indexer")
	errReadOnly     = fmt.Errorf("apex is a read-only indexer, blob submission not supported")
)

// BlobStubs holds stub methods for unsupported blob operations.
type BlobStubs struct{}

// GetCommitmentProof is not supported by the indexer.
func (s *BlobStubs) GetCommitmentProof(_ context.Context, _ uint64, _ []byte, _ []byte) (json.RawMessage, error) {
	return nil, errNotSupported
}

// Submit is not supported — apex is read-only.
func (s *BlobStubs) Submit(_ context.Context, _ json.RawMessage, _ json.RawMessage) (json.RawMessage, error) {
	return nil, errReadOnly
}

// ShareStub holds stub methods for the share namespace.
type ShareStub struct{}

// GetShare is not supported.
func (s *ShareStub) GetShare(_ context.Context, _ uint64, _ int, _ int) (json.RawMessage, error) {
	return nil, errNotSupported
}

// GetEDS is not supported.
func (s *ShareStub) GetEDS(_ context.Context, _ uint64) (json.RawMessage, error) {
	return nil, errNotSupported
}

// GetRange is not supported.
func (s *ShareStub) GetRange(_ context.Context, _ uint64, _ int, _ int) (json.RawMessage, error) {
	return nil, errNotSupported
}

// FraudStub holds stub methods for the fraud namespace.
type FraudStub struct{}

// Get is not supported.
func (s *FraudStub) Get(_ context.Context, _ string) (json.RawMessage, error) {
	return nil, errNotSupported
}

// Subscribe is not supported.
func (s *FraudStub) Subscribe(_ context.Context, _ string) (<-chan json.RawMessage, error) {
	return nil, errNotSupported
}

// BlobstreamStub holds stub methods for the blobstream namespace.
type BlobstreamStub struct{}

// GetDataRootTupleRoot is not supported.
func (s *BlobstreamStub) GetDataRootTupleRoot(_ context.Context, _ uint64, _ uint64) (json.RawMessage, error) {
	return nil, errNotSupported
}

// GetDataRootTupleInclusionProof is not supported.
func (s *BlobstreamStub) GetDataRootTupleInclusionProof(_ context.Context, _ uint64, _ uint64, _ uint64) (json.RawMessage, error) {
	return nil, errNotSupported
}
