package types

import (
	"encoding/hex"
	"fmt"
	"time"
)

// NamespaceSize is the size of a Celestia namespace in bytes.
const NamespaceSize = 29

// Namespace is a 29-byte Celestia namespace identifier.
type Namespace [NamespaceSize]byte

// NamespaceFromHex parses a hex-encoded namespace string.
func NamespaceFromHex(s string) (Namespace, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return Namespace{}, fmt.Errorf("invalid hex: %w", err)
	}
	if len(b) != NamespaceSize {
		return Namespace{}, fmt.Errorf("expected %d bytes, got %d", NamespaceSize, len(b))
	}
	var ns Namespace
	copy(ns[:], b)
	return ns, nil
}

// String returns the hex-encoded namespace.
func (n Namespace) String() string {
	return hex.EncodeToString(n[:])
}

// Blob represents a blob submitted to a Celestia namespace.
type Blob struct {
	Namespace    Namespace
	Data         []byte
	Commitment   []byte
	ShareVersion uint32
	Signer       []byte
	Index        int
}

// Header represents a Celestia block header.
type Header struct {
	Height    uint64
	Hash      []byte
	DataHash  []byte
	Time      time.Time
	RawHeader []byte
}

// SyncState represents the current state of the sync coordinator.
type SyncState int

const (
	Initializing SyncState = iota
	Backfilling
	Streaming
)

// String returns a human-readable sync state.
func (s SyncState) String() string {
	switch s {
	case Initializing:
		return "initializing"
	case Backfilling:
		return "backfilling"
	case Streaming:
		return "streaming"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// SyncStatus reports the current sync progress.
type SyncStatus struct {
	State         SyncState
	LatestHeight  uint64
	NetworkHeight uint64
}
