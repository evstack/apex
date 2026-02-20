package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/evstack/apex/pkg/backfill"
	"github.com/evstack/apex/pkg/types"
)

// ErrNotImplemented is returned until db backend adapters are wired in.
var ErrNotImplemented = errors.New("celestia-app db backfill is not implemented in this build")

// Source reads historical block/blob data directly from celestia-app's local DB.
// This package is the placeholder for backend-specific adapters (pebble/leveldb).
type Source struct {
	homeDir string
}

var _ backfill.Source = (*Source)(nil)

// NewSource validates the celestia-app home directory.
func NewSource(homeDir string) (*Source, error) {
	if homeDir == "" {
		return nil, fmt.Errorf("home dir is required")
	}
	dataDir := filepath.Join(homeDir, "data")
	if st, err := os.Stat(dataDir); err != nil || !st.IsDir() {
		if err == nil {
			err = fmt.Errorf("not a directory")
		}
		return nil, fmt.Errorf("invalid celestia-app data dir %q: %w", dataDir, err)
	}
	return &Source{homeDir: homeDir}, nil
}

// FetchHeight is not yet implemented.
func (s *Source) FetchHeight(_ context.Context, _ uint64, _ []types.Namespace) (*types.Header, []types.Blob, error) {
	return nil, nil, ErrNotImplemented
}

// Close is a no-op until backend adapters are wired in.
func (s *Source) Close() error {
	return nil
}
