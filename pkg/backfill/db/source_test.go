package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

func TestSourceFetchHeight(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		backend string
		layout  string
	}{
		{name: "leveldb-v1", backend: "leveldb", layout: layoutV1},
		{name: "leveldb-v2", backend: "leveldb", layout: layoutV2},
		{name: "pebble-v1", backend: "pebble", layout: layoutV1},
		{name: "pebble-v2", backend: "pebble", layout: layoutV2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			dbPath := filepath.Join(dir, "blockstore.db")

			ts := time.Unix(1_700_000_000, 123).UTC()
			hash := []byte("block-hash")
			dataHash := []byte("data-hash")
			txs := [][]byte{[]byte("plain-tx")}

			if err := writeTestBlock(dbPath, tc.backend, tc.layout, 42, hash, dataHash, ts, txs, 7); err != nil {
				t.Fatalf("writeTestBlock: %v", err)
			}

			src, err := NewSource(Config{
				Path:    dbPath,
				Backend: tc.backend,
				Layout:  "auto",
			}, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewSource: %v", err)
			}
			defer src.Close() //nolint:errcheck

			hdr, blobs, err := src.FetchHeight(context.Background(), 42, []types.Namespace{{}})
			if err != nil {
				t.Fatalf("FetchHeight: %v", err)
			}
			if hdr.Height != 42 {
				t.Fatalf("header height = %d, want 42", hdr.Height)
			}
			if string(hdr.Hash) != string(hash) {
				t.Fatalf("header hash mismatch: got %q want %q", hdr.Hash, hash)
			}
			if string(hdr.DataHash) != string(dataHash) {
				t.Fatalf("header data_hash mismatch: got %q want %q", hdr.DataHash, dataHash)
			}
			if !hdr.Time.Equal(ts) {
				t.Fatalf("header time = %s, want %s", hdr.Time, ts)
			}
			if len(blobs) != 0 {
				t.Fatalf("expected no blobs, got %d", len(blobs))
			}
		})
	}
}
