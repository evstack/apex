package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/orderedcode"
	"github.com/rs/zerolog"
	"github.com/syndtr/goleveldb/leveldb"
	ldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/evstack/apex/pkg/backfill"
	"github.com/evstack/apex/pkg/fetch"
	"github.com/evstack/apex/pkg/types"
)

const (
	layoutV1 = "v1"
	layoutV2 = "v2"
)

// Config controls direct celestia-app DB reads.
type Config struct {
	Path    string // celestia-app home/data/blockstore.db path
	Backend string // auto|pebble|leveldb
	Layout  string // auto|v1|v2
}

// Source reads historical block/blob data directly from celestia-app blockstore.db.
type Source struct {
	db      kvDB
	layout  string
	cfg     Config
	log     zerolog.Logger
	version string
}

var _ backfill.Source = (*Source)(nil)

// NewSource opens blockstore.db and auto-detects backend/layout when configured.
func NewSource(cfg Config, log zerolog.Logger) (*Source, error) {
	if cfg.Backend == "" {
		cfg.Backend = "auto"
	}
	if cfg.Layout == "" {
		cfg.Layout = "auto"
	}

	dbPath, err := normalizePath(cfg.Path)
	if err != nil {
		return nil, err
	}

	db, backend, err := openKV(dbPath, cfg.Backend)
	if err != nil {
		return nil, err
	}

	layout, version, err := detectLayout(db, cfg.Layout)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Source{
		db:      db,
		layout:  layout,
		cfg:     cfg,
		log:     log.With().Str("component", "backfill-db-source").Str("backend", backend).Str("layout", layout).Logger(),
		version: version,
	}, nil
}

func normalizePath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("celestia-app db path is required")
	}
	path = filepath.Clean(path)

	if st, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("stat db path %q: %w", path, err)
	} else if st.IsDir() {
		if filepath.Base(path) == "blockstore.db" {
			return path, nil
		}
		// Accept either home dir, data dir, or blockstore.db dir.
		candidates := []string{
			filepath.Join(path, "blockstore.db"),
			filepath.Join(path, "data", "blockstore.db"),
		}
		for _, c := range candidates {
			if cst, err := os.Stat(c); err == nil && cst.IsDir() {
				return c, nil
			}
		}
		return "", fmt.Errorf("path %q does not contain blockstore.db", path)
	}

	return "", fmt.Errorf("db path %q must be a directory", path)
}

func detectLayout(db kvDB, requested string) (layout string, version string, err error) {
	if requested != "auto" && requested != layoutV1 && requested != layoutV2 {
		return "", "", fmt.Errorf("invalid layout %q: must be auto|v1|v2", requested)
	}
	if requested == layoutV1 || requested == layoutV2 {
		return requested, "", nil
	}

	versionRaw, err := db.Get([]byte("version"))
	if err == nil {
		version = string(versionRaw)
		if version == layoutV2 {
			return layoutV2, version, nil
		}
	}

	// v1 has a stable state key.
	if _, err := db.Get([]byte("blockStore")); err == nil {
		return layoutV1, version, nil
	}

	// Fall back to v1 for legacy dbs without a version marker.
	return layoutV1, version, nil
}

const (
	// maxPartsTotal is the maximum number of block parts we will read.
	// CometBFT uses 64KB parts, so 4096 parts = 256MB which is well above
	// any real block size.
	maxPartsTotal = 4096

	// maxBlockSize is the maximum assembled block size (128MB).
	maxBlockSize = 128 << 20
)

// FetchHeight returns header and namespace-filtered blobs for one height.
func (s *Source) FetchHeight(_ context.Context, height uint64, namespaces []types.Namespace) (*types.Header, []types.Blob, error) {
	metaKey := blockMetaKey(s.layout, height)
	metaRaw, err := s.db.Get(metaKey)
	if err != nil {
		return nil, nil, fmt.Errorf("read block meta at height %d: %w", height, err)
	}

	meta, err := decodeBlockMeta(metaRaw)
	if err != nil {
		return nil, nil, fmt.Errorf("decode block meta at height %d: %w", height, err)
	}

	if meta.PartsTotal == 0 {
		return nil, nil, fmt.Errorf("invalid part set total 0 at height %d", height)
	}
	if meta.PartsTotal > maxPartsTotal {
		return nil, nil, fmt.Errorf("part set total %d exceeds maximum %d at height %d", meta.PartsTotal, maxPartsTotal, height)
	}

	rawBlock := make([]byte, 0, meta.PartsTotal*65536) // 64KB per part estimate
	for idx := uint32(0); idx < meta.PartsTotal; idx++ {
		partKey := blockPartKey(s.layout, height, idx)
		partRaw, err := s.db.Get(partKey)
		if err != nil {
			return nil, nil, fmt.Errorf("read block part %d at height %d: %w", idx, height, err)
		}
		partBytes, err := decodePartBytes(partRaw)
		if err != nil {
			return nil, nil, fmt.Errorf("decode block part %d at height %d: %w", idx, height, err)
		}
		if len(rawBlock)+len(partBytes) > maxBlockSize {
			return nil, nil, fmt.Errorf("assembled block exceeds %d bytes at height %d", maxBlockSize, height)
		}
		rawBlock = append(rawBlock, partBytes...)
	}

	block, err := decodeBlock(rawBlock)
	if err != nil {
		return nil, nil, fmt.Errorf("decode block at height %d: %w", height, err)
	}
	if block.Height != int64(height) {
		return nil, nil, fmt.Errorf("height mismatch in block: got %d want %d", block.Height, height)
	}

	hdr := &types.Header{
		Height:    height,
		Hash:      meta.BlockHash,
		DataHash:  block.DataHash,
		Time:      block.Time,
		RawHeader: rawBlock,
	}

	if len(namespaces) == 0 {
		return hdr, nil, nil
	}

	blobs, err := fetch.ExtractBlobsFromBlock(block.Txs, namespaces, height)
	if err != nil {
		return nil, nil, fmt.Errorf("extract blobs at height %d: %w", height, err)
	}
	if len(blobs) == 0 {
		return hdr, nil, nil
	}
	return hdr, blobs, nil
}

// Close closes the underlying DB handle.
func (s *Source) Close() error {
	return s.db.Close()
}

func blockMetaKey(layout string, height uint64) []byte {
	if layout == layoutV2 {
		key := []byte{0x00}
		key, _ = orderedcode.Append(key, int64(height))
		return key
	}
	return fmt.Appendf(nil, "H:%d", height)
}

func blockPartKey(layout string, height uint64, idx uint32) []byte {
	if layout == layoutV2 {
		key := []byte{0x01}
		key, _ = orderedcode.Append(key, int64(height), int64(idx))
		return key
	}
	return fmt.Appendf(nil, "P:%d:%d", height, idx)
}

type decodedMeta struct {
	BlockHash  []byte
	PartsTotal uint32
}

func decodeBlockMeta(raw []byte) (decodedMeta, error) {
	var out decodedMeta
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return decodedMeta{}, fmt.Errorf("invalid block meta tag")
		}
		buf = buf[n:]

		if num != 1 || typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return decodedMeta{}, fmt.Errorf("invalid block meta field %d", num)
			}
			buf = buf[n:]
			continue
		}

		blockIDBytes, n := protowire.ConsumeBytes(buf)
		if n < 0 {
			return decodedMeta{}, fmt.Errorf("invalid block_id bytes")
		}
		buf = buf[n:]

		hash, total, err := decodeBlockID(blockIDBytes)
		if err != nil {
			return decodedMeta{}, err
		}
		out.BlockHash = hash
		out.PartsTotal = total
	}

	if out.PartsTotal == 0 {
		return decodedMeta{}, fmt.Errorf("missing part_set_header.total")
	}
	return out, nil
}

func decodeBlockID(raw []byte) ([]byte, uint32, error) {
	var hash []byte
	var total uint32
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return nil, 0, fmt.Errorf("invalid block_id tag")
		}
		buf = buf[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return nil, 0, fmt.Errorf("invalid block_id field %d", num)
			}
			buf = buf[n:]
			continue
		}
		val, n := protowire.ConsumeBytes(buf)
		if n < 0 {
			return nil, 0, fmt.Errorf("invalid block_id bytes")
		}
		buf = buf[n:]
		switch num {
		case 1:
			hash = append([]byte(nil), val...)
		case 2:
			var err error
			total, err = decodePartSetHeaderTotal(val)
			if err != nil {
				return nil, 0, err
			}
		}
	}
	return hash, total, nil
}

func decodePartSetHeaderTotal(raw []byte) (uint32, error) {
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return 0, fmt.Errorf("invalid part_set_header tag")
		}
		buf = buf[n:]
		if num == 1 && typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return 0, fmt.Errorf("invalid part_set_header.total")
			}
			return uint32(v), nil
		}
		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return 0, fmt.Errorf("invalid part_set_header field %d", num)
		}
		buf = buf[n:]
	}
	return 0, fmt.Errorf("missing part_set_header.total")
}

func decodePartBytes(raw []byte) ([]byte, error) {
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return nil, fmt.Errorf("invalid part tag")
		}
		buf = buf[n:]
		if num == 2 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return nil, fmt.Errorf("invalid part.bytes")
			}
			return append([]byte(nil), v...), nil
		}
		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return nil, fmt.Errorf("invalid part field %d", num)
		}
		buf = buf[n:]
	}
	return nil, fmt.Errorf("missing part.bytes")
}

type decodedBlock struct {
	Height   int64
	Time     time.Time
	DataHash []byte
	Txs      [][]byte
}

func decodeBlock(raw []byte) (decodedBlock, error) {
	var out decodedBlock
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return decodedBlock{}, fmt.Errorf("invalid block tag")
		}
		buf = buf[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return decodedBlock{}, fmt.Errorf("invalid block field %d", num)
			}
			buf = buf[n:]
			continue
		}

		val, n := protowire.ConsumeBytes(buf)
		if n < 0 {
			return decodedBlock{}, fmt.Errorf("invalid block bytes field %d", num)
		}
		buf = buf[n:]

		switch num {
		case 1:
			if err := decodeHeader(val, &out); err != nil {
				return decodedBlock{}, err
			}
		case 2:
			txs, err := decodeDataTxs(val)
			if err != nil {
				return decodedBlock{}, err
			}
			out.Txs = txs
		}
	}
	return out, nil
}

func decodeHeader(raw []byte, out *decodedBlock) error {
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return fmt.Errorf("invalid header tag")
		}
		buf = buf[n:]
		switch {
		case num == 3 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return fmt.Errorf("invalid header.height")
			}
			out.Height = int64(v)
			buf = buf[n:]
		case num == 4 && typ == protowire.BytesType:
			tsRaw, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return fmt.Errorf("invalid header.time")
			}
			t, err := decodeTimestamp(tsRaw)
			if err != nil {
				return err
			}
			out.Time = t
			buf = buf[n:]
		case num == 7 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return fmt.Errorf("invalid header.data_hash")
			}
			out.DataHash = append([]byte(nil), v...)
			buf = buf[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return fmt.Errorf("invalid header field %d", num)
			}
			buf = buf[n:]
		}
	}
	return nil
}

func decodeTimestamp(raw []byte) (time.Time, error) {
	var (
		seconds int64
		nanos   int64
	)
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return time.Time{}, fmt.Errorf("invalid timestamp tag")
		}
		buf = buf[n:]
		if typ != protowire.VarintType {
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return time.Time{}, fmt.Errorf("invalid timestamp field %d", num)
			}
			buf = buf[n:]
			continue
		}
		v, n := protowire.ConsumeVarint(buf)
		if n < 0 {
			return time.Time{}, fmt.Errorf("invalid timestamp varint")
		}
		buf = buf[n:]
		switch num {
		case 1:
			seconds = int64(v)
		case 2:
			nanos = int64(v)
		}
	}
	return time.Unix(seconds, nanos).UTC(), nil
}

func decodeDataTxs(raw []byte) ([][]byte, error) {
	var txs [][]byte
	buf := raw
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return nil, fmt.Errorf("invalid data tag")
		}
		buf = buf[n:]
		if num == 1 && typ == protowire.BytesType {
			tx, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return nil, fmt.Errorf("invalid tx bytes")
			}
			txs = append(txs, append([]byte(nil), tx...))
			buf = buf[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return nil, fmt.Errorf("invalid data field %d", num)
		}
		buf = buf[n:]
	}
	return txs, nil
}

type kvDB interface {
	Get(key []byte) ([]byte, error)
	Close() error
}

type pebbleDB struct {
	db *pebble.DB
}

func (p *pebbleDB) Get(key []byte) ([]byte, error) {
	v, closer, err := p.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close() //nolint:errcheck
	return append([]byte(nil), v...), nil
}

func (p *pebbleDB) Close() error { return p.db.Close() }

type levelDB struct {
	db *leveldb.DB
}

func (l *levelDB) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, nil)
}

func (l *levelDB) Close() error { return l.db.Close() }

// probeKV checks whether the DB contains a known CometBFT blockstore marker.
// Returns true if either "version" (v2) or "blockStore" (v1) key is readable.
func probeKV(db kvDB) bool {
	if _, err := db.Get([]byte("version")); err == nil {
		return true
	}
	if _, err := db.Get([]byte("blockStore")); err == nil {
		return true
	}
	return false
}

func openKV(path, backend string) (kvDB, string, error) {
	switch backend {
	case "pebble":
		db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
		if err != nil {
			return nil, "", fmt.Errorf("open pebble blockstore %q: %w", path, err)
		}
		return &pebbleDB{db: db}, "pebble", nil
	case "leveldb":
		db, err := leveldb.OpenFile(path, &ldbopt.Options{ReadOnly: true})
		if err != nil {
			return nil, "", fmt.Errorf("open leveldb blockstore %q: %w", path, err)
		}
		return &levelDB{db: db}, "leveldb", nil
	case "auto":
		// Pebble can sometimes open LevelDB files due to format compatibility.
		// After opening, probe for a known marker key to confirm the backend
		// is reading valid data before committing to it.
		if db, err := pebble.Open(path, &pebble.Options{ReadOnly: true}); err == nil {
			kv := &pebbleDB{db: db}
			if probeKV(kv) {
				return kv, "pebble", nil
			}
			_ = kv.Close()
		}
		if db, err := leveldb.OpenFile(path, &ldbopt.Options{ReadOnly: true}); err == nil {
			kv := &levelDB{db: db}
			if probeKV(kv) {
				return kv, "leveldb", nil
			}
			_ = kv.Close()
		}
		return nil, "", fmt.Errorf("open blockstore %q: no backend could read marker keys", path)
	default:
		return nil, "", fmt.Errorf("invalid backend %q: must be auto|pebble|leveldb", backend)
	}
}

// Helpers used in tests.
type writableKV interface {
	put(key, val []byte) error
	close() error
}

type writablePebble struct{ db *pebble.DB }

func (w *writablePebble) put(key, val []byte) error { return w.db.Set(key, val, pebble.Sync) }
func (w *writablePebble) close() error              { return w.db.Close() }

type writableLevel struct{ db *leveldb.DB }

func (w *writableLevel) put(key, val []byte) error { return w.db.Put(key, val, nil) }
func (w *writableLevel) close() error              { return w.db.Close() }

func openWritable(path, backend string) (writableKV, error) {
	switch backend {
	case "pebble":
		db, err := pebble.Open(path, &pebble.Options{})
		if err != nil {
			return nil, err
		}
		return &writablePebble{db: db}, nil
	case "leveldb":
		db, err := leveldb.OpenFile(path, nil)
		if err != nil {
			return nil, err
		}
		return &writableLevel{db: db}, nil
	default:
		return nil, fmt.Errorf("unsupported backend %q", backend)
	}
}

func encodeTimestamp(t time.Time) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(t.Unix()))
	out = protowire.AppendTag(out, 2, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(t.Nanosecond()))
	return out
}

func encodeHeader(height uint64, t time.Time, dataHash []byte) []byte {
	var out []byte
	out = protowire.AppendTag(out, 3, protowire.VarintType)
	out = protowire.AppendVarint(out, height)
	out = protowire.AppendTag(out, 4, protowire.BytesType)
	out = protowire.AppendBytes(out, encodeTimestamp(t))
	out = protowire.AppendTag(out, 7, protowire.BytesType)
	out = protowire.AppendBytes(out, dataHash)
	return out
}

func encodeData(txs [][]byte) []byte {
	var out []byte
	for _, tx := range txs {
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, tx)
	}
	return out
}

func encodeBlock(height uint64, t time.Time, dataHash []byte, txs [][]byte) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, encodeHeader(height, t, dataHash))
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	out = protowire.AppendBytes(out, encodeData(txs))
	return out
}

func encodePart(raw []byte) []byte {
	var out []byte
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	out = protowire.AppendBytes(out, raw)
	return out
}

func encodePartSetHeader(total uint32) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(total))
	return out
}

func encodeBlockID(hash []byte, total uint32) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, hash)
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	out = protowire.AppendBytes(out, encodePartSetHeader(total))
	return out
}

func encodeBlockMeta(hash []byte, total uint32) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, encodeBlockID(hash, total))
	return out
}

func splitIntoParts(raw []byte, partSize int) [][]byte {
	if partSize <= 0 {
		partSize = len(raw)
	}
	if len(raw) == 0 {
		return [][]byte{{}}
	}
	var out [][]byte
	for i := 0; i < len(raw); i += partSize {
		j := min(i+partSize, len(raw))
		out = append(out, append([]byte(nil), raw[i:j]...))
	}
	return out
}

func writeTestBlock(path, backend, layout string, height uint64, hash, dataHash []byte, t time.Time, txs [][]byte, partSize int) error {
	w, err := openWritable(path, backend)
	if err != nil {
		return err
	}
	defer w.close() //nolint:errcheck

	if layout == layoutV2 {
		if err := w.put([]byte("version"), []byte(layoutV2)); err != nil {
			return err
		}
	} else {
		// minimal v1 marker
		if err := w.put([]byte("blockStore"), []byte{0x0a, 0x02, 0x08, 0x01}); err != nil {
			return err
		}
	}

	rawBlock := encodeBlock(height, t, dataHash, txs)
	parts := splitIntoParts(rawBlock, partSize)
	meta := encodeBlockMeta(hash, uint32(len(parts)))

	if err := w.put(blockMetaKey(layout, height), meta); err != nil {
		return err
	}
	for i, p := range parts {
		if err := w.put(blockPartKey(layout, height, uint32(i)), encodePart(p)); err != nil {
			return err
		}
	}
	return nil
}
