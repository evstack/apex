package fetch

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

// headerAPI defines the JSON-RPC stubs for the Celestia "header" namespace.
type headerAPI struct {
	GetByHeight func(ctx context.Context, height uint64) (json.RawMessage, error)
	NetworkHead func(ctx context.Context) (json.RawMessage, error)
	Subscribe   func(ctx context.Context) (<-chan json.RawMessage, error)
}

// blobAPI defines the JSON-RPC stubs for the Celestia "blob" namespace.
type blobAPI struct {
	GetAll func(ctx context.Context, height uint64, namespaces [][]byte) (json.RawMessage, error)
}

// CelestiaNodeFetcher implements DataFetcher using a Celestia node's JSON-RPC API.
type CelestiaNodeFetcher struct {
	header       headerAPI
	blob         blobAPI
	headerCloser jsonrpc.ClientCloser
	blobCloser   jsonrpc.ClientCloser
	log          zerolog.Logger
	mu           sync.Mutex
	closed       bool
}

// NewCelestiaNodeFetcher connects to a Celestia node at the given WebSocket address.
func NewCelestiaNodeFetcher(ctx context.Context, addr, token string, log zerolog.Logger) (*CelestiaNodeFetcher, error) {
	headers := http.Header{}
	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}

	f := &CelestiaNodeFetcher{log: log}

	var err error
	f.headerCloser, err = jsonrpc.NewClient(ctx, addr, "header", &f.header, headers)
	if err != nil {
		return nil, fmt.Errorf("connect header client: %w", err)
	}

	f.blobCloser, err = jsonrpc.NewClient(ctx, addr, "blob", &f.blob, headers)
	if err != nil {
		f.headerCloser()
		return nil, fmt.Errorf("connect blob client: %w", err)
	}

	return f, nil
}

func (f *CelestiaNodeFetcher) GetHeader(ctx context.Context, height uint64) (*types.Header, error) {
	raw, err := f.header.GetByHeight(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("header.GetByHeight(%d): %w", height, err)
	}
	return mapHeader(raw)
}

func (f *CelestiaNodeFetcher) GetBlobs(ctx context.Context, height uint64, namespaces []types.Namespace) ([]types.Blob, error) {
	nsBytes := namespacesToBytes(namespaces)
	raw, err := f.blob.GetAll(ctx, height, nsBytes)
	if err != nil {
		if isNotFoundErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("blob.GetAll(%d): %w", height, err)
	}

	return mapBlobs(raw, height)
}

func (f *CelestiaNodeFetcher) GetNetworkHead(ctx context.Context) (*types.Header, error) {
	raw, err := f.header.NetworkHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("header.NetworkHead: %w", err)
	}
	return mapHeader(raw)
}

func (f *CelestiaNodeFetcher) SubscribeHeaders(ctx context.Context) (<-chan *types.Header, error) {
	rawCh, err := f.header.Subscribe(ctx)
	if err != nil {
		return nil, fmt.Errorf("header.Subscribe: %w", err)
	}

	out := make(chan *types.Header, 64)
	go func() {
		defer close(out)
		for {
			select {
			case raw, ok := <-rawCh:
				if !ok {
					return
				}
				hdr, err := mapHeader(raw)
				if err != nil {
					f.log.Error().Err(err).Msg("failed to parse subscribed header")
					continue
				}
				select {
				case out <- hdr:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (f *CelestiaNodeFetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.headerCloser()
	f.blobCloser()
	return nil
}

// rpcExtendedHeader is the minimal JSON structure we parse from Celestia's ExtendedHeader.
type rpcExtendedHeader struct {
	Header rpcCometHeader `json:"header"`
	Commit rpcCommit      `json:"commit"`
}

type rpcCometHeader struct {
	Height   jsonInt64 `json:"height"`
	Time     string    `json:"time"`
	DataHash hexBytes  `json:"data_hash"`
}

type rpcCommit struct {
	BlockID rpcBlockID `json:"block_id"`
}

type rpcBlockID struct {
	Hash hexBytes `json:"hash"`
}

// rpcBlob is the minimal JSON structure we parse from Celestia's blob response.
type rpcBlob struct {
	Namespace    []byte `json:"namespace"`
	Data         []byte `json:"data"`
	ShareVersion uint32 `json:"share_version"`
	Commitment   []byte `json:"commitment"`
	Index        int    `json:"index"`
}

func mapHeader(raw json.RawMessage) (*types.Header, error) {
	var h rpcExtendedHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil, fmt.Errorf("unmarshal header: %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, h.Header.Time)
	if err != nil {
		return nil, fmt.Errorf("parse header time %q: %w", h.Header.Time, err)
	}

	return &types.Header{
		Height:    uint64(h.Header.Height),
		Hash:      []byte(h.Commit.BlockID.Hash),
		DataHash:  []byte(h.Header.DataHash),
		Time:      t,
		RawHeader: []byte(raw),
	}, nil
}

func mapBlobs(raw json.RawMessage, height uint64) ([]types.Blob, error) {
	// Celestia returns null/empty for no blobs.
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}

	var rpcBlobs []rpcBlob
	if err := json.Unmarshal(raw, &rpcBlobs); err != nil {
		return nil, fmt.Errorf("unmarshal blobs: %w", err)
	}

	blobs := make([]types.Blob, len(rpcBlobs))
	for i := range rpcBlobs {
		b := &rpcBlobs[i]
		var ns types.Namespace
		if len(b.Namespace) != types.NamespaceSize {
			return nil, fmt.Errorf("blob %d: invalid namespace size %d", i, len(b.Namespace))
		}
		copy(ns[:], b.Namespace)

		blobs[i] = types.Blob{
			Height:       height,
			Namespace:    ns,
			Data:         b.Data,
			Commitment:   b.Commitment,
			ShareVersion: b.ShareVersion,
			Index:        b.Index,
		}
	}
	return blobs, nil
}

func namespacesToBytes(nss []types.Namespace) [][]byte {
	out := make([][]byte, len(nss))
	for i, ns := range nss {
		b := make([]byte, types.NamespaceSize)
		copy(b, ns[:])
		out[i] = b
	}
	return out
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "blob: not found") ||
		strings.Contains(msg, "header: not found")
}

// jsonInt64 handles CometBFT's int64 fields encoded as JSON strings.
type jsonInt64 int64

func (i *jsonInt64) UnmarshalJSON(data []byte) error {
	// Try string first (CometBFT style: "12345").
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("parse int64 string %q: %w", s, err)
		}
		*i = jsonInt64(n)
		return nil
	}
	// Fall back to number.
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("parse int64: %w", err)
	}
	*i = jsonInt64(n)
	return nil
}

// hexBytes handles CometBFT's HexBytes (uppercase hex-encoded strings in JSON).
type hexBytes []byte

func (h *hexBytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*h = nil
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("decode hex: %w", err)
	}
	*h = b
	return nil
}
