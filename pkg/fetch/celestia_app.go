package fetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

// CelestiaAppFetcher implements DataFetcher using CometBFT RPC endpoints
// exposed by celestia-app (consensus node). This enables indexing without
// a Celestia DA node.
type CelestiaAppFetcher struct {
	baseURL    string
	wsURL      string
	httpClient *http.Client
	authToken  string
	log        zerolog.Logger
	mu         sync.Mutex
	closed     bool
	cancelSub  context.CancelFunc
}

// NewCelestiaAppFetcher creates a fetcher that reads from celestia-app's
// CometBFT RPC. No connection is established at construction time.
func NewCelestiaAppFetcher(baseURL, authToken string, log zerolog.Logger) (*CelestiaAppFetcher, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	// Derive WebSocket URL.
	wsScheme := "ws"
	if u.Scheme == "https" {
		wsScheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s/websocket", wsScheme, u.Host)

	return &CelestiaAppFetcher{
		baseURL: strings.TrimRight(baseURL, "/"),
		wsURL:   wsURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		authToken: authToken,
		log:       log.With().Str("component", "celestia-app-fetcher").Logger(),
	}, nil
}

// GetHeader fetches a block at the given height and returns a Header.
func (f *CelestiaAppFetcher) GetHeader(ctx context.Context, height uint64) (*types.Header, error) {
	endpoint := fmt.Sprintf("%s/block?height=%d", f.baseURL, height)
	result, err := f.doGet(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("get block at height %d: %w", height, err)
	}
	return f.parseBlockHeader(result, height)
}

// GetBlobs fetches a block and extracts blobs matching the given namespaces.
func (f *CelestiaAppFetcher) GetBlobs(ctx context.Context, height uint64, namespaces []types.Namespace) ([]types.Blob, error) {
	endpoint := fmt.Sprintf("%s/block?height=%d", f.baseURL, height)
	result, err := f.doGet(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("get block at height %d: %w", height, err)
	}

	var block cometBlockResult
	if err := json.Unmarshal(result, &block); err != nil {
		return nil, fmt.Errorf("unmarshal block result: %w", err)
	}

	txs := make([][]byte, len(block.Block.Data.Txs))
	for i, txB64 := range block.Block.Data.Txs {
		decoded, err := base64.StdEncoding.DecodeString(txB64)
		if err != nil {
			return nil, fmt.Errorf("decode tx %d: %w", i, err)
		}
		txs[i] = decoded
	}

	blobs, err := extractBlobsFromBlock(txs, namespaces, height)
	if err != nil {
		return nil, fmt.Errorf("extract blobs at height %d: %w", height, err)
	}
	if len(blobs) == 0 {
		return nil, nil
	}
	return blobs, nil
}

// GetNetworkHead returns the header at the latest block height.
func (f *CelestiaAppFetcher) GetNetworkHead(ctx context.Context) (*types.Header, error) {
	endpoint := fmt.Sprintf("%s/status", f.baseURL)
	result, err := f.doGet(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("get status: %w", err)
	}

	var status cometStatusResult
	if err := json.Unmarshal(result, &status); err != nil {
		return nil, fmt.Errorf("unmarshal status: %w", err)
	}

	headHeight := uint64(status.SyncInfo.LatestBlockHeight)
	return f.GetHeader(ctx, headHeight)
}

// SubscribeHeaders connects to the CometBFT WebSocket and subscribes to
// new block events. The returned channel is closed on context cancellation
// or connection error. No reconnection -- the coordinator handles gaps.
func (f *CelestiaAppFetcher) SubscribeHeaders(ctx context.Context) (<-chan *types.Header, error) {
	subCtx, cancel := context.WithCancel(ctx)

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("fetcher is closed")
	}
	if f.cancelSub != nil {
		f.cancelSub() // cancel previous subscription
	}
	f.cancelSub = cancel
	f.mu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	header := http.Header{}
	if f.authToken != "" {
		header.Set("Authorization", "Bearer "+f.authToken)
	}

	conn, resp, err := dialer.DialContext(subCtx, f.wsURL, header)
	if resp != nil && resp.Body != nil {
		resp.Body.Close() //nolint:errcheck
	}
	if err != nil {
		cancel()
		return nil, fmt.Errorf("websocket dial: %w", err)
	}

	// Subscribe to new block events.
	subscribeReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "subscribe",
		"params": map[string]string{
			"query": "tm.event='NewBlock'",
		},
	}
	if err := conn.WriteJSON(subscribeReq); err != nil {
		_ = conn.Close()
		cancel()
		return nil, fmt.Errorf("send subscribe: %w", err)
	}

	out := make(chan *types.Header, 64)
	go f.readHeaderLoop(subCtx, conn, out)

	return out, nil
}

func (f *CelestiaAppFetcher) readHeaderLoop(ctx context.Context, conn *websocket.Conn, out chan<- *types.Header) {
	defer close(out)
	defer conn.Close() //nolint:errcheck

	// Close the connection when context is cancelled to unblock ReadMessage.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				f.log.Error().Err(err).Msg("websocket read error")
			}
			return
		}

		hdr, err := f.parseWSBlockEvent(msg)
		if err != nil {
			f.log.Warn().Err(err).Msg("skip unparseable block event")
			continue
		}
		if hdr == nil {
			// Non-block message (e.g. subscription confirmation).
			continue
		}

		select {
		case out <- hdr:
		case <-ctx.Done():
			return
		}
	}
}

// Close cancels any active subscription and marks the fetcher as closed.
func (f *CelestiaAppFetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.cancelSub != nil {
		f.cancelSub()
	}
	return nil
}

// doGet performs an authenticated HTTP GET and extracts the "result" field.
func (f *CelestiaAppFetcher) doGet(ctx context.Context, url string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	if f.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+f.authToken)
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s: %w", url, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}

	var rpcResp cometRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	return rpcResp.Result, nil
}

func (f *CelestiaAppFetcher) parseBlockHeader(result json.RawMessage, height uint64) (*types.Header, error) {
	var block cometBlockResult
	if err := json.Unmarshal(result, &block); err != nil {
		return nil, fmt.Errorf("unmarshal block: %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, block.Block.Header.Time)
	if err != nil {
		return nil, fmt.Errorf("parse block time: %w", err)
	}

	return &types.Header{
		Height:    uint64(block.Block.Header.Height),
		Hash:      []byte(block.BlockID.Hash),
		DataHash:  []byte(block.Block.Header.DataHash),
		Time:      t,
		RawHeader: result,
	}, nil
}

func (f *CelestiaAppFetcher) parseWSBlockEvent(msg []byte) (*types.Header, error) {
	var event cometWSEvent
	if err := json.Unmarshal(msg, &event); err != nil {
		return nil, fmt.Errorf("unmarshal ws event: %w", err)
	}

	// Skip non-result messages (e.g. subscription confirmation has empty result).
	if len(event.Result.Data.Value) == 0 {
		return nil, nil //nolint:nilnil
	}

	var blockValue cometBlockEventValue
	if err := json.Unmarshal(event.Result.Data.Value, &blockValue); err != nil {
		return nil, fmt.Errorf("unmarshal block event value: %w", err)
	}

	hdr := blockValue.Block.Header
	t, err := time.Parse(time.RFC3339Nano, hdr.Time)
	if err != nil {
		return nil, fmt.Errorf("parse header time: %w", err)
	}

	raw, err := json.Marshal(blockValue)
	if err != nil {
		return nil, fmt.Errorf("marshal raw header: %w", err)
	}

	return &types.Header{
		Height:    uint64(hdr.Height),
		Hash:      []byte(blockValue.BlockID.Hash),
		DataHash:  []byte(hdr.DataHash),
		Time:      t,
		RawHeader: raw,
	}, nil
}

// CometBFT JSON-RPC response types.

type cometRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *cometRPCError  `json:"error,omitempty"`
}

type cometRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type cometBlockResult struct {
	BlockID cometBlockID `json:"block_id"`
	Block   cometBlock   `json:"block"`
}

type cometBlockID struct {
	Hash hexBytes `json:"hash"`
}

type cometBlock struct {
	Header cometHeader `json:"header"`
	Data   cometTxData `json:"data"`
}

type cometHeader struct {
	Height   jsonInt64 `json:"height"`
	Time     string    `json:"time"`
	DataHash hexBytes  `json:"data_hash"`
}

type cometTxData struct {
	Txs []string `json:"txs"` // base64-encoded
}

type cometStatusResult struct {
	SyncInfo cometSyncInfo `json:"sync_info"`
}

type cometSyncInfo struct {
	LatestBlockHeight jsonInt64 `json:"latest_block_height"`
}

// WebSocket event types.

type cometWSEvent struct {
	Result cometWSResult `json:"result"`
}

type cometWSResult struct {
	Data cometWSData `json:"data"`
}

type cometWSData struct {
	Value json.RawMessage `json:"value"`
}

type cometBlockEventValue struct {
	Block   cometEventBlock `json:"block"`
	BlockID cometBlockID    `json:"block_id"`
}

type cometEventBlock struct {
	Header cometHeader `json:"header"`
}
