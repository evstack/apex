package fetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"

	"github.com/evstack/apex/pkg/types"
)

func cometBlockResponse(height int, txsB64 []string, timeStr string) string {
	txsJSON, _ := json.Marshal(txsB64)
	return fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 1,
		"result": {
			"block_id": {
				"hash": "ABCD1234"
			},
			"block": {
				"header": {
					"height": "%d",
					"time": "%s",
					"data_hash": "DDDD"
				},
				"data": {
					"txs": %s
				}
			}
		}
	}`, height, timeStr, string(txsJSON))
}

func cometStatusResponse(height int) string {
	return fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 1,
		"result": {
			"sync_info": {
				"latest_block_height": "%d"
			}
		}
	}`, height)
}

func TestCelestiaAppFetcherGetHeader(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/block") {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		blockTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
		_, _ = fmt.Fprint(w, cometBlockResponse(42, nil, blockTime))
	}))
	defer ts.Close()

	f, err := NewCelestiaAppFetcher(ts.URL, "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}
	defer f.Close() //nolint:errcheck

	hdr, err := f.GetHeader(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetHeader: %v", err)
	}
	if hdr.Height != 42 {
		t.Errorf("Height = %d, want 42", hdr.Height)
	}
	if len(hdr.Hash) == 0 {
		t.Error("Hash is empty")
	}
}

func TestCelestiaAppFetcherGetBlobs(t *testing.T) {
	ns := testNS(1)

	blobTx := buildBlobTx([]byte("inner-sdk-tx"),
		rawBlob{Namespace: ns, Data: []byte("blob-data"), ShareCommitment: []byte("c1")},
	)
	blobTxB64 := base64.StdEncoding.EncodeToString(blobTx)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		blockTime := time.Now().UTC().Format(time.RFC3339Nano)
		_, _ = fmt.Fprint(w, cometBlockResponse(10, []string{blobTxB64}, blockTime))
	}))
	defer ts.Close()

	f, err := NewCelestiaAppFetcher(ts.URL, "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}
	defer f.Close() //nolint:errcheck

	var nsType types.Namespace
	copy(nsType[:], ns)

	blobs, err := f.GetBlobs(context.Background(), 10, []types.Namespace{nsType})
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if len(blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(blobs))
	}
	if string(blobs[0].Data) != "blob-data" {
		t.Errorf("Data = %q, want %q", blobs[0].Data, "blob-data")
	}
	if string(blobs[0].Commitment) != "c1" {
		t.Errorf("Commitment = %q, want %q", blobs[0].Commitment, "c1")
	}
}

func TestCelestiaAppFetcherGetBlobsNoMatch(t *testing.T) {
	ns := testNS(1)
	blobTx := buildBlobTx([]byte("tx"), rawBlob{Namespace: ns, Data: []byte("d")})
	blobTxB64 := base64.StdEncoding.EncodeToString(blobTx)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		blockTime := time.Now().UTC().Format(time.RFC3339Nano)
		_, _ = fmt.Fprint(w, cometBlockResponse(10, []string{blobTxB64}, blockTime))
	}))
	defer ts.Close()

	f, err := NewCelestiaAppFetcher(ts.URL, "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}
	defer f.Close() //nolint:errcheck

	var ns99 types.Namespace
	ns99[types.NamespaceSize-1] = 99

	blobs, err := f.GetBlobs(context.Background(), 10, []types.Namespace{ns99})
	if err != nil {
		t.Fatalf("GetBlobs: %v", err)
	}
	if blobs != nil {
		t.Errorf("expected nil for no matching blobs, got %d blobs", len(blobs))
	}
}

func TestCelestiaAppFetcherGetNetworkHead(t *testing.T) {
	var requestCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if strings.HasPrefix(r.URL.Path, "/status") {
			_, _ = fmt.Fprint(w, cometStatusResponse(50))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/block") {
			blockTime := time.Now().UTC().Format(time.RFC3339Nano)
			_, _ = fmt.Fprint(w, cometBlockResponse(50, nil, blockTime))
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	f, err := NewCelestiaAppFetcher(ts.URL, "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}
	defer f.Close() //nolint:errcheck

	hdr, err := f.GetNetworkHead(context.Background())
	if err != nil {
		t.Fatalf("GetNetworkHead: %v", err)
	}
	if hdr.Height != 50 {
		t.Errorf("Height = %d, want 50", hdr.Height)
	}
	if requestCount != 2 {
		t.Errorf("expected 2 HTTP requests (status + block), got %d", requestCount)
	}
}

func TestCelestiaAppFetcherSubscribeHeaders(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool { return true },
	}

	blockTime := time.Now().UTC().Format(time.RFC3339Nano)
	eventJSON := fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"id": 1,
		"result": {
			"data": {
				"type": "tendermint/event/NewBlock",
				"value": {
					"block": {
						"header": {
							"height": "100",
							"time": "%s",
							"data_hash": "AAAA"
						}
					},
					"block_id": {
						"hash": "BBBB"
					}
				}
			}
		}
	}`, blockTime)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/websocket" {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("upgrade: %v", err)
				return
			}
			defer conn.Close() //nolint:errcheck

			// Read the subscribe request.
			_, _, err = conn.ReadMessage()
			if err != nil {
				return
			}

			// Send subscription confirmation (empty result).
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))

			// Send a block event.
			_ = conn.WriteMessage(websocket.TextMessage, []byte(eventJSON))

			// Wait for client to close.
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}
	}))
	defer ts.Close()

	f, err := NewCelestiaAppFetcher(ts.URL, "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}
	defer f.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := f.SubscribeHeaders(ctx)
	if err != nil {
		t.Fatalf("SubscribeHeaders: %v", err)
	}

	select {
	case hdr := <-ch:
		if hdr.Height != 100 {
			t.Errorf("Height = %d, want 100", hdr.Height)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for header")
	}
}

func TestCelestiaAppFetcherCloseIdempotent(t *testing.T) {
	f, err := NewCelestiaAppFetcher("http://localhost:26657", "", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewCelestiaAppFetcher: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close (second): %v", err)
	}
}
