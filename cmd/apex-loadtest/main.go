// apex-loadtest tests JSON-RPC throughput and WebSocket subscriber limits
// against a running apex indexer.
//
// Usage:
//
//	apex-loadtest -addr stg-devnet-collect:8080 -rpc-concurrency 50 -rpc-duration 10s
//	apex-loadtest -addr stg-devnet-collect:8080 -subscribers 500 -sub-duration 30s
//	apex-loadtest -addr stg-devnet-collect:8080 -all
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "apex JSON-RPC address (host:port)")
	runAll := flag.Bool("all", false, "run all tests")

	// RPC throughput flags
	rpcConcurrency := flag.Int("rpc-concurrency", 50, "number of concurrent RPC workers")
	rpcDuration := flag.Duration("rpc-duration", 10*time.Second, "duration of RPC throughput test")
	rpcHeight := flag.Uint64("rpc-height", 0, "height to query (0 = fetch from health)")

	// Subscriber flags
	subCount := flag.Int("subscribers", 200, "number of WebSocket subscribers to open")
	subDuration := flag.Duration("sub-duration", 30*time.Second, "how long to hold subscribers open")
	subBatch := flag.Int("sub-batch", 50, "subscribers to add per batch")

	flag.Parse()

	if !*runAll && flag.NArg() == 0 && !isFlagSet("rpc-concurrency") && !isFlagSet("subscribers") {
		*runAll = true
	}

	height := *rpcHeight
	if height == 0 {
		h, err := fetchCurrentHeight(*addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get current height: %v\n", err)
			os.Exit(1)
		}
		height = h
		fmt.Printf("using height %d from health endpoint\n\n", height)
	}

	if *runAll || isFlagSet("rpc-concurrency") || isFlagSet("rpc-duration") {
		runRPCThroughput(*addr, height, *rpcConcurrency, *rpcDuration)
		fmt.Println()
	}

	if *runAll || isFlagSet("subscribers") || isFlagSet("sub-duration") {
		runSubscriberTest(*addr, *subCount, *subBatch, *subDuration)
	}
}

func isFlagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// --- RPC Throughput Test ---

type rpcRequest struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

func runRPCThroughput(addr string, height uint64, concurrency int, duration time.Duration) {
	fmt.Printf("=== RPC Throughput Test ===\n")
	fmt.Printf("target: %s, concurrency: %d, duration: %s\n", addr, concurrency, duration)
	fmt.Printf("methods: header.GetByHeight, header.LocalHead, blob.GetAll\n\n")

	methods := []struct {
		name   string
		params []any
	}{
		{"header.GetByHeight", []any{height}},
		{"header.LocalHead", []any{}},
		{"blob.GetAll", []any{height, nil}},
	}

	for _, m := range methods {
		stats := runMethodBench(addr, m.name, m.params, concurrency, duration)
		printStats(m.name, stats)
	}
}

type benchStats struct {
	total    int64
	errors   int64
	duration time.Duration
	latP50   time.Duration
	latP95   time.Duration
	latP99   time.Duration
	latMax   time.Duration
}

type sub struct {
	conn *websocket.Conn
	id   int
}

func runMethodBench(addr, method string, params []any, concurrency int, duration time.Duration) benchStats {
	body, _ := json.Marshal(rpcRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	})

	url := "http://" + addr
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	var total, errCount atomic.Int64
	latencies := make([]time.Duration, 0, 100000)
	var latMu sync.Mutex

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	start := time.Now()

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}

				t0 := time.Now()
				req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
				if reqErr != nil {
					errCount.Add(1)
					total.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req) //nolint:gosec // URL from CLI flag
				lat := time.Since(t0)

				if err != nil {
					if ctx.Err() != nil {
						return
					}
					errCount.Add(1)
					total.Add(1)
					continue
				}
				if _, copyErr := io.Copy(io.Discard, resp.Body); copyErr != nil {
					errCount.Add(1)
				}
				if closeErr := resp.Body.Close(); closeErr != nil {
					errCount.Add(1)
				}

				total.Add(1)
				if resp.StatusCode != 200 {
					errCount.Add(1)
				}

				latMu.Lock()
				latencies = append(latencies, lat)
				latMu.Unlock()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	stats := benchStats{
		total:    total.Load(),
		errors:   errCount.Load(),
		duration: elapsed,
	}
	if n := len(latencies); n > 0 {
		stats.latP50 = latencies[n*50/100]
		stats.latP95 = latencies[n*95/100]
		stats.latP99 = latencies[n*99/100]
		stats.latMax = latencies[n-1]
	}
	return stats
}

func printStats(method string, s benchStats) {
	rps := float64(s.total) / s.duration.Seconds()
	fmt.Printf("  %-25s %6d reqs  %8.1f req/s  err=%d  p50=%s  p95=%s  p99=%s  max=%s\n",
		method, s.total, rps, s.errors,
		s.latP50.Round(100*time.Microsecond),
		s.latP95.Round(100*time.Microsecond),
		s.latP99.Round(100*time.Microsecond),
		s.latMax.Round(100*time.Microsecond),
	)
}

// --- Subscriber Test ---

func runSubscriberTest(addr string, target, batch int, duration time.Duration) {
	fmt.Printf("=== Subscriber Stress Test ===\n")
	fmt.Printf("target: %s, max subscribers: %d, batch: %d, hold: %s\n\n", addr, target, batch, duration)

	wsURL := "ws://" + addr

	var subs []sub
	var connected, failed, eventsReceived atomic.Int64

	// Open subscribers in batches
	for i := 0; i < target; i += batch {
		end := i + batch
		if end > target {
			end = target
		}

		batchSubs, batchConnected, batchFailed := openSubscriberBatch(wsURL, i, end)
		subs = append(subs, batchSubs...)
		connected.Add(batchConnected)
		failed.Add(batchFailed)

		fmt.Printf("  batch %d-%d: connected=%d failed=%d\n",
			i, end-1, connected.Load(), failed.Load())
	}

	totalConnected := connected.Load()
	fmt.Printf("\n  total connected: %d / %d\n", totalConnected, target)

	if totalConnected == 0 {
		fmt.Println("  no subscribers connected, skipping event collection")
		return
	}

	// Read events from all subscribers for the duration
	readCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func(s sub) {
			defer wg.Done()
			if err := s.conn.SetReadDeadline(time.Now().Add(duration + 5*time.Second)); err != nil {
				return
			}
			for {
				if readCtx.Err() != nil {
					return
				}
				_, _, err := s.conn.ReadMessage()
				if err != nil {
					return
				}
				eventsReceived.Add(1)
			}
		}(s)
	}

	// Print progress every 5s
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-readCtx.Done():
				return
			case <-ticker.C:
				fmt.Printf("  events received: %d\n", eventsReceived.Load())
			}
		}
	}()

	wg.Wait()

	fmt.Printf("\n  final: %d events from %d subscribers over %s\n",
		eventsReceived.Load(), totalConnected, duration)
	fmt.Printf("  avg events/subscriber: %.1f\n",
		float64(eventsReceived.Load())/float64(totalConnected))

	// Cleanup
	for _, s := range subs {
		_ = s.conn.Close()
	}
}

func openSubscriberBatch(wsURL string, start, end int) ([]sub, int64, int64) {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		subs      []sub
		connected atomic.Int64
		failed    atomic.Int64
	)

	for j := start; j < end; j++ {
		wg.Add(1)
		id := j
		go func() {
			defer wg.Done()
			conn, err := dialAndSubscribe(wsURL, id)
			if err != nil {
				failed.Add(1)
				if id < 5 || id%100 == 0 {
					fmt.Fprintf(os.Stderr, "  sub %d: dial failed: %v\n", id, err)
				}
				return
			}
			connected.Add(1)
			mu.Lock()
			subs = append(subs, sub{conn: conn, id: id})
			mu.Unlock()
		}()
	}
	wg.Wait()
	return subs, connected.Load(), failed.Load()
}

func dialAndSubscribe(wsURL string, id int) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, resp, err := dialer.Dial(wsURL, nil)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if err != nil {
		return nil, err
	}

	req := rpcRequest{
		Jsonrpc: "2.0",
		Method:  "header.Subscribe",
		Params:  []any{},
		ID:      id + 1,
	}
	if err := conn.WriteJSON(req); err != nil {
		closeErr := conn.Close()
		return nil, errors.Join(err, closeErr)
	}
	return conn, nil
}

// --- Helpers ---

func fetchCurrentHeight(addr string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req) //nolint:gosec // URL from CLI flag
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	var health struct {
		LatestHeight uint64 `json:"latest_height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return 0, err
	}
	return health.LatestHeight, nil
}
