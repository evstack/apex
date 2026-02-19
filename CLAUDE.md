# Apex — Celestia Namespace Indexer

Lightweight indexer that watches Celestia namespaces, stores blobs/headers in SQLite, and exposes them via JSON-RPC, gRPC, and REST health endpoints. Includes Prometheus observability, a CLI client, and multi-stage Docker build.

## Build Commands

```bash
just build     # compile to bin/apex
just test      # go test -race ./...
just lint      # golangci-lint run
just fmt       # gofumpt -w .
just check     # tidy + lint + test + build (CI equivalent)
just run       # build and run
just clean     # remove bin/
just tidy      # go mod tidy
```

## Architecture

### Data Flow

```
Celestia Node → Fetcher → Sync Coordinator → Store (SQLite)
                                            → Notifier → Subscribers
                          ↕
                     API (JSON-RPC + gRPC + Health)
```

The sync coordinator runs in two phases: **backfill** (historical blocks in batches) then **streaming** (live via header subscription). Height observers publish events to the notifier which fans out to API subscribers.

### File Structure

```
cmd/apex/
  main.go             CLI entrypoint, server wiring, graceful shutdown
  client.go           Thin HTTP JSON-RPC client for CLI commands
  status.go           `apex status` command (health endpoint)
  blob_cmd.go         `apex blob get|list` commands
  config_cmd.go       `apex config validate|show` commands

config/
  config.go           Config structs (DataSource, Storage, RPC, Sync, Metrics, Log)
  load.go             YAML loading, validation, env var override, template generation

pkg/types/
  types.go            Domain types: Namespace, Blob, Header, SyncState, SyncStatus

pkg/store/
  store.go            Store interface (PutBlobs, GetBlobs, PutHeader, GetHeader, sync state)
  sqlite.go           SQLite implementation with metrics instrumentation
  migrations/         SQL migration files

pkg/fetch/
  fetcher.go          DataFetcher + ProofForwarder interfaces
  celestia_node.go    Celestia node-api client (headers, blobs, subscriptions, proofs)

pkg/sync/
  coordinator.go      Sync lifecycle: initialize → backfill → stream, tracks heights
  backfill.go         Concurrent batch backfill with configurable batch size/concurrency
  subscription.go     Header subscription manager for live streaming

pkg/api/
  service.go          API service layer (blob/header queries, proof forwarding, subscriptions)
  notifier.go         Event fan-out to subscribers with bounded buffers
  health.go           /health and /health/ready HTTP endpoints, HealthStatus JSON
  jsonrpc/            JSON-RPC server (go-jsonrpc), blob/header/subscription handlers
  grpc/               gRPC server, protobuf service implementations
    gen/apex/v1/      Generated protobuf Go code

pkg/metrics/
  metrics.go          Recorder interface (nil-safe), nopRecorder, PromRecorder (Prometheus)
  server.go           HTTP server for /metrics endpoint

proto/apex/v1/        Protobuf definitions (blob, header, types)

Dockerfile            Multi-stage build (golang builder + distroless runtime)
```

### Key Interfaces

- **`store.Store`** — persistence (SQLite impl, instrumented with metrics)
- **`fetch.DataFetcher`** — block data retrieval (Celestia node client)
- **`fetch.ProofForwarder`** — proof/inclusion forwarding to upstream node
- **`metrics.Recorder`** — nil-safe metrics abstraction (Prometheus or no-op)
- **`api.StatusProvider`** — sync status for health endpoints (implemented by coordinator)

### Ports (defaults)

| Port  | Protocol | Purpose          |
|-------|----------|------------------|
| :8080 | HTTP     | JSON-RPC + health|
| :9090 | TCP      | gRPC             |
| :9091 | HTTP     | Prometheus /metrics |

### Config

YAML with strict unknown-field rejection. Auth token via `APEX_AUTH_TOKEN` env var only (not in config file). See `config/config.go` for all fields and `DefaultConfig()` for defaults.

## Conventions

- Go 1.25+ (`go.mod` specifies 1.25.0)
- SQLite via `modernc.org/sqlite` (CGo-free)
- Config: YAML (`gopkg.in/yaml.v3`), strict unknown-field rejection
- Logging: `rs/zerolog`
- CLI: `spf13/cobra`
- Metrics: `prometheus/client_golang` behind nil-safe `Recorder` interface
- JSON-RPC: `filecoin-project/go-jsonrpc`
- gRPC: `google.golang.org/grpc` + `google.golang.org/protobuf`
- Protobuf codegen: `buf` (`buf.yaml` + `buf.gen.yaml`)
- Linter: golangci-lint v2 (.golangci.yml v2 format), gocyclo max 15
- Formatter: gofumpt
- Build runner: just (justfile)

## Dependencies

- Only add deps that are strictly necessary
- Prefer stdlib where reasonable
- No CGo dependencies (cross-compilation constraint)

## Testing

- All tests use `-race`
- Table-driven tests preferred
- Test files alongside source (`_test.go`)
- No test frameworks beyond stdlib `testing`
