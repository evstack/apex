# Open Issues

## Phase 1: Core Storage + Sync Engine

### ~~#10 — SQLite storage layer implementation~~ (CLOSED)
Implemented in `pkg/store/sqlite.go`. Schema migrations via `embed`, WAL mode, `MaxOpenConns(1)`. Unit tests in `pkg/store/sqlite_test.go`.

### ~~#11 — Sync engine: backfill, streaming, and gap recovery~~ (CLOSED)
Implemented in `pkg/sync/`. Coordinator state machine (init -> backfill -> stream), Backfiller with batched concurrent workers, SubscriptionManager with gap detection. Tests in `pkg/sync/coordinator_test.go` and `pkg/sync/backfill_test.go`.

### ~~#12 — CelestiaNodeFetcher: upstream RPC client~~ (CLOSED)
Implemented in `pkg/fetch/celestia_node.go`. Uses `go-jsonrpc` (not celestia-openrpc, which is archived). Slim response types for CometBFT JSON. Tests in `pkg/fetch/celestia_node_test.go`.

### ~~#13 — Configuration system: YAML loading and validation~~ (CLOSED — pre-existing)
Already implemented in `config/` during scaffolding.

## Phase 2: API Layer

### ~~#14 — Subscription improvements: backpressure, contiguity, multi-namespace~~ (CLOSED)
Implemented in `pkg/api/notifier.go`. Configurable buffer (default 64), contiguous delivery tracking with gap warnings, multi-namespace filtering, non-blocking publish with 75% capacity warning. SQLite read/write pool split in `pkg/store/sqlite.go`. Observer hook in coordinator (`WithObserver`). Tests in `pkg/api/notifier_test.go`.

### ~~#2 — Implement Celestia node JSON-RPC compatibility layer~~ (CLOSED)
Implemented in `pkg/api/jsonrpc/`. Shared service layer in `pkg/api/service.go`. Blob module: Get, GetAll, Subscribe (WS), GetProof/Included (upstream forwarding), GetCommitmentProof (stub), Submit (stub). Header module: GetByHeight, LocalHead, NetworkHead, Subscribe (WS). Stub modules: share, fraud, blobstream. Proof forwarding via `ProofForwarder` interface in `pkg/fetch/fetcher.go`. HTTP/WS server wired in `cmd/apex/main.go`. Tests in `pkg/api/service_test.go` and `pkg/api/jsonrpc/server_test.go`.

### ~~#3 — Add gRPC API alongside JSON-RPC~~ (CLOSED)
Implemented in `pkg/api/grpc/`. Proto definitions in `proto/apex/v1/` (types, blob, header). Generated code in `pkg/api/grpc/gen/`. BlobService: Get, GetAll, Subscribe (server-streaming). HeaderService: GetByHeight, LocalHead, NetworkHead, Subscribe (server-streaming). Buf config for proto generation (`buf.yaml`, `buf.gen.yaml`). Separate port (default `:9090`). Graceful shutdown. Tests in `pkg/api/grpc/server_test.go`.

## Phase 3: Polish

### #7 — Observability: metrics, logging, and tracing
Prometheus metrics (OTel), structured logging (`slog`), OpenTelemetry tracing. Metrics for sync engine, API, store, subscriptions, node info. Configurable log level. Unified `SyncStatus()` endpoint. Prometheus endpoint on configurable address.

### #15 — CLI tooling: apex command-line interface
Minimal CLI using `spf13/cobra`. Commands: `start`, `status`, `blob get`, `blob list`, `config validate`, `config show`, `version`. CLI commands are thin wrappers around JSON-RPC client. JSON output default, `--format table` option.

### #16 — Health and readiness endpoints
`Ready()` -> bool (RPC accepting, sync running, store accessible). `Health()` -> HealthStatus (sync_state, heights, lag, upstream status, uptime, version). `GET /health` HTTP endpoint (200/503). Maps to k8s probes.

## Phase 4: Transaction Submission

### #4 — Evaluate custom transaction submission client vs celestia-node client
Decision issue: custom vs celestia-node submission client. Leaning custom for tighter control, fewer deps, optimized retries/nonces. Parent for #5, #8, #9, #17, #18, #19.

### #5 — Multi-account support for transaction submission
Multiple funded accounts to avoid nonce contention. Account pool with round-robin or LRU assignment. Independent nonce tracking per account. Balance monitoring. Graceful degradation on unhealthy accounts. Depends on #4.

### #8 — Nonce management for transaction submission
Local nonce tracking and correction. Startup: query on-chain sequence. Optimistic increment on broadcast. Re-query on ErrWrongSequence (no regex parsing). Thread-safe per-account mutex. Max 3 retry attempts. Parent: #4.

### #9 — Keyring and signing for transaction submission
Key management and tx signing. Option A (v1): direct key file with secp256k1. Option B: cosmos-sdk keyring. Option C (future): remote signer/KMS. SIGN_MODE_DIRECT. Key rotation via SIGHUP. Never log key material. Parent: #4.

### #17 — Gas estimation for blob submission
Deterministic gas estimation for MsgPayForBlobs. Pure function from blob sizes. Formula: `gas = gasToConsume(blobSizes, gasPerBlobByte) + (txSizeCostPerByte * bytesPerBlobInfo * numBlobs) + pfbGasFixedCost`. Configurable gas price with buffer and safety cap. No network calls. Parent: #4.

### #18 — Rate limiting and circuit breaker for transaction submission
Backpressure: concurrency semaphore (default 4 in-flight), exponential backoff (1s-30s with jitter), circuit breaker (closed/open/half-open, 50% failure threshold over sliding window of 20). Parent: #4.

### #19 — Transaction confirmation and lifecycle tracking
Tx lifecycle: BroadcastTxSync -> mempool -> confirmed/evicted/rejected. Confirmation polling (1s interval, 2min timeout). Eviction resubmission. Structured error reporting. Unconfirmed tx tracking. Parent: #4.

### #24 — Isolate cosmos-sdk dependency in a separate Go submodule
Core apex must not import cosmos-sdk. All cosmos-sdk code lives in `submit/` submodule with its own `go.mod`. Boundary interface: `BlobSubmitter` with no cosmos-sdk types. Build tags or conditional import for read-only deployments.

## Future / Enhancement

### #1 — Implement Fiber DA spec compatibility (FiberFetcher)
`FiberFetcher` implementing `DataFetcher` interface. Conforms to fibre-da-spec. Blocked on spec stabilization and Fiber network availability.

### #6 — Add S3 storage backend for stateless deployment
S3-compatible Store implementation for stateless deployments. Objects keyed by `namespace/height/commitment`. Local LRU cache for hot data. Hybrid option: SQLite sync state + S3 blob data.

### #20 — Bulk import/export for bootstrapping
`apex export` and `apex import` commands. SQL INSERT format. Optional gzip compression. Transaction-wrapped import. Also: `apex snapshot` via SQLite backup API.

### #21 — Track celestia-node API v1 migration (blob_v1, share_v1)
Tracking issue for upstream breaking changes. blob_v1: proofs_only option, GetProof removed (folded into Get), Included deprecated. share_v1: new method signatures. Support both v0 and v1.

### #22 — Add GetBlobByCommitment endpoint (retrieve without height)
New API: `blob.GetByCommitment(namespace, commitment)`. Schema index on `(namespace, commitment)`. Differentiator over celestia-node for rollups with external ordering.

### #23 — TLS support for RPC server and upstream connections
TLS for outbound (wss://, gRPC TLS, custom CA) and inbound (JSON-RPC, gRPC). Certificate/key config. Skip-verify for dev.

### #25 — Benchmarks and metrics collection for launch blog post
Collect storage, performance, resource footprint, simplicity, and reliability benchmarks for a single namespace. After Phase 2 is complete. Compare against celestia-node.

### #26 — CelestiaAppFetcher: ingest from celestia-app (consensus node)
Implement `CelestiaAppFetcher` satisfying the `DataFetcher` interface. Connects to a celestia-app (consensus/full) node via CometBFT RPC or gRPC instead of a celestia-node (light/bridge). Enables indexing without running a DA node — useful for operators already running validators or full nodes. Query blocks via `/block` + `/block_results`, extract blobs from block data. Header subscription via CometBFT WebSocket `/subscribe`. Evaluate whether to use CometBFT RPC (HTTP) or gRPC (cosmos-sdk dependency tradeoff — may need the `submit/` submodule pattern from #24).
