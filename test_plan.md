# Namespace Indexer Service Design Proposal

## Context

**Problem**: celestia-node stores all DA data (hundreds of GB), but ev-node only needs blobs from a few specific namespaces (eden, xo, collect - estimated 10-20GB total).

**Goal**: Create a lightweight "Namespace Indexer" service that:
1. Indexes only preferred namespaces (dramatically reduces storage)
2. Serves ev-node via the same JSON-RPC API (no ev-node modifications)
3. Supports resync from any DA height
4. Enables future fiber integration without ev-node changes

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Namespace Indexer                           │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ Data Fetcher │───▶│ Sync Engine  │───▶│ SQLite Store     │  │
│  │ (pluggable)  │    │              │    │ (namespace data) │  │
│  └──────────────┘    └──────────────┘    └────────┬─────────┘  │
│         ▲                    │                    │            │
│         │                    ▼                    ▼            │
│  ┌──────┴──────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ celestia-   │    │ Subscription │    │ JSON-RPC Server  │  │
│  │ node / fiber│    │ Manager      │───▶│ (blob + header)  │  │
│  └─────────────┘    └──────────────┘    └────────┬─────────┘  │
│                                                   │            │
└───────────────────────────────────────────────────┼────────────┘
                                                    │
                                                    ▼
                                              ┌───────────┐
                                              │  ev-node  │
                                              └───────────┘
```

---

## API Compatibility

The indexer implements the same JSON-RPC interface ev-node expects from celestia-node.

### Required Modules (from celestia-node `api/client/read_client.go`)

| Module | Implementation | Notes |
|--------|----------------|-------|
| `blob` | Full | Core functionality |
| `header` | Full | Required for blob context |
| `share` | Stub | Return error "not supported" |
| `fraud` | Stub | Return error "not supported" |
| `blobstream` | Stub | Return error "not supported" |

### Blob API Methods (from celestia-node `nodebuilder/blob/blob.go`)

| Method | Status | Implementation |
|--------|--------|----------------|
| `Get(height, namespace, commitment)` | **Supported** | Query SQLite by height+ns+commitment |
| `GetAll(height, namespaces[])` | **Supported** | Query SQLite, filter to tracked namespaces |
| `Subscribe(namespace)` | **Supported** | Real-time streaming via subscription manager |
| `GetProof(height, namespace, commitment)` | **Supported** | Store and retrieve NMT proofs |
| `Included(height, namespace, proof, commitment)` | **Supported** | Verify against stored data |
| `GetCommitmentProof(...)` | **Not Supported** | Requires full EDS (return error) |
| `Submit(...)` | **Not Supported** | Read-only service (return error) |

### Header API Methods

| Method | Status | Implementation |
|--------|--------|----------------|
| `GetByHeight(height)` | **Supported** | Local store or upstream fallback |
| `Subscribe()` | **Supported** | Stream new headers |
| `LocalHead()` | **Supported** | Last synced height |
| `NetworkHead()` | **Supported** | Query upstream |

---

## Storage Design (SQLite)

SQLite is preferred over Badger for simpler queries and easier debugging.

```sql
-- Sync checkpoint
CREATE TABLE sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_synced_height INTEGER NOT NULL DEFAULT 0,
    start_height INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Headers (minimal, for API responses)
CREATE TABLE headers (
    height INTEGER PRIMARY KEY,
    hash BLOB NOT NULL,
    data_hash BLOB NOT NULL,
    raw_header BLOB NOT NULL,  -- JSON serialized
    time TIMESTAMP NOT NULL
);

-- Indexed blobs (only tracked namespaces)
CREATE TABLE blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    height INTEGER NOT NULL,
    namespace BLOB NOT NULL,       -- 29 bytes
    commitment BLOB NOT NULL,      -- 32 bytes
    data BLOB NOT NULL,
    share_version INTEGER NOT NULL,
    signer BLOB,
    blob_index INTEGER NOT NULL,
    UNIQUE(height, namespace, commitment)
);

CREATE INDEX idx_blobs_ns_height ON blobs(namespace, height);

-- Tracked namespaces config
CREATE TABLE namespaces (
    namespace BLOB PRIMARY KEY,
    name TEXT,
    enabled BOOLEAN DEFAULT TRUE
);
```

---

## Sync Engine

### State Machine

```
INITIALIZING → BACKFILLING → STREAMING
                    ↑            │
                    └────────────┘ (gap detected)
```

### Resync Mechanism

1. **Startup**: Load checkpoint, determine start height
2. **Backfill**: Fetch heights `[start, network_head]` in batches
   - Configurable batch size (default: 100)
   - Concurrent fetchers (default: 4)
   - Checkpoint saved every N heights
3. **Streaming**: Subscribe to new headers, fetch blobs as they arrive
4. **Gap Recovery**: If gap detected, switch to backfill mode

### Data Source Interface (Pluggable)

```go
type DataFetcher interface {
    GetHeader(ctx context.Context, height uint64) (*header.ExtendedHeader, error)
    GetBlobs(ctx context.Context, height uint64, ns Namespace) ([]*blob.Blob, error)
    GetNetworkHead(ctx context.Context) (uint64, error)
    SubscribeHeaders(ctx context.Context) (<-chan *header.ExtendedHeader, error)
    Close() error
}
```

**Implementations**:
- `CelestiaNodeFetcher`: Connect to celestia-node via JSON-RPC
- `FiberFetcher`: Future - connect directly to fiber network

### Bulk Import (for large backlogs)

For initial setup with large history (millions of heights), support direct DB import:

```bash
# Export from another indexer or celestia-node
indexer export --namespaces eden,xo --from 1000000 --to 2000000 --output dump.sql

# Import into new indexer
indexer import --input dump.sql
```

This bypasses RPC overhead for bulk historical data while RPC handles real-time sync.

---

## Package Structure

```
apex/
├── cmd/indexer/main.go           # Entry point
├── config/
│   ├── config.go                 # Config types
│   └── load.go                   # TOML loading
├── pkg/
│   ├── api/
│   │   ├── server.go             # JSON-RPC server
│   │   ├── blob.go               # Blob API impl
│   │   └── header.go             # Header API impl
│   ├── sync/
│   │   ├── coordinator.go        # Sync orchestration
│   │   ├── backfill.go           # Historical sync
│   │   └── subscriptions.go      # Subscription manager
│   ├── fetch/
│   │   ├── fetcher.go            # Interface
│   │   ├── celestia_node.go      # celestia-node impl
│   │   └── fiber.go              # Future fiber impl
│   └── store/
│       ├── store.go              # Interface
│       └── sqlite.go             # SQLite impl
├── go.mod
├── Makefile
└── Dockerfile
```

---

## Configuration

```toml
# config.toml
[namespaces]
eden = "000000000000000000000000000000000000000000000000000000006564656e"
xo = "00000000000000000000000000000000000000000000000000000000786f"
collect = "000000000000000000000000000000000000000000000000636f6c6c656374"

[data_source]
type = "celestia-node"           # or "fiber" (future)
endpoint = "http://localhost:26658"
auth_token = ""

[storage]
path = "./data/indexer.db"

[rpc]
address = "0.0.0.0:26659"

[sync]
start_height = 1000000           # DA height to start from
backfill_batch_size = 100
backfill_concurrency = 4
```

---

## Critical Files to Reference (in celestia-node)

| File | Purpose |
|------|---------|
| `nodebuilder/blob/blob.go` | Module interface ev-node expects |
| `blob/service.go` | Reference impl for Subscribe/Get/GetAll |
| `api/client/read_client.go` | Shows required modules (Blob, Header, Share, Fraud, Blobstream) |
| `api/rpc/server.go` | JSON-RPC server pattern using go-jsonrpc |

---

## Verification Plan

1. **Unit Tests**: Mock DataFetcher, test sync logic and API handlers
2. **Integration Test**:
   - Start indexer pointing at local celestia-node
   - Configure with test namespaces
   - Verify Get/GetAll/Subscribe work correctly
3. **ev-node Compatibility**:
   - Point ev-node at indexer instead of celestia-node
   - Verify no changes needed to ev-node
   - Test subscription streaming under load

---

## Implementation Order

1. **Phase 1**: Core storage + sync engine
   - SQLite store implementation
   - Sync coordinator with backfill
   - CelestiaNodeFetcher (upstream RPC client)

2. **Phase 2**: API layer
   - JSON-RPC server setup
   - Blob module (Get, GetAll, Subscribe)
   - Header module (GetByHeight, Subscribe, LocalHead)

3. **Phase 3**: Polish
   - Stub modules (Share, Fraud, Blobstream)
   - Metrics/logging
   - Dockerfile + deployment

4. **Phase 4** (Future): Fiber integration
   - Implement FiberFetcher
   - Config switch between data sources

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Proof storage** | No | Saves ~30% storage; `Included()` returns true if blob exists |
| **Resync mechanism** | Both RPC + DB import | RPC for normal use, optional DB import for large bulk loads |
| **Repository** | Standalone repo | Clean separation, independent releases, easier deployment |

---

## Summary

This Namespace Indexer provides a drop-in replacement for celestia-node when you only need specific namespaces. It:

- **Reduces storage** from 100s GB → 10-20GB by indexing only tracked namespaces
- **Maintains API compatibility** so ev-node works without modification
- **Supports flexible resync** via RPC or bulk DB import
- **Enables future fiber integration** by abstracting the data source

The modular design with pluggable `DataFetcher` means switching from celestia-node to fiber is just swapping the implementation - no ev-node changes needed.
