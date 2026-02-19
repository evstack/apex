# Running Apex

## Prerequisites

- Go 1.25+ (build from source) or Docker
- A Celestia node to index from:
  - **DA node** (`celestia-node`) -- full blob data + proof forwarding
  - **Consensus node** (`celestia-app`) -- blob data via CometBFT RPC, no proof support

## Install

```bash
# From source
just build
# Binary at bin/apex

# Or with go install
go install github.com/evstack/apex/cmd/apex@latest
```

## Configuration

Generate a default config file:

```bash
apex init
# Creates apex.yaml in the current directory
```

### Minimal Config (DA Node)

```yaml
data_source:
  type: "node"
  celestia_node_url: "http://localhost:26658"
  namespaces:
    - "0000000000000000000000000000000000000000000000000000deadbeef"
```

Set the auth token via environment variable (never in the config file):

```bash
export APEX_AUTH_TOKEN="your-celestia-node-auth-token"
```

### Minimal Config (Consensus Node)

```yaml
data_source:
  type: "app"
  celestia_app_url: "http://localhost:26657"
  namespaces:
    - "0000000000000000000000000000000000000000000000000000deadbeef"
```

No auth token needed for celestia-app. Proof endpoints (`blob.GetProof`, `blob.Included`) are unavailable in this mode.

### Full Config Reference

```yaml
data_source:
  type: "node"                                    # "node" or "app"
  celestia_node_url: "http://localhost:26658"      # required when type: node
  celestia_app_url: "http://localhost:26657"       # required when type: app
  namespaces: []                                   # hex-encoded, 29 bytes each

storage:
  db_path: "apex.db"                               # SQLite database path

rpc:
  listen_addr: ":8080"                             # JSON-RPC + health
  grpc_listen_addr: ":9090"                        # gRPC

sync:
  start_height: 0                                  # 0 = genesis
  batch_size: 64                                   # headers per backfill batch
  concurrency: 4                                   # concurrent fetch workers

subscription:
  buffer_size: 64                                  # event buffer per subscriber

metrics:
  enabled: true
  listen_addr: ":9091"                             # Prometheus /metrics

log:
  level: "info"                                    # trace/debug/info/warn/error/fatal/panic
  format: "json"                                   # json or console
```

Validate a config without starting the server:

```bash
apex config validate --config apex.yaml
apex config show --config apex.yaml
```

## Running

### Binary

```bash
apex start --config apex.yaml
```

### Docker

```bash
docker build -t apex .
docker run -p 8080:8080 -p 9090:9090 -p 9091:9091 \
  -e APEX_AUTH_TOKEN="your-token" \
  -v $(pwd)/apex.yaml:/config/apex.yaml \
  -v $(pwd)/data:/data \
  apex --config /config/apex.yaml
```

The image is based on `distroless/static` and runs as non-root (UID 65532).

### just

```bash
just run start --config apex.yaml
```

## CLI Commands

```
apex init                              Generate default config file
apex start --config apex.yaml          Start the indexer
apex status [--addr http://...]        Check health endpoint
apex version                           Print version

apex config validate --config FILE     Validate config
apex config show --config FILE         Print resolved config

apex blob get HEIGHT NAMESPACE INDEX   Get a single blob
apex blob list HEIGHT NAMESPACE        List blobs at height
apex blob get-by-commitment HEX        Get blob by commitment (no height needed)
```

All CLI data commands output JSON by default. Use `--format table` for human-readable output.

## Health Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Returns sync state, latest/network height, lag, version, uptime |
| `GET /health/ready` | Returns 200 when syncing or streaming, 503 otherwise |

## API

### JSON-RPC (port 8080)

Celestia-node compatible methods:

- `blob.Get`, `blob.GetAll`, `blob.GetByCommitment`, `blob.Subscribe`
- `blob.GetProof`, `blob.Included` (DA node mode only, forwarded upstream)
- `header.GetByHeight`, `header.LocalHead`, `header.NetworkHead`, `header.Subscribe`

### gRPC (port 9090)

- `apex.v1.BlobService`: Get, GetAll, GetByCommitment, Subscribe (server-streaming)
- `apex.v1.HeaderService`: GetByHeight, LocalHead, NetworkHead, Subscribe (server-streaming)

## Monitoring

When `metrics.enabled: true`, Prometheus metrics are served at `http://localhost:9091/metrics`. Metrics cover sync progress, API request counts, store query durations, and subscription health.
