# Apex

Lightweight Celestia namespace indexer. Watches namespaces on the Celestia network, stores blobs and headers in SQLite, and serves them over JSON-RPC, gRPC, and REST.

## Features

- **Dual data source**: Index from a Celestia DA node (`celestia-node`) or directly from a consensus node (`celestia-app`)
- **Backfill + streaming**: Catches up on historical blocks, then streams live via header subscriptions with automatic gap recovery
- **Query by commitment**: Retrieve blobs by cryptographic commitment alone -- no height required
- **JSON-RPC + gRPC**: Celestia-node compatible JSON-RPC API alongside a gRPC API with server-streaming subscriptions
- **Observability**: Prometheus metrics, structured logging (zerolog), health/readiness endpoints
- **Zero CGo**: Pure Go, cross-compiles cleanly

## Quick Start

```bash
# Generate default config (writes config.yaml)
apex init

# Set auth token (DA node mode)
export APEX_AUTH_TOKEN="your-token"

# Edit config.yaml: add namespaces, adjust URLs
# Start indexing
apex start
```

## Build

Requires Go 1.25+ and [just](https://github.com/casey/just).

```bash
just build          # compile to bin/apex
just test           # go test -race ./...
just lint           # golangci-lint
just check          # tidy + lint + test + build
```

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 8080 | HTTP | JSON-RPC + health endpoints |
| 9090 | TCP | gRPC |
| 9091 | HTTP | Prometheus /metrics |

## Documentation

- [Running Apex](docs/running.md) -- setup, configuration, Docker, CLI

## License

See [LICENSE](LICENSE).
