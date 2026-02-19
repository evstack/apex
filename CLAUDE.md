# Apex — Celestia Namespace Indexer

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

Apex is a lightweight indexer that watches Celestia namespaces, stores blobs/headers in SQLite, and exposes them via an HTTP API.

```
cmd/apex/       CLI entrypoint (cobra)
config/         YAML config loading and validation
pkg/types/      Domain types (Namespace, Blob, Header, SyncState)
pkg/store/      Storage interface (SQLite impl in Phase 1)
pkg/fetch/      Data fetcher interface (Celestia node client in Phase 1)
pkg/sync/       Sync coordinator (backfill + streaming)
pkg/api/        HTTP API server (Phase 2)
```

## Conventions

- Go 1.23 minimum (slog, range-over-func available)
- SQLite via `modernc.org/sqlite` (CGo-free)
- Config: YAML (`gopkg.in/yaml.v3`), strict unknown-field rejection
- Logging: `rs/zerolog`
- CLI: `spf13/cobra`
- Linter: golangci-lint v2 (.golangci.yml v2 format)
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
