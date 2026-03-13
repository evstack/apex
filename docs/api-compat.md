# API Compatibility Policy

Apex exposes two external API surfaces with different compatibility rules.

## JSON-RPC

JSON-RPC is the compatibility surface.

- Method names and core response shapes are intended to remain compatible with `celestia-node`.
- Compatibility takes priority over API cleanup when the two are in tension.
- Behavioral quirks inherited for compatibility should be preserved unless upstream compatibility requirements change.
- Operational protections for JSON-RPC should be added outside the method contract when possible:
  - auth
  - reverse-proxy rate limits
  - request size and timeout limits

## gRPC

gRPC is Apex-owned and may evolve independently.

- gRPC may add stricter validation and explicit limits.
- gRPC should prefer stable, well-specified semantics over mirroring JSON-RPC quirks.
- Transport-level improvements should land in gRPC first when they do not belong to the shared domain model.

Current intentional differences:

- `BlobService.GetAll` enforces a namespace cap; JSON-RPC `blob.GetAll` does not.
- gRPC blob subscriptions emit only matching blob events; JSON-RPC remains compatibility-oriented.

## Internal Service Boundary

Shared correctness belongs in the internal service and storage layers.

- Store invariants, sync correctness, and read semantics should not diverge by transport.
- Compatibility shims belong at the JSON-RPC adapter edge.
- gRPC handlers should call service methods instead of reaching directly into store or fetcher internals.

## Testing Rule

When a transport difference is intentional, it should be documented here and pinned by tests.
