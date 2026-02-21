# Stateful Runtime

Stateful Runtime is a Rust-based edge runtime for low-latency, high-QPS workloads. It runs application bundles that define:

- `app.yaml` for limits, ingestion sources, and HTTP endpoints
- `schema.yaml` for entities, indexes, and retention
- JavaScript handlers for route logic and ingestion transforms

## Current MVP Capabilities

- YAML bundle loading and validation
- In-memory entity store with:
  - primary key lookup
  - hash index lookup
  - sorted range lookup
- Retention sweeper with tombstone cleanup
- JavaScript execution via `deno_core`:
  - route handlers (`route.handle(request)`)
  - ingestion hook (`on_ingest(eventType, data, context)`)
  - custom store ops exposed to scripts
- Kafka ingestion with stable consumer group IDs and at-least-once semantics
- Dynamic HTTP routes loaded from `app.yaml`
- `/healthz`, `/readyz`, and `/metrics` endpoints
- Rate limiting and concurrency limiting

## Build

```bash
cargo build
```

## Run

```bash
cargo run -- --bundle-path ./examples/ads-counter --bind 0.0.0.0:8080 --metrics-bind 0.0.0.0:9090
```

Environment variables:

- `BUNDLE_PATH` (optional if `--bundle-path` is provided)
- `BIND` (optional if `--bind` is provided)
- `METRICS_BIND` (optional if `--metrics-bind` is provided)
- `KAFKA_BROKERS` (used if `app.yaml` Kafka source does not define brokers)

## Bundle Layout

```text
bundle/
├── app.yaml
├── schema.yaml
└── scripts/
    ├── on_ingest.js
    └── routes/
        ├── feed.js
        └── get_post.js
```

## JavaScript Contracts

Route handler:

- Define `const route = { handle(request) { ... } }`
- Return: `{ status, headers, body }`

Ingestion handler:

- Define `function on_ingest(eventType, data, context) { ... }`
- Return an array of store ops:
  - upsert: `{ op: "upsert", entity_type, key, value }`
  - delete: `{ op: "delete", entity_type, key }`

Available store ops in JS:

- `Deno.core.ops.op_store_get(entityType, key)` -> JSON string
- `Deno.core.ops.op_store_upsert(entityType, key, valueJson)` -> void
- `Deno.core.ops.op_store_delete(entityType, key)` -> void
- `Deno.core.ops.op_store_index_lookup(indexName, value)` -> JSON string array
- `Deno.core.ops.op_store_range_lookup(indexName, start, end, limit)` -> JSON string array

## Example Bundles

- `examples/ads-counter`
- `examples/feed`

## Systemd Deployment

Artifacts:

- Unit file: `deploy/systemd/stateful-runtime.service`
- Env template: `deploy/systemd/runtime.env.example`

Suggested install paths:

- Binary: `/opt/stateful-runtime/stateful-runtime`
- Bundles: `/opt/stateful-runtime/examples/*`
- Runtime env: `/etc/stateful-runtime/runtime.env`

## Notes

- This MVP defaults to JSON payloads for HTTP and Kafka.
- In-memory state is derived from ingest streams; restart behavior depends on replaying Kafka topics.
