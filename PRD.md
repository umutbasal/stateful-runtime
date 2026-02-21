# PRD: Stateful Edge Runtime for Low-Latency, High-QPS Workloads

**Document status:** Draft
**Owner:** (you)
**Last updated:** 2026-02-21
**Audience:** Engineering, Product, DevRel, Ops/SRE, Security

---

## 1) Summary

Build a **stateful, scriptable runtime** optimized for **low latency** and **high QPS** at the edge (or near-edge), with a **static, VM-like pricing model**. Users define:

* **Data model + indexes + retention** (YAML schema)
* **Ingestion** (Kafka + optional HTTP ingestion)
* **Query + endpoint behavior** (JavaScript hooks/handlers)
* **Capacity** (CPU/RAM/limits like max entities, max messages, retention), chosen as a “plan” comparable to buying VMs

Users deploy as:

* **Direct-to-VM** (single binary + app bundle), or
* **Kubernetes** (Helm chart/operator optional)

Positioning: “Like Cloudflare Workers in developer experience (scripts + endpoints), but designed for **stateful, in-memory indexed data** and **very high QPS/low p99 latency** with predictable static pricing.”

---

## 2) Problem Statement

Teams building real-time products (ads, feeds, personalization, counters, fraud/rate-limits, session state) often need:

* **Sub-millisecond to single-digit millisecond** query latency
* **High QPS** with predictable cost
* **Simple programmable query logic** close to the data
* **Fast ingestion** of events (Kafka topics)
* **In-memory state** with **indexes** and **retention** policies

Existing options force tradeoffs:

* “Edge compute” platforms are often stateless or have limited state semantics.
* General databases offer durability and features but can be costly, slower at p99, and harder to program for custom query logic at the edge.
* Teams end up building bespoke caches + state services.

---

## 3) Goals and Non-Goals

### Goals

1. **Generic** runtime: no hardcoded domain logic; behavior configured via schema and JS scripts.
2. **Low latency / high QPS**: optimized for hot-path reads/writes and indexed lookups.
3. **Predictable pricing**: static plans resembling VM shapes; no “per-request” surprise bills.
4. **Multiple deployment targets**: VM-first + Kubernetes.
5. **Flexible data formats**: JSON first; add Protobuf and Cap’n Proto for performance and efficiency.
6. **Easy to adopt**: app bundle format; local dev; simple deploy.
7. **Operationally safe**: strict resource limits, rate limiting, robust observability.

### Non-Goals (initially)

* Global multi-tenant edge network operated by us (Cloudflare-style POPs).
  *We can support multi-region installs, but assume customer-managed infra initially.*
* Full durable database semantics (multi-record transactions, complex query language).
* Exactly-once ingestion guarantees (initially aim for at-least-once with idempotency support).
* Arbitrary untrusted JS multi-tenant sandboxing across customers in the same process (initially single-tenant per instance).

---

## 4) Target Users and Personas

### Persona A: Growth/Ads Engineer

Needs extremely fast segment/campaign lookups, counters, and lightweight scoring. Lives in Kafka + real-time pipelines.

### Persona B: Social/Feed Engineer

Needs timeline reads, time-range queries, and ranking logic. Needs fast fan-out reads and time-based retention.

### Persona C: Platform/Infra Engineer

Wants predictable capacity planning (CPU/RAM), stable ops, good metrics, and straightforward deployment (VM/K8s).

---

## 5) Use Cases

### 5.1 Ads / Real-Time Decisioning

* Ingest impressions/clicks/conversions
* Maintain per-campaign counters and rate limits
* Serve low-latency lookup/scoring endpoints

### 5.2 Social Media / Feeds

* Ingest post create/delete/engage events
* Index by author, time, conversation
* Serve `/feed`, `/post/:id`, `/conversation/:id`

### 5.3 Personalization / Feature Serving

* User/session state
* Feature flags + counters
* Small windowed history

### 5.4 Abuse / Rate Limiting

* Sliding window counters
* Bloom-like membership checks
* Per-IP/user throttles

---

## 6) Product Scope and Key Concepts

### 6.1 App Bundle

A deployable artifact containing:

* `app.yaml` (limits, endpoints, formats, auth, runtime settings)
* `schema.yaml` (entities/indexes/events)
* `scripts/` (ingest/query/route handlers)
* `schemas/` optional (.proto / .capnp)
* Optional test fixtures + load tests

### 6.2 Instance (Runtime Process)

One running unit on a VM/pod:

* Executes JS handlers through deno_core (V8-backed) isolate pool
* Maintains in-memory indexed store
* Consumes Kafka and/or accepts HTTP ingestion
* Serves HTTP edge endpoints and admin APIs
* Enforces limits (CPU, memory, concurrency, rate caps)

### 6.3 Multi-Region

Same app deployed in multiple regions:

* Each region consumes relevant Kafka topics (shared or region-scoped)
* Read requests go to nearest region
* Consistency model: eventual by default

---

## 7) User Journey

1. **Define schema**: entities + indexes + retention (YAML)
2. **Write scripts**:

   * `on_ingest` transforms events into store ops
   * Route handlers serve queries (`/v1/feed`, etc.)
3. **Choose plan**: CPU/RAM/limits (static)
4. **Deploy**:

   * VM: install binary + app bundle; systemd start
   * K8s: helm install with ConfigMaps/Secrets
5. **Operate**:

   * Monitor QPS/latency/memory/index sizes
   * Scale horizontally by adding instances and partitioning consumption
   * Roll out new app bundle versions

---

## 8) Functional Requirements

### 8.1 Configuration and App Definition

**FR-1** Support `app.yaml` + `schema.yaml` as distinct configs.
**FR-2** Validate configs on boot (schema consistency, endpoint uniqueness, script presence, limits sane).
**FR-3** Support versioning metadata in `app.yaml` for rollouts and debugging.

**Acceptance criteria**

* Invalid config fails fast with actionable error messages.
* Runtime exposes `/healthz` and `/readyz` reflecting config and script load success.

---

### 8.2 Data Model and Store

**FR-4** Generic entity store with:

* Primary key lookup
* Hash index lookup (exact match)
* Sorted index lookup (range/time)
* Optional probabilistic membership index (Bloom-like) (v2 if needed)

**FR-5** Support retention policies:

* Per-index retention and/or per-entity TTL windows
* Eviction must remove from **primary store + all indexes**
* Tombstone handling for deletes (bounded size + TTL)

**FR-6** Store operations:

* upsert, delete, get, batch get
* index lookups returning keys and/or entities

**Acceptance criteria**

* Under retention trimming, memory usage decreases and entities are actually removed.
* Deletion does not leave stale index references.

---

### 8.3 Ingestion

**FR-7** Kafka ingestion (MVP):

* Consume topics and parse events
* Dispatch to `on_ingest` script (optional)
* Apply resulting store ops

**FR-8** Encoding support:

* JSON (MVP)
* Protobuf (v1)
* Cap’n Proto (v1/v2 depending on effort)

**FR-9** Ingestion correctness:

* At-least-once processing
* Provide idempotency hooks (e.g., event id or monotonic offsets) for user logic

**Acceptance criteria**

* Kafka consumers use a stable group id per deployment, not per thread.
* Concurrency limiter actually bounds in-flight ingestion work.

---

### 8.4 Query and Edge Endpoints

**FR-10** HTTP edge endpoint router:

* Endpoints defined in `app.yaml` (method, path, handler)
* Handler is a JS function call (not eval per request)
* Request/response as JSON (MVP), optional binary later

**FR-11** Optional gRPC admin/query API:

* Keep existing gRPC for internal tooling and admin operations
* Separate “edge endpoints” from “admin endpoints”

**FR-12** Script contracts:

* `on_ingest(eventType, data, context)`
* `on_query(params, context)` (legacy compatibility)
* `route.handle(request)` returns `{ status, headers, body }`

**Acceptance criteria**

* Endpoint handler execution time and memory are bounded by configured limits.
* Typical handler hot path does not repeatedly parse/stringify unnecessarily.

---

### 8.5 Runtime Limits and Isolation

**FR-13** Enforce per-instance limits:

* max concurrent requests
* JS per-call CPU budget (timebox via deno_core op deadlines)
* V8 heap limit per isolate (`--max-old-space-size` equivalent via deno_core)
* store memory budget (soft + hard limits)
* per-request max payload/response sizes
* rate limits (token bucket)

**Acceptance criteria**

* Runaway scripts are interrupted and return 429/503 or configured error.
* Memory budget enforcement triggers shedding/eviction or fails closed based on policy.

---

### 8.6 Observability and Operations

**FR-14** Metrics (Prometheus):

* QPS (by endpoint)
* latency histograms (p50/p90/p99)
* JS execution time + errors
* store sizes (entities per type, index sizes)
* retention eviction count
* ingestion lag + throughput

**FR-15** Structured logs + tracing:

* request id propagation
* script error stack traces (sanitized)
* ingestion event errors with topic/partition/offset

**FR-16** Health endpoints:

* `/healthz` liveness
* `/readyz` readiness (Kafka subscribed, scripts loaded, store ready)

---

### 8.7 Deployment

**FR-17** VM deployment:

* Single static binary
* App bundle directory mounted locally
* Systemd example unit and env config

**FR-18** Kubernetes deployment:

* Helm chart (Deployment + Service + HPA optional)
* ConfigMaps for yaml/scripts
* Secrets for Kafka credentials
* Resource requests/limits mapped to plan

---

## 9) Non-Functional Requirements (Performance, Reliability, Security)

### 9.1 Performance Targets (initial)

These are directional and should be validated via benchmarks.

* **Edge HTTP**:

  * p50 latency: **< 2ms** (in-region, warm)
  * p99 latency: **< 10ms** (in-region, warm)
* **Throughput**:

  * Sustained: **100k+ RPS per instance** for simple handlers (plan dependent)
* **Ingestion**:

  * Able to keep up with configured Kafka partitions under nominal load
* **Resource overhead**:

  * Minimal per-request allocations; stable memory usage with retention enabled

### 9.2 Reliability

* Graceful degradation under overload:

  * request shedding with 429/503
  * bounded queues
* Safe restarts:

  * on restart, state rebuild via Kafka catch-up (document that state is derived)

### 9.3 Security

* Admin endpoints require authentication (mTLS or token)
* Script sandbox boundaries:

  * no filesystem by default
  * no network by default (unless explicitly enabled)
* Input validation:

  * request size limits
  * schema validation optional (fast path vs strict mode)

---

## 10) Pricing and Packaging

### Principle

Static pricing like “buying VMs,” with transparent limits.

### Plan examples (illustrative)

* **S**: 2 vCPU / 4GB RAM / 100k RPS target / 4 JS runtimes
* **M**: 4 vCPU / 8GB RAM / 200k RPS target / 8 JS runtimes
* **L**: 8 vCPU / 16GB RAM / 400k+ RPS target / 16 JS runtimes

**Included**:

* Fixed memory budget
* Fixed concurrency + rate caps
* Fixed retention capacity knobs

**Add-ons (later)**:

* Multi-region replication tooling
* Managed control plane (if you choose to operate it)

---

## 11) MVP Definition

### MVP must include

* App bundle format (`app.yaml` + `schema.yaml` + scripts)
* In-memory store with hash/sorted indexes
* Retention that truly evicts entities
* Kafka ingestion (JSON)
* HTTP edge endpoints with per-route JS handler
* Prometheus metrics + health endpoints
* VM deployment documentation

### MVP explicitly excludes

* Protobuf/Cap’n Proto ingestion (unless time permits)
* Multi-region orchestration automation (document patterns instead)
* Multi-tenant hosting

---

## 12) Milestones (Suggested)

### Phase 0: Hardening the current prototype (Engineering)

* Fix ingestion group id semantics
* Fix concurrency limiter correctness
* Fix retention eviction correctness (remove from primary + all indexes)
* Replace JS isolate pool mutex round-robin with atomic dispatch
* Reduce JSON stringify/parse overhead on hot path
* Add HTTP router with dynamic endpoints

### Phase 1: MVP “Generic Edge Runtime”

* `app.yaml` implemented + endpoint handlers
* Plan/limits enforced
* Packaging + VM deploy
* Basic docs + 2 example apps (ads counter + feed)

### Phase 2: Performance + Formats

* Protobuf ingestion
* Cap’n Proto ingestion
* Benchmark suite + tuned defaults

### Phase 3: Kubernetes-first + Multi-region patterns

* Helm chart hardened
* Regional deployment playbooks
* Optional operator for safe rollouts

---

## 13) Success Metrics

### Product metrics

* Time-to-first-endpoint: < 30 minutes from clone to running example
* Upgrade/rollback success rate > 99%

### Performance metrics

* p99 latency under load meets plan targets
* Stable memory profile under retention (no growth over time)
* Ingestion lag bounded (within acceptable SLA)

### Adoption metrics

* Number of apps deployed
* Retention/index usage diversity
* % of workloads using HTTP endpoints vs gRPC admin

---

## 14) Risks and Mitigations

### Risk: JS sandbox runaway (CPU / memory)

**Mitigation:** strict CPU timeboxing via deno_core op cancellation and V8 heap limits; per-isolate memory cap; terminate and respawn isolate on violation.

### Risk: Retention/index correctness causes memory leaks or stale reads

**Mitigation:** single “source-of-truth” TTL index per entity; background sweeper with invariant tests; fuzz tests.

### Risk: Kafka consumption duplicates / wrong semantics

**Mitigation:** stable consumer group id; idempotency patterns; observability on duplicates.

### Risk: Hot-path serialization overhead

**Mitigation:** structured bindings (avoid stringify/parse), binary ingestion formats, optional binary response.

### Risk: Multi-region consistency expectations

**Mitigation:** document consistency model clearly; provide recommended patterns; later add conflict handling features.

---

## 15) Open Questions (to finalize in later iteration)

(These don’t block drafting, but will need decisions.)

1. **Durability:** Is state always derived from Kafka (replay), or do we optionally persist snapshots?
2. **Multi-region:** Do we support active-active with shared topics, or region-scoped topics by default?
3. **Isolation model:** Single-tenant per instance only, or multi-app per instance with strong limits?
4. **Scripting:** Do we allow network egress in scripts? If yes, how do we limit it?
5. **Schema enforcement:** Strict field typing at ingestion/query boundaries vs best-effort.
6. **Control plane:** Do we build a CLI + manifests only, or a hosted control plane later?

---

## 16) Appendix: Example `app.yaml` (Draft)

```yaml
app:
  name: stateful-edge
  version: 0.1.0

limits:
  max_concurrent_requests: 2000
  rate_limits:
    query_rps: 100000
    ingest_rps: 200000

  js:
    runtime: deno_core        # V8-backed via deno_core
    pool_size: 8
    memory_limit_bytes: 134217728
    max_cpu_ms_per_request: 2

  store:
    max_bytes: 12000000000
    max_entity_bytes: 16384
    tombstone_ttl_seconds: 3600

formats:
  kafka:
    encoding: json   # json | protobuf | capnp
  http:
    request: json
    response: json

ingestion:
  - type: kafka
    topics:
      - name: post-events
        entity: post

endpoints:
  - name: feed
    method: GET
    path: /v1/feed
    handler: scripts/routes/feed.js

  - name: get_post
    method: GET
    path: /v1/post/:id
    handler: scripts/routes/get_post.js

admin:
  enable_grpc: true
  enable_execute: true
```

---

If you want, I can also produce (as part of the PRD package) a **one-page “Positioning + Messaging” section** and a **benchmark plan** (workload models + target hardware + metrics) so the “low latency high QPS” claim is measurable from day one.

---

## 17) References

* [Roll your own JavaScript runtime - Deno Blog](https://deno.com/blog/roll-your-own-javascript-runtime): walkthrough for building a custom JS runtime using `deno_core` + `tokio`; covers `JsRuntime`, V8 isolate lifecycle, op registration (`#[op2]`), extensions, and ESM module loading.
