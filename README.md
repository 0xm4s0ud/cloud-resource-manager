# Cloud Management Plane (Capacity Dedication Service)

## Index
1. [Summary](#summary)
2. [Project Overview](#project-overview)
3. [Project and Business Requirements](#project-and-business-requirements)
4. [Domain Models](#domain-models)
5. [Use Cases](#use-cases)
6. [Software Architecture](#software-architecture)
7. [User Stories](#user-stories)
8. [Prerequisites and Install](#prerequisites-and-install)
9. [Tests](#tests)
10. [APIs and Endpoints](#apis-and-endpoints)
11. [Known Issues and Limitations](#known-issues-and-limitations)

## Summary

This repository implements a capacity feasibility service for cloud environments. It provides:

- Soft reservations with a TTL (holds)
- Hard dedication on confirmation
- Concurrency-safe, all-or-nothing multi-resource reservations
- Idempotent request handling
- Background expiry via a sweeper process
- Request lifecycle auditability and operational observability

The service is a demo implementation focused on business logic, concurrency correctness, HA patterns, and explainability (not real VM provisioning, tenant auth, or billing).

## Project Overview

### What it does

Clients can:

- Query capacity by resource pool (and optionally customer-specific remaining quota)
- Create a reservation request for capacity, either by `product_id`/`quantity` or by raw resource `lines`
- List and inspect request state, including line-level breakdown and event history
- Confirm a reservation to convert holds into dedications
- Cancel or extend a reservation before it expires

Reservations automatically expire via a sweeper job, releasing the reserved capacity and updating state.

### Core concepts

Availability and admission are enforced by atomic conditional updates to counters stored in Postgres:

- `resource_pools.reserved_amount` / `resource_pools.dedicated_amount`
- `customer_quotas.used_amount`

This repository intentionally models correctness using aggregate counters (shelf-wide per resource kind) rather than per-host placement, because it is a feasibility/control-plane logic demo.

## Project and Business Requirements

### Business / product requirements

1. Provide an API for capacity feasibility so customers can safely “reserve” before actual provisioning.
2. Prevent over-allocation under concurrent requests.
3. Allow clients to retry safely after network failures.
4. Ensure reservations do not remain pinned forever (TTL + expiry).
5. Provide explainability: every state transition must be auditable and attributable.
6. Handle multi-resource requests atomically: either all required resources are reserved or the request is rejected.

### Engineering requirements

1. **Correctness authority is Postgres**: reservation decisions are made by conditional `UPDATE` statements that either succeed for the requested amount or fail.
2. **Idempotency**: `POST /v1/requests` requires `Idempotency-Key` and is enforced by a unique constraint on `(customer_id, idempotency_key)`. The implementation additionally stores a request fingerprint (`body_hash`) to detect key reuse with different bodies.
3. **Bounded lifecycle**: reservations are terminal-only (`RESERVED`, `REJECTED`, `COMPLETED`, `CANCELLED`, `EXPIRED`), with TTL expiry and a capped extension policy.
4. **Safe concurrency**:
   - Deterministic lock/update ordering across multiple pools (prevents deadlocks)
   - Conditional state transition updates (prevents races between confirm/cancel and expiry)
5. **Operational readiness**:
   - `GET /v1/ready` checks Postgres; Redis is treated as non-critical
   - `GET /metrics` exposes Prometheus metrics
6. **Scalability via separation of concerns**:
   - API server is stateless (horizontal scale behind a load balancer)
   - Sweeper is a separate deployable (scale-out possible with advisory lock)

## Domain Models

The service’s persistent domain model is defined in `internal/domain/types.go` and documented in `docs/DATA_MODEL.md`.

### Enums

#### `RequestStatus`

- `RESERVED`: soft-reserved and awaiting confirm/cancel/extend until TTL expiry
- `REJECTED`: reservation failed during feasibility (insufficient capacity/quota/invalid input)
- `COMPLETED`: confirmed and dedicated
- `CANCELLED`: client cancelled and resources released
- `EXPIRED`: TTL expired (sweeper released resources)

#### `HoldStatus`

- `ACTIVE`: hold counts against `reserved_amount`
- `RELEASED`: hold released on cancel
- `DEDICATED`: converted during confirm
- `EXPIRED`: released by sweeper expiry

#### `EventType`

- `created`, `reserved`, `rejected`, `confirmed`, `cancelled`, `expired`, `extended`

#### `Reason`

- `INSUFFICIENT_CAPACITY`, `QUOTA_EXCEEDED`
- `INVALID_RESOURCE_KIND`, `PRODUCT_NOT_FOUND`
- `MISSING_IDEMPOTENCY_KEY`, `IDEMPOTENCY_KEY_REUSED`
- `INVALID_INPUT`
- `TOO_MANY_ACTIVE_HOLDS`
- `EXTENSION_LIMIT`, `EXPIRED`
- `NOT_FOUND`, `CONFLICT`, `UNAVAILABLE`, `NOT_IMPLEMENTED`

### Entities

#### 1. `ResourcePool`

Represents an inventory bucket per resource kind (aggregate capacity).

Key fields:

- `kind`: resource identifier (example: `cpu-cores`, `memory-mb`, `disk-gb`, `gpu-units`, `public-ip`, `managed-service-slots`)
- `unit`: unit label for display
- `total`: shelf size
- `oversubscribe_factor`: multiplier used to compute effective capacity
- `dedicated_amount`: hard committed counter
- `reserved_amount`: soft reservation counter
- `policy`: JSON with `splittable`, `min_grain`, `max_grain`

Methods:

- `Available()`: display helper for `floor(total * oversubscribe_factor) - dedicated_amount - reserved_amount` (never used as the sole admission authority; reserve uses conditional UPDATE checks).

#### 2. `Product`

SKU / product catalog entry that expands into one or more resource lines.

Key fields:

- `code`, `name`
- `resource_template`: map from `resource_kind` to an `int64` amount (per unit quantity)
- Requests may reference `product_id` + `quantity` and the service expands to `RequestLine` records.

#### 3. `CustomerQuota`

Per-customer contract cap for each resource kind.

Key fields:

- `customer_id`
- `resource_kind`
- `limit`: total allowed by contract
- `used_amount`: quota consumption counter

Methods:

- `Remaining()`: `max(0, limit - used_amount)`

#### 4. `CapacityRequest`

The aggregate root for a single checkout/reservation flow.

Key fields:

- `id`
- `customer_id`
- `idempotency_key`
- `body_hash`: SHA-256 fingerprint of the normalized create request (product or lines)
- `product_id` (nullable when using raw lines)
- `quantity`
- `status`
- `reject_reason` (nullable, for rejected requests)
- `expires_at` (nullable; set when reserved)
- `extension_count` (cap enforced)
- `correlation_id`: client trace/request identifier
- `created_at`, `updated_at`

#### 5. `RequestLine`

Resolved resource amounts per pool for a request.

Key fields:

- `request_id`, `pool_id`
- `kind` (resource kind)
- `amount` (int64, validated for bounds)

#### 6. `Hold`

Soft reservation row(s) tied to a request (one per resource pool involved).

Key fields:

- `request_id`, `pool_id`
- `amount`
- `status` (`ACTIVE`, `RELEASED`, `DEDICATED`, `EXPIRED`)
- timestamps

Holds are the mechanism that “pins” reserved capacity until cancellation or expiry.

#### 7. `Dedication`

Hard allocation record created on confirm.

Key fields:

- `request_id`, `pool_id`
- `hold_id`: which hold was converted
- `amount`

In this demo, confirming only persists dedications; there is no OpenStack / hypervisor integration.

#### 8. `RequestEvent`

Append-only audit trail of request lifecycle transitions.

Key fields:

- `request_id`
- `event_type`
- `reason` (human-readable reason; can include structured details)
- `at` (timestamp)

## Use Cases

1. **Customer reservation (product-based)**: client selects a product (e.g., a VM flavor) and quantity, receives a request that is reserved or rejected.
2. **Customer reservation (raw lines)**: internal system provides explicit resource lines to reserve.
3. **Capacity discovery**: client polls `GET /v1/capacity` to show available capacity and optionally customer-specific remaining quota.
4. **Confirm capacity**: client converts a reserved request into dedications.
5. **Cancel**: release reserved capacity early.
6. **Extend TTL**: push out expiry, limited by max extensions and wall time.
7. **Recovery / support tooling**: operator traces why a request was reserved/rejected via `request_events` and consistent reason codes.

## Software Architecture

### High-level components

The code is wired using `uber-go/fx`:

- `cmd/server`: HTTP API server
- `cmd/sweeper`: background sweeper loop for expiry
- `internal/httpapi`: request handlers (chi router)
- `internal/service`: `CapacityService` implements domain logic + transaction boundaries
- `internal/repository/postgres`: Postgres adapter implementing atomic reserve/quota logic
- `internal/repository/redisx`: Redis adapters for caching and idempotency fast path (degrades gracefully)
- `internal/observability`: request logging and metrics
- `migrations/`: schema migrations applied at startup

### Request lifecycle (state machine)

1. Create request (idempotent)
2. Either:
   - `RESERVED` (holds inserted, pool/quota counters updated atomically), with `expires_at`
   - or `REJECTED` (no partial holds left behind), with structured reason code
3. While `RESERVED`:
   - `confirm` -> `COMPLETED` (reserved -> dedicated, holds become dedicated)
   - `cancel` -> `CANCELLED` (releases reserved counters)
   - `extend` -> extends `expires_at` (bounded by max extensions and max wall time)
4. After TTL expiry:
   - sweeper transitions request/holds to terminal states and releases capacity

```mermaid
stateDiagram-v2
  [*] --> RESERVED: Create + feasibility OK
  [*] --> REJECTED: Create + feasibility fails

  RESERVED --> COMPLETED: confirm
  RESERVED --> CANCELLED: cancel
  RESERVED --> RESERVED: extend (capped)
  RESERVED --> EXPIRED: TTL expiry / sweeper

  COMPLETED --> [*]
  CANCELLED --> [*]
  EXPIRED --> [*]
  REJECTED --> [*]
```

### Concurrency and correctness mechanics

The reserve path uses a single Postgres transaction per request. For each resource line, it performs a conditional update:

- Pool reservation: only increments `reserved_amount` when enough effective capacity remains.
- Quota consumption: only increments `customer_quotas.used_amount` when quota limit would not be exceeded.

If any line fails, the entire transaction rolls back, and the service persists a `REJECTED` request in a separate best-effort transaction with an idempotent retry behavior.

### Redis roles

Redis is used for performance, not correctness:

- `Idempotency-Key` fast path (optional): reduces repeated DB lookups
- Short TTL cache for `GET /v1/capacity` only

If Redis is unavailable, the service still functions via Postgres.

## User Stories

1. As a customer operator, I want to request capacity reservation using a product flavor so that I can request the same resource configuration repeatedly and idempotently.
2. As an integration service, I want to retry `POST /v1/requests` safely after timeouts without accidentally reserving twice.
3. As a capacity analyst, I want to see pool availability and my customer’s remaining quota so that I can understand why requests are rejected.
4. As a provisioning orchestrator, I want to confirm a reservation quickly so that the system converts soft holds into a hard allocation record.
5. As a customer, if I no longer need capacity, I want to cancel so resources are released immediately and other requests can succeed.
6. As a scheduler, I need to extend TTL up to a policy cap so short-term planning delays do not break reservation flows.
7. As a support engineer, I want to fetch request details and event history so I can explain the outcome using consistent reason codes.
8. As an operator, I need the sweeper to reliably release expired holds even during failures, with metrics and audit logs proving what happened.

```mermaid
flowchart LR
  U1["Customer Operator\n(Story 1)"] -->|Reserve (POST /v1/requests)| URS["Capacity Requests"]
  U2["Integration Service\n(Story 2)"] -->|Idempotent retry| URS
  U3["Capacity Analyst\n(Story 3)"] -->|View capacity (GET /v1/capacity)| CAP["Capacity View"]
  U4["Provisioning Orchestrator\n(Story 4)"] -->|Confirm (POST /v1/requests/{id}/confirm)| CONF["Confirm + Dedicate"]
  U5["Customer\n(Story 5)"] -->|Cancel (POST /v1/requests/{id}/cancel)| CANCEL["Cancel + Release"]
  U6["Scheduler\n(Story 6)"] -->|Extend (POST /v1/requests/{id}/extend)| EXT["Extend TTL (capped)"]
  U7["Support Engineer\n(Story 7)"] -->|Inspect request (GET /v1/requests/{id})| INSPECT["Request details + events"]
  U8["Operator\n(Story 8)"] -->|Expiry reconciliation| SWEEP["Sweeper + Metrics + Audit"]
```

## Prerequisites and Install

### Local / demo prerequisites

- `Go` (module expects Go 1.25 according to project docs)
- `Docker` and `docker compose` (optional, but used for the default demo)
- Access to a Postgres database (for production-like local runs)
- Access to a Redis database (optional; service degrades gracefully if Redis is down)

### Option A: Run with Docker Compose (recommended)

```bash
cd deploy
docker compose up -d --build
```

Then:

- API: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (default `admin` / `admin`)

### Option B: Run API locally (Postgres + Redis via Compose, then `go run`)

```bash
cd deploy && docker compose up -d postgres redis
cd ..
export POSTGRES_DSN='postgres://capacity:capacity@localhost:5432/capacity?sslmode=disable'
export REDIS_ADDR=localhost:6379
go run ./cmd/server
```

Optional for sweeper:

```bash
go run ./cmd/sweeper
```

### Configuration

Environment variables used by `internal/config/config.go`:

- `HTTP_ADDR` (default `:8080`)
- `METRICS_ADDR` (optional)
- `POSTGRES_DSN` (default points to local compose DSN)
- `REDIS_ADDR` (default `localhost:6379`)
- `RESERVE_TTL` (default `15m`)
- `MAX_EXTENSIONS` (default `3`)
- `MAX_RESERVE_WALL_TIME` (default `60m`)
- `CAPACITY_CACHE_TTL` (default `2s`)
- `HTTP_READ_TIMEOUT` / `HTTP_WRITE_TIMEOUT` (defaults `10s`)
- `SWEEPER_INTERVAL` (default `30s`)
- `RETRY_AFTER_SECONDS` (default `5`)
- `MIGRATIONS_PATH` (default `migrations`)
- `MAX_ACTIVE_HOLDS_PER_CUSTOMER` (default `10`)

## Tests

Recommended commands:

```bash
go test ./internal/domain ./internal/httpapi -count=1
go test ./internal/service -count=1 -race   # integration + concurrency (Docker required)
go test ./internal/service -short          # skip containers
```

Notes:

- Unit tests focus on request lifecycle transitions, idempotency logic, and atomic reserve/quota correctness.
- Integration tests use `testcontainers-go` and are skipped by `-short`.

## APIs and Endpoints

Base path: `/v1`

All JSON responses use `Content-Type: application/json`.
All mutating responses include the idempotency/request trace header behavior as implemented by observability middleware (and `X-Request-Id` propagation/generation).

### Health / readiness / metrics

- `GET /v1/health`
  - `200` `{ "status": "ok" }`
- `GET /v1/ready`
  - `200` when Postgres is reachable
  - `503` when Postgres is not ready (Redis is treated as non-critical)
- `GET /metrics`
  - Prometheus scrape endpoint

### Capacity and products

#### `GET /v1/products`

Response:

```json
{ "products": [ /* product objects */ ] }
```

#### `GET /v1/capacity?customer_id=...`

Query param `customer_id` is optional; if missing, the handler also checks header `X-Customer-Id`.

Response:

```json
{ "capacity": [ /* pool views */ ] }
```

### Requests (reservation lifecycle)

#### Create: `POST /v1/requests`

Required header:

- `Idempotency-Key`: client-generated idempotency key (string; typically a UUID)

Optional header:

- `X-Customer-Id`: if `customer_id` is not provided/derived from body

Request body (product-based):

```json
{
  "customer_id": "cust-42",
  "product_id": "vm-medium",
  "quantity": 2
}
```

Request body (raw lines):

```json
{
  "customer_id": "cust-42",
  "lines": [
    { "resource_kind": "cpu-cores", "amount": 4 },
    { "resource_kind": "memory-mb", "amount": 8192 }
  ]
}
```

Response:

- `201` with the `CapacityRequest` when reserved
- `409` with the rejected request when infeasible
- Error responses map domain “Reason” codes to HTTP status codes

Example (create):

```bash
curl -s http://localhost:8080/v1/products

curl -s -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust-demo","product_id":"vm-small","quantity":1}' \
  http://localhost:8080/v1/requests
```

#### List: `GET /v1/requests?customer_id=...&status=RESERVED`

If `customer_id` query param is empty, the handler checks header `X-Customer-Id`.

Response:

```json
{ "requests": [ /* capacity requests */ ] }
```

#### Get details: `GET /v1/requests/{id}`

Response shape:

```json
{
  "request": { /* CapacityRequest */ },
  "lines": [ /* RequestLine */ ],
  "events": [ /* RequestEvent */ ]
}
```

#### Confirm: `POST /v1/requests/{id}/confirm`

Converts all active holds into dedications and transitions the request to `COMPLETED`.

Response: the updated `CapacityRequest`.

Example:

```bash
curl -s -X POST http://localhost:8080/v1/requests/<request-id>/confirm
```

#### Cancel: `POST /v1/requests/{id}/cancel`

Releases holds and transitions the request to `CANCELLED`.

Example:

```bash
curl -s -X POST http://localhost:8080/v1/requests/<request-id>/cancel
```

#### Extend: `POST /v1/requests/{id}/extend`

Extends `expires_at` by one TTL window, bounded by:

- max extensions (`MAX_EXTENSIONS`, default `3`)
- max wall time since first reserve (`MAX_RESERVE_WALL_TIME`, default `60m`)

Example:

```bash
curl -s -X POST http://localhost:8080/v1/requests/<request-id>/extend
```

### Error handling model (reason codes)

Error response shape:

```json
{
  "error": {
    "reason": "INSUFFICIENT_CAPACITY",
    "message": "pool cpu-cores has insufficient available capacity",
    "details": { "pool": "cpu-cores", "requested": 8, "available": 3 }
  }
}
```

HTTP status mapping is handled in `internal/httpapi/handler.go` by translating domain “Reason” codes.

## Known Issues and Limitations

This repository documents known issues in `docs/KNOWN_ISSUES.md`. Highlights:

- **Aggregate pools vs real placement fragmentation**: the service may reserve capacity that cannot actually be placed in a real system with host-level constraints.
- **Dual-writer on confirm**: in production, confirm would need an atomic coordination between Postgres and OpenStack provisioning (not implemented in this demo).
- **Counter drift risk**: denormalized counters are not automatically reconciled against aggregates; a reconciler would be added in production.
- **Idempotency body mismatch**: mitigated here via `body_hash`, but production should also prune old idempotency rows.
- **Hold-based economic DoS**: concurrent reserved requests can starve capacity; mitigated by a per-customer active holds cap and should be complemented by rate limiting.
- **Clock skew on expiry**: sweep/confirm expiry comparisons should rely on DB time for production-grade correctness.

See `docs/KNOWN_ISSUES.md` for the complete list and recommended mitigation directions.

## What I developed with help of AI

This project was built with assistance across the following areas:

- Test cases (unit tests and concurrency-focused suites)
- Adapters and infrastructure wiring (repository adapters, service wiring, and supporting components)
- Generating documents and organizing documentation content into a cohesive structure
- Generating Prometheus/Grafana assets, specifically:
  - Prometheus/Grafana metric dashboard JSON (dashboards), not the metrics themselves
- The `deploy` section related to Docker Compose (container/service configuration and run instructions)

