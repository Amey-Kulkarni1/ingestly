# Ingestly

![CI Pipeline](https://github.com/Amey-Kulkarni1/ingestly/actions/workflows/ci.yml/badge.svg)

A high-throughput, fault-tolerant webhook & event ingestion service built with **FastAPI**, **RabbitMQ**, **PostgreSQL**, and **Docker**.

The service acknowledges incoming webhooks within milliseconds (`202 Accepted`), persists events atomically with a **Transactional Outbox**, enforces two-tier idempotency, handles transient failures via **multi-tier non-blocking TTL retry queues** (`5s -> 30s -> 5m`), routes poisoned messages to a Dead-Letter Queue (DLQ), and exposes authenticated query, replay, retention, and observability APIs.

---

## Architecture Overview

```text
[ External Producer / GitHub Webhook ]
                   |
                   v
           [ FastAPI Ingress ]  <-- Streaming size cap, Rate Limiter, HMAC-SHA256 + Timestamp check, Correlation ID
             /           \
(Atomic Transaction)    (Immediate Publish)
           v               v
[ Postgres: raw_events ]  [ RabbitMQ: events.inbound ]
[ Postgres: outbox     ]          |
      |                           v
(Outbox Reconciler:       [ Worker Consumer ] (Prefetch: 50, Concurrency: 25, SIGTERM drain)
 SELECT ... FOR UPDATE            |
 SKIP LOCKED)                     +-----------------------+
      |                           | (Success)             | (Transient Error)
      +-------------------------> v                       v
                            [ Postgres ]          [ RabbitMQ: events.retry.{5s,30s,5m} ]
                          (processed_events)              | (Dead-letters back after TTL)
                                                          v
                                                  [ RabbitMQ: events.inbound ]
                                                          | (Max retries / Permanent Error)
                                                          v
                                                  [ RabbitMQ: events.dlq ]
                                                  [ Postgres: failed_events ]
                                                          ^
                                                          |
                                                  [ POST /replay API ] (Atomic via Outbox, X-API-Key)
```

---

## Tech Stack

- **Ingress API:** FastAPI, Uvicorn (async HTTP)
- **Message Broker:** RabbitMQ 3.13 (AMQP 0-9-1 with persistent queues, publisher confirms, and DLX)
- **AMQP Client:** `aio-pika` (async RabbitMQ client)
- **Database & Migrations:** PostgreSQL 16 (async via SQLAlchemy 2.0 & `asyncpg`), Alembic
- **Testing:** Pytest, `pytest-asyncio`, `aiosqlite` (unit), PostgreSQL + RabbitMQ (integration)
- **Observability:** Structured JSON logging (`correlation_id`), Prometheus (`/metrics`, outbox backlog & age gauges), Grafana + alerting rules
- **Tooling & CI:** `ruff`, `mypy`, GitHub Actions, Docker Compose

---

## One-Command Quickstart

```bash
cp .env.example .env   # Edit secrets before running in non-local environments
docker compose up --build
```

### Services & Ports

| Service | Port | Description |
| :--- | :--- | :--- |
| **FastAPI Ingress** | `http://localhost:8000` | Ingest, Query, Replay, Health, Metrics & Admin Console (`/dashboard`) |
| **RabbitMQ Web UI** | `http://localhost:15672` | Management console (credentials from `.env`) |
| **Prometheus** | `http://localhost:9090` | Metrics scraper and query interface |
| **Grafana** | `http://localhost:3000` | Dashboards & outbox backlog alerts (credentials from `.env`) |
| **PostgreSQL** | `localhost:5432` | Storage (credentials from `.env`) |

---

## Design Decisions

1. **Transactional Outbox with `FOR UPDATE SKIP LOCKED`:**
   Ingest and replay both write `OutboxEvent` rows in the same PostgreSQL transaction as `RawEvent` state changes, eliminating dual-write drift if RabbitMQ is unreachable. Multiple API replicas poll `outbox_events` concurrently using `SELECT ... FOR UPDATE SKIP LOCKED` so no two instances double-publish the same row.
2. **Two-Tier Idempotency:**
   - *Ingress Tier:* Atomic unique constraint on `(delivery_id, source)` in `raw_events`. Duplicate deliveries return `200 OK` (`status: "duplicate"`).
   - *Worker Tier:* Unique constraints on `processed_events` (`delivery_id`, `raw_event_id`). The worker commits `processed_events` + `raw_events.status = 'processed'` *before* calling `message.ack()`, so a crash before `ack()` results in an idempotent redelivery rather than lost or duplicated work.
3. **Multi-Tier Non-Blocking TTL Retries:**
   Instead of a single fixed-delay retry queue (which causes head-of-line blocking), transient failures route through tiered dead-letter queues (`events.retry.5s`, `events.retry.30s`, `events.retry.5m`), immediately freeing worker concurrency slots. Permanent errors (`PermanentEventError`, malformed JSON) route directly to `events.dlq`.
4. **Streaming Body Cap & Constant-Time Auth:**
   Ingress checks `Content-Length` and streams `request.stream()` with a running byte cap before buffering or parsing JSON, preventing memory exhaustion. API keys and HMAC-SHA256 signatures are verified with `hmac.compare_digest` against the raw request bytes, with optional `X-Webhook-Timestamp` replay-window enforcement.
5. **Batched Retention & Indexed Keyset Pagination:**
   `run_retention_cleanup` deletes expired rows in bounded batches (`batch_size=1000`) to avoid long table locks. List queries use keyset cursor pagination backed by composite indexes on `(received_at, id)` and `(status, received_at, id)`.

---

## Known Limitations

- **In-Memory Rate Limiter:** The sliding-window rate limiter (`app/security.py`) stores counters per process; in a multi-replica deployment, replace it with a Redis-backed limiter for cluster-wide enforcement.
- **Fixed Retry Tier Topology:** Retry delays (`RETRY_DELAYS_SECONDS="5,30,300"`) map to pre-declared RabbitMQ queues; changing delays requires declaring new queues or purging old ones.
- **Optional Webhook Timestamp Header:** Replay-window validation (`X-Webhook-Timestamp`) is enforced when the header is present; producers like standard GitHub webhooks that omit a timestamp header rely on HMAC + `(delivery_id, source)` deduplication.

---

## Benchmarks & End-to-End Verification

### Test Environment
- **Host:** AMD Ryzen 5 7535HS (6 cores / 12 threads), 14.8 GB RAM, Microsoft Windows 11 Enterprise
- **Runtime:** Docker Desktop 29.9.3 / Docker Compose v5.7.0 (all 6 containers + load generator co-located on the same machine)

### Unloaded vs. Saturated Load Benchmarks

Run via [scripts/send_fake_events.py](scripts/send_fake_events.py):

```bash
# Unloaded (concurrency 5, 15% duplicates, 0% simulated errors)
python scripts/send_fake_events.py --api-key <INGEST_API_KEY> --count 200 --concurrency 5 --dup-rate 0.15 --error-rate 0.0

# Saturated (concurrency 20, 15% duplicates, 10% simulated errors)
python scripts/send_fake_events.py --api-key <INGEST_API_KEY> --count 500 --concurrency 20 --dup-rate 0.15 --error-rate 0.10
```

| Profile | Sent | Accepted (`202`) | Duplicates (`200`) | Throughput | Avg | p50 | p95 | p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Unloaded** (`c=5`) | 200 | 176 | 24 (12.0%) | **81.2 evt/s** | 59.90 ms | **60.66 ms** | **69.07 ms** | **80.49 ms** |
| **Saturated** (`c=20`) | 500 | 438 | 62 (12.4%) | **94.9 evt/s** | 203.84 ms | **170.63 ms** | **430.88 ms** | **625.55 ms** |

### End-to-End Drain & Deduplication Proof

After both runs (`176 + 438 = 614` accepted events) and a 40s drain window for the `5s -> 30s` retry tiers to exhaust on `error.simulate` events, PostgreSQL verified zero lost or stuck events and zero duplicate leaks:

- `SELECT count(*), count(DISTINCT (delivery_id, source)) FROM raw_events;` → **`614` / `614`** (0 duplicate leaks)
- `SELECT status, count(*) FROM raw_events GROUP BY status;` → **`584 processed` + `30 failed` = `614` terminal** (`0` stuck in `received`/`queued`/`processing`)
- `SELECT count(*) FROM outbox_events WHERE published = false;` → **`0`**

### Request-Path Latency Breakdown & Tradeoff

Profiling `save_and_publish_event` inside the `api` container (20-request average against live PostgreSQL + RabbitMQ) shows **11.06 ms** of in-container work per `202`:
- DB commit #1 (`raw_events` + `outbox_events` insert): **5.01 ms**
- RabbitMQ publish (`publisher_confirms=True`): **3.03 ms**
- DB commit #2 (`outbox_events.published = true`): **3.01 ms**

External host-to-container HTTP latency adds Docker Desktop NAT loopback overhead, and at `c=20` Little's Law (`20 / 94.9 evt/s ≈ 211 ms`) reflects queueing in front of the single Uvicorn worker. If lower ingest p50 is prioritized over immediate delivery, skipping the synchronous RabbitMQ publish + second commit on the request path and relying solely on the outbox reconciler removes ~55% of in-container work in exchange for up to a 2s polling delay.

---

## Running Tests & Quality Checks

```bash
# Lint & Type Check
ruff check app worker tests
mypy app worker

# Unit & Logic Tests (in-memory SQLite)
pytest -v

# Full Integration Tests (against live PostgreSQL + RabbitMQ)
INTEGRATION_DATABASE_URL=postgresql+asyncpg://postgres:<pw>@localhost:5432/event_pipeline \
INTEGRATION_RABBITMQ_URL=amqp://guest:<pw>@localhost:5672/ \
pytest -v
```

---

## License

[MIT](LICENSE)
