# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Site Reliability Engineers (SREs), Platform Engineers, and backend on-call operators responsible for event pipeline reliability, queue health, webhook ingestion integrity, and incident triage.

## Product Purpose

Provide a high-throughput, fault-tolerant event ingestion pipeline and real-time operations console. The service guarantees zero message loss via transactional outboxing, enforces two-tier deduplication, isolates malformed and poisoned payloads into a Dead-Letter Queue (DLQ), and enables operators to inspect and replay failed events with one click.

## Positioning

A production-grade ingestion backbone combining sub-millisecond HTTP acknowledgement (`202 Accepted`) with durable PostgreSQL transactional outboxing, non-blocking TTL retry loops, and live operator control over DLQ inspection and replay.

## Operating Context

Used by on-call engineers during live incidents or daily operations. Operators monitor RabbitMQ queue backlog, track ingestion throughput and worker processing rates, inspect failure reasons across heterogeneous webhook sources (GitHub, payment providers, security feeds), and execute manual or bulk replays after downstream recovery.

## Capabilities and Constraints

- Sub-millisecond HTTP ingress with HMAC-SHA256 signature verification (GitHub `X-Hub-Signature-256`, Bearer auth, API keys).
- Two-tier idempotency: atomic deduplication at ingress via `(delivery_id, source)` constraint, and worker-tier deduplication via `processed_events`.
- Transactional outbox pattern with background reconciler ensuring queue delivery consistency.
- Non-blocking RabbitMQ TTL retry queues with exponential backoff and dead-lettering (`events.dlq`) after max attempts.
- Protected event replay API (`POST /api/v1/events/{id}/replay`) guarded by state checks (`WHERE status = 'failed'`).
- Integrated Prometheus telemetry (`/metrics`), health probes (`/health/live`, `/health/ready`), and automated retention cleanup.

## Brand Commitments

- Product Name: Ingestly (Webhook & Event Ingestion Service).
- Voice & Tone: Tactical, precise, high information density, calm and authoritative.

## Evidence on Hand

- Runnable FastAPI backend with SQLAlchemy models, worker consumer, and test suite in `app/`, `worker/`, and `tests/`.
- Working operational dashboard interface in `app/templates/dashboard.html`.
- Production traffic and failure simulators in `scripts/send_fake_events.py` and `scripts/simulate_traffic.py`.
- Sample real-world log payloads in `sample_logs/`.

## Product Principles

1. Zero Data Loss: Any acknowledged event must be durably stored in PostgreSQL before acceptance or guaranteed by the transactional outbox reconciler.
2. Calm Incident Triage: Present critical telemetry (queue backlog, DLQ counts, failure root causes) with zero visual ambiguity.
3. Safe, Idempotent Operations: Replays and retries must never double-process or corrupt downstream consumers.
4. Fast Path Stays Fast: Keep ingress lightweight and sub-millisecond, leaving processing, transformations, and retries to asynchronous workers.

## Accessibility & Inclusion

High-contrast readability across operational dark themes (WCAG AA), accessible status iconography that does not rely exclusively on color, and full keyboard navigation for triage and replay actions.

