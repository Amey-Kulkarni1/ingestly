import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import aio_pika
from prometheus_client import Counter, Histogram, start_http_server
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_maker
from app.logging_config import configure_logging, correlation_id_ctx
from app.models import FailedEvent, ProcessedEvent, RawEvent

logger = logging.getLogger("worker")


class PermanentEventError(Exception):
    """Fatal error that must not be retried (e.g. malformed schema, unparseable payload)."""
    pass

EVENTS_PROCESSED_COUNTER = Counter(
    "worker_events_processed_total",
    "Total events processed successfully by worker",
    ["event_type"]
)
EVENTS_RETRIED_COUNTER = Counter(
    "worker_events_retried_total",
    "Total events retried due to transient failures",
    ["event_type"]
)
EVENTS_DLQ_COUNTER = Counter(
    "worker_events_dlq_total",
    "Total events routed to dead-letter queue",
    ["event_type"]
)
PROCESSING_LATENCY = Histogram(
    "worker_processing_duration_seconds",
    "Duration of event processing in seconds"
)


def get_retry_tiers() -> List[Tuple[str, int]]:
    """Parse configured retry delays into (queue_name, delay_seconds) tiers."""
    delays = [
        max(1, int(float(x.strip())))
        for x in settings.RETRY_DELAYS_SECONDS.split(",")
        if x.strip()
    ]
    if not delays:
        delays = [5, 30, 300]
    tiers: List[Tuple[str, int]] = []
    for d in delays:
        label = f"{d // 60}m" if d >= 60 and d % 60 == 0 else f"{d}s"
        tiers.append((f"{settings.EVENTS_RETRY}.{label}", d))
    return tiers


def select_retry_queue(attempt: int) -> str:
    """Pick the retry tier queue for the given 0-based attempt."""
    tiers = get_retry_tiers()
    idx = min(max(attempt, 0), len(tiers) - 1)
    return tiers[idx][0]


def normalize_event(event_type: str, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Transform and normalize incoming payload into a standard schema."""
    return {
        "event_type": event_type,
        "normalized_at": datetime.now(timezone.utc).isoformat(),
        "attributes": raw_payload,
        "processed_version": "v1.0"
    }


async def handle_message_processing(data: Dict[str, Any], session: AsyncSession) -> None:
    raw_event_id = data.get("raw_event_id")
    delivery_id = data.get("delivery_id")
    event_type = data.get("event_type")
    payload = data.get("payload")

    if not raw_event_id or not delivery_id or not event_type:
        raise PermanentEventError("Missing mandatory envelope fields (raw_event_id, delivery_id, event_type)")

    if payload is not None and not isinstance(payload, dict):
        raise PermanentEventError(f"Payload must be a dictionary, got {type(payload).__name__}")

    payload = payload or {}

    # Simulation hook: if event_type is 'error.simulate', raise exception to test retries and DLQ
    if event_type == "error.simulate":
        raise ValueError("Simulated transient processing error for fault-tolerance demonstration")

    # Worker-level Idempotency Check (pre-check for redeliveries after crash between commit and ack)
    existing_processed = await session.execute(
        select(ProcessedEvent).where(ProcessedEvent.delivery_id == delivery_id)
    )
    if existing_processed.scalar_one_or_none():
        raw_event_res = await session.execute(
            select(RawEvent).where(RawEvent.id == raw_event_id)
        )
        raw_event = raw_event_res.scalar_one_or_none()
        if raw_event and raw_event.status != "processed":
            raw_event.status = "processed"
            await session.commit()
        logger.info("Worker: event %s already processed. Skipping.", delivery_id)
        return

    # Normalize data
    normalized = normalize_event(event_type, payload)

    # Insert into processed_events (backed by DB unique constraint)
    processed_record = ProcessedEvent(
        raw_event_id=raw_event_id,
        delivery_id=delivery_id,
        event_type=event_type,
        normalized_data=normalized,
    )
    session.add(processed_record)

    try:
        raw_event_res = await session.execute(
            select(RawEvent).where(RawEvent.id == raw_event_id)
        )
        raw_event = raw_event_res.scalar_one_or_none()
        if raw_event:
            raw_event.status = "processed"

        await session.commit()
    except IntegrityError:
        await session.rollback()
        logger.info("Worker: event %s duplicate caught by DB constraint. Skipping.", delivery_id)
        return

    logger.info("Successfully processed event id=%d, delivery_id=%s", raw_event_id, delivery_id)


async def record_failure(
    raw_event_id: int,
    delivery_id: str,
    error_msg: str,
    retry_count: int,
    session: AsyncSession
) -> None:
    failure = FailedEvent(
        raw_event_id=raw_event_id,
        delivery_id=delivery_id,
        error_message=error_msg,
        retry_count=retry_count
    )
    session.add(failure)

    raw_event_res = await session.execute(
        select(RawEvent).where(RawEvent.id == raw_event_id)
    )
    raw_event = raw_event_res.scalar_one_or_none()
    if raw_event:
        raw_event.status = "failed"

    await session.commit()


async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel
) -> None:
    try:
        body = json.loads(message.body.decode("utf-8"))
    except Exception as parse_err:
        logger.error("Permanent error - unparseable JSON: %s", parse_err)
        await message.reject(requeue=False)
        return

    raw_event_id = body.get("raw_event_id")
    delivery_id = body.get("delivery_id")
    event_type = body.get("event_type", "unknown")
    attempt = body.get("attempt", 0)
    correlation_id = body.get("correlation_id", "-")
    cid_token = correlation_id_ctx.set(correlation_id)

    try:
        with PROCESSING_LATENCY.time():
            async with async_session_maker() as session:
                try:
                    await handle_message_processing(body, session)
                except PermanentEventError as perm_err:
                    EVENTS_DLQ_COUNTER.labels(event_type=event_type).inc()
                    logger.error("Permanent error on event %s. Routing directly to DLQ: %s", delivery_id, perm_err)
                    if raw_event_id and delivery_id:
                        await record_failure(
                            raw_event_id=raw_event_id,
                            delivery_id=delivery_id,
                            error_msg=f"Permanent: {perm_err}",
                            retry_count=attempt,
                            session=session
                        )
                    await message.reject(requeue=False)
                    return
                except Exception as exc:
                    logger.warning(
                        "Transient error processing event id=%s (attempt %d/%d): %s",
                        raw_event_id, attempt + 1, settings.MAX_RETRIES, exc
                    )

                    if attempt + 1 < settings.MAX_RETRIES:
                        EVENTS_RETRIED_COUNTER.labels(event_type=event_type).inc()
                        body["attempt"] = attempt + 1
                        retry_msg = aio_pika.Message(
                            body=json.dumps(body).encode("utf-8"),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            content_type="application/json",
                            headers={"x-attempt": attempt + 1}
                        )
                        # Non-blocking tiered TTL retry queue: avoids head-of-line blocking
                        retry_queue = select_retry_queue(attempt)
                        await channel.default_exchange.publish(
                            retry_msg,
                            routing_key=retry_queue
                        )
                        await message.ack()
                    else:
                        EVENTS_DLQ_COUNTER.labels(event_type=event_type).inc()
                        logger.error("Event %s exceeded max retries. Routing to DLQ.", delivery_id)
                        if raw_event_id and delivery_id:
                            await record_failure(
                                raw_event_id=raw_event_id,
                                delivery_id=delivery_id,
                                error_msg=str(exc),
                                retry_count=attempt + 1,
                                session=session
                            )
                        await message.reject(requeue=False)
                    return

            EVENTS_PROCESSED_COUNTER.labels(event_type=event_type).inc()
            # Manual ACK only after DB commit succeeds; if ack fails, broker redelivers idempotently
            await message.ack()
            logger.info("Completed event %s", delivery_id)
    finally:
        correlation_id_ctx.reset(cid_token)


async def run_worker() -> None:
    # Start metrics server on port 8001
    try:
        start_http_server(8001)
        logger.info("Worker Prometheus metrics server listening on port 8001")
    except Exception as exc:
        logger.warning("Could not start worker metrics server: %s", exc)

    logger.info("Connecting worker to RabbitMQ at %s...", settings.RABBITMQ_URL)
    connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=50)

    # Primary inbound queue with DLX
    queue = await channel.declare_queue(
        settings.EVENTS_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": settings.EVENTS_DLQ,
        }
    )

    # Multi-tier non-blocking TTL retry queues: each tier dead-letters back to primary queue
    for tier_queue, delay_sec in get_retry_tiers():
        await channel.declare_queue(
            tier_queue,
            durable=True,
            arguments={
                "x-message-ttl": delay_sec * 1000,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": settings.EVENTS_QUEUE,
            }
        )

    # DLQ queue
    await channel.declare_queue(settings.EVENTS_DLQ, durable=True)

    logger.info("Worker started with concurrency pool (25). Listening on '%s'...", settings.EVENTS_QUEUE)

    semaphore = asyncio.Semaphore(25)
    in_flight: Dict[asyncio.Task, aio_pika.abc.AbstractIncomingMessage] = {}
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError:
            # Windows ProactorEventLoop fallback
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(shutdown_event.set))

    async def worker_task(msg):
        async with semaphore:
            try:
                await process_message(msg, channel)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Unhandled error in worker_task: %s", exc)

    def _on_done(t: asyncio.Task) -> None:
        in_flight.pop(t, None)

    try:
        async with queue.iterator() as queue_iter:
            iter_task = asyncio.create_task(queue_iter.__anext__())
            shutdown_wait = asyncio.create_task(shutdown_event.wait())
            while not shutdown_event.is_set():
                done, _ = await asyncio.wait(
                    {iter_task, shutdown_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if shutdown_wait in done:
                    iter_task.cancel()
                    break
                try:
                    message = iter_task.result()
                except StopAsyncIteration:
                    break
                task = asyncio.create_task(worker_task(message))
                in_flight[task] = message
                task.add_done_callback(_on_done)
                iter_task = asyncio.create_task(queue_iter.__anext__())
            if not shutdown_wait.done():
                shutdown_wait.cancel()
    finally:
        pending_tasks = list(in_flight.keys())
        logger.info("Worker draining %d in-flight tasks...", len(pending_tasks))
        if pending_tasks:
            done_tasks, unfinished = await asyncio.wait(
                pending_tasks,
                timeout=settings.WORKER_SHUTDOWN_TIMEOUT_SECONDS,
            )
            for t in unfinished:
                msg = in_flight.get(t)
                t.cancel()
                if msg is not None and not msg.processed:
                    try:
                        await msg.nack(requeue=True)
                    except Exception:
                        pass
            await asyncio.gather(*unfinished, return_exceptions=True)
        await channel.close()
        await connection.close()
        logger.info("Worker gracefully stopped.")


if __name__ == "__main__":
    configure_logging(service="worker")
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker shutdown gracefully.")

