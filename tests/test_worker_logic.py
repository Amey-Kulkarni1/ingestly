import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ProcessedEvent, RawEvent
from worker.consumer import PermanentEventError, handle_message_processing, normalize_event


def test_normalize_event():
    payload = {"foo": "bar", "count": 42}
    normalized = normalize_event("test.type", payload)
    assert normalized["event_type"] == "test.type"
    assert normalized["attributes"] == payload
    assert "normalized_at" in normalized
    assert normalized["processed_version"] == "v1.0"


@pytest.mark.asyncio
async def test_worker_processing_success(db_session: AsyncSession):
    # 1. Seed raw event
    raw = RawEvent(
        delivery_id="worker-test-1",
        source="test",
        event_type="test.success",
        payload={"key": "val"},
        status="received"
    )
    db_session.add(raw)
    await db_session.commit()
    await db_session.refresh(raw)

    # 2. Worker handles processing
    msg_data = {
        "raw_event_id": raw.id,
        "delivery_id": raw.delivery_id,
        "event_type": raw.event_type,
        "payload": raw.payload,
    }
    await handle_message_processing(msg_data, db_session)

    # 3. Verify processed event exists and status updated
    processed_res = await db_session.execute(
        select(ProcessedEvent).where(ProcessedEvent.delivery_id == "worker-test-1")
    )
    processed = processed_res.scalar_one_or_none()
    assert processed is not None
    assert processed.raw_event_id == raw.id

    raw_refreshed = await db_session.get(RawEvent, raw.id)
    assert raw_refreshed.status == "processed"


@pytest.mark.asyncio
async def test_worker_processing_idempotent(db_session: AsyncSession):
    raw = RawEvent(
        delivery_id="worker-idempotent-1",
        source="test",
        event_type="test.repeat",
        payload={"x": 1},
        status="received"
    )
    db_session.add(raw)
    await db_session.commit()
    await db_session.refresh(raw)

    msg_data = {
        "raw_event_id": raw.id,
        "delivery_id": raw.delivery_id,
        "event_type": raw.event_type,
        "payload": raw.payload,
    }

    # First run
    await handle_message_processing(msg_data, db_session)
    # Second run with same delivery_id
    await handle_message_processing(msg_data, db_session)

    # Ensure only 1 record exists in processed_events
    records = (await db_session.execute(
        select(ProcessedEvent).where(ProcessedEvent.delivery_id == "worker-idempotent-1")
    )).scalars().all()
    assert len(records) == 1


@pytest.mark.asyncio
async def test_worker_simulated_error(db_session: AsyncSession):
    msg_data = {
        "raw_event_id": 999,
        "delivery_id": "error-test-1",
        "event_type": "error.simulate",
        "payload": {},
    }
    with pytest.raises(ValueError, match="Simulated transient processing error"):
        await handle_message_processing(msg_data, db_session)


@pytest.mark.asyncio
async def test_worker_permanent_error_on_malformed_envelope(db_session: AsyncSession):
    # Missing required field raw_event_id
    invalid_msg = {
        "delivery_id": "bad-envelope-01",
        "event_type": "some.event",
    }
    with pytest.raises(PermanentEventError, match="Missing mandatory envelope fields"):
        await handle_message_processing(invalid_msg, db_session)

    # Invalid payload type (must be dict)
    invalid_payload_msg = {
        "raw_event_id": 1,
        "delivery_id": "bad-payload-01",
        "event_type": "some.event",
        "payload": "not-a-dict",
    }
    with pytest.raises(PermanentEventError, match="Payload must be a dictionary"):
        await handle_message_processing(invalid_payload_msg, db_session)


def test_event_state_machine_transitions():
    from app.models import EventStatus, can_transition

    # Valid transitions
    assert can_transition(EventStatus.RECEIVED, EventStatus.QUEUED)
    assert can_transition(EventStatus.RECEIVED, EventStatus.PROCESSING)
    assert can_transition(EventStatus.FAILED, EventStatus.RECEIVED)
    assert can_transition(EventStatus.FAILED, EventStatus.REPLAYED)

    # Invalid transitions
    assert not can_transition(EventStatus.PROCESSED, EventStatus.RECEIVED)
    assert not can_transition(EventStatus.PROCESSED, EventStatus.FAILED)


def test_retry_tier_queue_selection():
    from worker.consumer import select_retry_queue

    assert select_retry_queue(0) == "events.retry.5s"
    assert select_retry_queue(1) == "events.retry.30s"
    assert select_retry_queue(2) == "events.retry.5m"
    assert select_retry_queue(99) == "events.retry.5m"


@pytest.mark.asyncio
async def test_worker_retry_exhaustion_routes_to_dlq(db_session: AsyncSession, monkeypatch):
    import json
    from unittest.mock import AsyncMock, MagicMock

    from sqlalchemy import select

    from app.models import FailedEvent
    from worker import consumer

    raw = RawEvent(
        delivery_id="dlq-exhaust-01",
        source="test",
        event_type="error.simulate",
        payload={},
        status="received",
    )
    db_session.add(raw)
    await db_session.commit()
    await db_session.refresh(raw)

    class _SessionCtx:
        async def __aenter__(self):
            return db_session

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(consumer, "async_session_maker", lambda: _SessionCtx())

    # Attempt 0 -> published to events.retry.5s and acked
    msg_0 = MagicMock()
    msg_0.body = json.dumps({
        "raw_event_id": raw.id,
        "delivery_id": raw.delivery_id,
        "event_type": "error.simulate",
        "payload": {},
        "attempt": 0,
    }).encode("utf-8")
    msg_0.ack = AsyncMock()
    msg_0.reject = AsyncMock()

    channel = MagicMock()
    channel.default_exchange = MagicMock()
    channel.default_exchange.publish = AsyncMock()

    await consumer.process_message(msg_0, channel)
    msg_0.ack.assert_awaited_once()
    channel.default_exchange.publish.assert_awaited_once()
    assert channel.default_exchange.publish.await_args.kwargs["routing_key"] == "events.retry.5s"

    # Attempt 2 (MAX_RETRIES=3 exhausted) -> rejected to DLQ and FailedEvent recorded
    msg_final = MagicMock()
    msg_final.body = json.dumps({
        "raw_event_id": raw.id,
        "delivery_id": raw.delivery_id,
        "event_type": "error.simulate",
        "payload": {},
        "attempt": 2,
    }).encode("utf-8")
    msg_final.ack = AsyncMock()
    msg_final.reject = AsyncMock()

    await consumer.process_message(msg_final, channel)
    msg_final.reject.assert_awaited_once_with(requeue=False)

    failed = (await db_session.execute(
        select(FailedEvent).where(FailedEvent.raw_event_id == raw.id)
    )).scalar_one()
    assert failed.retry_count == 3
    await db_session.refresh(raw)
    assert raw.status == "failed"


@pytest.mark.asyncio
async def test_worker_crash_between_commit_and_ack_is_idempotent(db_session: AsyncSession, monkeypatch):
    import json
    from unittest.mock import AsyncMock, MagicMock

    from sqlalchemy import select

    from worker import consumer

    raw = RawEvent(
        delivery_id="crash-ack-01",
        source="test",
        event_type="order.created",
        payload={"amount": 10},
        status="received",
    )
    db_session.add(raw)
    await db_session.commit()
    await db_session.refresh(raw)

    class _SessionCtx:
        async def __aenter__(self):
            return db_session

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(consumer, "async_session_maker", lambda: _SessionCtx())

    # Simulate crash right after DB commit when calling ack()
    msg_1 = MagicMock()
    msg_1.body = json.dumps({
        "raw_event_id": raw.id,
        "delivery_id": raw.delivery_id,
        "event_type": raw.event_type,
        "payload": raw.payload,
    }).encode("utf-8")
    msg_1.ack = AsyncMock(side_effect=ConnectionResetError("Lost broker before ack"))
    msg_1.reject = AsyncMock()
    channel = MagicMock()

    with pytest.raises(ConnectionResetError):
        await consumer.process_message(msg_1, channel)

    # Redelivery succeeds cleanly without double-inserting ProcessedEvent
    msg_2 = MagicMock()
    msg_2.body = msg_1.body
    msg_2.ack = AsyncMock()
    msg_2.reject = AsyncMock()

    await consumer.process_message(msg_2, channel)
    msg_2.ack.assert_awaited_once()

    rows = (await db_session.execute(
        select(ProcessedEvent).where(ProcessedEvent.delivery_id == "crash-ack-01")
    )).scalars().all()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_batched_retention_cleanup(db_session: AsyncSession):
    from datetime import datetime, timedelta, timezone

    from sqlalchemy import func, select

    from app.models import OutboxEvent
    from app.retention import run_retention_cleanup

    old_ts = datetime.now(timezone.utc) - timedelta(days=10)
    for i in range(5):
        raw = RawEvent(
            delivery_id=f"ret-{i}",
            source="test",
            event_type="ret.test",
            payload={},
            status="processed",
            received_at=old_ts,
        )
        db_session.add(raw)
        await db_session.flush()
        db_session.add(OutboxEvent(raw_event_id=raw.id, payload={}, published=True, created_at=old_ts))
    await db_session.commit()

    res = await run_retention_cleanup(db_session, batch_size=2)
    assert res["outbox_deleted"] == 5
    assert res["raw_events_deleted"] == 5
    remaining = (await db_session.execute(select(func.count(RawEvent.id)))).scalar_one()
    assert remaining == 0



