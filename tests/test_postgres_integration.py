import asyncio
import os
import subprocess
import sys
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PG_URL = os.environ.get("INTEGRATION_DATABASE_URL")
RMQ_URL = os.environ.get("INTEGRATION_RABBITMQ_URL")

pytestmark = pytest.mark.skipif(
    not PG_URL,
    reason="Set INTEGRATION_DATABASE_URL to run real PostgreSQL integration tests",
)


@pytest.fixture(scope="module")
def migrated_pg_url() -> str:
    env = os.environ.copy()
    env["DATABASE_URL"] = PG_URL  # type: ignore[assignment]
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        env=env,
    )
    return PG_URL  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_postgres_skip_locked_outbox_concurrency(migrated_pg_url: str):
    """Two concurrent transactions using FOR UPDATE SKIP LOCKED claim disjoint outbox batches."""
    engine = create_async_engine(migrated_pg_url, echo=False)
    SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    from app.models import OutboxEvent, RawEvent

    tag = uuid.uuid4().hex[:8]
    async with SessionLocal() as seed_sess:
        for i in range(4):
            raw = RawEvent(
                delivery_id=f"pg-lock-{tag}-{i}",
                source="pg-test",
                event_type="order.created",
                payload={"i": i},
                status="received",
            )
            seed_sess.add(raw)
            await seed_sess.flush()
            seed_sess.add(OutboxEvent(raw_event_id=raw.id, payload={"i": i}, published=False))
        await seed_sess.commit()

    async with SessionLocal() as sess_a, SessionLocal() as sess_b:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published == False)  # noqa: E712
            .order_by(OutboxEvent.id.asc())
            .limit(2)
            .with_for_update(skip_locked=True)
        )
        rows_a = (await sess_a.execute(stmt)).scalars().all()
        rows_b = (await sess_b.execute(stmt)).scalars().all()

        ids_a = {r.id for r in rows_a}
        ids_b = {r.id for r in rows_b}
        assert len(ids_a) == 2
        assert len(ids_b) == 2
        assert ids_a.isdisjoint(ids_b)

        await sess_a.rollback()
        await sess_b.rollback()

    await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_concurrent_replay_race(migrated_pg_url: str):
    """Concurrent replay requests against real Postgres yield exactly one 200 and one 409."""
    from app.config import settings
    from app.database import get_db
    from app.main import app
    from app.models import RawEvent

    engine = create_async_engine(migrated_pg_url, echo=False)
    SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    tag = uuid.uuid4().hex[:8]
    async with SessionLocal() as sess:
        raw = RawEvent(
            delivery_id=f"pg-replay-{tag}",
            source="pg-test",
            event_type="order.created",
            payload={"x": 1},
            status="failed",
        )
        sess.add(raw)
        await sess.commit()
        await sess.refresh(raw)
        raw_id = raw.id

    async def override_get_db():
        async with SessionLocal() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            r1, r2 = await asyncio.gather(
                ac.post(f"/api/v1/events/{raw_id}/replay", headers={"X-API-Key": settings.REPLAY_API_KEY}),
                ac.post(f"/api/v1/events/{raw_id}/replay", headers={"X-API-Key": settings.REPLAY_API_KEY}),
            )
        codes = sorted([r1.status_code, r2.status_code])
        assert codes == [200, 409]
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_explain_uses_indexes(migrated_pg_url: str):
    """Verify PostgreSQL query plans for keyset pagination and outbox polling."""
    engine = create_async_engine(migrated_pg_url, echo=False)
    async with engine.connect() as conn:
        await conn.execute(text("SET enable_seqscan = off"))
        plan_events = (
            await conn.execute(
                text(
                    "EXPLAIN SELECT id, received_at FROM raw_events "
                    "WHERE status = 'received' ORDER BY received_at DESC, id DESC LIMIT 20"
                )
            )
        ).scalars().all()
        plan_outbox = (
            await conn.execute(
                text(
                    "EXPLAIN SELECT id FROM outbox_events "
                    "WHERE published = false ORDER BY id ASC LIMIT 50"
                )
            )
        ).scalars().all()
    await engine.dispose()

    assert any("Index" in line for line in plan_events), f"Expected index scan in: {plan_events}"
    assert any("Index" in line for line in plan_outbox), f"Expected index scan in: {plan_outbox}"


@pytest.mark.asyncio
async def test_rabbitmq_down_outbox_drains_on_recovery(migrated_pg_url: str):
    """Failure injection: RabbitMQ unreachable during ingest -> outbox holds event -> drains on recovery."""
    if not RMQ_URL:
        pytest.skip("Set INTEGRATION_RABBITMQ_URL to run RabbitMQ recovery test")

    from app.config import settings
    from app.database import get_db
    from app.main import app, dispatch_pending_outbox
    from app.models import OutboxEvent
    from app.queue import rabbitmq_client

    engine = create_async_engine(migrated_pg_url, echo=False)
    SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with SessionLocal() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    orig_url = rabbitmq_client.amqp_url
    delivery_id = f"rmq-down-{uuid.uuid4().hex[:8]}"

    try:
        # 1. Force RabbitMQ client to an unreachable port
        await rabbitmq_client.close()
        rabbitmq_client.connection = None
        rabbitmq_client.channel = None
        rabbitmq_client.amqp_url = "amqp://guest:guest@127.0.0.1:59999/"

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/events",
                json={"delivery_id": delivery_id, "event_type": "order.created", "data": {"ok": True}},
                headers={"X-API-Key": settings.INGEST_API_KEY},
            )
            assert resp.status_code == 202

        async with SessionLocal() as sess:
            row = (
                await sess.execute(
                    select(OutboxEvent).where(OutboxEvent.published == False)  # noqa: E712
                )
            ).scalars().all()
            assert any(r.payload.get("delivery_id") == delivery_id for r in row)

        # 2. Restore real RabbitMQ URL and drain outbox
        rabbitmq_client.amqp_url = RMQ_URL
        await rabbitmq_client.connect()
        async with SessionLocal() as sess:
            drained = await dispatch_pending_outbox(sess, batch_size=100)
            assert drained >= 1

        async with SessionLocal() as sess:
            still_pending = (
                await sess.execute(
                    select(OutboxEvent).where(OutboxEvent.published == False)  # noqa: E712
                )
            ).scalars().all()
            assert not any(r.payload.get("delivery_id") == delivery_id for r in still_pending)
    finally:
        rabbitmq_client.amqp_url = orig_url
        await rabbitmq_client.close()
        app.dependency_overrides.clear()
        await engine.dispose()

