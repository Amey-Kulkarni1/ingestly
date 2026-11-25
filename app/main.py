import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from prometheus_client import Gauge
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import events, health, ingest
from app.config import settings
from app.database import async_session_maker, engine
from app.logging_config import configure_logging, correlation_id_ctx
from app.models import OutboxEvent
from app.queue import rabbitmq_client
from app.retention import run_retention_cleanup
from app.security import verify_admin_auth

configure_logging(service="api")
logger = logging.getLogger("event_service")

OUTBOX_BACKLOG_SIZE = Gauge(
    "outbox_backlog_size",
    "Number of unpublished outbox events waiting to be dispatched",
)
OUTBOX_OLDEST_AGE_SECONDS = Gauge(
    "outbox_oldest_pending_age_seconds",
    "Age in seconds of the oldest unpublished outbox event",
)


async def refresh_outbox_metrics(session: AsyncSession) -> None:
    """Update Prometheus gauges for outbox backlog size and oldest pending age."""
    row = (
        await session.execute(
            select(func.count(OutboxEvent.id), func.min(OutboxEvent.created_at)).where(
                OutboxEvent.published == False
            )
        )
    ).one()
    count, oldest_ts = row[0] or 0, row[1]
    OUTBOX_BACKLOG_SIZE.set(count)
    if oldest_ts is not None:
        if oldest_ts.tzinfo is None:
            oldest_ts = oldest_ts.replace(tzinfo=timezone.utc)
        age = max(0.0, (datetime.now(timezone.utc) - oldest_ts).total_seconds())
        OUTBOX_OLDEST_AGE_SECONDS.set(age)
    else:
        OUTBOX_OLDEST_AGE_SECONDS.set(0.0)


async def dispatch_pending_outbox(session: AsyncSession, batch_size: int = 50) -> int:
    """Claim and publish unpublished outbox rows using FOR UPDATE SKIP LOCKED."""
    res = await session.execute(
        select(OutboxEvent)
        .where(OutboxEvent.published == False)
        .order_by(OutboxEvent.id.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    pending = res.scalars().all()
    if not pending:
        return 0

    published_count = 0
    for item in pending:
        try:
            headers = {}
            cid = item.payload.get("correlation_id") if isinstance(item.payload, dict) else None
            if cid:
                headers["x-correlation-id"] = cid
            await rabbitmq_client.publish(settings.EVENTS_QUEUE, item.payload, headers=headers)
            item.published = True
            published_count += 1
        except Exception as exc:
            logger.warning("Outbox publish failed for outbox_id=%d: %s", item.id, exc)
            break

    if published_count > 0:
        await session.commit()
    return published_count


async def run_outbox_dispatcher():
    """Background reconciler to deliver any outbox records that missed immediate publish."""
    while True:
        try:
            await asyncio.sleep(2)
            async with async_session_maker() as session:
                if rabbitmq_client.channel and not rabbitmq_client.channel.is_closed:
                    await dispatch_pending_outbox(session)
                await refresh_outbox_metrics(session)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Outbox reconciler iteration failed: %s", exc)


async def run_retention_loop():
    """Periodic automated retention pruning of published outbox and stale raw events."""
    if not settings.ENABLE_AUTO_RETENTION:
        return

    while True:
        try:
            # Check every 15 minutes
            await asyncio.sleep(900)
            async with async_session_maker() as session:
                await run_retention_cleanup(session)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Automated retention loop failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Connecting to RabbitMQ...")
    try:
        await rabbitmq_client.connect()
    except Exception as exc:
        logger.warning("RabbitMQ connection deferred or failed: %s", exc)

    outbox_task = asyncio.create_task(run_outbox_dispatcher())
    retention_task = asyncio.create_task(run_retention_loop())

    yield

    outbox_task.cancel()
    retention_task.cancel()
    try:
        await asyncio.gather(outbox_task, retention_task, return_exceptions=True)
    except Exception:
        pass

    logger.info("Shutting down resources...")
    await rabbitmq_client.close()
    await engine.dispose()
    logger.info("Shutdown complete.")


app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    description="Reliable Webhook/Event Ingestion Pipeline with RabbitMQ & PostgreSQL",
    lifespan=lifespan,
)


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    cid = request.headers.get("x-correlation-id") or str(uuid.uuid4())
    token = correlation_id_ctx.set(cid)
    try:
        response = await call_next(request)
        response.headers.setdefault("X-Correlation-ID", cid)
        return response
    finally:
        correlation_id_ctx.reset(token)

app.include_router(ingest.router)
app.include_router(events.router)
app.include_router(health.router)

template_file = Path(__file__).parent / "templates" / "dashboard.html"


@app.get("/", response_class=HTMLResponse, include_in_schema=False, dependencies=[Depends(verify_admin_auth)])
@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"], dependencies=[Depends(verify_admin_auth)])
async def serve_dashboard():
    if template_file.exists():
        return HTMLResponse(
            content=template_file.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
        )
    return HTMLResponse(content="<h1>Dashboard template not found</h1>", status_code=404)

