import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, cast

from sqlalchemy import CursorResult, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import OutboxEvent, RawEvent

logger = logging.getLogger(__name__)


async def _batched_delete(
    session: AsyncSession,
    model,
    condition,
    batch_size: int = 1000,
) -> int:
    """Delete matching rows in bounded batches to avoid long table locks."""
    total_deleted = 0
    while True:
        id_rows = (
            await session.execute(
                select(model.id).where(condition).limit(batch_size)
            )
        ).scalars().all()
        if not id_rows:
            break
        res = cast(CursorResult[Any], await session.execute(delete(model).where(model.id.in_(id_rows))))
        deleted = res.rowcount or 0
        total_deleted += deleted
        await session.commit()
        if deleted < batch_size:
            break
    return total_deleted


async def run_retention_cleanup(session: AsyncSession, batch_size: int = 1000) -> Dict[str, int]:
    """
    Prunes stale published outbox records and expired raw events in bounded batches.
    """
    now = datetime.now(timezone.utc)
    outbox_cutoff = now - timedelta(hours=settings.OUTBOX_RETENTION_HOURS)
    raw_cutoff = now - timedelta(days=settings.RAW_EVENT_RETENTION_DAYS)

    # 1. Prune published outbox rows in batches
    outbox_deleted = await _batched_delete(
        session,
        OutboxEvent,
        (OutboxEvent.published == True) & (OutboxEvent.created_at < outbox_cutoff),
        batch_size=batch_size,
    )

    # 2. Prune expired raw events in batches (cascades to processed and failed records)
    raw_deleted = await _batched_delete(
        session,
        RawEvent,
        RawEvent.received_at < raw_cutoff,
        batch_size=batch_size,
    )

    if outbox_deleted > 0 or raw_deleted > 0:
        logger.info(
            "Retention cleanup completed: purged %d published outbox rows and %d expired raw events",
            outbox_deleted,
            raw_deleted
        )

    return {
        "outbox_deleted": outbox_deleted,
        "raw_events_deleted": raw_deleted,
    }


async def get_storage_stats(session: AsyncSession) -> Dict[str, int]:
    """Retrieves current row counts across all event tables for storage monitoring."""
    raw_count = (await session.execute(select(func.count(RawEvent.id)))).scalar_one()
    outbox_pending = (await session.execute(
        select(func.count(OutboxEvent.id)).where(OutboxEvent.published == False)
    )).scalar_one()
    outbox_total = (await session.execute(select(func.count(OutboxEvent.id)))).scalar_one()

    return {
        "raw_events_total": raw_count,
        "outbox_pending": outbox_pending,
        "outbox_total": outbox_total,
    }

