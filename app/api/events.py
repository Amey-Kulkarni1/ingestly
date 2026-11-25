import base64
import logging
from datetime import datetime
from typing import Any, Optional, cast

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from sqlalchemy import CursorResult, and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.database import get_db
from app.models import FailedEvent, OutboxEvent, RawEvent
from app.queue import rabbitmq_client
from app.retention import run_retention_cleanup
from app.schemas import (
    EventDetailResponse,
    PaginatedEventsResponse,
    ProcessedEventResponse,
    RawEventResponse,
    ReplayResponse,
    RetentionCleanupResponse,
)
from app.security import verify_admin_auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Events Query & Management"])


@router.get("/events", response_model=PaginatedEventsResponse, summary="List events with filtering and keyset pagination")
async def list_events(
    status: Optional[str] = Query(None, description="Filter by status: received, processed, failed"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    source: Optional[str] = Query(None, description="Filter by source"),
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None, description="Keyset cursor for O(1) pagination without offset scan"),
    db: AsyncSession = Depends(get_db),
):
    query = select(RawEvent).options(selectinload(RawEvent.processed_record))
    count_query = select(func.count(RawEvent.id))

    if status:
        query = query.where(RawEvent.status == status)
        count_query = count_query.where(RawEvent.status == status)
    if event_type:
        query = query.where(RawEvent.event_type == event_type)
        count_query = count_query.where(RawEvent.event_type == event_type)
    if source:
        query = query.where(RawEvent.source == source)
        count_query = count_query.where(RawEvent.source == source)

    total = (await db.execute(count_query)).scalar_one()

    # Keyset (cursor-based) pagination seek on (received_at, id)
    if cursor:
        try:
            decoded = base64.urlsafe_b64decode(cursor.encode("utf-8")).decode("utf-8")
            cur_ts_str, cur_id_str = decoded.split("|")
            cur_ts = datetime.fromisoformat(cur_ts_str)
            cur_id = int(cur_id_str)

            query = query.where(
                or_(
                    RawEvent.received_at < cur_ts,
                    and_(RawEvent.received_at == cur_ts, RawEvent.id < cur_id)
                )
            )
        except Exception as exc:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid pagination cursor: {exc}"
            )

        query = query.order_by(RawEvent.received_at.desc(), RawEvent.id.desc()).limit(limit + 1)
    else:
        # Fallback to offset if specified, otherwise index-ordered
        query = query.order_by(RawEvent.received_at.desc(), RawEvent.id.desc()).limit(limit + 1)
        if offset > 0:
            query = query.offset(offset)

    result = await db.execute(query)
    rows = result.scalars().all()

    has_more = len(rows) > limit
    items = rows[:limit]

    pending_outbox_ids: set[int] = set()
    if items:
        item_ids = [r.id for r in items]
        pending_outbox_ids = set(
            (await db.execute(
                select(OutboxEvent.raw_event_id).where(
                    OutboxEvent.raw_event_id.in_(item_ids),
                    OutboxEvent.published == False
                )
            )).scalars().all()
        )

    response_items = []
    for r in items:
        item_model = RawEventResponse.model_validate(r)
        item_model.outbox_published = r.id not in pending_outbox_ids
        response_items.append(item_model)

    next_cursor = None
    if has_more and items:
        last = items[-1]
        next_cursor = base64.urlsafe_b64encode(
            f"{last.received_at.isoformat()}|{last.id}".encode("utf-8")
        ).decode("utf-8")

    return PaginatedEventsResponse(
        items=response_items,
        total=total,
        limit=limit,
        offset=offset if not cursor else None,
        next_cursor=next_cursor,
        has_more=has_more,
    )


@router.post(
    "/events/retention/cleanup",
    response_model=RetentionCleanupResponse,
    summary="Trigger automated retention cleanup",
    dependencies=[Depends(verify_admin_auth)],
)
async def trigger_retention_cleanup(db: AsyncSession = Depends(get_db)):
    res = await run_retention_cleanup(db)
    return RetentionCleanupResponse(
        outbox_deleted=res["outbox_deleted"],
        raw_events_deleted=res["raw_events_deleted"],
        message=f"Purged {res['outbox_deleted']} outbox rows and {res['raw_events_deleted']} expired raw events."
    )


@router.get("/events/facets", summary="Get distinct event types, sources, and statuses for column filtering")
async def get_event_facets(db: AsyncSession = Depends(get_db)):
    ev_types_query = select(RawEvent.event_type).distinct()
    sources_query = select(RawEvent.source).distinct()
    statuses_query = select(RawEvent.status).distinct()

    ev_types = (await db.execute(ev_types_query)).scalars().all()
    sources = (await db.execute(sources_query)).scalars().all()
    statuses = (await db.execute(statuses_query)).scalars().all()

    return {
        "event_types": sorted(filter(None, ev_types)),
        "sources": sorted(filter(None, sources)),
        "statuses": sorted(filter(None, statuses)),
    }


@router.get("/events/{event_id}", response_model=EventDetailResponse, summary="Get event details and processing history")
async def get_event(event_id: int, db: AsyncSession = Depends(get_db)):
    query = (
        select(RawEvent)
        .options(selectinload(RawEvent.processed_record))
        .where(RawEvent.id == event_id)
    )
    result = await db.execute(query)
    raw_event = result.scalar_one_or_none()

    if not raw_event:
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Event not found")

    failures_query = select(FailedEvent).where(FailedEvent.raw_event_id == event_id)
    failures_res = await db.execute(failures_query)
    failures = failures_res.scalars().all()

    failure_history = [
        {
            "id": f.id,
            "error_message": f.error_message,
            "retry_count": f.retry_count,
            "failed_at": f.failed_at.isoformat(),
        }
        for f in failures
    ]

    return EventDetailResponse(
        raw_event=RawEventResponse.model_validate(raw_event),
        processed_record=(
            ProcessedEventResponse.model_validate(raw_event.processed_record)
            if raw_event.processed_record
            else None
        ),
        failure_history=failure_history,
    )


@router.post(
    "/events/{event_id}/replay",
    response_model=ReplayResponse,
    summary="Replay an event to the queue",
    dependencies=[Depends(verify_admin_auth)],
)
async def replay_event(
    event_id: int,
    db: AsyncSession = Depends(get_db),
):

    # Atomic conditional update + outbox insert: guards against replay races and RabbitMQ outages
    stmt = (
        update(RawEvent)
        .where(RawEvent.id == event_id, RawEvent.status == "failed")
        .values(status="received")
    )
    res = cast(CursorResult[Any], await db.execute(stmt))
    if res.rowcount == 0:
        existing_status = (await db.execute(select(RawEvent.status).where(RawEvent.id == event_id))).scalar_one_or_none()
        if existing_status is None:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Event not found")
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail=f"Cannot replay event in status '{existing_status}'. Only 'failed' events can be replayed."
        )

    raw_event = (await db.execute(select(RawEvent).where(RawEvent.id == event_id))).scalar_one()
    queue_payload = {
        "raw_event_id": raw_event.id,
        "delivery_id": raw_event.delivery_id,
        "event_type": raw_event.event_type,
        "source": raw_event.source,
        "payload": raw_event.payload,
        "attempt": 0,
        "is_replay": True,
    }
    outbox_entry = OutboxEvent(
        raw_event_id=raw_event.id,
        payload=queue_payload,
        published=False,
    )
    db.add(outbox_entry)
    await db.commit()

    try:
        await rabbitmq_client.publish(queue_name=settings.EVENTS_QUEUE, payload=queue_payload)
        outbox_entry.published = True
        await db.commit()
    except Exception as exc:
        logger.warning("Immediate replay publish failed for event %d (outbox reconciler will deliver): %s", event_id, exc)

    return ReplayResponse(
        status="replayed",
        event_id=raw_event.id,
        delivery_id=raw_event.delivery_id,
        message="Event successfully re-enqueued for processing."
    )

