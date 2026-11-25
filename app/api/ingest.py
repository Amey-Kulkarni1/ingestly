import hmac
import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from prometheus_client import Counter, Histogram
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import (
    GitHubWebhookAdapter,
    SignatureVerificationError,
    verify_hmac_sha256,
    verify_hmac_timestamp,
)
from app.config import settings
from app.database import get_db
from app.models import OutboxEvent, RawEvent
from app.queue import rabbitmq_client
from app.schemas import IngestEventRequest, IngestResponse
from app.security import rate_limiter, read_body_capped

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Ingest"])

EVENTS_RECEIVED_COUNTER = Counter(
    "events_received_total",
    "Total incoming events received",
    ["source", "event_type", "status"]
)
INGEST_LATENCY = Histogram(
    "event_ingest_duration_seconds",
    "Duration of event ingest endpoint in seconds"
)


def verify_ingest_auth(request: Request, raw_body: bytes) -> None:
    """Enforces strict authentication via HMAC-SHA256 signature or Ingestion API Key."""
    api_key = request.headers.get("x-api-key")
    auth_header = request.headers.get("authorization", "")
    bearer = auth_header[7:].strip() if auth_header.startswith("Bearer ") else None

    # Check API key or Bearer token with constant-time comparison
    if (api_key and hmac.compare_digest(api_key, settings.INGEST_API_KEY)) or (
        bearer and hmac.compare_digest(bearer, settings.INGEST_API_KEY)
    ):
        return

    # Optional timestamp replay window check when header is supplied
    ts_header = request.headers.get("x-webhook-timestamp") or request.headers.get("x-timestamp")
    if not verify_hmac_timestamp(ts_header, settings.WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: webhook timestamp outside allowed tolerance window"
        )

    # Check HMAC signature against raw request bytes
    sig_header = request.headers.get("x-hub-signature-256") or request.headers.get("x-signature-256")
    if sig_header and verify_hmac_sha256(settings.WEBHOOK_SECRET, raw_body, sig_header):
        return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized: valid HMAC signature or Ingestion API Key required"
    )


async def save_and_publish_event(
    delivery_id: str,
    source: str,
    event_type: str,
    payload_data: dict,
    correlation_id: str,
    response: Response,
    db: AsyncSession
) -> IngestResponse:
    # 1. Atomic Tier 1 deduplication
    raw_event = RawEvent(
        delivery_id=delivery_id,
        source=source,
        event_type=event_type,
        payload=payload_data,
        status="received",
    )
    db.add(raw_event)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        response.status_code = status.HTTP_200_OK
        EVENTS_RECEIVED_COUNTER.labels(source=source, event_type=event_type, status="duplicate").inc()
        logger.info("Duplicate event ignored: delivery_id=%s, source=%s", delivery_id, source)
        return IngestResponse(
            status="duplicate",
            delivery_id=delivery_id,
            message="Event has already been received and queued/processed."
        )

    # 2. Transactional Outbox
    queue_payload = {
        "raw_event_id": raw_event.id,
        "delivery_id": raw_event.delivery_id,
        "event_type": raw_event.event_type,
        "source": raw_event.source,
        "payload": raw_event.payload,
        "correlation_id": correlation_id,
        "attempt": 0,
    }
    outbox_entry = OutboxEvent(
        raw_event_id=raw_event.id,
        payload=queue_payload,
        published=False
    )
    db.add(outbox_entry)
    await db.commit()

    # 3. Queue publish
    try:
        await rabbitmq_client.publish(
            queue_name=settings.EVENTS_QUEUE,
            payload=queue_payload,
            headers={"x-correlation-id": correlation_id}
        )
        outbox_entry.published = True
        await db.commit()
    except Exception as exc:
        logger.warning("Immediate publish failed for delivery_id=%s (outbox reconciler will deliver): %s", delivery_id, exc)

    EVENTS_RECEIVED_COUNTER.labels(source=source, event_type=event_type, status="accepted").inc()
    logger.info("[%s] Accepted event id=%d, delivery_id=%s", correlation_id, raw_event.id, delivery_id)

    return IngestResponse(
        status="accepted",
        delivery_id=delivery_id,
        message="Event accepted and queued for processing."
    )


@router.post(
    "/events",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest incoming event"
)
async def ingest_event(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db)
):
    rate_limiter.check(request)
    with INGEST_LATENCY.time():
        raw_body = await read_body_capped(request, settings.MAX_BODY_BYTES)
        verify_ingest_auth(request, raw_body)

        try:
            event_in = IngestEventRequest.model_validate_json(raw_body)
        except ValidationError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors())

        correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        response.headers["X-Correlation-ID"] = correlation_id

        delivery_id = event_in.delivery_id or str(uuid.uuid4())
        return await save_and_publish_event(
            delivery_id=delivery_id,
            source=event_in.source,
            event_type=event_in.event_type,
            payload_data=event_in.data,
            correlation_id=correlation_id,
            response=response,
            db=db
        )


@router.post(
    "/webhooks/github",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest GitHub webhook with signature check"
)
async def ingest_github_webhook(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db)
):
    rate_limiter.check(request)
    with INGEST_LATENCY.time():
        correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        response.headers["X-Correlation-ID"] = correlation_id

        raw_body = await read_body_capped(request, settings.MAX_BODY_BYTES)

        headers_dict = {k.lower(): v for k, v in request.headers.items()}
        ts_header = headers_dict.get("x-webhook-timestamp") or headers_dict.get("x-timestamp")
        if not verify_hmac_timestamp(ts_header, settings.WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized: webhook timestamp outside allowed tolerance window"
            )
        try:
            parsed = GitHubWebhookAdapter.parse_and_verify(
                raw_body=raw_body,
                headers=headers_dict,
                secret=settings.WEBHOOK_SECRET
            )
        except SignatureVerificationError as err:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(err))
        except ValueError as err:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(err))

        delivery_id = parsed.delivery_id or str(uuid.uuid4())
        return await save_and_publish_event(
            delivery_id=delivery_id,
            source=parsed.source,
            event_type=parsed.event_type,
            payload_data=parsed.data,
            correlation_id=correlation_id,
            response=response,
            db=db
        )
