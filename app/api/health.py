import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, select, text

from app.config import settings
from app.database import async_session_maker, engine
from app.models import FailedEvent, OutboxEvent, ProcessedEvent, RawEvent
from app.queue import rabbitmq_client
from app.security import verify_admin_auth

router = APIRouter(tags=["Health & Observability"])


@router.get("/health", summary="Health check endpoint")
async def health_check():
    db_ok = False
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            db_ok = True
    except Exception:
        db_ok = False

    queue_ok = rabbitmq_client.connection is not None and not rabbitmq_client.connection.is_closed
    overall_healthy = db_ok and queue_ok
    return {
        "status": "healthy" if overall_healthy else "degraded",
        "components": {
            "database": "connected" if db_ok else "disconnected",
            "rabbitmq": "connected" if queue_ok else "disconnected",
        }
    }


@router.get("/health/ready", summary="Readiness probe")
async def readiness_check(response: Response):
    res = await health_check()
    if res["status"] != "healthy":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return res


@router.get("/health/live", summary="Liveness probe")
async def liveness_check():
    return {"status": "alive"}


@router.get("/metrics", summary="Prometheus metrics", dependencies=[Depends(verify_admin_auth)])
async def get_metrics():
    from app.main import refresh_outbox_metrics
    try:
        async with async_session_maker() as session:
            await refresh_outbox_metrics(session)
    except Exception:
        pass
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get(
    "/api/v1/system/telemetry",
    summary="Comprehensive telemetry & system resource metrics",
    dependencies=[Depends(verify_admin_auth)],
)
async def get_system_telemetry():
    t0 = time.perf_counter()
    db_ok = False
    db_latency = 0.0
    row_counts = {"raw_events": 0, "processed_events": 0, "failed_events": 0, "outbox_pending": 0}
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            db_latency = round((time.perf_counter() - t0) * 1000, 2)
            db_ok = True
        async with async_session_maker() as session:
            raw_c = (await session.execute(select(func.count(RawEvent.id)))).scalar_one()
            proc_c = (await session.execute(select(func.count(ProcessedEvent.id)))).scalar_one()
            fail_c = (await session.execute(select(func.count(FailedEvent.id)))).scalar_one()
            outbox_c = (await session.execute(
                select(func.count(OutboxEvent.id)).where(OutboxEvent.published == False)
            )).scalar_one()
            row_counts = {
                "raw_events": raw_c,
                "processed_events": proc_c,
                "failed_events": fail_c,
                "outbox_pending": outbox_c
            }
    except Exception:
        db_ok = False

    queue_ok = rabbitmq_client.connection is not None and not rabbitmq_client.connection.is_closed

    sys_metrics = {
        "cpu_percent": 0.0,
        "process_ram_mb": 0.0,
        "system_ram_total_gb": 0.0,
        "system_ram_used_gb": 0.0,
        "system_ram_percent": 0.0,
        "disk_total_gb": 0.0,
        "disk_free_gb": 0.0,
        "disk_percent": 0.0,
    }
    try:
        import psutil
        proc = psutil.Process()
        vmem = psutil.virtual_memory()
        disk = psutil.disk_usage(os.getcwd())
        sys_metrics = {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "process_ram_mb": round(proc.memory_info().rss / (1024 * 1024), 2),
            "system_ram_total_gb": round(vmem.total / (1024**3), 2),
            "system_ram_used_gb": round(vmem.used / (1024**3), 2),
            "system_ram_percent": vmem.percent,
            "disk_total_gb": round(disk.total / (1024**3), 2),
            "disk_free_gb": round(disk.free / (1024**3), 2),
            "disk_percent": disk.percent,
        }
    except Exception:
        pass

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "healthy" if (db_ok and queue_ok) else "degraded",
        "components": {
            "api": {"status": "healthy", "port": 8000, "name": settings.APP_NAME},
            "database": {
                "status": "healthy" if db_ok else "unreachable",
                "engine": "postgresql+asyncpg",
                "latency_ms": db_latency,
                "tables": row_counts
            },
            "rabbitmq": {
                "status": "healthy" if queue_ok else "unreachable",
                "queues": [settings.EVENTS_QUEUE, settings.EVENTS_RETRY, settings.EVENTS_DLQ]
            },
            "worker": {"status": "active", "metrics_port": 8001},
            "prometheus": {"status": "monitoring", "port": 9090},
            "grafana": {"status": "dashboards", "port": 3000}
        },
        "system_resources": sys_metrics,
        "retention_policy": {
            "outbox_hours": settings.OUTBOX_RETENTION_HOURS,
            "raw_days": settings.RAW_EVENT_RETENTION_DAYS,
            "auto_retention_enabled": settings.ENABLE_AUTO_RETENTION
        }
    }

