import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_ingest_event_success(client: AsyncClient):
    payload = {
        "delivery_id": "test-deliv-001",
        "source": "fake-generator",
        "event_type": "order.created",
        "data": {"order_id": "ord_101", "amount": 99.50}
    }
    response = await client.post("/api/v1/events", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["delivery_id"] == "test-deliv-001"


@pytest.mark.asyncio
async def test_ingest_event_deduplication(client: AsyncClient):
    payload = {
        "delivery_id": "test-duplicate-001",
        "source": "fake-generator",
        "event_type": "user.signup",
        "data": {"user_id": "usr_55"}
    }

    # First ingest -> Accepted (202)
    resp1 = await client.post("/api/v1/events", json=payload)
    assert resp1.status_code == 202
    assert resp1.json()["status"] == "accepted"

    # Second ingest with same delivery_id -> Duplicate (200 OK)
    resp2 = await client.post("/api/v1/events", json=payload)
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["status"] == "duplicate"
    assert data2["delivery_id"] == "test-duplicate-001"


@pytest.mark.asyncio
async def test_list_events_and_detail(client: AsyncClient):
    # Ingest two events
    await client.post("/api/v1/events", json={
        "delivery_id": "deliv-item-1",
        "event_type": "item.added",
        "data": {"item": "apple"}
    })
    await client.post("/api/v1/events", json={
        "delivery_id": "deliv-item-2",
        "event_type": "item.removed",
        "data": {"item": "orange"}
    })

    # List
    list_resp = await client.get("/api/v1/events")
    assert list_resp.status_code == 200
    list_data = list_resp.json()
    assert list_data["total"] == 2
    assert len(list_data["items"]) == 2

    # Get single event detail
    first_id = list_data["items"][0]["id"]
    detail_resp = await client.get(f"/api/v1/events/{first_id}")
    assert detail_resp.status_code == 200
    detail_data = detail_resp.json()
    assert detail_data["raw_event"]["id"] == first_id


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert "components" in data


@pytest.mark.asyncio
async def test_metrics_endpoint(client: AsyncClient):
    from app.config import settings

    unauth = await client.get("/metrics")
    assert unauth.status_code == 401

    resp = await client.get("/metrics", headers={"X-API-Key": settings.REPLAY_API_KEY})
    assert resp.status_code == 200
    assert "events_received_total" in resp.text
    assert "outbox_backlog_size" in resp.text
    assert "outbox_oldest_pending_age_seconds" in resp.text


@pytest.mark.asyncio
async def test_dashboard_endpoint(client: AsyncClient):
    from app.config import settings

    unauth = await client.get("/dashboard")
    assert unauth.status_code == 401

    resp = await client.get("/dashboard", headers={"X-API-Key": settings.REPLAY_API_KEY})
    assert resp.status_code == 200
    assert "Ingestly" in resp.text


@pytest.mark.asyncio
async def test_event_facets(client: AsyncClient):
    await client.post("/api/v1/events", json={
        "delivery_id": "facet-test-1",
        "source": "custom-source",
        "event_type": "facet.created",
        "data": {}
    })
    resp = await client.get("/api/v1/events/facets")
    assert resp.status_code == 200
    data = resp.json()
    assert "facet.created" in data["event_types"]
    assert "custom-source" in data["sources"]
    assert "received" in data["statuses"]


@pytest.mark.asyncio
async def test_replay_event_atomic_guard(client: AsyncClient, db_session):
    from sqlalchemy import select

    from app.models import RawEvent

    # Ingest event
    ingest_resp = await client.post("/api/v1/events", json={
        "delivery_id": "replay-guard-01",
        "event_type": "order.created",
        "data": {"order_id": "ord_999"}
    })
    assert ingest_resp.status_code == 202

    raw_event = (await db_session.execute(
        select(RawEvent).where(RawEvent.delivery_id == "replay-guard-01")
    )).scalar_one()

    # 1. Missing or invalid API key -> 401 Unauthorized
    unauth_resp = await client.post(f"/api/v1/events/{raw_event.id}/replay")
    assert unauth_resp.status_code == 401

    from app.config import settings
    headers = {"X-API-Key": settings.REPLAY_API_KEY}

    # 2. Non-failed replay should be rejected with 409 Conflict
    conflict_resp = await client.post(f"/api/v1/events/{raw_event.id}/replay", headers=headers)
    assert conflict_resp.status_code == 409

    # 3. Mark event as failed
    raw_event.status = "failed"
    await db_session.commit()

    # 4. First replay succeeds
    ok_resp = await client.post(f"/api/v1/events/{raw_event.id}/replay", headers=headers)
    assert ok_resp.status_code == 200
    assert ok_resp.json()["status"] == "replayed"

    # 5. Immediate second replay fails with 409 Conflict (race condition guarded)
    second_replay = await client.post(f"/api/v1/events/{raw_event.id}/replay", headers=headers)
    assert second_replay.status_code == 409


@pytest.mark.asyncio
async def test_ingest_creates_outbox_event(client: AsyncClient, db_session):
    from sqlalchemy import select

    from app.models import OutboxEvent

    resp = await client.post("/api/v1/events", json={
        "delivery_id": "outbox-test-01",
        "event_type": "payment.succeeded",
        "data": {"amount": 50.0}
    })
    assert resp.status_code == 202

    outbox_records = (await db_session.execute(
        select(OutboxEvent)
    )).scalars().all()
    matching = [r for r in outbox_records if r.payload.get("delivery_id") == "outbox-test-01"]
    assert len(matching) == 1
    assert matching[0].published is True


@pytest.mark.asyncio
async def test_correlation_id_and_liveness(client: AsyncClient):
    resp = await client.post(
        "/api/v1/events",
        json={"event_type": "cid.test", "data": {}},
        headers={"X-Correlation-ID": "cid-custom-1234"}
    )
    assert resp.status_code == 202
    assert resp.headers.get("X-Correlation-ID") == "cid-custom-1234"

    live_resp = await client.get("/health/live")
    assert live_resp.status_code == 200
    assert live_resp.json()["status"] == "alive"


@pytest.mark.asyncio
async def test_github_webhook_signature_verification(client: AsyncClient):
    import hashlib
    import hmac
    import json

    from app.config import settings
    payload = {"action": "opened", "issue": {"id": 101, "title": "Bug found"}}
    raw_bytes = json.dumps(payload).encode("utf-8")
    secret = settings.WEBHOOK_SECRET

    valid_sig = "sha256=" + hmac.new(secret.encode(), raw_bytes, hashlib.sha256).hexdigest()

    # 1. Valid signature
    resp = await client.post(
        "/api/v1/webhooks/github",
        content=raw_bytes,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Delivery": "gh-deliv-001",
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": valid_sig
        }
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "accepted"

    # 2. Invalid signature
    bad_resp = await client.post(
        "/api/v1/webhooks/github",
        content=raw_bytes,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Delivery": "gh-deliv-002",
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": "sha256=invalid_hash"
        }
    )
    assert bad_resp.status_code == 401


@pytest.mark.asyncio
async def test_strict_ingest_auth_rejection_and_acceptance(client: AsyncClient):
    import hashlib
    import hmac
    import json

    from httpx import ASGITransport
    from httpx import AsyncClient as BareClient

    from app.config import settings
    from app.main import app

    payload = {"event_type": "auth.test", "data": {"user": "bob"}}
    raw_bytes = json.dumps(payload).encode("utf-8")

    transport = ASGITransport(app=app)
    async with BareClient(transport=transport, base_url="http://test") as bare_client:
        # 1. No auth header -> 401 Unauthorized
        resp_no_auth = await bare_client.post("/api/v1/events", content=raw_bytes, headers={"Content-Type": "application/json"})
        assert resp_no_auth.status_code == 401
        assert "valid HMAC signature or Ingestion API Key required" in resp_no_auth.json()["detail"]

        # 2. Invalid API Key -> 401 Unauthorized
        resp_bad_key = await bare_client.post(
            "/api/v1/events",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-API-Key": "wrong-key"}
        )
        assert resp_bad_key.status_code == 401

        # 3. Valid API Key via X-API-Key -> 202 Accepted
        resp_api_key = await bare_client.post(
            "/api/v1/events",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-API-Key": settings.INGEST_API_KEY}
        )
        assert resp_api_key.status_code == 202

        # 4. Valid Bearer Token via Authorization header -> 202 Accepted
        resp_bearer = await bare_client.post(
            "/api/v1/events",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {settings.INGEST_API_KEY}"}
        )
        assert resp_bearer.status_code == 202

        # 5. Valid HMAC SHA-256 Signature -> 202 Accepted
        sig = "sha256=" + hmac.new(settings.WEBHOOK_SECRET.encode(), raw_bytes, hashlib.sha256).hexdigest()
        resp_hmac = await bare_client.post(
            "/api/v1/events",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": sig}
        )
        assert resp_hmac.status_code == 202


@pytest.mark.asyncio
async def test_keyset_cursor_pagination(client: AsyncClient):
    # Ingest 5 events
    for i in range(5):
        await client.post("/api/v1/events", json={
            "delivery_id": f"cursor-deliv-{i}",
            "event_type": "cursor.event",
            "data": {"seq": i}
        })

    # Page 1: limit 2
    p1 = await client.get("/api/v1/events?limit=2")
    assert p1.status_code == 200
    d1 = p1.json()
    assert len(d1["items"]) == 2
    assert d1["has_more"] is True
    assert d1["next_cursor"] is not None

    # Page 2: using cursor
    cursor = d1["next_cursor"]
    p2 = await client.get(f"/api/v1/events?limit=2&cursor={cursor}")
    assert p2.status_code == 200
    d2 = p2.json()
    assert len(d2["items"]) == 2
    # Ensure items in page 2 are different from page 1
    p1_ids = {item["id"] for item in d1["items"]}
    p2_ids = {item["id"] for item in d2["items"]}
    assert p1_ids.isdisjoint(p2_ids)


@pytest.mark.asyncio
async def test_retention_cleanup_endpoint(client: AsyncClient):
    from app.config import settings

    # Missing API key -> 401
    bad_resp = await client.post("/api/v1/events/retention/cleanup")
    assert bad_resp.status_code == 401

    # Valid API key -> 200
    ok_resp = await client.post(
        "/api/v1/events/retention/cleanup",
        headers={"X-API-Key": settings.REPLAY_API_KEY}
    )
    assert ok_resp.status_code == 200
    data = ok_resp.json()
    assert "outbox_deleted" in data
    assert "raw_events_deleted" in data


@pytest.mark.asyncio
async def test_system_telemetry_endpoint(client: AsyncClient):
    from app.config import settings

    unauth = await client.get("/api/v1/system/telemetry")
    assert unauth.status_code == 401

    resp = await client.get("/api/v1/system/telemetry", headers={"X-API-Key": settings.REPLAY_API_KEY})
    assert resp.status_code == 200
    data = resp.json()
    assert "components" in data
    assert "system_resources" in data
    assert "retention_policy" in data
    assert data["components"]["database"]["engine"] == "postgresql+asyncpg"


@pytest.mark.asyncio
async def test_outbox_recovery_after_publish_failure(client: AsyncClient, db_session):
    from unittest.mock import AsyncMock, patch

    from sqlalchemy import select

    from app.main import dispatch_pending_outbox
    from app.models import OutboxEvent
    from app.queue import rabbitmq_client

    # Simulate RabbitMQ down during ingest
    with patch.object(rabbitmq_client, "publish", new_callable=AsyncMock, side_effect=ConnectionError("RabbitMQ down")):
        resp = await client.post("/api/v1/events", json={
            "delivery_id": "outbox-recover-01",
            "event_type": "order.created",
            "data": {"order_id": "ord_rec"}
        })
        assert resp.status_code == 202

    outbox = (await db_session.execute(select(OutboxEvent))).scalar_one()
    assert outbox.published is False

    # RabbitMQ recovers -> reconciler drains outbox
    with patch.object(rabbitmq_client, "publish", new_callable=AsyncMock, return_value=True) as mock_pub:
        count = await dispatch_pending_outbox(db_session)
        assert count == 1
        assert mock_pub.await_count == 1

    await db_session.refresh(outbox)
    assert outbox.published is True


@pytest.mark.asyncio
async def test_replay_uses_outbox_when_rabbitmq_down(client: AsyncClient, db_session):
    from unittest.mock import AsyncMock, patch

    from sqlalchemy import select

    from app.config import settings
    from app.models import OutboxEvent, RawEvent
    from app.queue import rabbitmq_client

    resp = await client.post("/api/v1/events", json={
        "delivery_id": "replay-outbox-01",
        "event_type": "order.created",
        "data": {}
    })
    assert resp.status_code == 202

    raw_event = (await db_session.execute(
        select(RawEvent).where(RawEvent.delivery_id == "replay-outbox-01")
    )).scalar_one()
    raw_event.status = "failed"
    await db_session.commit()

    with patch.object(rabbitmq_client, "publish", new_callable=AsyncMock, side_effect=ConnectionError("RabbitMQ down")):
        replay_resp = await client.post(
            f"/api/v1/events/{raw_event.id}/replay",
            headers={"X-API-Key": settings.REPLAY_API_KEY}
        )
        assert replay_resp.status_code == 200

    await db_session.refresh(raw_event)
    assert raw_event.status == "received"
    pending = (await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.raw_event_id == raw_event.id, OutboxEvent.published == False)
    )).scalars().all()
    assert len(pending) == 1


@pytest.mark.asyncio
async def test_payload_size_cap_and_hmac_timestamp_window(client: AsyncClient):
    import hashlib
    import hmac
    import json
    import time

    from app.config import settings

    # Oversized payload rejected with 413
    oversized = b"x" * (settings.MAX_BODY_BYTES + 10)
    resp_413 = await client.post(
        "/api/v1/events",
        content=oversized,
        headers={"Content-Type": "application/json"}
    )
    assert resp_413.status_code == 413

    # Expired HMAC timestamp rejected with 401
    payload = json.dumps({"event_type": "ts.test", "data": {}}).encode("utf-8")
    sig = "sha256=" + hmac.new(settings.WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    old_ts = str(int(time.time()) - 3600)
    resp_ts = await client.post(
        "/api/v1/webhooks/github",
        content=payload,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Delivery": "gh-ts-expired",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": sig,
            "X-Webhook-Timestamp": old_ts,
        }
    )
    assert resp_ts.status_code == 401


@pytest.mark.asyncio
async def test_rate_limiting_returns_429(client: AsyncClient, monkeypatch):
    from app.config import settings
    from app.security import rate_limiter

    monkeypatch.setattr(settings, "RATE_LIMIT_ENABLED", True)
    monkeypatch.setattr(settings, "RATE_LIMIT_REQUESTS", 2)
    monkeypatch.setattr(settings, "RATE_LIMIT_WINDOW_SECONDS", 60)
    rate_limiter.reset()

    for i in range(2):
        r = await client.post("/api/v1/events", json={"delivery_id": f"rl-{i}", "event_type": "rl.test", "data": {}})
        assert r.status_code == 202

    r_limited = await client.post("/api/v1/events", json={"delivery_id": "rl-3", "event_type": "rl.test", "data": {}})
    assert r_limited.status_code == 429
    assert "Retry-After" in r_limited.headers


@pytest.mark.asyncio
async def test_list_events_sort_and_column_filters(client: AsyncClient):
    await client.post("/api/v1/events", json={"delivery_id": "sf-1", "source": "alpha", "event_type": "z.type", "data": {}})
    await client.post("/api/v1/events", json={"delivery_id": "sf-2", "source": "beta", "event_type": "a.type", "data": {}})

    # Sort by source asc
    r_sort = await client.get("/api/v1/events?sort_by=source&sort_dir=asc")
    assert r_sort.status_code == 200
    sources = [item["source"] for item in r_sort.json()["items"]]
    assert sources == ["alpha", "beta"]

    # Filter by outbox_published=true and event_id
    first_id = r_sort.json()["items"][0]["id"]
    r_filt = await client.get(f"/api/v1/events?event_id={first_id}&outbox_published=true")
    assert r_filt.status_code == 200
    assert r_filt.json()["total"] == 1
    assert r_filt.json()["items"][0]["id"] == first_id


@pytest.mark.asyncio
async def test_reset_ingestly_requires_auth_and_confirmation(client: AsyncClient):
    from app.config import settings

    await client.post("/api/v1/events", json={"delivery_id": "rst-1", "event_type": "rst.test", "data": {}})

    # Missing admin key -> 401
    r_unauth = await client.post("/api/v1/events/reset", json={"confirm": "clear-ingestly"})
    assert r_unauth.status_code == 401

    # Wrong confirmation string -> 400
    r_bad = await client.post(
        "/api/v1/events/reset",
        json={"confirm": "wrong"},
        headers={"X-API-Key": settings.REPLAY_API_KEY},
    )
    assert r_bad.status_code == 400

    # Valid admin key + exact confirmation -> 200 and empty table
    r_ok = await client.post(
        "/api/v1/events/reset",
        json={"confirm": "clear-ingestly"},
        headers={"X-API-Key": settings.REPLAY_API_KEY},
    )
    assert r_ok.status_code == 200
    assert r_ok.json()["raw_events_deleted"] >= 1

    r_list = await client.get("/api/v1/events")
    assert r_list.json()["total"] == 0







