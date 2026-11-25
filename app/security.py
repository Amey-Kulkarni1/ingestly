import base64
import hmac
import time
from collections import defaultdict, deque
from typing import Deque, Dict

from fastapi import HTTPException, Request, status

from app.config import settings


def _extract_bearer(request: Request) -> str | None:
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()
    return None


def _extract_basic_password(request: Request) -> str | None:
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Basic "):
        return None
    try:
        decoded = base64.b64decode(auth_header[6:].strip()).decode("utf-8")
        _, password = decoded.split(":", 1)
        return password
    except Exception:
        return None


def _is_valid_admin_token(token: str | None) -> bool:
    if not token or not settings.REPLAY_API_KEY:
        return False
    return hmac.compare_digest(token, settings.REPLAY_API_KEY)


def verify_admin_auth(request: Request) -> None:
    """Require REPLAY_API_KEY via X-API-Key, Bearer token, or HTTP Basic auth."""
    api_key = request.headers.get("x-api-key")
    bearer = _extract_bearer(request)
    basic_pw = _extract_basic_password(request)

    if any(_is_valid_admin_token(cand) for cand in (api_key, bearer, basic_pw)):
        return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized: valid admin API key required",
        headers={"WWW-Authenticate": 'Basic realm="Event Ingestion Console"'},
    )


async def read_body_capped(request: Request, max_bytes: int | None = None) -> bytes:
    """Read request body incrementally and abort with 413 before exceeding max_bytes."""
    limit = max_bytes if max_bytes is not None else settings.MAX_BODY_BYTES
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > limit:
                raise HTTPException(
                    status_code=413,
                    detail="Payload exceeds maximum allowed size",
                )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Content-Length header",
            )

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Payload exceeds maximum allowed size",
            )
        chunks.append(chunk)
    return b"".join(chunks)


class SlidingWindowRateLimiter:
    """In-memory per-client sliding-window rate limiter."""

    def __init__(self) -> None:
        self._hits: Dict[str, Deque[float]] = defaultdict(deque)

    def reset(self) -> None:
        self._hits.clear()

    def check(self, request: Request) -> None:
        if not settings.RATE_LIMIT_ENABLED:
            return
        client_ip = request.client.host if request.client else "unknown"
        key = request.headers.get("x-api-key") or client_ip
        now = time.monotonic()
        window = settings.RATE_LIMIT_WINDOW_SECONDS
        bucket = self._hits[key]

        while bucket and bucket[0] <= now - window:
            bucket.popleft()

        if len(bucket) >= settings.RATE_LIMIT_REQUESTS:
            retry_after = max(1, int(window - (now - bucket[0])))
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(retry_after)},
            )
        bucket.append(now)


rate_limiter = SlidingWindowRateLimiter()

