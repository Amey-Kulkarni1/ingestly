import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional


class SignatureVerificationError(Exception):
    pass


def verify_hmac_timestamp(timestamp_header: Optional[str], tolerance_seconds: int = 300) -> bool:
    """Validate optional webhook timestamp header to prevent replay attacks."""
    if not timestamp_header:
        return True
    try:
        ts = float(timestamp_header)
    except ValueError:
        return False
    return abs(time.time() - ts) <= tolerance_seconds


def verify_hmac_sha256(secret: str, raw_body: bytes, signature_header: str) -> bool:
    if not secret or not signature_header:
        return False

    prefix = "sha256="
    actual_sig = signature_header[len(prefix):] if signature_header.startswith(prefix) else signature_header

    expected_sig = hmac.new(
        key=secret.encode("utf-8"),
        msg=raw_body,
        digestmod=hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(actual_sig, expected_sig)


class WebhookPayload:
    def __init__(self, delivery_id: Optional[str], source: str, event_type: str, data: Dict[str, Any]):
        self.delivery_id = delivery_id
        self.source = source
        self.event_type = event_type
        self.data = data


class GitHubWebhookAdapter:
    source = "github"

    @staticmethod
    def parse_and_verify(raw_body: bytes, headers: Dict[str, str], secret: Optional[str] = None) -> WebhookPayload:
        sig_header = headers.get("x-hub-signature-256")
        if secret:
            if not sig_header or not verify_hmac_sha256(secret, raw_body, sig_header):
                raise SignatureVerificationError("Invalid or missing X-Hub-Signature-256")

        try:
            parsed = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Malformed JSON body: {exc}")

        delivery_id = headers.get("x-github-delivery") or parsed.get("delivery_id")
        event_name = headers.get("x-github-event") or parsed.get("event_type", "webhook")
        event_type = f"github.{event_name}" if not str(event_name).startswith("github.") else event_name

        return WebhookPayload(
            delivery_id=delivery_id,
            source="github",
            event_type=event_type,
            data=parsed
        )


class GenericWebhookAdapter:
    source = "generic"

    @staticmethod
    def parse_and_verify(raw_body: bytes, headers: Dict[str, str], secret: Optional[str] = None) -> WebhookPayload:
        sig_header = headers.get("x-signature-256") or headers.get("x-hub-signature-256")
        if sig_header and secret:
            if not verify_hmac_sha256(secret, raw_body, sig_header):
                raise SignatureVerificationError("Invalid HMAC signature")

        try:
            parsed = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Malformed JSON body: {exc}")

        return WebhookPayload(
            delivery_id=parsed.get("delivery_id"),
            source=parsed.get("source", "fake-generator"),
            event_type=parsed.get("event_type", "generic.event"),
            data=parsed.get("data", parsed)
        )


ADAPTERS = {
    "github": GitHubWebhookAdapter,
    "generic": GenericWebhookAdapter,
}


def resolve_adapter(headers: Dict[str, str]):
    if "x-github-event" in headers or "x-github-delivery" in headers:
        return GitHubWebhookAdapter
    return GenericWebhookAdapter

