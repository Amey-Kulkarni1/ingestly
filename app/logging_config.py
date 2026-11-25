import contextvars
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

correlation_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="-")


class JsonLogFormatter(logging.Formatter):
    """Formats log records as single-line JSON with correlation_id."""

    def __init__(self, service: str = "api") -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "correlation_id": correlation_id_ctx.get(),
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, default=str)


def configure_logging(service: str = "api", level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter(service=service))
    root.handlers = [handler]

