"""Logging configuration and contextual fields."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

_log_context: ContextVar[dict[str, Any]] = ContextVar("telecrime_log_context", default={})


class ContextFilter(logging.Filter):
    """Inject contextual fields into log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _log_context.get({})
        for key, value in ctx.items():
            setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    """JSON log formatter with optional context fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in ("args", "msg", "name", "levelname", "levelno", "pathname", "filename",
                       "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
                       "created", "msecs", "relativeCreated", "thread", "threadName",
                       "processName", "process"):
                continue
            if key not in payload and isinstance(value, (str, int, float, bool, type(None))):
                payload[key] = value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True)


def configure_logging(json_logs: bool | None = None) -> None:
    """Configure root logging with optional JSON formatting."""
    if json_logs is None:
        json_logs = os.environ.get("TELECRIME_LOG_JSON", "").lower() in ("1", "true", "yes")

    handler = logging.StreamHandler()
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(message)s"))

    root = logging.getLogger()
    root.handlers = []
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.addFilter(ContextFilter())

    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Temporarily add contextual fields to log records."""
    current = _log_context.get({})
    merged = {**current, **fields}
    token = _log_context.set(merged)
    try:
        yield
    finally:
        _log_context.reset(token)
