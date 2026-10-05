"""Standard-library logging with run context fields.

Job code wraps its work in ``log_context(run_id=..., dataset=..., object_key=...)``;
every record emitted inside carries those fields as ``key=value`` pairs.
"""

import contextlib
import logging
import sys
from collections.abc import Generator
from contextvars import ContextVar
from datetime import UTC, datetime

_context: ContextVar[dict[str, str] | None] = ContextVar("evedw_log_context", default=None)

CONTEXT_KEYS = ("run_id", "job", "dataset", "object_key")


def _current() -> dict[str, str]:
    return _context.get() or {}


@contextlib.contextmanager
def log_context(**fields: str | None) -> Generator[None]:
    merged = dict(_current())
    for key, value in fields.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value
    token = _context.set(merged)
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, str]:
    return dict(_current())


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _current().items():
            setattr(record, key, value)
        return True


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
        parts = [timestamp, record.levelname.lower(), record.name, record.getMessage()]
        for key in CONTEXT_KEYS:
            value = getattr(record, key, None)
            if value is not None:
                parts.append(f"{key}={value}")
        line = " ".join(parts)
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    if any(isinstance(h, _Handler) for h in root.handlers):
        return
    handler = _Handler(sys.stderr)
    handler.setFormatter(KeyValueFormatter())
    handler.addFilter(ContextFilter())
    root.addHandler(handler)


class _Handler(logging.StreamHandler):  # type: ignore[type-arg]
    """Marker subclass so setup_logging is idempotent."""
