"""
app/core/logging.py — Structured logging configuration.

Configures Python's standard logging to emit JSON lines in production
(easy to ingest into Datadog / CloudWatch / Loki) and human-readable text in
development.  Third-party library logs (SQLAlchemy, httpx, uvicorn) are
bridged through structlog so the entire log stream has a consistent format
— no mixed plain-text / JSON lines in the same output.

Usage
-----
Call configure_logging() exactly once at application startup, before any
logger is used.  The workers import this module and call it in their
main() entry points.

Structured context
------------------
Use structlog.contextvars.bind_contextvars() to attach per-request or
per-job context that will appear on every log line for that async task:

    structlog.contextvars.bind_contextvars(job_run_id=str(run.id))
    logger.info("executor.run_started")
    # → {"event": "executor.run_started", "job_run_id": "...", ...}

Always call structlog.contextvars.clear_contextvars() in a finally block.
"""

import logging
import sys
from typing import Optional

import structlog

from app.core.config import get_settings


def configure_logging() -> None:
    """
    Call once at application startup (before any log statements).

    - JSON renderer in production / log-sink environments (LOG_FORMAT=json).
    - ConsoleRenderer for local development (LOG_FORMAT=text).
    - Standard-library logging is bridged through structlog so third-party
      libraries (SQLAlchemy, httpx, uvicorn) emit the same JSON format.
    """
    s = get_settings()

    shared_processors: list = [
        # Merge any contextvars (job_run_id, worker_id, etc.) bound via
        # structlog.contextvars.bind_contextvars() into every log record.
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer = (
        structlog.processors.JSONRenderer()
        if s.LOG_FORMAT == "json"
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=shared_processors
        + [
            # Wrap for stdlib ProcessorFormatter so foreign (third-party) log
            # records flow through the same chain.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        # stdlib.BoundLogger (not bare BoundLogger) is required for the
        # stdlib bridge to work correctly.
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        # foreign_pre_chain processes log records from stdlib loggers
        # (SQLAlchemy, httpx, etc.) before the renderer runs.
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, s.LOG_LEVEL.upper(), logging.INFO))

    # Suppress noisy third-party loggers that fire on every request/query.
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
