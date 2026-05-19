"""
app/main.py — FastAPI application factory.

Responsibilities:
  - Lifespan: init DB, configure logging/tracing, shut down cleanly.
  - Exception handlers: translate domain exceptions to HTTP responses so that
    HTTP status code decisions are never made inside domain or worker code.
  - Route registration.
  - Prometheus metrics mount.
"""

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from app.api.routes import dlq, health, jobs, subscriptions
from app.core.config import get_settings
from app.core.exceptions import (
    JobNotFoundError,
    SchedulerError,
    SubscriptionNotFoundError,
    SubscriptionValidationError,
)
from app.core.logging import configure_logging
from app.infrastructure.db.session import close_db, init_db
from observability.setup import configure_tracing


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager.

    Startup order matters:
      1. configure_logging() — so all subsequent startup messages are structured.
      2. init_db() — pool ready before any request handler runs.
      3. configure_tracing() — OTLP exporter connected.

    Shutdown order (reverse):
      1. close_db() — drain pool connections.
    """
    configure_logging()
    await init_db()
    configure_tracing()
    yield
    await close_db()


def create_app() -> FastAPI:
    s = get_settings()
    app = FastAPI(
        title="Payment Scheduler",
        version="1.0.0",
        debug=s.DEBUG,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://backend-portfolio-two-ebon.vercel.app/", 
            "http://localhost:3000",              
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Exception handlers ────────────────────────────────────────────────────
    # Translate domain exceptions → HTTP responses here, not in route handlers.
    # This keeps HTTP status code logic at the API boundary and out of domain /
    # infrastructure / worker code.

    @app.exception_handler(SubscriptionNotFoundError)
    async def subscription_not_found_handler(
        request: Request, exc: SubscriptionNotFoundError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"detail": exc.message},
        )

    @app.exception_handler(JobNotFoundError)
    async def job_not_found_handler(
        request: Request, exc: JobNotFoundError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"detail": exc.message},
        )

    @app.exception_handler(SubscriptionValidationError)
    async def subscription_validation_handler(
        request: Request, exc: SubscriptionValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"detail": exc.message},
        )

    @app.exception_handler(SchedulerError)
    async def scheduler_error_handler(
        request: Request, exc: SchedulerError
    ) -> JSONResponse:
        # Catch-all for any unhandled domain error.  Returns 500 so the client
        # knows not to retry immediately; structured logging captures the full
        # traceback via the middleware / uvicorn error handler.
        return JSONResponse(
            status_code=500,
            content={"detail": "An internal error occurred."},
        )

    # ── Routes ────────────────────────────────────────────────────────────────

    app.include_router(
        subscriptions.router,
        prefix="/subscriptions",
        tags=["subscriptions"],
    )
    app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
    app.include_router(dlq.router, prefix="/dlq", tags=["dlq"])
    app.include_router(health.router, prefix="/health", tags=["health"])

    # Prometheus scrape endpoint — mounted last so it cannot conflict with app routes.
    app.mount("/metrics", make_asgi_app())

    return app


app = create_app()
