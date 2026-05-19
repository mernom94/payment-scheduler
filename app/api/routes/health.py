"""
Health check endpoints — used by load balancers and k8s probes.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.session import get_db

router = APIRouter()


@router.get("/live")
async def liveness():
    """Always returns 200 — process is alive."""
    return {"status": "ok"}


@router.get("/ready")
async def readiness(db: AsyncSession = Depends(get_db)):
    """Returns 200 only when DB is reachable."""
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ready", "db": "ok"}
    except Exception as exc:
        return {"status": "not_ready", "db": str(exc)}, 503
