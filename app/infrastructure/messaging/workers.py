"""
app/infrastructure/messaging/workers.py — Base worker class.

All background workers inherit from BaseWorker.  Provides:

  - Graceful shutdown via stop() — sets a flag checked between ticks.
    The current tick always runs to completion before the worker exits.
  - Error isolation: uncaught exceptions inside tick() are logged and the
    loop continues.  Workers never die from a transient error.
  - Consistent startup / shutdown structured logging.
  - Configurable poll interval (set as a class attribute or overridden at
    init time).

Usage::

    class ExecutorWorker(BaseWorker):
        name = "executor_worker"

        @property
        def poll_interval(self) -> float:
            return get_settings().EXECUTOR_POLL_INTERVAL_S

        async def tick(self) -> None:
            await self._process_batch()
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Abstract base class for all background workers.

    Subclasses must:
      - Set ``name`` (used in log messages and task names).
      - Implement ``tick()`` — called once per poll cycle.
      - Define ``poll_interval`` (seconds between ticks) as a property or
        class attribute.

    Subclasses should NOT override ``run()`` unless they need fundamentally
    different loop semantics (e.g. event-driven rather than poll-based).
    """

    #: Human-readable worker name used in log events and asyncio task names.
    name: str = "worker"

    @property
    def poll_interval(self) -> float:
        """Seconds to sleep between ticks.  Override in subclass."""
        return 5.0

    def __init__(self) -> None:
        self._stop: bool = False

    def stop(self) -> None:
        """
        Signal the worker to stop after the current tick completes.

        Thread-safe: can be called from a signal handler or another coroutine.
        Does not cancel the current tick — the worker drains cleanly.
        """
        logger.info("%s.stop_requested", self.name)
        self._stop = True

    async def run(self) -> None:
        """
        Main event loop.  Calls tick() repeatedly until stop() is called.

        Exceptions raised by tick() are caught, logged, and the loop continues.
        asyncio.CancelledError propagates immediately (allows task cancellation).
        """
        logger.info("%s.started", self.name)

        try:
            while not self._stop:
                try:
                    await self.tick()

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "%s.tick_failed — worker will continue after next sleep",
                        self.name,
                    )

                if not self._stop:
                    await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            logger.info("%s.cancelled", self.name)
            raise

        finally:
            logger.info("%s.stopped", self.name)

    @abstractmethod
    async def tick(self) -> None:
        """
        Single unit of work.  Called once per poll cycle.

        Must be idempotent: if the process crashes mid-tick, the next tick
        should be able to resume or detect and skip already-completed work.
        """
