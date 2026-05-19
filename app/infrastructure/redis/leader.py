"""
app/infrastructure/redis/leader.py — Redis-based leader election with fencing tokens.

Algorithm
---------
1. Candidate attempts ``SET NX PX <ttl>`` with a unique token.
2. If acquired: record epoch (monotonically increasing integer in Redis) and
   start a background heartbeat loop that renews the TTL via a Lua CAS script.
3. Fencing token (``leader_epoch``) is written to every DB row the leader
   creates, allowing downstream consumers to detect and discard stale writes
   from a deposed leader.
4. On loss or crash: another candidate wins, increments the epoch, and takes
   over.

Correctness properties
----------------------
- ``try_acquire`` uses SET NX — at most one winner per TTL window.
- Heartbeat renewal uses a Lua CAS (GET → PEXPIRE) — a deposed leader can
  never renew after another leader has taken the key.
- ``verify_still_leader`` performs a GET before any critical DB write,
  tightening the split-brain window to the execution gap between the GET and
  the first DB write (typically < 1 ms in the same event loop tick).
- ``release`` uses a Lua CAS DELETE — a deposed leader cannot accidentally
  delete the new leader's key.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import redis.asyncio as aioredis

from app.core.config import get_settings
from app.core.constants import REDIS_LEADER_EPOCH_KEY, REDIS_LEADER_KEY
from app.core.exceptions import LeaderElectionError

logger = logging.getLogger(__name__)


class LeaderElection:
    """
    Async leader election backed by Redis.

    Typical usage in a worker::

        election = LeaderElection(create_redis())
        try:
            while running:
                await election.try_acquire()
                if election.is_leader:
                    if not await election.verify_still_leader():
                        continue
                    await do_leader_work(epoch=election.epoch)
                await asyncio.sleep(interval)
        finally:
            await election.release()
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        # Token uniquely identifies this process instance.  Using pid + monotonic
        # nanoseconds gives a high-entropy token that survives pod restarts within
        # the same second.
        self._token = f"{os.getpid()}:{time.monotonic_ns()}"
        self._epoch: int = 0
        self._is_leader: bool = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def is_leader(self) -> bool:
        """True if this instance currently holds the leader lock."""
        return self._is_leader

    @property
    def epoch(self) -> int:
        """
        Monotonically increasing fencing token for the current leadership term.

        Write this to every DB row created during leadership so that a new
        leader with a higher epoch can identify and ignore stale rows.
        """
        return self._epoch

    async def try_acquire(self) -> bool:
        """
        Attempt to become leader.

        Returns True if leadership was acquired, False if another instance
        already holds the lock.  Safe to call repeatedly — subsequent calls
        while already the leader are no-ops (returns True without re-acquiring).
        """
        if self._is_leader:
            return True

        s = get_settings()
        acquired = await self._redis.set(
            REDIS_LEADER_KEY,
            self._token,
            nx=True,
            px=s.LEADER_LOCK_TTL_MS,
        )
        if acquired:
            self._epoch = await self._increment_epoch()
            self._is_leader = True
            logger.info(
                "leader.acquired",
                extra={"epoch": self._epoch, "token": self._token[:16]},
            )
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name=f"leader-heartbeat-epoch-{self._epoch}",
            )
        return bool(acquired)

    async def release(self) -> None:
        """
        Voluntarily release the leader lock.

        Safe to call even if this instance is not the leader (no-op).
        Cancels the heartbeat task before releasing so the TTL is not renewed
        after the key is deleted.
        """
        if not self._is_leader:
            return

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        await self._release_if_owner()
        self._is_leader = False
        logger.info("leader.released", extra={"epoch": self._epoch})

    async def verify_still_leader(self) -> bool:
        """
        Re-check ownership immediately before a critical DB write.

        Reduces the split-brain window: the heartbeat loop renews the TTL
        but does not guarantee continuous ownership between renewals.  Calling
        this just before writing ensures we hold the key at that instant.

        Returns False (and clears is_leader) if the key has been taken by
        another instance.  The caller should abort the write.
        """
        if not self._is_leader:
            return False

        current = await self._redis.get(REDIS_LEADER_KEY)
        still_leader = current == self._token.encode()

        if not still_leader:
            # Cancel the heartbeat — it will fail anyway but cleaner to stop it.
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            self._is_leader = False
            logger.warning(
                "leader.lost_on_verify",
                extra={"epoch": self._epoch},
            )

        return still_leader

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """
        Periodically renew the leader TTL via a Lua CAS script.

        Exits when leadership is lost or the task is cancelled.
        """
        s = get_settings()
        interval = s.LEADER_HEARTBEAT_INTERVAL_S
        while self._is_leader:
            await asyncio.sleep(interval)
            if not self._is_leader:
                return
            renewed = await self._renew()
            if not renewed:
                self._is_leader = False
                logger.warning(
                    "leader.heartbeat_failed",
                    extra={"epoch": self._epoch},
                )
                return
            logger.debug("leader.heartbeat_ok", extra={"epoch": self._epoch})

    async def _renew(self) -> bool:
        """
        Extend TTL only if we still hold the key (Lua CAS).

        Returns False if another instance has taken the key.
        """
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        s = get_settings()
        result = await self._redis.eval(
            script, 1, REDIS_LEADER_KEY, self._token, s.LEADER_LOCK_TTL_MS
        )
        return bool(result)

    async def _release_if_owner(self) -> None:
        """
        Delete the leader key only if our token still matches (Lua CAS).

        Prevents a deposed leader from accidentally deleting the new leader's key.
        """
        script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """
        await self._redis.eval(script, 1, REDIS_LEADER_KEY, self._token)

    async def _increment_epoch(self) -> int:
        return int(await self._redis.incr(REDIS_LEADER_EPOCH_KEY))


def create_redis() -> aioredis.Redis:
    """Create a Redis connection from settings."""
    return aioredis.from_url(get_settings().REDIS_URL, decode_responses=False)
