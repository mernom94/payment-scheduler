"""
tests/unit/test_leader_election.py — LeaderElection unit tests.

Tests the Redis-based leader election logic with a mocked Redis client.
No real Redis required.
"""

import asyncio
import uuid

import pytest
import pytest_asyncio

from app.infrastructure.redis.leader import LeaderElection


@pytest.fixture
def election(mock_redis):
    return LeaderElection(mock_redis)


class TestTryAcquire:
    @pytest.mark.asyncio
    async def test_acquire_succeeds_when_redis_set_returns_true(
        self, election, mock_redis
    ):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1

        result = await election.try_acquire()

        assert result is True
        assert election.is_leader is True
        assert election.epoch == 1

    @pytest.mark.asyncio
    async def test_acquire_fails_when_redis_set_returns_none(
        self, election, mock_redis
    ):
        mock_redis.set.return_value = None  # Redis SET NX returns None on conflict

        result = await election.try_acquire()

        assert result is False
        assert election.is_leader is False

    @pytest.mark.asyncio
    async def test_already_leader_returns_true_without_re_acquiring(
        self, election, mock_redis
    ):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1
        await election.try_acquire()

        # Reset call counts to verify second call does not hit Redis
        mock_redis.set.reset_mock()
        result = await election.try_acquire()

        assert result is True
        mock_redis.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_acquire_starts_heartbeat_task(self, election, mock_redis):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1

        await election.try_acquire()

        assert election._heartbeat_task is not None
        assert not election._heartbeat_task.done()

        # Cleanup
        election._heartbeat_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await election._heartbeat_task


class TestVerifyStillLeader:
    @pytest.mark.asyncio
    async def test_returns_true_when_token_matches(self, election, mock_redis):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1
        await election.try_acquire()

        mock_redis.get.return_value = election._token.encode()
        result = await election.verify_still_leader()

        assert result is True
        assert election.is_leader is True

    @pytest.mark.asyncio
    async def test_returns_false_when_token_mismatches(self, election, mock_redis):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1
        await election.try_acquire()

        mock_redis.get.return_value = b"other-leader-token"
        result = await election.verify_still_leader()

        assert result is False
        assert election.is_leader is False

    @pytest.mark.asyncio
    async def test_returns_false_when_key_expired(self, election, mock_redis):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1
        await election.try_acquire()

        mock_redis.get.return_value = None  # Key expired
        result = await election.verify_still_leader()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_not_leader(self, election, mock_redis):
        """verify_still_leader short-circuits when is_leader is False."""
        assert election.is_leader is False
        result = await election.verify_still_leader()
        assert result is False
        mock_redis.get.assert_not_called()


class TestRelease:
    @pytest.mark.asyncio
    async def test_release_clears_is_leader(self, election, mock_redis):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1
        mock_redis.eval.return_value = 1
        await election.try_acquire()

        assert election.is_leader is True
        await election.release()
        assert election.is_leader is False

    @pytest.mark.asyncio
    async def test_release_is_noop_when_not_leader(self, election, mock_redis):
        """release() must be safe to call even when not the leader."""
        await election.release()  # should not raise
        mock_redis.eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_release_cancels_heartbeat_task(self, election, mock_redis):
        mock_redis.set.return_value = True
        mock_redis.incr.return_value = 1
        mock_redis.eval.return_value = 1
        await election.try_acquire()

        heartbeat_task = election._heartbeat_task
        await election.release()

        assert heartbeat_task.done()
