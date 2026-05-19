"""
tests/unit/test_state_machine.py — JobRunStateMachine unit tests.

Tests the state machine in isolation — no DB, no Redis, no network.
Every valid and invalid transition is covered, plus the error_message
truncation and the assert_in_state guard.
"""

import uuid
from unittest.mock import MagicMock

import pytest

from app.core.exceptions import InvalidJobStateError
from app.domain.jobs.state_machine import (
    VALID_RUN_TRANSITIONS,
    JobRunStateMachine,
)
from app.domain.models import JobRunStatus


def make_run(status: JobRunStatus, attempt: int = 1) -> MagicMock:
    """
    Create a minimal mock that looks like a JobRun ORM object.
    The state machine only reads/writes .status, .id, .attempt, .error_message.
    """
    run = MagicMock()
    run.id = uuid.uuid4()
    run.status = status.value
    run.attempt = attempt
    run.error_message = None
    return run


class TestValidTransitions:
    def test_pending_to_running(self):
        run = make_run(JobRunStatus.PENDING)
        JobRunStateMachine(run).transition_to(JobRunStatus.RUNNING)
        assert run.status == JobRunStatus.RUNNING

    def test_pending_to_failed(self):
        """Recovery path: reset stuck PENDING run to FAILED before next attempt."""
        run = make_run(JobRunStatus.PENDING)
        JobRunStateMachine(run).transition_to(JobRunStatus.FAILED)
        assert run.status == JobRunStatus.FAILED

    def test_running_to_succeeded(self):
        run = make_run(JobRunStatus.RUNNING)
        JobRunStateMachine(run).transition_to(JobRunStatus.SUCCEEDED)
        assert run.status == JobRunStatus.SUCCEEDED

    def test_running_to_failed(self):
        run = make_run(JobRunStatus.RUNNING)
        JobRunStateMachine(run).transition_to(JobRunStatus.FAILED)
        assert run.status == JobRunStatus.FAILED

    def test_running_to_dead(self):
        run = make_run(JobRunStatus.RUNNING)
        JobRunStateMachine(run).transition_to(JobRunStatus.DEAD)
        assert run.status == JobRunStatus.DEAD

    def test_failed_to_pending_retry(self):
        run = make_run(JobRunStatus.FAILED)
        JobRunStateMachine(run).transition_to(JobRunStatus.PENDING)
        assert run.status == JobRunStatus.PENDING

    def test_failed_to_dead_non_retryable(self):
        run = make_run(JobRunStatus.FAILED)
        JobRunStateMachine(run).transition_to(JobRunStatus.DEAD)
        assert run.status == JobRunStatus.DEAD


class TestInvalidTransitions:
    """Every invalid transition must raise InvalidJobStateError."""

    @pytest.mark.parametrize(
        "from_state, to_state",
        [
            # PENDING cannot skip to terminal states
            (JobRunStatus.PENDING, JobRunStatus.SUCCEEDED),
            (JobRunStatus.PENDING, JobRunStatus.DEAD),
            # RUNNING cannot go back to PENDING
            (JobRunStatus.RUNNING, JobRunStatus.PENDING),
            # SUCCEEDED is terminal
            (JobRunStatus.SUCCEEDED, JobRunStatus.PENDING),
            (JobRunStatus.SUCCEEDED, JobRunStatus.RUNNING),
            (JobRunStatus.SUCCEEDED, JobRunStatus.FAILED),
            (JobRunStatus.SUCCEEDED, JobRunStatus.DEAD),
            # DEAD is terminal
            (JobRunStatus.DEAD, JobRunStatus.PENDING),
            (JobRunStatus.DEAD, JobRunStatus.RUNNING),
            (JobRunStatus.DEAD, JobRunStatus.FAILED),
            (JobRunStatus.DEAD, JobRunStatus.SUCCEEDED),
        ],
    )
    def test_invalid_transition_raises(
        self, from_state: JobRunStatus, to_state: JobRunStatus
    ):
        run = make_run(from_state)
        with pytest.raises(InvalidJobStateError) as exc_info:
            JobRunStateMachine(run).transition_to(to_state)
        assert exc_info.value.current == from_state.value
        assert exc_info.value.attempted == to_state.value


class TestErrorMessageHandling:
    def test_error_message_stored_on_failed_transition(self):
        run = make_run(JobRunStatus.RUNNING)
        JobRunStateMachine(run).transition_to(
            JobRunStatus.FAILED, error_message="bunq timeout"
        )
        assert run.error_message == "bunq timeout"

    def test_error_message_stored_on_dead_transition(self):
        run = make_run(JobRunStatus.RUNNING)
        JobRunStateMachine(run).transition_to(
            JobRunStatus.DEAD, error_message="exhausted all retries"
        )
        assert run.error_message == "exhausted all retries"

    def test_error_message_truncated_to_max_len(self):
        from app.core.constants import ERROR_MESSAGE_MAX_LEN
        run = make_run(JobRunStatus.RUNNING)
        long_message = "x" * (ERROR_MESSAGE_MAX_LEN + 500)
        JobRunStateMachine(run).transition_to(
            JobRunStatus.FAILED, error_message=long_message
        )
        assert len(run.error_message) == ERROR_MESSAGE_MAX_LEN

    def test_error_message_not_set_on_succeeded_transition(self):
        """error_message should not be touched for non-failure transitions."""
        run = make_run(JobRunStatus.RUNNING)
        run.error_message = None
        JobRunStateMachine(run).transition_to(
            JobRunStatus.SUCCEEDED, error_message="should be ignored"
        )
        assert run.error_message is None


class TestCanTransitionTo:
    def test_returns_true_for_valid_transition(self):
        run = make_run(JobRunStatus.PENDING)
        machine = JobRunStateMachine(run)
        assert machine.can_transition_to(JobRunStatus.RUNNING) is True

    def test_returns_false_for_invalid_transition(self):
        run = make_run(JobRunStatus.SUCCEEDED)
        machine = JobRunStateMachine(run)
        assert machine.can_transition_to(JobRunStatus.PENDING) is False

    def test_returns_false_for_all_transitions_from_dead(self):
        run = make_run(JobRunStatus.DEAD)
        machine = JobRunStateMachine(run)
        for state in JobRunStatus:
            assert machine.can_transition_to(state) is False


class TestAssertInState:
    def test_passes_when_in_allowed_state(self):
        run = make_run(JobRunStatus.RUNNING)
        # Should not raise
        JobRunStateMachine(run).assert_in_state(JobRunStatus.RUNNING)

    def test_passes_when_in_one_of_multiple_allowed_states(self):
        run = make_run(JobRunStatus.FAILED)
        JobRunStateMachine(run).assert_in_state(
            JobRunStatus.RUNNING, JobRunStatus.FAILED
        )

    def test_raises_when_not_in_allowed_state(self):
        run = make_run(JobRunStatus.PENDING)
        with pytest.raises(InvalidJobStateError):
            JobRunStateMachine(run).assert_in_state(JobRunStatus.RUNNING)


class TestTransitionTableCompleteness:
    """
    Meta-test: ensure every JobRunStatus appears as a key in VALID_RUN_TRANSITIONS.
    Adding a new status without updating the transition table will fail this test.
    """

    def test_all_states_have_transition_entry(self):
        for state in JobRunStatus:
            assert state in VALID_RUN_TRANSITIONS, (
                f"{state!r} is missing from VALID_RUN_TRANSITIONS.  "
                f"Add it even if it transitions to an empty set (terminal state)."
            )
