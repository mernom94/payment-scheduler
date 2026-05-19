"""
app/domain/jobs/state_machine.py — Job and JobRun state machine.

A pure-Python, side-effect-free state machine with no ORM or framework
dependencies.  It only knows about valid transitions and raises
InvalidJobStateError for illegal ones.  All persistence is the caller's
responsibility.

Having this as a separate, stateless module makes it trivially unit-testable:
no DB, no Redis, no HTTP required.

Valid JobRun transitions
------------------------
    PENDING   → RUNNING    (executor claims the run)
    PENDING   → FAILED     (recovery: resets a stuck run before creating next attempt)
    RUNNING   → SUCCEEDED  (bunq call succeeded, idempotency key inserted)
    RUNNING   → FAILED     (bunq call failed, retry will be scheduled)
    RUNNING   → DEAD       (retries exhausted; moved to DLQ)
    FAILED    → PENDING    (retry: new JobRun row created by executor/recovery)
    FAILED    → DEAD       (non-retryable failure; moved to DLQ immediately)

Terminal states: SUCCEEDED, DEAD.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from app.core.exceptions import InvalidJobStateError
from app.domain.models import JobRunStatus

if TYPE_CHECKING:
    # Avoid importing ORM at module level — keeps the domain layer
    # framework-free and importable without a running DB.
    from app.infrastructure.db.models import JobRun

logger = logging.getLogger(__name__)

# Valid transitions.  Key = current state, value = allowed next states.
VALID_RUN_TRANSITIONS: dict[JobRunStatus, set[JobRunStatus]] = {
    JobRunStatus.PENDING: {
        JobRunStatus.RUNNING,
        JobRunStatus.FAILED,   # recovery path: reset to FAILED before next attempt
    },
    JobRunStatus.RUNNING: {
        JobRunStatus.SUCCEEDED,
        JobRunStatus.FAILED,
        JobRunStatus.DEAD,
    },
    JobRunStatus.SUCCEEDED: set(),  # Terminal
    JobRunStatus.FAILED: {
        JobRunStatus.PENDING,       # retry: executor schedules a new run
        JobRunStatus.DEAD,          # non-retryable: immediately to DLQ
    },
    JobRunStatus.DEAD: set(),       # Terminal (DLQ)
}


class JobRunStateMachine:
    """
    Validates and applies state transitions to a JobRun ORM object.

    Usage::

        machine = JobRunStateMachine(run)
        machine.transition_to(JobRunStatus.RUNNING)
        # run.status is now "RUNNING" — caller must flush/commit to DB.

    The machine mutates the ORM object in-place so that the caller's active
    session tracks the change and includes it in the next flush/commit.
    """

    def __init__(self, run: "JobRun") -> None:
        self._run = run

    @property
    def current_state(self) -> JobRunStatus:
        return JobRunStatus(self._run.status)

    def can_transition_to(self, target: JobRunStatus) -> bool:
        """Return True if the transition is valid without raising."""
        allowed = VALID_RUN_TRANSITIONS.get(self.current_state, set())
        return target in allowed

    def transition_to(
        self,
        target: JobRunStatus,
        *,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Apply a state transition, mutating the JobRun object.

        Raises InvalidJobStateError if the transition is not permitted.
        The caller is responsible for flushing/committing the session.

        Args:
            target: The desired next state.
            error_message: Stored in run.error_message when transitioning to
                FAILED or DEAD.  Truncated to ERROR_MESSAGE_MAX_LEN.
        """
        if not self.can_transition_to(target):
            raise InvalidJobStateError(
                current=self._run.status,
                attempted=target.value,
            )

        from app.core.constants import ERROR_MESSAGE_MAX_LEN

        previous = self._run.status
        self._run.status = target.value

        if error_message and target in (JobRunStatus.FAILED, JobRunStatus.DEAD):
            self._run.error_message = error_message[:ERROR_MESSAGE_MAX_LEN]

        logger.info(
            "job_run.state_transition",
            extra={
                "job_run_id": str(self._run.id),
                "from_state": previous,
                "to_state": target.value,
                "attempt": self._run.attempt,
            },
        )

    def assert_in_state(self, *allowed_states: JobRunStatus) -> None:
        """
        Assert the run is in one of the allowed states.
        Useful as a precondition guard at the start of a processing method.

        Raises InvalidJobStateError if the current state is not in the list.
        """
        if self.current_state not in allowed_states:
            raise InvalidJobStateError(
                current=self._run.status,
                attempted=f"one of {[s.value for s in allowed_states]}",
            )
