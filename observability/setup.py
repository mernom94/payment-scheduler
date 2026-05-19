"""
observability/setup.py — Prometheus metrics and OpenTelemetry tracing.

Logging is handled separately in app/core/logging.py so that it is
available to workers and the API independently of the metrics/tracing stack.

Metrics naming convention
-------------------------
All metric names are snake_case with an application-scoped prefix.
Labels use past-tense outcomes so dashboards aggregate by result:
  outcome="succeeded" | "failed" | "dead"
  outcome="ok"        | "error"  | "not_leader"

Counter pairs
-------------
DLQ metrics use a counter pair (total / resolved) rather than a gauge so the
in-flight value (dlq_open = entries_total - resolved_total) never drifts or
resets on pod restarts.  The legacy DLQ_SIZE gauge is retained for dashboards
that cannot do arithmetic yet.
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Gauge, Histogram

from app.core.config import get_settings

# ── Scheduler metrics ─────────────────────────────────────────────────────────

SCHEDULER_RUNS_TOTAL = Counter(
    "scheduler_runs_total",
    "Total number of scheduler ticks",
    ["outcome"],  # ok | error | not_leader
)

JOBS_CREATED_TOTAL = Counter(
    "jobs_created_total",
    "Total ScheduledJobs created by the scheduler",
)

SCHEDULER_LAG_SECONDS = Gauge(
    "scheduler_lag_seconds",
    "Seconds between scheduled_for and the scheduler tick that created the job",
)

# ── Executor metrics ──────────────────────────────────────────────────────────

EXECUTOR_JOBS_TOTAL = Counter(
    "executor_jobs_total",
    "Total job_runs processed by the executor",
    ["outcome"],  # succeeded | failed | dead
)

JOB_LATENCY_SECONDS = Histogram(
    "job_latency_seconds",
    "End-to-end job execution latency from claimed_at to finished_at",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

RETRY_COUNTER = Counter(
    "job_retries_total",
    "Total job retry attempts scheduled",
)

EXECUTOR_THROUGHPUT = Gauge(
    "executor_jobs_per_second",
    "Rolling executor throughput (jobs/s over last window)",
)

# ── Recovery metrics ──────────────────────────────────────────────────────────

RECOVERY_RUNS_TOTAL = Counter(
    "recovery_runs_total",
    "Total stuck runs recovered (transitioned out of RUNNING)",
)

# ── DLQ metrics ───────────────────────────────────────────────────────────────

DLQ_ENTRIES_TOTAL = Counter(
    "dlq_entries_total",
    "Cumulative DLQ entries created",
)

DLQ_RESOLVED_TOTAL = Counter(
    "dlq_resolved_total",
    "Cumulative DLQ entries resolved (re-queued or dismissed)",
)

# Retained for backwards-compatibility with dashboards using gauge semantics.
# Refreshed from the DB on demand via the /dlq health endpoint.
DLQ_SIZE = Gauge(
    "dlq_size",
    "Current unresolved DLQ entry count (refreshed periodically; "
    "prefer dlq_entries_total - dlq_resolved_total for accuracy)",
)


def record_dlq_entry() -> None:
    """Increment DLQ counters.  Call whenever a new DLQ entry is created."""
    DLQ_ENTRIES_TOTAL.inc()
    DLQ_SIZE.inc()


def record_dlq_resolved() -> None:
    """Increment resolved counter.  Call whenever a DLQ entry is actioned."""
    DLQ_RESOLVED_TOTAL.inc()
    DLQ_SIZE.dec()


# ── OpenTelemetry tracing ─────────────────────────────────────────────────────

_tracer: trace.Tracer | None = None


def configure_tracing() -> trace.Tracer:
    """
    Initialise the OTLP tracer provider.

    Called lazily by get_tracer() on first use.  Idempotent — subsequent calls
    replace the global provider (acceptable since this is called once at startup).
    """
    s = get_settings()
    resource = Resource.create({"service.name": s.APP_NAME})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=s.OTLP_ENDPOINT, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(s.APP_NAME)


def get_tracer() -> trace.Tracer:
    """Return the singleton tracer, initialising it on first call."""
    global _tracer
    if _tracer is None:
        _tracer = configure_tracing()
    return _tracer
