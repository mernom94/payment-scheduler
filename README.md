# Payment Scheduler

A correctness-focused distributed backend for executing recurring financial payments against the [bunq](https://www.bunq.com/) sandbox API. Built with financial correctness as the primary constraint — not throughput, not developer convenience.

The system implements the reliability primitives that scheduled payment infrastructure demands: leader-elected scheduling with fencing tokens, exactly-once external execution semantics via deterministic idempotency keys, `SELECT FOR UPDATE SKIP LOCKED` for concurrent worker safety, CAS-based crash recovery, and DAG dependency ordering across schedule cycles. These are not optional features — they are load-bearing correctness guarantees without which money moves incorrectly under failure.

---

## Why This Exists

Scheduling a recurring payment (call bunq on a cron schedule → store the result) appears simple. It becomes dangerous the moment you consider distributed failure modes:

- What happens if the scheduler runs on two nodes simultaneously and creates two jobs for the same subscription window?
- What if a worker crashes after bunq accepts the payment but before the database is updated?
- What if two executor instances race to claim and execute the same job?
- What if a retry fires the same payment a second time and the idempotency key is wrong?
- What if a DAG downstream job executes before its upstream dependency completes?

Every one of these scenarios results in either a duplicate payment (money sent twice) or a lost/inconsistent state (money sent, but marked as failed). Neither is acceptable. This project addresses each failure mode explicitly, with corresponding tests.

---

## Reliability & Correctness Goals

| Goal | Mechanism |
|---|---|
| No duplicate jobs per subscription window | `UNIQUE(subscription_id, scheduled_for)` + `ON CONFLICT DO NOTHING` |
| No duplicate external payments on retry | Deterministic idempotency key per `(job_run_id, attempt)` sent as `X-Bunq-Client-Request-Id` |
| No two workers executing the same run | `SELECT FOR UPDATE SKIP LOCKED` + atomic claim with `lock_expires_at` |
| Single active scheduler / recovery | Redis `SET NX PX` leader election with heartbeat renewal |
| Split-brain write prevention | `verify_still_leader()` before every write batch + `created_by_epoch` fencing token on every row |
| Worker crash recovery | `lock_expires_at` expiry detected by recovery worker; CAS-style reset to `PENDING` |
| No concurrent recovery race | CAS UPDATE re-checks `status=RUNNING AND lock_expires_at < now` atomically |
| DAG ordering correctness | Dependency check scoped to current `scheduled_for` window; prior cycle's `DONE` rows do not satisfy current cycle |
| Subscription protection on sustained failure | Auto-pause after 3 consecutive DLQ entries; `consecutive_failures` reset on any success |

---

## Features

### Subscription Scheduling

- Accepts subscription creation via REST API with `cron_expression`, `timezone`, and `payment_config`
- Scheduler worker evaluates subscriptions with `next_fire_at <= now + lookahead_window` each tick
- Creates `scheduled_jobs` and initial `job_runs` in a single transaction via `INSERT … ON CONFLICT DO NOTHING`
- Advances `next_fire_at` using `croniter` with full timezone awareness (DST-correct via `pytz`)
- Subscriptions have three states: `ACTIVE`, `PAUSED`, `CANCELLED`; only `ACTIVE` subscriptions are scheduled
- Invalid cron expressions that slip past creation-time validation pause the subscription rather than spinning the scheduler

### Idempotency Guarantees

- Two-layer idempotency on every payment execution:
  1. **External layer** — `X-Bunq-Client-Request-Id` set to `sha256(job_run_id:attempt)` ensures bunq returns the prior result for any repeated call with the same key rather than creating a new payment
  2. **Internal gate** — `INSERT INTO idempotency_keys … ON CONFLICT DO NOTHING RETURNING key` is the atomicity gate: only the worker that inserts the key proceeds to update `job_run` and `scheduled_job` status; any concurrent worker detects the conflict via `NULL` return and exits without writing state
- The pre-check + write pattern (TOCTOU) was explicitly avoided: the `INSERT … RETURNING` is a single atomic operation, not a read-then-write

### Retry System with Exponential Backoff

- Failed runs are rescheduled with jittered exponential backoff: `min(base * 2^(attempt-1), max_backoff) × uniform(0.5, 1.0)` seconds
- Retry delay is enforced at the database layer via `retry_after` column — the executor claim query hard-filters `retry_after IS NULL OR retry_after <= now`; no application restart or new worker instance can bypass the backoff
- Policy is per-subscription, stored as JSONB: `max_attempts`, `base_backoff_s`, `max_backoff_s`, `jitter` — unknown keys silently ignored for forward compatibility
- After `max_attempts`, the run is moved to the DLQ and `consecutive_failures` is incremented on the subscription

### Leader Election with Fencing Tokens

- Scheduler and recovery workers compete for leadership via Redis `SET NX PX` — only one instance wins at a time
- Each leader acquisition increments a monotonic epoch counter (`INCR`) in Redis; the epoch is written to every `scheduled_job` row as `created_by_epoch`
- Heartbeat renewal uses a Lua CAS script: `if GET(key) == token then PEXPIRE … end` — renewal fails atomically if the key has expired and been claimed by another instance
- `verify_still_leader()` is called immediately before every write batch — guards against GC pauses that delay heartbeat renewal beyond the lock TTL
- Release uses a Lua CAS script to prevent a process from releasing a lock it no longer owns after TTL expiry

### Transactional Job Creation

- Scheduler creates `scheduled_job` and initial `job_run` in the same database flush within a single async session — either both rows exist or neither does
- `ON CONFLICT DO NOTHING` on `UNIQUE(fingerprint)` and `UNIQUE(subscription_id, scheduled_for)` absorbs duplicate creation attempts from split-brain schedulers without raising errors
- `created_by_epoch` on every `scheduled_job` row provides an audit trail for any rows written by a deposed leader

### Concurrent Executor Safety

- `SELECT FOR UPDATE SKIP LOCKED` batch claim — rows locked by one worker are invisible to others; no shared in-memory state required between executor instances
- Claim transaction writes `worker_id`, `claimed_at`, and `lock_expires_at` before committing — state is visible and recoverable even if the worker crashes immediately after claim
- Batch tasks run via `asyncio.gather`; unhandled task exceptions are caught and logged at the outer batch level so one failed task does not suppress others
- Each executor instance generates a unique `worker_id` from `{env_WORKER_ID or pid}:{uuid4}` — logged on every execution span

### DAG Job Dependencies

- `depends_on` JSONB array on `scheduled_job` allows jobs to declare upstream dependencies within the same subscription batch
- Dependency check is scoped to the same `scheduled_for` window — a `DONE` status from a prior schedule cycle (e.g. last month) does not satisfy the check for the current cycle
- Jobs whose upstream dependency permanently fails (status `DEAD`) are detected by the recovery worker and cancelled; pending runs for those jobs are marked `FAILED` with an explicit reason
- Jobs with satisfied dependencies proceed immediately; no polling or external coordination required

### Worker Crash Recovery

- `lock_expires_at = claimed_at + executor_lock_timeout_s` acts as a heartbeat deadline — a crashed executor cannot renew it
- Recovery worker scans for `RUNNING` runs with expired `lock_expires_at` without holding locks: it fetches candidate IDs, then CAS-updates each one individually
- CAS UPDATE: `WHERE id = ? AND status = 'RUNNING' AND lock_expires_at < now` — a no-op if another recovery instance already transitioned the row, making concurrent recovery safe and idempotent
- Recovered runs with remaining attempts get a new `PENDING` run with `retry_after=NULL` (no additional backoff for crash — the original attempt already waited)
- Recovered runs with exhausted retries are routed to the DLQ directly

### Dead Letter Queue

- After retry exhaustion or non-retryable error, runs are marked `DEAD` and a `DeadLetterEntry` is created with a snapshot of `payment_config`, `attempt`, and `worker_id`
- Non-retryable error classes (`ConfigurationError` for invalid `payment_config`, `BunqPaymentError` for 4xx responses) route directly to DLQ regardless of remaining attempts
- DLQ entries are inspectable and resolvable via `GET /dlq/` and `POST /dlq/{id}/resolve`
- Subscription auto-pause triggers at 3 consecutive DLQ entries; `consecutive_failures` resets to 0 on any successful run

### PostgreSQL-Specific Guarantees

- `INSERT … ON CONFLICT DO NOTHING` for job creation — absorbs duplicate attempts from split-brain schedulers atomically
- Partial index on `job_runs`: `WHERE status = 'PENDING'` — keeps the executor claim query fast even with a large history of terminal (`SUCCEEDED`, `FAILED`, `DEAD`) rows
- `UUID` primary keys throughout via PostgreSQL `uuid` type
- `JSONB` for `payment_config` and `retry_policy` — flexible schema with indexed query support
- Timezone-aware timestamps (`DateTime(timezone=True)`) throughout
- Foreign keys with `ON DELETE CASCADE` — orphaned `job_runs` and DLQ entries are cleaned up automatically

### Observability & Logging

- Structured JSON logging via `structlog` — every log line is machine-parseable
- Per-execution context vars (`job_run_id`, `job_id`, `attempt`, `worker_id`) bound at run entry, cleared at exit — propagated to every log line within the execution without threading through the call stack
- All worker state transitions logged with explicit event names: `executor_run_started`, `executor_run_succeeded`, `executor_retry_scheduled`, `executor_run_moved_to_dlq`, `recovery_run_reset`, `leader_acquired`, `leader_lost`
- OpenTelemetry spans for `scheduler.tick`, `executor.execute_run`, and `bunq.create_payment` — exported via OTLP to Jaeger
- Prometheus metrics on execution outcomes, latency, retry counts, and DLQ depth

### Background Workers

- **Scheduler worker** — leader-elected; polls subscriptions every 10 seconds; creates jobs and advances `next_fire_at`
- **Executor workers** — stateless, N replicas; claim and execute `PENDING` job_runs every 2 seconds; horizontally scalable
- **Recovery worker** — leader-elected; scans for stuck `RUNNING` runs and orphaned DAG jobs every 30 seconds
- Each worker runs as a separate process — crashes are isolated; the API stays up if a worker dies
- All workers handle `SIGTERM` gracefully, completing the current tick before exiting

---

## System Architecture

### High-Level Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                            Client / Caller                           │
└──────────────────────────────────┬───────────────────────────────────┘
                                   │  POST /subscriptions
                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         FastAPI  (main.py)                           │
│         /subscriptions  /jobs  /dlq  /health  /metrics               │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ├─────────────────────┬──────────────────────┐
           │                     │                      │
    ┌──────▼──────┐       ┌──────▼──────┐       ┌───────▼──────┐
    │  Scheduler  │       │  Executor   │       │   Recovery   │
    │  (leader-   │       │  (N workers │       │  (leader-    │
    │   elected)  │       │  SKIP LOCK) │       │   elected)   │
    └──────┬──────┘       └──────┬──────┘       └───────┬──────┘
           │                     │                      │
           └──────────┬──────────┘                      │
                      │                                 │
     ┌────────────────▼─────────────────────────────────▼──────┐
     │                  PostgreSQL (source of truth)            │
     │  subscriptions · scheduled_jobs · job_runs               │
     │  idempotency_keys · dead_letter_queue                    │
     └──────────────────────────────────────────────────────────┘
                      │
     ┌────────────────▼────────────────────┐
     │  Redis (coordination only)          │
     │  leader election · fencing epoch    │
     └────────────────┬────────────────────┘
                      │
     ┌────────────────▼────────────────────┐
     │         bunq Sandbox API            │
     │  (idempotent per Request-Id header) │
     └─────────────────────────────────────┘
```

### Request Lifecycle (POST /subscriptions + Execution)

```
Client
  │
  │  POST /subscriptions  {cron_expression, timezone, payment_config, retry_policy}
  ▼
PaymentConfig.from_dict() — validate amount, IBAN, currency, name
  │
  ├─ INSERT INTO subscriptions (status=ACTIVE, next_fire_at=first_fire)
  └─ return 201 SubscriptionResponse


Scheduler tick (leader only):
  │
  ├─ verify_still_leader()  →  false?  abort tick
  │
  ├─ SELECT subscriptions
  │    WHERE status=ACTIVE AND next_fire_at <= now + lookahead
  │    FOR UPDATE SKIP LOCKED
  │    LIMIT 100
  │
  ├─ for each subscription:
  │    fingerprint = sha256(sub_id:scheduled_for)
  │    INSERT INTO scheduled_jobs … ON CONFLICT DO NOTHING RETURNING id
  │    INSERT INTO job_runs (attempt=1, status=PENDING) … ON CONFLICT DO NOTHING
  │    UPDATE subscriptions SET next_fire_at = next_cron_fire
  │
  └─ COMMIT


Executor tick (any worker):
  │
  ├─ SELECT job_runs
  │    WHERE status=PENDING AND (retry_after IS NULL OR retry_after <= now)
  │    FOR UPDATE SKIP LOCKED LIMIT batch_size
  │
  ├─ UPDATE job_runs SET status=RUNNING, worker_id=?, lock_expires_at=now+300s
  │    COMMIT  (fast — releases lock; recovery can take over if worker dies)
  │
  ├─ for each run (concurrent asyncio tasks):
  │    idempotency_key = sha256(job_run_id:attempt)
  │    PaymentConfig.from_dict(subscription.payment_config)  — fail fast on bad config
  │    DAG dependency check (scoped to same scheduled_for window)
  │    bunq.create_payment(X-Bunq-Client-Request-Id=idempotency_key)
  │
  │    INSERT INTO idempotency_keys … ON CONFLICT DO NOTHING RETURNING key
  │    → key inserted (this worker owns the success path):
  │        UPDATE job_runs  → SUCCEEDED
  │        UPDATE scheduled_jobs → DONE
  │        UPDATE subscriptions  SET consecutive_failures=0
  │        COMMIT (atomic — all or nothing)
  │
  │    → key conflict (another worker already persisted success):
  │        log warning, exit — state is already correct
  │
  └─ on failure:
       is_retryable + not exhausted → INSERT job_runs (attempt=N+1, retry_after=backoff)
       exhausted / non-retryable   → INSERT dead_letter_queue, consecutive_failures++
                                      if consecutive_failures >= 3: PAUSE subscription
```

### Job and Run State Machines

```
Job status:
              ┌─────────┐
      ──────► │  READY  │
              └────┬────┘
                   │ executor picks up
                   ▼
              ┌─────────┐
              │ BLOCKED │  (waiting on DAG dependency)
              └────┬────┘
                   │ dependency DONE
                   ▼
              ┌─────────┐
              │  DONE   │  (terminal — all runs succeeded)
              └─────────┘

              upstream DAG fails permanently
                   │
                   ▼
             ┌───────────┐
             │ CANCELLED │  (terminal)
             └───────────┘


Run status:
              ┌─────────┐
      ──────► │ PENDING │ ◄─────────────────────┐
              └────┬────┘                        │ retry (backoff)
                   │ executor claims             │
                   ▼                             │
             ┌─────────┐                  ┌──────┴──────┐
             │ RUNNING │ ─── failure ───► │   FAILED    │
             └────┬────┘                  └─────────────┘
                  │ success                      ▲
                  ▼                              │
            ┌──────────┐                exhausted / non-retryable
            │ SUCCEEDED│                         │
            └──────────┘                   ┌─────┴──────┐
                                           │    DEAD    │  (terminal — DLQ)
                                           └────────────┘
```

### Worker Lifecycle (Executor)

```
Each tick():
  1. _process_batch()
     └─ SELECT job_runs WHERE status=PENDING AND retry_after <= now
        FOR UPDATE SKIP LOCKED LIMIT batch_size
        → UPDATE to RUNNING with lock_expires_at
        → COMMIT (fast; lock released)

  2. asyncio.gather(*[_execute_run(run) for run in batch])

  3. For each run:
     └─ Load job + subscription (single JOIN query)
        PaymentConfig.from_dict() — raise ConfigurationError if invalid (non-retryable)
        _dag_dependencies_satisfied() — check all depends_on jobs are DONE (same window)
        bunq.create_payment(idempotency_key=sha256(run_id:attempt))
        INSERT idempotency_keys ON CONFLICT DO NOTHING RETURNING key
        → inserted: UPDATE job_run → SUCCEEDED, job → DONE, sub.consecutive_failures = 0
        → conflict: exit (idempotent — another worker persisted success)
        on BunqTransientError / Exception: _handle_failure(is_retryable=True)
        on BunqPaymentError (4xx): _handle_failure(is_retryable=False)
        on ConfigurationError: _handle_failure(is_retryable=False)
```

### Recovery Worker Lifecycle

```
Each tick() — leader only:
  1. _recover_stuck_runs()
     └─ SELECT job_run IDs WHERE status=RUNNING AND lock_expires_at < now LIMIT 50
        (no locks held — just IDs)
        for each run_id:
          UPDATE job_runs
            SET status=FAILED, error_message='Recovered: lock expired', ...
            WHERE id=? AND status='RUNNING' AND lock_expires_at < now
          RETURNING job_id, attempt      ← CAS: no-op if already transitioned
          → claimed: check retry policy
              not exhausted → INSERT job_runs (attempt=N+1, retry_after=NULL)
              exhausted → _move_to_dlq()

  2. _cancel_orphaned_dag_jobs()
     └─ SELECT jobs WHERE status IN (PENDING, READY, BLOCKED) AND depends_on != []
        for each job:
          check if any dependency has a DEAD run
          → found: UPDATE job → CANCELLED, UPDATE pending runs → FAILED
```

---

## Project Structure

```
.
├── app/
│   ├── api/
│   │   └── routes/
│   │       ├── subscriptions.py      # CRUD + resume endpoint
│   │       ├── jobs.py               # Job and run queries
│   │       ├── dlq.py                # DLQ inspection + resolution
│   │       └── health.py             # Liveness + readiness probes
│   ├── core/
│   │   └── config.py                 # Pydantic-settings; all env vars validated at startup
│   ├── domain/
│   │   └── models.py                 # Pure domain: RetryPolicy, IdempotencyKey, JobFingerprint, PaymentConfig
│   ├── infrastructure/
│   │   ├── db/
│   │   │   ├── models.py             # SQLAlchemy 2.0 async ORM models
│   │   │   └── session.py            # Async engine + session factory
│   │   ├── redis/
│   │   │   └── leader.py             # LeaderElection: SET NX, Lua CAS renew/release, epoch
│   │   └── bunq/
│   │       └── client.py             # BunqClient: idempotent payments, error classification
│   ├── workers/
│   │   ├── scheduler.py              # SchedulerWorker: leader-elected cron engine
│   │   ├── executor.py               # ExecutorWorker: concurrent job processor
│   │   └── recovery.py              # RecoveryWorker: CAS stuck-run recovery, DAG orphan cancel
│   └── main.py                       # FastAPI application factory
│
├── migrations/
│   ├── env.py                        # Alembic async env
│   └── versions/
│       ├── 0001_initial.py           # Full schema: tables, constraints, indexes
│       └── 0002_retry_after_and_constraints.py   # retry_after column + partial index
│
├── observability/
│   ├── setup.py                      # structlog + Prometheus metrics + OTEL tracing
│   └── prometheus.yml                # Scrape config
│
├── tests/
│   ├── unit/                         # No I/O. Pure Python. Fast.
│   │   ├── test_domain.py
│   │   └── test_scheduler_logic.py
│   ├── integration/                  # Real PostgreSQL. Verify DB behaviour under concurrency.
│   │   ├── conftest.py
│   │   └── test_executor.py
│   └── failure/                      # Crash simulation, CAS correctness, backoff enforcement
│       └── test_failure_injection.py
│
├── docker-compose.yml                # PostgreSQL 16 + Redis 7 + Prometheus + Jaeger
├── Dockerfile
├── alembic.ini
└── pyproject.toml
```

---

## Database Design

### Tables

#### `subscriptions`
The canonical recurring payment configuration. `cron_expression` and `timezone` drive scheduling. `payment_config` is JSONB — validated against `PaymentConfig` at execution time.

```sql
CREATE TABLE subscriptions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 VARCHAR(255) NOT NULL,
    cron_expression      VARCHAR(100) NOT NULL,
    timezone             VARCHAR(64)  NOT NULL DEFAULT 'UTC',
    next_fire_at         TIMESTAMPTZ,
    payment_config       JSONB        NOT NULL,
    retry_policy         JSONB        NOT NULL DEFAULT '{}',
    status               VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    consecutive_failures INTEGER      NOT NULL DEFAULT 0,
    paused_at            TIMESTAMPTZ,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_subscriptions_next_fire_at_status ON subscriptions(next_fire_at, status);
```

#### `scheduled_jobs`
One record per (subscription, schedule window). `fingerprint` and the `(subscription_id, scheduled_for)` pair both carry UNIQUE constraints — belt-and-suspenders against split-brain duplicate creation. `created_by_epoch` records the leader fencing token.

```sql
CREATE TABLE scheduled_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id  UUID        NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    scheduled_for    TIMESTAMPTZ NOT NULL,
    fingerprint      VARCHAR(64) NOT NULL,
    depends_on       JSONB       NOT NULL DEFAULT '[]',
    status           VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_by_epoch BIGINT      NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_job_per_window   UNIQUE (subscription_id, scheduled_for),
    CONSTRAINT uq_job_fingerprint  UNIQUE (fingerprint)
);
CREATE INDEX ix_scheduled_jobs_status_scheduled_for ON scheduled_jobs(status, scheduled_for);
CREATE INDEX ix_scheduled_jobs_subscription_id      ON scheduled_jobs(subscription_id);
```

#### `job_runs`
One record per execution attempt. `worker_id` and `lock_expires_at` implement the heartbeat-based ownership model. `retry_after` enforces backoff at the query layer.

```sql
CREATE TABLE job_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID        NOT NULL REFERENCES scheduled_jobs(id) ON DELETE CASCADE,
    attempt         INTEGER     NOT NULL DEFAULT 1,
    worker_id       VARCHAR(255),
    claimed_at      TIMESTAMPTZ,
    lock_expires_at TIMESTAMPTZ,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_after     TIMESTAMPTZ,                        -- NULL = eligible immediately
    bunq_payment_id VARCHAR(255),
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    CONSTRAINT uq_run_per_attempt UNIQUE (job_id, attempt)
);
CREATE INDEX ix_job_runs_status          ON job_runs(status);
CREATE INDEX ix_job_runs_lock_expires_at ON job_runs(lock_expires_at);
CREATE INDEX ix_job_runs_pending_claimable ON job_runs(retry_after)
    WHERE status = 'PENDING';   -- partial index; terminal rows excluded
```

#### `idempotency_keys`
The internal success gate. One record per `(job_run_id, attempt)`. The `INSERT … ON CONFLICT DO NOTHING RETURNING key` pattern is the single atomic operation that determines which worker owns the success write path.

```sql
CREATE TABLE idempotency_keys (
    key               VARCHAR(64)  PRIMARY KEY,    -- sha256(run_id:attempt)
    job_run_id        UUID         NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    external_provider VARCHAR(50)  NOT NULL DEFAULT 'bunq',
    response_snapshot JSONB,                       -- stores bunq payment_id
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

#### `dead_letter_queue`
Stores exhausted or non-retryable runs with a full payload snapshot for operational inspection.

```sql
CREATE TABLE dead_letter_queue (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_run_id       UUID  NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    job_id           UUID  NOT NULL,
    subscription_id  UUID  NOT NULL,
    failure_reason   TEXT  NOT NULL,
    error_class      VARCHAR(255),
    payload_snapshot JSONB NOT NULL,               -- payment_config + attempt + worker_id
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at      TIMESTAMPTZ,
    resolved_by      VARCHAR(255)
);
CREATE INDEX ix_dlq_job_id          ON dead_letter_queue(job_id);
CREATE INDEX ix_dlq_subscription_id ON dead_letter_queue(subscription_id);
CREATE INDEX ix_dlq_created_at      ON dead_letter_queue(created_at);
```

### Transaction Boundaries

Every operation requiring atomicity uses an explicit transaction boundary:

- **Job + Run creation**: single session flush — `scheduled_job` and `job_run` either both exist or neither does
- **Run claim**: separate fast transaction — writes `RUNNING` + `lock_expires_at` and commits before any external I/O; recovery can take over if the worker dies
- **Success persistence**: `INSERT idempotency_keys` + `UPDATE job_run → SUCCEEDED` + `UPDATE scheduled_job → DONE` + `UPDATE subscriptions.consecutive_failures = 0` — single transaction; partial success is not possible
- **DLQ routing**: `UPDATE job_run → DEAD` + `INSERT dead_letter_queue` + `UPDATE subscriptions.consecutive_failures++` — single transaction; may also include `UPDATE subscriptions → PAUSED`

---

## Reliability Guarantees

### Idempotency

The idempotency key `sha256(job_run_id:attempt)` is deterministic — two workers executing the same attempt of the same run produce the same key. This means:

- The same key is sent to bunq on every retry, so bunq returns the same result rather than creating a new payment
- The `INSERT INTO idempotency_keys` gate ensures only one worker transitions `job_run` to `SUCCEEDED` for a given `(run_id, attempt)` pair — the second worker detects the conflict via `NULL` return and exits without writing

### Retries

Retry policy is per-subscription, loaded at execution time from `subscription.retry_policy`. The `retry_after` column is the authoritative backoff schedule — no application-level logic can bypass it.

Backoff schedule with default policy (`base_backoff_s=60`, `max_backoff_s=3600`, `jitter=true`):

| Attempt | Base delay | With jitter |
|---|---|---|
| 1 | 60s | 30–60s |
| 2 | 120s | 60–120s |
| 3 | 240s | 120–240s |
| 4 | 480s | 240–480s |
| 5 | DEAD | — |

### Stuck Run Recovery

If a worker sets `status=RUNNING` and then crashes before writing `SUCCEEDED` or `FAILED`, the row remains `RUNNING` indefinitely — no future executor tick will claim it (status is not `PENDING`).

Every recovery tick scans for `RUNNING` rows with expired `lock_expires_at` and uses a CAS UPDATE to transition them to `FAILED`. The CAS WHERE clause (`status=RUNNING AND lock_expires_at < now`) makes concurrent recovery safe: only one recovery instance transitions any given row.

### Distributed Locks and Leader Election

The Redis lock prevents two schedulers or two recovery workers from running simultaneously. Without it:
- Two schedulers could both read the same `ACTIVE` subscriptions, both create `scheduled_jobs`, and both attempt to advance `next_fire_at` — producing double-advanced schedules even if the UNIQUE constraints absorb the duplicate rows
- Two recovery workers could both scan the same stuck run, both insert a next-attempt row, and both trigger DLQ routing — producing two DLQ entries for the same run

The lock uses a unique per-acquisition token so a process cannot release a lock it no longer owns after TTL expiry. `verify_still_leader()` adds a second check immediately before write batches.

**Important**: this implementation uses single-node Redis `SET NX PX`. For a Redis cluster, Redlock across 3–5 nodes would be needed. The design is explicit about this tradeoff.

### Financial Correctness Guarantees

- All monetary amounts are stored in `payment_config` JSONB as decimal strings — never floats — and parsed via Python `Decimal` at execution time
- `PaymentConfig.from_dict` validates amount (positive, valid decimal), IBAN (present and non-empty), currency (3-char ISO code), and counterparty name (non-empty) before any payment attempt
- Invalid config raises `ConfigurationError` — non-retryable, routes directly to DLQ; retrying a bad config against bunq is never attempted
- Decimal parsing uses Python's `Decimal(str(amount))` — no floating-point representation errors at any stage

---

## API Documentation

### POST /subscriptions/

Create a new recurring payment subscription.

**Request**

```http
POST /subscriptions/
Content-Type: application/json

{
  "name": "Monthly Invoice",
  "description": "Supplier monthly invoice payment",
  "cron_expression": "0 9 1 * *",
  "timezone": "Europe/Amsterdam",
  "payment_config": {
    "amount": "150.00",
    "currency": "EUR",
    "counterparty_iban": "NL02ABNA0123456789",
    "counterparty_name": "Supplier BV",
    "description": "Monthly invoice"
  },
  "retry_policy": {
    "max_attempts": 5,
    "base_backoff_s": 60,
    "max_backoff_s": 3600,
    "jitter": true
  }
}
```

**Response (201 Created)**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Monthly Invoice",
  "cron_expression": "0 9 1 * *",
  "timezone": "Europe/Amsterdam",
  "status": "ACTIVE",
  "next_fire_at": "2025-06-01T07:00:00Z",
  "created_at": "2025-05-13T10:00:00Z"
}
```

**Validation errors (422)**

| Field | Rule |
|---|---|
| `cron_expression` | must be a valid 5-field cron string |
| `timezone` | must be a valid IANA timezone identifier |
| `payment_config.amount` | positive, valid decimal string |
| `payment_config.counterparty_iban` | required, non-empty |
| `payment_config.currency` | 3-character ISO 4217 code |

### GET /subscriptions/

List subscriptions with optional `status` filter and `limit`/`offset` pagination.

### GET /subscriptions/{id}

Retrieve a subscription by UUID. Returns `404` if not found.

### PATCH /subscriptions/{id}

Update `cron_expression`, `timezone`, `payment_config`, or `retry_policy`. Returns the updated subscription.

### POST /subscriptions/{id}/resume

Resume a `PAUSED` subscription. Recomputes `next_fire_at` from the current time and sets `status=ACTIVE`. Returns `409` if not currently paused.

### GET /jobs/

List scheduled jobs. Supports `subscription_id` and `status` filters with `limit`/`offset` pagination.

### GET /jobs/{id}/runs

List all runs for a job ordered by `attempt` ascending.

### GET /dlq/

List unresolved DLQ entries with optional `subscription_id` filter.

### POST /dlq/{id}/resolve

Mark a DLQ entry as resolved with a `resolved_by` string. Does not automatically retry the payment.

### Health Endpoints

```http
GET /health/live
# → {"status": "ok"}

GET /health/ready
# → {"status": "ready", "database": "ok"}
# → 503 if database is unreachable
```

### Metrics

```http
GET /metrics
# → Prometheus text format
```

---

## Running Locally

### Prerequisites

- Python 3.11+
- Docker (for PostgreSQL and Redis)
- A bunq sandbox API key from the [bunq developer portal](https://www.bunq.com/en/sandbox)

### Environment Variables

```bash
cp .env.example .env
# Edit .env — at minimum set BUNQ_API_KEY and BUNQ_MONETARY_ACCOUNT_ID
```

Key variables:

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_DSN` | PostgreSQL async DSN | `postgresql+asyncpg://postgres:postgres@localhost:5432/payment_scheduler` |
| `REDIS_URL` | Redis DSN | `redis://localhost:6379/0` |
| `BUNQ_API_KEY` | bunq sandbox API key | *(required)* |
| `BUNQ_MONETARY_ACCOUNT_ID` | Source account ID for payments | *(required)* |
| `BUNQ_BASE_URL` | bunq API base URL | `https://public-api.sandbox.bunq.com/v1` |
| `WORKER_ID` | Unique worker identity (logged on all spans) | `""` (auto-generated from pid) |
| `SCHEDULER_INTERVAL_S` | Scheduler tick cadence (seconds) | `10` |
| `SCHEDULER_LOOKAHEAD_S` | How far ahead to schedule jobs | `60` |
| `EXECUTOR_POLL_INTERVAL_S` | Executor idle poll interval | `2` |
| `EXECUTOR_BATCH_SIZE` | Max job_runs claimed per poll | `10` |
| `EXECUTOR_LOCK_TIMEOUT_S` | Seconds before recovery resets a stuck run | `300` |
| `RECOVERY_INTERVAL_S` | Recovery worker tick cadence | `30` |
| `LEADER_LOCK_TTL_MS` | Redis leader lock TTL (ms) | `15000` |
| `LEADER_HEARTBEAT_INTERVAL_S` | Leader heartbeat renewal interval (seconds) | `5` |
| `OTLP_ENDPOINT` | OTLP gRPC trace exporter endpoint | `http://localhost:4317` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Start Dependencies

```bash
docker-compose up postgres redis -d
# Starts PostgreSQL on :5432 and Redis on :6379
```

### Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

### Run Migrations

```bash
alembic upgrade head
```

### Start the API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Or with auto-reload for development:

```bash
LOG_LEVEL=DEBUG uvicorn app.main:app --reload
```

### Start Workers

Each worker runs as a separate process:

```bash
WORKER_ID=scheduler-1  python -m app.workers.scheduler &
WORKER_ID=executor-1   python -m app.workers.executor &
WORKER_ID=executor-2   python -m app.workers.executor &
WORKER_ID=recovery-1   python -m app.workers.recovery &
```

Or bring up the entire stack including observability:

```bash
docker-compose up --build
```

### Run Tests

```bash
# Unit tests only (no PostgreSQL required)
pytest tests/unit/ -v

# Failure injection tests (no DB required)
pytest tests/failure/ -v

# Integration tests (requires PostgreSQL)
TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/payment_scheduler_test \
    pytest tests/integration/ -v

# Full suite with coverage
pytest --cov=app --cov-report=term-missing
```

---

## Testing Strategy

### Unit Tests (`tests/unit/`)

Pure Python — no I/O, no database, no Redis. Run in milliseconds. Cover:

- `RetryPolicy` — `next_delay_s` monotonicity across attempts, `is_exhausted` boundary conditions, jitter range
- `IdempotencyKey.for_run` — determinism across calls, uniqueness across different `(run_id, attempt)` pairs
- `JobFingerprint.for_job` — determinism, uniqueness across different `(subscription_id, scheduled_for)` pairs
- `PaymentConfig.from_dict` — all validation paths: non-positive amount, missing required fields, malformed decimal, invalid currency length, empty IBAN/name
- `SchedulerWorker._next_fire` — timezone-aware cron evaluation, invalid expression returning `None`

### Integration Tests (`tests/integration/`)

Require a real PostgreSQL instance (`TEST_DATABASE_URL`). Skipped automatically when the variable is unset. Use `asyncpg` via SQLAlchemy against a real schema. These tests verify behaviour that unit tests cannot: constraint violations, `SELECT FOR UPDATE SKIP LOCKED` mutual exclusion, `ON CONFLICT DO NOTHING` idempotency under concurrent sessions.

- **Happy path**: run transitions `PENDING → RUNNING → SUCCEEDED`; `IdempotencyKeyRecord` inserted; `ScheduledJob` set to `DONE`; `consecutive_failures` reset to 0
- **Idempotency conflict**: simulate two workers reaching the same `(run_id, attempt)` — only one `SUCCEEDED` write lands
- **DLQ routing**: `BunqPaymentError` (4xx) bypasses retry and routes to `dead_letter_queue`
- **Auto-pause**: three consecutive DLQ entries trigger `subscription.status = PAUSED`
- **DAG blocking**: executor leaves a run unclaimed when its dependency is not yet `DONE` in the same schedule window

**Why PostgreSQL integration tests matter**: SQLite does not support `FOR UPDATE SKIP LOCKED`, does not enforce UNIQUE constraints across concurrent transactions the same way, and does not support partial indexes. Any test that exercises queue semantics or correctness under concurrency must run against PostgreSQL to be meaningful.

### Failure Injection Tests (`tests/failure/`)

Assert on correctness invariants under adversarial conditions without requiring live infrastructure:

- **Lock expiry**: asserts `lock_expires_at` is set strictly in the future at claim time
- **CAS correctness**: asserts the CAS WHERE clause matches only `RUNNING` rows with expired locks; a row already transitioned to `FAILED` does not match — simulates concurrent recovery
- **CAS stuck-run match**: asserts the positive case — a `RUNNING` row with expired lock is correctly matched
- **Retry backoff ordering**: asserts `retry_after_attempt_1 < retry_after_attempt_2 < retry_after_attempt_3` with jitter disabled
- **Backoff enforcement**: asserts `retry_after` is strictly in the future and that the claim query filter respects it
- **Exhaustion boundary**: asserts `policy.is_exhausted(max_attempts)` is true and `is_exhausted(max_attempts - 1)` is false

---

## Failure Scenarios

### Worker Crash Mid-Payment

**Scenario**: Executor sets `status=RUNNING` and `lock_expires_at=now+300s`, calls bunq, bunq accepts the payment, the process is killed before writing `SUCCEEDED`.

**Recovery**: Run remains `RUNNING`. After `EXECUTOR_LOCK_TIMEOUT_S` seconds, the recovery worker's CAS UPDATE matches the row (`status=RUNNING AND lock_expires_at < now`) and transitions it to `FAILED`. A new `job_run` with `attempt=N+1` and `retry_after=NULL` is inserted. On re-execution, the same `sha256(run_id:attempt)` idempotency key is sent to bunq — if bunq already processed the payment, it returns the prior result. The `INSERT INTO idempotency_keys` then conflicts, and the success path is not double-written.

**Outcome**: Payment submitted exactly once. State updated exactly once.

### Duplicate Scheduler Tick (Split-Brain)

**Scenario**: Two scheduler instances both believe they are the leader (e.g. one recovering from a GC pause). Both evaluate the same subscription as due and attempt to create a `scheduled_job`.

**Recovery**: First INSERT succeeds. Second hits `UNIQUE(uq_job_fingerprint)` and is absorbed by `ON CONFLICT DO NOTHING`. No duplicate job or run is created. The `verify_still_leader()` call on the deposed leader detects leadership loss on the next tick and stops writing.

**Outcome**: Exactly one job per subscription window, regardless of how many scheduler instances attempted to create it.

### Concurrent Recovery Race

**Scenario**: Two recovery worker instances both scan at the same time and both find the same stuck `RUNNING` run.

**Recovery**: Both issue the same CAS UPDATE. The first succeeds and returns `RETURNING job_id, attempt`. The second finds the row already in `FAILED` state — `status=RUNNING` no longer holds — returns zero rows. Only one next-attempt `job_run` is inserted; `ON CONFLICT DO NOTHING` on `UNIQUE(job_id, attempt)` absorbs any race on the insert.

**Outcome**: Stuck run recovered exactly once. No duplicate next-attempt runs.

### Retry Exhaustion

**Scenario**: bunq returns a non-transient error (e.g. insufficient funds, invalid account) on every attempt.

**Recovery**: After `max_attempts`, `_handle_failure` calls `_move_to_dlq`. Run is marked `DEAD`, `DeadLetterEntry` created with a snapshot of `payment_config` and `attempt`. `consecutive_failures` is incremented. If `consecutive_failures >= 3`, `subscription.status` is set to `PAUSED`. Resume requires `POST /subscriptions/{id}/resume` after operator inspection.

### Invalid Payment Configuration

**Scenario**: A subscription's `payment_config` contains an invalid amount or missing IBAN that passes API-level validation but fails at execution time.

**Recovery**: `PaymentConfig.from_dict` raises `ValueError`, caught as `ConfigurationError`. Routes directly to DLQ with `is_retryable=False` — no bunq API call is ever made. The subscription must be updated and resumed before any further execution.

### DAG Upstream Permanently Fails

**Scenario**: An upstream job's run reaches `DEAD` status after retry exhaustion. Downstream jobs are still `READY` or `PENDING`.

**Recovery**: Recovery worker's `_cancel_orphaned_dag_jobs` scan detects the `DEAD` upstream run. Downstream job transitions to `CANCELLED`; its pending run is marked `FAILED` with `error_message="DAG dependency {dep_id} permanently failed"`.

### Redis Outage

- **Leader lock unavailable**: Scheduler and recovery workers cannot acquire leadership. They skip their ticks. Executors continue processing already-clocked `PENDING` runs unaffected — they do not use Redis.
- **Heartbeat renewal fails**: The heartbeat loop sets `is_leader=False` and logs `leader_heartbeat_failed_lost_leadership`. The worker stops writing until it re-acquires. A Redis failure causes the leader to defensively step down — it cannot cause split-brain writes.

---

## Security Considerations

### Secret Management

No secrets are hard-coded. All credentials (`BUNQ_API_KEY`, `POSTGRES_DSN`, `REDIS_URL`) are loaded from environment variables at startup via `pydantic-settings`. In production, these should be injected via a secrets manager (AWS Secrets Manager, HashiCorp Vault, Kubernetes Secrets) — not `.env` files on disk.

### Sensitive Data in Logs

IBANs and payment amounts appear in the `bunq_payment_request` log event at the point of API submission. Worker identity (`worker_id`) and correlation identifiers (`job_run_id`, `job_id`) are logged without financial data. No credentials appear in any log line.

### SQL Injection Prevention

All database queries use SQLAlchemy parameterised query construction — no string interpolation into SQL. State filter values in list endpoints are validated against enum definitions before reaching queries.

---

## Observability

### Structured Logging

All log output is JSON via `structlog`. Context vars (`job_run_id`, `job_id`, `attempt`, `worker_id`) are bound at execution entry via `structlog.contextvars.bind_contextvars` and cleared on exit — every log line within an execution carries full correlation context.

Key log events and their fields:

| Event | Key Fields |
|---|---|
| `scheduler_job_created` | `job_id`, `subscription_id`, `scheduled_for`, `epoch` |
| `executor_run_started` | `job_run_id`, `job_id`, `attempt`, `worker_id` |
| `executor_run_succeeded` | `payment_id`, `elapsed_s` |
| `executor_retry_scheduled` | `next_attempt`, `delay_s`, `retry_after` |
| `executor_run_moved_to_dlq` | `job_run_id` |
| `recovery_run_reset` | `next_attempt` |
| `subscription_paused_too_many_failures` | `subscription_id`, `consecutive_failures` |
| `leader_acquired` / `leader_lost` | `epoch`, `token` |

### Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `scheduler_runs_total` | Counter | `outcome` (ok, error, not_leader) | Scheduler tick outcomes |
| `jobs_created_total` | Counter | — | Jobs enqueued by the scheduler |
| `executor_jobs_total` | Counter | `outcome` (succeeded, failed, dead) | Executor outcomes per run |
| `job_latency_seconds` | Histogram | — | End-to-end execution wall time |
| `scheduler_lag_seconds` | Gauge | — | Age of oldest due job at tick time |
| `job_retries_total` | Counter | — | Cumulative retries |
| `dlq_entries_total` | Counter | — | Cumulative DLQ entries created |
| `dlq_resolved_total` | Counter | — | Cumulative DLQ entries resolved |
| `dlq_size` | Gauge | — | Current unresolved DLQ depth |
| `recovery_runs_total` | Counter | — | Stuck runs recovered |

DLQ open gauge: `dlq_entries_total - dlq_resolved_total` — avoids permanent gauge drift from counter resets.

Prometheus UI: `http://localhost:9090`

### Tracing (OpenTelemetry → Jaeger)

Spans emitted for:

- `scheduler.tick` — `epoch` attribute; covers full subscription scan and job creation batch
- `executor.execute_run` — `job_run_id`, `job_id`, `attempt`, `worker_id`; parent span for the full execution path
- `bunq.create_payment` — `idempotency_key`, `currency`, `bunq_payment_id` (on success)

Exported via OTLP gRPC using `BatchSpanProcessor` — non-blocking for the async event loop.

Jaeger UI: `http://localhost:16686`

### Operational Debugging

```bash
# Find subscriptions due for firing
SELECT id, name, next_fire_at, status
FROM subscriptions
WHERE status = 'ACTIVE' AND next_fire_at <= now()
ORDER BY next_fire_at;

# Find stuck RUNNING runs
SELECT id, job_id, attempt, worker_id, lock_expires_at
FROM job_runs
WHERE status = 'RUNNING' AND lock_expires_at < now();

# Executor throughput by outcome (last hour)
SELECT status, COUNT(*)
FROM job_runs
WHERE created_at > now() - interval '1 hour'
GROUP BY status;

# DLQ entries for a subscription
SELECT dlq.id, dlq.failure_reason, dlq.created_at, jr.attempt
FROM dead_letter_queue dlq
JOIN job_runs jr ON jr.id = dlq.job_run_id
WHERE dlq.subscription_id = '<subscription_id>'
  AND dlq.resolved_at IS NULL
ORDER BY dlq.created_at DESC;

# Jobs written by each leader epoch (detect split-brain writes)
SELECT created_by_epoch, COUNT(*), MIN(created_at), MAX(created_at)
FROM scheduled_jobs
GROUP BY created_by_epoch
ORDER BY created_by_epoch DESC;

# Verify retry_after is being respected
SELECT id, attempt, retry_after, status
FROM job_runs
WHERE status = 'PENDING'
ORDER BY retry_after NULLS FIRST;
```

---

## Production Improvements / Future Work

### Message Queue (Kafka / SQS)

The current design uses PostgreSQL `FOR UPDATE SKIP LOCKED` as the queue substrate. This is correct and operationally simple but has a throughput ceiling: connection pool exhaustion precedes CPU exhaustion at high volume. At scale, replacing with Kafka or SQS provides consumer groups, replay, backpressure, and better operational visibility. The executor's poll-and-claim loop is a bounded change to migrate.

### Outbox Pattern for Job Creation

The scheduler currently writes `scheduled_jobs` directly in the same session as reading `subscriptions`. An outbox pattern — write a pending-dispatch record transactionally, consume via a CDC pipeline or background reader — would decouple scheduling from execution routing and enable fan-out to multiple executor pools by job type or tenant.

### Partitioned Job Queue

All executors poll a single `job_runs` table. At high volume, even the partial index on `status='PENDING'` grows. Partitioning by `subscription_id % N` or by `scheduled_for` date range would allow partition-pruned scans and independent vacuum on historical partitions.

### Circuit Breakers

The bunq client retries on all transient errors. A circuit breaker (e.g. `aiobreaker`) would open after N consecutive failures and fast-fail new attempts during a cooldown, preventing a bunq outage from exhausting retries across all subscriptions simultaneously and triggering mass auto-pauses.

### Distributed Cron / Multi-Region Leader Election

The current Redis `SET NX PX` leader election is single-node. In a multi-region deployment, clock skew and network partitions create windows where two nodes both hold the lock. etcd leases or ZooKeeper ephemeral nodes provide stronger guarantees. Alternatively, distributing scheduler shards by `subscription_id % N` (one range per scheduler instance, no election needed) removes the single point of failure without coordination complexity.

### Trace Context Propagation

The scheduler creates a trace span when generating a job but does not store the W3C `traceparent` header in the `scheduled_job` row. Executor spans therefore start new trace roots rather than continuing the originating scheduler trace. Storing trace context in a JSONB column would enable end-to-end traces from subscription fire through to bunq payment confirmation.

### `zoneinfo` Migration

Timezone handling uses `pytz`, deprecated in Python 3.9+ in favour of the stdlib `zoneinfo` module. Migrating would eliminate the `pytz` dependency and use the IANA database bundled with the OS.

---

## Example Commands

```bash
# ── Start infrastructure
docker-compose up -d

# ── Database
alembic upgrade head
alembic downgrade -1
alembic history --verbose

# ── API
uvicorn app.main:app --host 0.0.0.0 --port 8000
LOG_LEVEL=DEBUG uvicorn app.main:app --reload

# ── Workers
WORKER_ID=scheduler-1  python -m app.workers.scheduler
WORKER_ID=executor-1   python -m app.workers.executor
WORKER_ID=recovery-1   python -m app.workers.recovery

# ── Tests
pytest tests/unit/ -v
pytest tests/failure/ -v
TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/test_scheduler \
    pytest tests/integration/ -v
pytest --cov=app --cov-report=html -q

# ── Operational queries
psql $POSTGRES_DSN -c "SELECT status, COUNT(*) FROM job_runs GROUP BY status;"
psql $POSTGRES_DSN -c "SELECT status, COUNT(*) FROM subscriptions GROUP BY status;"
psql $POSTGRES_DSN -c "SELECT COUNT(*) FROM dead_letter_queue WHERE resolved_at IS NULL;"
```

---

## Engineering Philosophy

**Financial systems prioritise correctness over throughput.** A payment scheduler that fires jobs at high volume but occasionally double-charges or misses a payment is not a scheduler — it is a liability. Every architectural decision in this project is made with correctness as the primary constraint and throughput as a secondary one.

**Idempotency is non-negotiable.** Any operation that moves money must be safe to retry. This means deterministic idempotency keys, duplicate detection at every layer (external API header, internal DB gate), and idempotent state updates (`ON CONFLICT DO NOTHING`, `RETURNING`-based ownership). An operation that is "probably idempotent" is not idempotent.

**Explicit failure handling is critical.** The most dangerous failures in payment systems are not the ones that raise exceptions — they are the ones that succeed partially. A payment that bunq accepted but the application does not know about. A job_run stuck in `RUNNING` because a worker crashed. This project handles these explicitly: `lock_expires_at` heartbeats, CAS-style recovery, and idempotency key conflicts all address specific partial-failure scenarios rather than relying on "it probably won't happen."

**Reliability is designed, not added later.** The idempotency key strategy, the SKIP LOCKED claim model, the fencing token on every scheduler write, and the CAS recovery pattern were built in from the start. Adding these patterns to an existing payment system is significantly harder than building them in — and usually happens after the first incident, not before.
