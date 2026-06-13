-- Durable execution: runs become rows in a Postgres-backed work queue.
-- Workers claim due runs with FOR UPDATE SKIP LOCKED, hold a lease while
-- executing, and a reaper requeues runs whose worker died (lease expired).
-- Retries back off exponentially; exhausted runs land in the dead-letter
-- state instead of silently failing.

ALTER TABLE runs ADD COLUMN IF NOT EXISTS attempt          INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS max_attempts     INTEGER NOT NULL DEFAULT 3;
-- Epoch seconds when the run is next eligible to be claimed (0 = immediately).
ALTER TABLE runs ADD COLUMN IF NOT EXISTS next_attempt_at  BIGINT  NOT NULL DEFAULT 0;
-- While running: the worker holding the run and when its claim expires.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS lease_until      BIGINT;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS worker_id        TEXT;
-- Cooperative cancellation: checked by the engine between steps.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT FALSE;
-- Immutable snapshot of the workflow steps at enqueue time, so editing a
-- workflow never changes what an in-flight or retried run executes.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS steps            JSONB;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS workflow_version INTEGER NOT NULL DEFAULT 1;

-- Workflow definitions get a monotonically increasing version (bumped on
-- every update) so runs can record exactly what they executed.
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;

-- Queue claim path: status + due time.
CREATE INDEX IF NOT EXISTS idx_runs_queue ON runs(status, next_attempt_at);
