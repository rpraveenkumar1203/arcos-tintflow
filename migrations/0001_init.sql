-- TintFlow schema (independent database). Tenants are referenced by id only;
-- there is no FK to the monolith — services own their data.

CREATE TABLE IF NOT EXISTS workflows (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    -- Ordered JSON array of steps: [{ "id", "kind", "config" }, …]
    steps       JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_by  TEXT,
    created_at  BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflows_tenant ON workflows(tenant_id, updated_at DESC);

-- A single execution of a workflow. `context` carries data between steps and is
-- persisted so a run paused for approval can resume.
CREATE TABLE IF NOT EXISTS runs (
    id            TEXT PRIMARY KEY,
    workflow_id   TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    tenant_id     TEXT NOT NULL,
    -- queued | running | waiting_approval | succeeded | failed | canceled
    status        TEXT NOT NULL DEFAULT 'queued',
    trigger       TEXT NOT NULL DEFAULT 'manual',   -- manual | schedule | webhook
    cursor        INTEGER NOT NULL DEFAULT 0,        -- next step index to execute
    context       JSONB NOT NULL DEFAULT '{}'::jsonb,
    error         TEXT,
    created_at    BIGINT NOT NULL,
    started_at    BIGINT,
    finished_at   BIGINT
);
CREATE INDEX IF NOT EXISTS idx_runs_workflow ON runs(workflow_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_tenant   ON runs(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status   ON runs(status);

-- Per-step execution record — the execution log.
CREATE TABLE IF NOT EXISTS run_steps (
    id          TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_index  INTEGER NOT NULL,
    step_id     TEXT NOT NULL,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,                       -- succeeded | failed | skipped
    output      JSONB NOT NULL DEFAULT '{}'::jsonb,
    error       TEXT,
    started_at  BIGINT NOT NULL,
    finished_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_steps_run ON run_steps(run_id, step_index);

-- Cron schedules that enqueue runs. `next_due` is recomputed after each fire.
CREATE TABLE IF NOT EXISTS schedules (
    id          TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    tenant_id   TEXT NOT NULL,
    cron        TEXT NOT NULL,                        -- 5-field: m h dom mon dow
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_run    BIGINT,
    created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedules_enabled ON schedules(enabled);

-- Inbound webhook triggers. A POST to /hooks/{token} starts the workflow.
CREATE TABLE IF NOT EXISTS webhooks (
    token       TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    tenant_id   TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  BIGINT NOT NULL
);

-- Human approval gates. A run with an `approval` step pauses and creates a row
-- here; resolving it resumes the run.
CREATE TABLE IF NOT EXISTS approvals (
    id          TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    tenant_id   TEXT NOT NULL,
    step_id     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',      -- pending | approved | rejected
    note        TEXT,
    decided_by  TEXT,
    created_at  BIGINT NOT NULL,
    decided_at  BIGINT
);
CREATE INDEX IF NOT EXISTS idx_approvals_run ON approvals(run_id);
CREATE INDEX IF NOT EXISTS idx_approvals_status ON approvals(tenant_id, status);

-- Reusable workflow templates (global when tenant_id IS NULL).
CREATE TABLE IF NOT EXISTS templates (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    steps       JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_templates_tenant ON templates(tenant_id);
