//! PostgreSQL persistence for TintFlow. Its own database — no shared tables
//! with the monolith.

use crate::model::*;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Duration;

#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("serialization: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    // ── Workflows ───────────────────────────────────────────────────────────
    pub async fn create_workflow(&self, w: &Workflow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO workflows (id, tenant_id, name, description, steps, enabled, created_by, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(&w.id).bind(&w.tenant_id).bind(&w.name).bind(&w.description)
        .bind(serde_json::to_value(&w.steps)?).bind(w.enabled).bind(&w.created_by)
        .bind(w.created_at).bind(w.updated_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_workflow(&self, tenant_id: &str, id: &str) -> Result<Option<Workflow>, DbError> {
        let row = sqlx::query("SELECT * FROM workflows WHERE id = $1 AND tenant_id = $2")
            .bind(id).bind(tenant_id).fetch_optional(&self.pool).await?;
        row.map(workflow_from_row).transpose()
    }

    pub async fn list_workflows(&self, tenant_id: &str) -> Result<Vec<Workflow>, DbError> {
        let rows = sqlx::query("SELECT * FROM workflows WHERE tenant_id = $1 ORDER BY updated_at DESC")
            .bind(tenant_id).fetch_all(&self.pool).await?;
        rows.into_iter().map(workflow_from_row).collect()
    }

    pub async fn update_workflow(&self, w: &Workflow) -> Result<bool, DbError> {
        let r = sqlx::query(
            "UPDATE workflows SET name=$3, description=$4, steps=$5, enabled=$6, updated_at=$7,
                                  version = version + 1
             WHERE id=$1 AND tenant_id=$2",
        )
        .bind(&w.id).bind(&w.tenant_id).bind(&w.name).bind(&w.description)
        .bind(serde_json::to_value(&w.steps)?).bind(w.enabled).bind(now_secs())
        .execute(&self.pool).await?;
        Ok(r.rows_affected() > 0)
    }

    pub async fn delete_workflow(&self, tenant_id: &str, id: &str) -> Result<bool, DbError> {
        let r = sqlx::query("DELETE FROM workflows WHERE id=$1 AND tenant_id=$2")
            .bind(id).bind(tenant_id).execute(&self.pool).await?;
        Ok(r.rows_affected() > 0)
    }

    // ── Runs ────────────────────────────────────────────────────────────────
    pub async fn create_run(&self, r: &Run) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO runs (id, workflow_id, tenant_id, status, trigger, cursor, context, created_at,
                               attempt, max_attempts, next_attempt_at, steps, workflow_version, idempotency_key)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
        )
        .bind(&r.id).bind(&r.workflow_id).bind(&r.tenant_id).bind(&r.status)
        .bind(&r.trigger).bind(r.cursor).bind(&r.context).bind(r.created_at)
        .bind(r.attempt).bind(r.max_attempts).bind(r.next_attempt_at)
        .bind(r.steps.as_ref().map(serde_json::to_value).transpose()?)
        .bind(r.workflow_version)
        .bind(&r.idempotency_key)
        .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_run_by_idempotency_key(&self, tenant_id: &str, idempotency_key: &str) -> Result<Option<Run>, DbError> {
        let row = sqlx::query("SELECT * FROM runs WHERE tenant_id=$1 AND idempotency_key=$2")
            .bind(tenant_id).bind(idempotency_key).fetch_optional(&self.pool).await?;
        Ok(row.map(run_from_row))
    }

    pub async fn get_run(&self, tenant_id: &str, id: &str) -> Result<Option<Run>, DbError> {
        let row = sqlx::query("SELECT * FROM runs WHERE id=$1 AND tenant_id=$2")
            .bind(id).bind(tenant_id).fetch_optional(&self.pool).await?;
        Ok(row.map(run_from_row))
    }

    pub async fn list_runs(&self, tenant_id: &str, workflow_id: Option<&str>, status: Option<&str>, limit: i64) -> Result<Vec<Run>, DbError> {
        let rows = sqlx::query(
            "SELECT * FROM runs
             WHERE tenant_id = $1
               AND ($2::text IS NULL OR workflow_id = $2)
               AND ($3::text IS NULL OR status = $3)
             ORDER BY created_at DESC LIMIT $4",
        )
        .bind(tenant_id).bind(workflow_id).bind(status).bind(limit.clamp(1, 500))
        .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(run_from_row).collect())
    }

    /// Persist a run's progress (status, cursor, context, error, retry state,
    /// timestamps).
    pub async fn save_run_progress(&self, r: &Run) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE runs SET status=$2, cursor=$3, context=$4, error=$5, started_at=$6, finished_at=$7,
                             attempt=$8, next_attempt_at=$9
             WHERE id=$1",
        )
        .bind(&r.id).bind(&r.status).bind(r.cursor).bind(&r.context).bind(&r.error)
        .bind(r.started_at).bind(r.finished_at)
        .bind(r.attempt).bind(r.next_attempt_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    // ── Durable work queue ──────────────────────────────────────────────────

    /// Atomically claim up to `limit` due runs for `worker_id`. Uses
    /// FOR UPDATE SKIP LOCKED so concurrent workers never double-claim.
    /// Claiming increments `attempt` and takes a lease until `now + lease_secs`.
    pub async fn claim_due_runs(&self, worker_id: &str, now: i64, lease_secs: i64, limit: i64) -> Result<Vec<Run>, DbError> {
        let rows = sqlx::query(
            "UPDATE runs SET status='running', worker_id=$1, lease_until=$2,
                             attempt = attempt + 1,
                             started_at = COALESCE(started_at, $3)
             WHERE id IN (
                 SELECT id FROM runs
                 WHERE status IN ('queued','retrying','sleeping') AND next_attempt_at <= $3
                 ORDER BY next_attempt_at, created_at
                 LIMIT $4
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING *",
        )
        .bind(worker_id).bind(now + lease_secs).bind(now).bind(limit.clamp(1, 100))
        .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(run_from_row).collect())
    }

    /// Extend the lease on a running run (called between steps so a healthy
    /// worker is never reaped mid-run). Returns true if cancellation was requested.
    pub async fn extend_lease(&self, run_id: &str, lease_until: i64) -> Result<bool, DbError> {
        let row = sqlx::query("UPDATE runs SET lease_until=$2 WHERE id=$1 AND status='running' RETURNING cancel_requested")
            .bind(run_id).bind(lease_until).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| r.get::<bool, _>("cancel_requested")).unwrap_or(false))
    }

    /// Requeue (or dead-letter) runs whose worker disappeared: status is
    /// 'running' but the lease expired. The claim already counted the attempt,
    /// so the reaper only decides between another try and the dead-letter
    /// queue. A NULL lease on a running run can only be a legacy row orphaned
    /// by the old fire-and-forget engine (claims always set a lease), so those
    /// are reclaimed too. Returns how many runs were reclaimed.
    pub async fn reap_stale_runs(&self, now: i64) -> Result<u64, DbError> {
        let r = sqlx::query(
            "UPDATE runs SET
                 status = CASE WHEN attempt >= max_attempts THEN 'dead_letter' ELSE 'retrying' END,
                 error  = COALESCE(error, '') || '[worker lease expired] ',
                 next_attempt_at = $1 + 15,
                 finished_at = CASE WHEN attempt >= max_attempts THEN $1 ELSE NULL END,
                 worker_id = NULL, lease_until = NULL
             WHERE status='running' AND (lease_until IS NULL OR lease_until < $1)",
        )
        .bind(now).execute(&self.pool).await?;
        Ok(r.rows_affected())
    }

    /// Flag a run for cooperative cancellation. Queued/retrying/waiting
    /// runs cancel immediately; running runs cancel at the next step boundary.
    /// Returns the updated run, or None if it doesn't exist / is terminal.
    pub async fn request_cancel(&self, tenant_id: &str, id: &str, now: i64) -> Result<Option<Run>, DbError> {
        let row = sqlx::query(
            "UPDATE runs SET
                 cancel_requested = TRUE,
                 status = CASE WHEN status IN ('queued','retrying','waiting_approval','sleeping','waiting_subworkflow') THEN 'canceled' ELSE status END,
                 error  = CASE WHEN status IN ('queued','retrying','waiting_approval','sleeping','waiting_subworkflow') THEN 'canceled by user' ELSE error END,
                 finished_at = CASE WHEN status IN ('queued','retrying','waiting_approval','sleeping','waiting_subworkflow') THEN $3 ELSE finished_at END
             WHERE id=$1 AND tenant_id=$2
               AND status IN ('queued','retrying','waiting_approval','running','sleeping','waiting_subworkflow')
             RETURNING *",
        )
        .bind(id).bind(tenant_id).bind(now)
        .fetch_optional(&self.pool).await?;
        Ok(row.map(run_from_row))
    }

    /// Requeue a finished run for another execution. `restart` re-runs from
    /// step 0 with a fresh context; otherwise execution resumes at the failed
    /// step with the accumulated context intact. Attempts reset so the run
    /// gets a full retry budget. Returns None unless the run is in a
    /// retryable terminal state.
    pub async fn requeue_run(&self, tenant_id: &str, id: &str, restart: bool) -> Result<Option<Run>, DbError> {
        let row = sqlx::query(
            "UPDATE runs SET
                 status='queued', attempt=0, error=NULL, next_attempt_at=0,
                 cancel_requested=FALSE, worker_id=NULL, lease_until=NULL, finished_at=NULL,
                 cursor   = CASE WHEN $3 THEN 0 ELSE cursor END,
                 context  = CASE WHEN $3 THEN '{}'::jsonb ELSE context END,
                 started_at = CASE WHEN $3 THEN NULL ELSE started_at END
             WHERE id=$1 AND tenant_id=$2 AND status IN ('failed','dead_letter','canceled')
             RETURNING *",
        )
        .bind(id).bind(tenant_id).bind(restart)
        .fetch_optional(&self.pool).await?;
        Ok(row.map(run_from_row))
    }



    pub async fn insert_run_step(&self, s: &RunStep) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO run_steps (id, run_id, step_index, step_id, kind, status, output, error, started_at, finished_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(&s.id).bind(&s.run_id).bind(s.step_index).bind(&s.step_id).bind(&s.kind)
        .bind(&s.status).bind(&s.output).bind(&s.error).bind(s.started_at).bind(s.finished_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_run_steps(&self, run_id: &str) -> Result<Vec<RunStep>, DbError> {
        let rows = sqlx::query("SELECT * FROM run_steps WHERE run_id=$1 ORDER BY step_index")
            .bind(run_id).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(run_step_from_row).collect())
    }

    // ── Schedules ─────────────────────────────────────────────────────────────
    pub async fn create_schedule(&self, s: &Schedule) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO schedules (id, workflow_id, tenant_id, cron, enabled, created_at)
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(&s.id).bind(&s.workflow_id).bind(&s.tenant_id).bind(&s.cron).bind(s.enabled).bind(s.created_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_enabled_schedules(&self) -> Result<Vec<Schedule>, DbError> {
        let rows = sqlx::query("SELECT * FROM schedules WHERE enabled = TRUE")
            .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(schedule_from_row).collect())
    }

    pub async fn list_schedules(&self, tenant_id: &str, workflow_id: &str) -> Result<Vec<Schedule>, DbError> {
        let rows = sqlx::query(
            "SELECT * FROM schedules WHERE tenant_id=$1 AND workflow_id=$2 ORDER BY created_at",
        )
        .bind(tenant_id).bind(workflow_id)
        .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(schedule_from_row).collect())
    }

    pub async fn set_schedule_last_run(&self, id: &str, ts: i64) -> Result<(), DbError> {
        sqlx::query("UPDATE schedules SET last_run=$2 WHERE id=$1")
            .bind(id).bind(ts).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn delete_schedule(&self, tenant_id: &str, id: &str) -> Result<bool, DbError> {
        let r = sqlx::query("DELETE FROM schedules WHERE id=$1 AND tenant_id=$2")
            .bind(id).bind(tenant_id).execute(&self.pool).await?;
        Ok(r.rows_affected() > 0)
    }

    // ── Webhooks ────────────────────────────────────────────────────────────
    pub async fn create_webhook(&self, w: &Webhook) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO webhooks (token, workflow_id, tenant_id, enabled, created_at) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(&w.token).bind(&w.workflow_id).bind(&w.tenant_id).bind(w.enabled).bind(w.created_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_webhook(&self, token: &str) -> Result<Option<Webhook>, DbError> {
        let row = sqlx::query("SELECT * FROM webhooks WHERE token=$1 AND enabled=TRUE")
            .bind(token).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| Webhook {
            token: r.get("token"),
            workflow_id: r.get("workflow_id"),
            tenant_id: r.get("tenant_id"),
            enabled: r.get("enabled"),
            created_at: r.get("created_at"),
        }))
    }

    // ── Approvals ───────────────────────────────────────────────────────────
    pub async fn create_approval(&self, a: &Approval) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO approvals (id, run_id, tenant_id, step_id, status, created_at) VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(&a.id).bind(&a.run_id).bind(&a.tenant_id).bind(&a.step_id).bind(&a.status).bind(a.created_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_approval(&self, tenant_id: &str, id: &str) -> Result<Option<Approval>, DbError> {
        let row = sqlx::query("SELECT * FROM approvals WHERE id=$1 AND tenant_id=$2")
            .bind(id).bind(tenant_id).fetch_optional(&self.pool).await?;
        Ok(row.map(approval_from_row))
    }

    /// Pending approvals for a tenant, newest first — backs the Approvals queue.
    pub async fn list_pending_approvals(&self, tenant_id: &str, limit: i64) -> Result<Vec<Approval>, DbError> {
        let rows = sqlx::query(
            "SELECT * FROM approvals WHERE tenant_id=$1 AND status='pending' ORDER BY created_at DESC LIMIT $2",
        )
        .bind(tenant_id).bind(limit.clamp(1, 500)).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(approval_from_row).collect())
    }

    pub async fn resolve_approval(&self, id: &str, status: &str, decided_by: &str, note: Option<&str>) -> Result<bool, DbError> {
        let r = sqlx::query(
            "UPDATE approvals SET status=$2, decided_by=$3, note=$4, decided_at=$5
             WHERE id=$1 AND status='pending'",
        )
        .bind(id).bind(status).bind(decided_by).bind(note).bind(now_secs())
        .execute(&self.pool).await?;
        Ok(r.rows_affected() > 0)
    }

    // ── Templates ───────────────────────────────────────────────────────────
    pub async fn create_template(&self, t: &Template) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO templates (id, tenant_id, name, description, steps, created_at) VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(&t.id).bind(&t.tenant_id).bind(&t.name).bind(&t.description)
        .bind(serde_json::to_value(&t.steps)?).bind(t.created_at)
        .execute(&self.pool).await?;
        Ok(())
    }

    /// Templates visible to a tenant: its own plus global (tenant_id IS NULL).
    pub async fn list_templates(&self, tenant_id: &str) -> Result<Vec<Template>, DbError> {
        let rows = sqlx::query("SELECT * FROM templates WHERE tenant_id = $1 OR tenant_id IS NULL ORDER BY name")
            .bind(tenant_id).fetch_all(&self.pool).await?;
        rows.into_iter().map(template_from_row).collect()
    }
}

// ── Webhook lives here to keep model.rs free of storage concerns ────────────
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Webhook {
    pub token: String,
    pub workflow_id: String,
    pub tenant_id: String,
    pub enabled: bool,
    pub created_at: i64,
}

// ── Row → model mappers ─────────────────────────────────────────────────────
fn workflow_from_row(r: sqlx::postgres::PgRow) -> Result<Workflow, DbError> {
    let steps: serde_json::Value = r.get("steps");
    Ok(Workflow {
        id: r.get("id"),
        tenant_id: r.get("tenant_id"),
        name: r.get("name"),
        description: r.get("description"),
        steps: serde_json::from_value(steps)?,
        enabled: r.get("enabled"),
        version: r.get("version"),
        created_by: r.get("created_by"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    })
}

fn run_from_row(r: sqlx::postgres::PgRow) -> Run {
    let steps = r
        .get::<Option<serde_json::Value>, _>("steps")
        .and_then(|v| serde_json::from_value(v).ok());
    Run {
        id: r.get("id"),
        workflow_id: r.get("workflow_id"),
        tenant_id: r.get("tenant_id"),
        status: r.get("status"),
        trigger: r.get("trigger"),
        cursor: r.get("cursor"),
        context: r.get("context"),
        error: r.get("error"),
        idempotency_key: r.get("idempotency_key"),
        attempt: r.get("attempt"),
        max_attempts: r.get("max_attempts"),
        next_attempt_at: r.get("next_attempt_at"),
        cancel_requested: r.get("cancel_requested"),
        steps,
        workflow_version: r.get("workflow_version"),
        created_at: r.get("created_at"),
        started_at: r.get("started_at"),
        finished_at: r.get("finished_at"),
    }
}

fn run_step_from_row(r: sqlx::postgres::PgRow) -> RunStep {
    RunStep {
        id: r.get("id"),
        run_id: r.get("run_id"),
        step_index: r.get("step_index"),
        step_id: r.get("step_id"),
        kind: r.get("kind"),
        status: r.get("status"),
        output: r.get("output"),
        error: r.get("error"),
        started_at: r.get("started_at"),
        finished_at: r.get("finished_at"),
    }
}

fn schedule_from_row(r: sqlx::postgres::PgRow) -> Schedule {
    Schedule {
        id: r.get("id"),
        workflow_id: r.get("workflow_id"),
        tenant_id: r.get("tenant_id"),
        cron: r.get("cron"),
        enabled: r.get("enabled"),
        last_run: r.get("last_run"),
        created_at: r.get("created_at"),
    }
}

fn approval_from_row(r: sqlx::postgres::PgRow) -> Approval {
    Approval {
        id: r.get("id"),
        run_id: r.get("run_id"),
        tenant_id: r.get("tenant_id"),
        step_id: r.get("step_id"),
        status: r.get("status"),
        note: r.get("note"),
        decided_by: r.get("decided_by"),
        created_at: r.get("created_at"),
        decided_at: r.get("decided_at"),
    }
}

fn template_from_row(r: sqlx::postgres::PgRow) -> Result<Template, DbError> {
    let steps: serde_json::Value = r.get("steps");
    Ok(Template {
        id: r.get("id"),
        tenant_id: r.get("tenant_id"),
        name: r.get("name"),
        description: r.get("description"),
        steps: serde_json::from_value(steps)?,
        created_at: r.get("created_at"),
    })
}

// ── Integration tests ───────────────────────────────────────────────────────
// Require a live TintFlow Postgres with migrations applied. Use a disposable DB:
//   $env:TINTFLOW_DATABASE_URL = "postgres://tintflow:tintflow@localhost:5433/tintflow"
//   cargo test --manifest-path services/tintflow/Cargo.toml -- --ignored
#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> Db {
        let url = std::env::var("TINTFLOW_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| "postgres://tintflow:tintflow@localhost:5433/tintflow".to_string());
        Db::connect(&url).await.expect("connect tintflow test db (set TINTFLOW_DATABASE_URL)")
    }

    fn sample_workflow() -> Workflow {
        let now = now_secs();
        Workflow {
            id: format!("wf_{}", uuid::Uuid::new_v4().simple()),
            tenant_id: format!("t_{}", uuid::Uuid::new_v4().simple()),
            name: "Test WF".into(),
            description: "d".into(),
            steps: vec![
                Step { id: "a".into(), kind: "log".into(), config: serde_json::json!({"message":"hi"}) },
                Step { id: "b".into(), kind: "delay".into(), config: serde_json::json!({"seconds":0}) },
            ],
            enabled: true,
            version: 1,
            created_by: Some("tester".into()),
            created_at: now,
            updated_at: now,
        }
    }

    fn sample_run(wf: &Workflow) -> Run {
        crate::scheduler::new_run(wf, "manual")
    }

    #[tokio::test] #[ignore]
    async fn workflow_crud_roundtrip() {
        let db = test_db().await;
        let wf = sample_workflow();
        db.create_workflow(&wf).await.unwrap();

        let got = db.get_workflow(&wf.tenant_id, &wf.id).await.unwrap().unwrap();
        assert_eq!(got.name, "Test WF");
        assert_eq!(got.steps.len(), 2);
        assert_eq!(got.steps[0].kind, "log");

        let mut updated = got.clone();
        updated.name = "Renamed".into();
        assert!(db.update_workflow(&updated).await.unwrap());
        assert_eq!(db.get_workflow(&wf.tenant_id, &wf.id).await.unwrap().unwrap().name, "Renamed");

        assert!(db.list_workflows(&wf.tenant_id).await.unwrap().iter().any(|w| w.id == wf.id));
        assert!(db.delete_workflow(&wf.tenant_id, &wf.id).await.unwrap());
        assert!(db.get_workflow(&wf.tenant_id, &wf.id).await.unwrap().is_none());
    }

    #[tokio::test] #[ignore]
    async fn run_lifecycle_and_step_log() {
        let db = test_db().await;
        let wf = sample_workflow();
        db.create_workflow(&wf).await.unwrap();

        let mut run = sample_run(&wf);
        db.create_run(&run).await.unwrap();

        db.insert_run_step(&RunStep {
            id: format!("rs_{}", uuid::Uuid::new_v4().simple()),
            run_id: run.id.clone(), step_index: 0, step_id: "a".into(), kind: "log".into(),
            status: "succeeded".into(), output: serde_json::json!({"logged":"hi"}), error: None,
            started_at: now_secs(), finished_at: now_secs(),
        }).await.unwrap();

        run.status = run_status::SUCCEEDED.into();
        run.finished_at = Some(now_secs());
        db.save_run_progress(&run).await.unwrap();

        let got = db.get_run(&wf.tenant_id, &run.id).await.unwrap().unwrap();
        assert_eq!(got.status, run_status::SUCCEEDED);
        let steps = db.list_run_steps(&run.id).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].output["logged"], "hi");
        assert!(db.list_runs(&wf.tenant_id, Some(&wf.id), None, 10).await.unwrap().iter().any(|r| r.id == run.id));
        assert!(db.list_runs(&wf.tenant_id, None, Some(run_status::SUCCEEDED), 10).await.unwrap().iter().any(|r| r.id == run.id));
        assert!(!db.list_runs(&wf.tenant_id, None, Some(run_status::DEAD_LETTER), 10).await.unwrap().iter().any(|r| r.id == run.id));
    }

    #[tokio::test] #[ignore]
    async fn queue_claim_reap_cancel_requeue() {
        let db = test_db().await;
        let wf = sample_workflow();
        db.create_workflow(&wf).await.unwrap();
        let now = now_secs();

        // Claim: a queued run is claimable exactly once.
        let run = sample_run(&wf);
        db.create_run(&run).await.unwrap();
        let claimed = db.claim_due_runs("w1", now, 60, 10).await.unwrap();
        let mine: Vec<_> = claimed.iter().filter(|r| r.id == run.id).collect();
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].attempt, 1);
        assert_eq!(mine[0].status, run_status::RUNNING);
        assert!(!db.claim_due_runs("w2", now, 60, 10).await.unwrap().iter().any(|r| r.id == run.id));

        // Reap: an expired lease requeues as retrying (attempts remain).
        db.extend_lease(&run.id, now - 1).await.unwrap();
        assert!(db.reap_stale_runs(now).await.unwrap() >= 1);
        let got = db.get_run(&wf.tenant_id, &run.id).await.unwrap().unwrap();
        assert_eq!(got.status, run_status::RETRYING);

        // Cancel: a retrying run cancels immediately.
        let canceled = db.request_cancel(&wf.tenant_id, &run.id, now).await.unwrap().unwrap();
        assert_eq!(canceled.status, run_status::CANCELED);

        // Requeue: a canceled run can be retried with a fresh budget.
        let requeued = db.requeue_run(&wf.tenant_id, &run.id, false).await.unwrap().unwrap();
        assert_eq!(requeued.status, run_status::QUEUED);
        assert_eq!(requeued.attempt, 0);
        assert!(!requeued.cancel_requested);

        // Terminal runs other than failed/dead_letter/canceled can't requeue.
        let mut done = db.get_run(&wf.tenant_id, &run.id).await.unwrap().unwrap();
        done.status = run_status::SUCCEEDED.into();
        db.save_run_progress(&done).await.unwrap();
        assert!(db.requeue_run(&wf.tenant_id, &run.id, true).await.unwrap().is_none());
    }

    #[tokio::test] #[ignore]
    async fn approval_create_and_resolve() {
        let db = test_db().await;
        let wf = sample_workflow();
        db.create_workflow(&wf).await.unwrap();
        let mut run = sample_run(&wf);
        run.status = run_status::WAITING_APPROVAL.into();
        run.started_at = Some(now_secs());
        db.create_run(&run).await.unwrap();
        let ap = Approval {
            id: format!("ap_{}", uuid::Uuid::new_v4().simple()),
            run_id: run.id.clone(), tenant_id: wf.tenant_id.clone(), step_id: "gate".into(),
            status: "pending".into(), note: None, decided_by: None, created_at: now_secs(), decided_at: None,
        };
        db.create_approval(&ap).await.unwrap();
        assert_eq!(db.get_approval(&wf.tenant_id, &ap.id).await.unwrap().unwrap().status, "pending");
        assert!(db.resolve_approval(&ap.id, "approved", "boss", Some("ok")).await.unwrap());
        // Second resolve is a no-op (already decided).
        assert!(!db.resolve_approval(&ap.id, "rejected", "boss", None).await.unwrap());
        assert_eq!(db.get_approval(&wf.tenant_id, &ap.id).await.unwrap().unwrap().status, "approved");
    }

    #[tokio::test] #[ignore]
    async fn schedule_webhook_and_templates() {
        let db = test_db().await;
        let wf = sample_workflow();
        db.create_workflow(&wf).await.unwrap();

        let sched = Schedule {
            id: format!("sch_{}", uuid::Uuid::new_v4().simple()),
            workflow_id: wf.id.clone(), tenant_id: wf.tenant_id.clone(),
            cron: "*/5 * * * *".into(), enabled: true, last_run: None, created_at: now_secs(),
        };
        db.create_schedule(&sched).await.unwrap();
        assert!(db.list_enabled_schedules().await.unwrap().iter().any(|s| s.id == sched.id));
        db.set_schedule_last_run(&sched.id, now_secs()).await.unwrap();

        let hook = Webhook {
            token: format!("whk_{}", uuid::Uuid::new_v4().simple()),
            workflow_id: wf.id.clone(), tenant_id: wf.tenant_id.clone(), enabled: true, created_at: now_secs(),
        };
        db.create_webhook(&hook).await.unwrap();
        assert_eq!(db.get_webhook(&hook.token).await.unwrap().unwrap().workflow_id, wf.id);

        let tpl = Template {
            id: format!("tpl_{}", uuid::Uuid::new_v4().simple()),
            tenant_id: Some(wf.tenant_id.clone()), name: "T".into(), description: "".into(),
            steps: wf.steps.clone(), created_at: now_secs(),
        };
        db.create_template(&tpl).await.unwrap();
        assert!(db.list_templates(&wf.tenant_id).await.unwrap().iter().any(|t| t.id == tpl.id));

        db.delete_schedule(&wf.tenant_id, &sched.id).await.unwrap();
    }
}
