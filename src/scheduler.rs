//! Background scheduler. Ticks once per minute, fires any enabled schedule whose
//! cron matches the current minute, and executes the resulting run. Dedupe is by
//! minute bucket: a schedule fires at most once per calendar minute.

use crate::{cron, db::Db, model::*};
use std::sync::Arc;

pub fn spawn(db: Arc<Db>) {
    tokio::spawn(async move {
        // Align loosely to the top of each minute, then tick every 60s.
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            ticker.tick().await;
            if let Err(e) = tick(&db).await {
                tracing::warn!(error = %e, "scheduler tick failed");
            }
        }
    });
}

async fn tick(db: &Db) -> Result<(), crate::db::DbError> {
    let now = now_secs();
    let minute_bucket = now / 60;
    let schedules = db.list_enabled_schedules().await?;
    for s in schedules {
        // Skip if already fired in this minute bucket.
        if s.last_run.map(|t| t / 60) == Some(minute_bucket) {
            continue;
        }
        if !cron::matches(&s.cron, now) {
            continue;
        }
        if let Err(e) = fire(db, &s).await {
            tracing::warn!(schedule = %s.id, error = %e, "schedule fire failed");
        } else {
            db.set_schedule_last_run(&s.id, now).await?;
        }
    }
    Ok(())
}

async fn fire(db: &Db, s: &Schedule) -> Result<(), crate::db::DbError> {
    let Some(wf) = db.get_workflow(&s.tenant_id, &s.workflow_id).await? else {
        return Ok(()); // workflow deleted out from under the schedule
    };
    if !wf.enabled {
        return Ok(());
    }
    let run = new_run(&wf, "schedule");
    db.create_run(&run).await?;
    tracing::info!(schedule = %s.id, run = %run.id, workflow = %wf.id, "schedule fired");
    // The durable queue worker picks it up; wake it so dispatch is immediate.
    crate::worker::kick();
    Ok(())
}

/// Build a fresh queued run for a workflow, snapshotting its steps so later
/// edits to the workflow can't change what this run executes.
pub fn new_run(wf: &Workflow, trigger: &str) -> Run {
    Run {
        id: format!("run_{}", uuid::Uuid::new_v4().simple()),
        workflow_id: wf.id.clone(),
        tenant_id: wf.tenant_id.clone(),
        status: run_status::QUEUED.to_string(),
        trigger: trigger.to_string(),
        cursor: 0,
        context: serde_json::json!({}),
        error: None,
        idempotency_key: None,
        attempt: 0,
        max_attempts: 3,
        next_attempt_at: 0,
        cancel_requested: false,
        steps: Some(wf.steps.clone()),
        workflow_version: wf.version,
        created_at: now_secs(),
        started_at: None,
        finished_at: None,
    }
}
