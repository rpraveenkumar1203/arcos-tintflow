//! NATS event bus. TintFlow publishes run-lifecycle events so other services
//! (the monolith, notifications) can react without polling. Entirely optional:
//! if `NATS_URL` is unset or the broker is down, publishing is a no-op
//! (fail-open) — event emission must never break workflow execution.
//!
//! Subjects:
//!   tintflow.run.succeeded   { run_id, workflow_id, tenant_id }
//!   tintflow.run.retrying    { run_id, workflow_id, tenant_id, error, attempt, max_attempts, next_attempt_at }
//!   tintflow.run.dead_letter { run_id, workflow_id, tenant_id, error, attempt }
//!   tintflow.run.canceled    { run_id, workflow_id, tenant_id }
//!   tintflow.approval.requested { run_id, workflow_id, tenant_id, step_id }

use std::sync::OnceLock;

static BUS: OnceLock<Option<async_nats::Client>> = OnceLock::new();

/// Connect to NATS once at startup. Safe to call with `None`/empty to disable.
pub async fn init(url: Option<&str>) {
    let client = match url {
        Some(u) if !u.trim().is_empty() => match async_nats::connect(u).await {
            Ok(c) => {
                tracing::info!(url = %u, "connected to NATS");
                Some(c)
            }
            Err(e) => {
                tracing::warn!(url = %u, error = %e, "NATS unavailable — events disabled");
                None
            }
        },
        _ => None,
    };
    let _ = BUS.set(client);
}

/// Publish a JSON event on `subject`. No-op when NATS is disabled/unreachable.
pub async fn publish(subject: &str, payload: serde_json::Value) {
    if let Some(Some(client)) = BUS.get() {
        let bytes = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(_) => return,
        };
        if let Err(e) = client.publish(subject.to_string(), bytes.into()).await {
            tracing::warn!(subject, error = %e, "failed to publish NATS event");
        }
    }
}

// ── Typed helpers for the run lifecycle ───────────────────────────────────────
use crate::model::Run;

pub async fn run_succeeded(run: &Run) {
    publish("tintflow.run.succeeded", serde_json::json!({
        "run_id": run.id, "workflow_id": run.workflow_id, "tenant_id": run.tenant_id,
    })).await;
}

pub async fn run_retrying(run: &Run) {
    publish("tintflow.run.retrying", serde_json::json!({
        "run_id": run.id, "workflow_id": run.workflow_id, "tenant_id": run.tenant_id,
        "error": run.error, "attempt": run.attempt, "max_attempts": run.max_attempts,
        "next_attempt_at": run.next_attempt_at,
    })).await;
}

pub async fn run_dead_letter(run: &Run) {
    publish("tintflow.run.dead_letter", serde_json::json!({
        "run_id": run.id, "workflow_id": run.workflow_id, "tenant_id": run.tenant_id,
        "error": run.error, "attempt": run.attempt,
    })).await;
}

pub async fn run_canceled(run: &Run) {
    publish("tintflow.run.canceled", serde_json::json!({
        "run_id": run.id, "workflow_id": run.workflow_id, "tenant_id": run.tenant_id,
    })).await;
}

pub async fn approval_requested(run: &Run, step_id: &str) {
    publish("tintflow.approval.requested", serde_json::json!({
        "run_id": run.id, "workflow_id": run.workflow_id, "tenant_id": run.tenant_id,
        "step_id": step_id,
    })).await;
}
