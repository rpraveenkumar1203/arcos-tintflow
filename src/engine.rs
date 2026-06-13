//! Workflow execution engine. Runs steps sequentially from `run.cursor`,
//! recording each as a `run_step` (the execution log) and threading outputs
//! through `run.context[step_id]`. An `approval` step pauses the run; resuming
//! requeues it from the saved cursor.
//!
//! Durability model: runs are CLAIMED from the Postgres queue by `worker.rs`
//! (never executed fire-and-forget), the worker's lease is renewed at every
//! step boundary, a failed step requeues the run with exponential backoff
//! until `max_attempts` is exhausted (then it dead-letters), and cancellation
//! is cooperative — checked between steps.

use crate::db::Db;
use crate::model::*;

/// How long a claim is valid without renewal. Individual steps are bounded
/// well below this (http 20s, delay ≤30s), so a live worker always renews in
/// time; only a dead worker lets the lease lapse for the reaper.
pub const LEASE_SECS: i64 = 60;

/// Execute a run that was already claimed (status=running, attempt counted).
/// Drives it to a terminal state, an approval pause, or a backoff requeue.
pub async fn execute_claimed(db: &Db, run: &mut Run) -> Result<(), crate::db::DbError> {
    // Runs execute their enqueue-time snapshot so a concurrent workflow edit
    // can never change in-flight behavior. Pre-snapshot rows (steps = NULL)
    // fall back to the live definition.
    let steps: Vec<Step> = match run.steps.clone() {
        Some(s) => s,
        None => db
            .get_workflow(&run.tenant_id, &run.workflow_id)
            .await?
            .map(|wf| wf.steps)
            .unwrap_or_default(),
    };

    run.error = None;
    let mut idx = run.cursor as usize;
    while idx < steps.len() {
        // Cooperative cancel: take effect at step boundaries.
        if run.cancel_requested || db.cancel_requested(&run.id).await? {
            run.status = run_status::CANCELED.to_string();
            run.error = Some("canceled by user".to_string());
            run.finished_at = Some(now_secs());
            db.save_run_progress(run).await?;
            crate::metrics::RUNS_CANCELED_TOTAL.inc();
            crate::events::run_canceled(run).await;
            return Ok(());
        }

        let step = &steps[idx];

        if step.kind == "approval" {
            // Pause: create a pending approval and stop. Resume re-enters here.
            let approval = Approval {
                id: format!("ap_{}", uuid::Uuid::new_v4().simple()),
                run_id: run.id.clone(),
                tenant_id: run.tenant_id.clone(),
                step_id: step.id.clone(),
                status: "pending".to_string(),
                note: None,
                decided_by: None,
                created_at: now_secs(),
                decided_at: None,
            };
            db.create_approval(&approval).await?;
            run.cursor = idx as i32; // resume AT the approval step
            run.status = run_status::WAITING_APPROVAL.to_string();
            db.save_run_progress(run).await?;
            crate::events::approval_requested(run, &step.id).await;
            return Ok(());
        }

        // Renew the claim before the (bounded) step so the reaper never
        // steals a run from a healthy worker.
        db.extend_lease(&run.id, now_secs() + LEASE_SECS).await?;

        let started = now_secs();
        let result = execute_step(step, &run.context, &run.tenant_id).await;
        let finished = now_secs();

        let (status, output, error) = match result {
            Ok(out) => ("succeeded", out, None),
            Err(e) => ("failed", serde_json::json!({}), Some(e)),
        };

        db.insert_run_step(&RunStep {
            id: format!("rs_{}", uuid::Uuid::new_v4().simple()),
            run_id: run.id.clone(),
            step_index: idx as i32,
            step_id: step.id.clone(),
            kind: step.kind.clone(),
            status: status.to_string(),
            output: output.clone(),
            error: error.clone(),
            started_at: started,
            finished_at: finished,
        }).await?;

        if let Some(err) = error {
            run.cursor = idx as i32;
            if run.attempt >= run.max_attempts {
                // Retry budget exhausted: park in the dead-letter queue for
                // operator inspection / manual retry.
                run.status = run_status::DEAD_LETTER.to_string();
                run.error = Some(err);
                run.finished_at = Some(finished);
                db.save_run_progress(run).await?;
                crate::metrics::RUNS_DEAD_LETTER_TOTAL.inc();
                crate::events::run_dead_letter(run).await;
            } else {
                // Requeue with backoff; the retry resumes AT the failed step
                // with the accumulated context intact.
                run.status = run_status::RETRYING.to_string();
                run.error = Some(err);
                run.next_attempt_at = now_secs() + backoff_secs(run.attempt);
                db.save_run_progress(run).await?;
                crate::metrics::RUNS_RETRIED_TOTAL.inc();
                crate::events::run_retrying(run).await;
            }
            return Ok(());
        }

        // Thread the output into the shared context under the step id.
        if let Some(obj) = run.context.as_object_mut() {
            obj.insert(step.id.clone(), output);
        }
        idx += 1;
        run.cursor = idx as i32;
        db.save_run_progress(run).await?;
    }

    run.status = run_status::SUCCEEDED.to_string();
    run.finished_at = Some(now_secs());
    db.save_run_progress(run).await?;
    crate::events::run_succeeded(run).await;
    Ok(())
}

/// Run one step. Returns its JSON output or an error string for the log.
async fn execute_step(step: &Step, ctx: &serde_json::Value, tenant_id: &str) -> Result<serde_json::Value, String> {
    match step.kind.as_str() {
        "log" => {
            let msg = step.config.get("message").and_then(|v| v.as_str()).unwrap_or("");
            tracing::info!(step = %step.id, "log: {msg}");
            Ok(serde_json::json!({ "logged": msg }))
        }
        "delay" => {
            // Bounded so a misconfigured workflow can't pin a worker forever.
            let secs = step.config.get("seconds").and_then(|v| v.as_u64()).unwrap_or(0).min(30);
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            Ok(serde_json::json!({ "delayed_secs": secs }))
        }
        "http" => execute_http(step).await,
        "getData" | "ai" | "notify" | "email" | "text" => {
            execute_monolith_step(step, ctx, tenant_id).await
        }
        other => Err(format!("unknown step kind '{other}'")),
    }
}

/// Delegates a smart step (getData / ai / notify / text) to the monolith via
/// the internal service-to-service endpoint. The monolith handles SQL gen,
/// LLM calls, connector dispatch, and template interpolation.
async fn execute_monolith_step(
    step: &Step,
    ctx: &serde_json::Value,
    tenant_id: &str,
) -> Result<serde_json::Value, String> {
    let monolith_url = std::env::var("MONOLITH_URL")
        .unwrap_or_else(|_| "http://api:3000".to_string());
    let token = std::env::var("INTERNAL_API_TOKEN")
        .unwrap_or_else(|_| "changeme".to_string());

    let url = format!("{monolith_url}/internal/tintflow/step");
    let body = serde_json::json!({
        "tenant_id": tenant_id,
        "step_kind": step.kind,
        "params": step.config,
        "context": ctx,
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|e| e.to_string())?;

    let resp = client
        .post(&url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("monolith call failed: {e}"))?;

    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        return Err(format!("monolith returned {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|e| format!("invalid monolith response: {e}"))
}

async fn execute_http(step: &Step) -> Result<serde_json::Value, String> {
    let url = step.config.get("url").and_then(|v| v.as_str())
        .ok_or("http step requires a 'url'")?;
    let method = step.config.get("method").and_then(|v| v.as_str()).unwrap_or("GET").to_uppercase();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| e.to_string())?;

    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
    let mut req = client.request(method, url);

    if let Some(headers) = step.config.get("headers").and_then(|v| v.as_object()) {
        for (k, v) in headers {
            if let Some(vs) = v.as_str() {
                req = req.header(k, vs);
            }
        }
    }
    if let Some(body) = step.config.get("body") {
        req = req.json(body);
    }

    let resp = req.send().await.map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    // Try to surface JSON bodies structurally; fall back to a string.
    let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
    if !(200..300).contains(&status) {
        return Err(format!("http {status}"));
    }
    Ok(serde_json::json!({ "status": status, "body": body }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn log_step_succeeds() {
        let step = Step { id: "s1".into(), kind: "log".into(), config: serde_json::json!({"message":"hi"}) };
        let out = execute_step(&step, &serde_json::json!({}), "t1").await.unwrap();
        assert_eq!(out["logged"], "hi");
    }

    #[tokio::test]
    async fn unknown_step_errors() {
        let step = Step { id: "s1".into(), kind: "frobnicate".into(), config: serde_json::json!({}) };
        assert!(execute_step(&step, &serde_json::json!({}), "t1").await.is_err());
    }

    #[tokio::test]
    async fn http_requires_url() {
        let step = Step { id: "s1".into(), kind: "http".into(), config: serde_json::json!({}) };
        let err = execute_step(&step, &serde_json::json!({}), "t1").await.unwrap_err();
        assert!(err.contains("url"));
    }
}
