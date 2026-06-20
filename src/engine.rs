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
use std::sync::OnceLock;

static MONOLITH_URL: OnceLock<String> = OnceLock::new();
static INTERNAL_API_TOKEN: OnceLock<String> = OnceLock::new();

fn monolith_url() -> &'static str {
    MONOLITH_URL.get_or_init(|| std::env::var("MONOLITH_URL").unwrap_or_else(|_| "http://api:3000".to_string()))
}

fn internal_api_token() -> &'static str {
    INTERNAL_API_TOKEN.get_or_init(|| std::env::var("INTERNAL_API_TOKEN").unwrap_or_else(|_| "changeme".to_string()))
}

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
        if run.cancel_requested {
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

        if step.kind == "delay" {
            let secs = step.config.get("seconds").and_then(|v| v.as_u64()).unwrap_or(0);
            let started = now_secs();
            let output = serde_json::json!({ "delayed_secs": secs });
            
            db.insert_run_step(&RunStep {
                id: format!("rs_{}", uuid::Uuid::new_v4().simple()),
                run_id: run.id.clone(),
                step_index: idx as i32,
                step_id: step.id.clone(),
                kind: step.kind.clone(),
                status: "succeeded".to_string(),
                output: output.clone(),
                error: None,
                started_at: started,
                finished_at: started,
            }).await?;
            
            if let Some(obj) = run.context.as_object_mut() {
                obj.insert(step.id.clone(), output);
            }
            run.cursor = idx as i32 + 1;
            run.status = run_status::SLEEPING.to_string();
            run.next_attempt_at = now_secs() + secs as i64;
            db.save_run_progress(run).await?;
            return Ok(());
        }

        // Renew the claim before the (bounded) step so the reaper never
        // steals a run from a healthy worker.
        if db.extend_lease(&run.id, now_secs() + LEASE_SECS).await? {
            run.cancel_requested = true;
            run.status = run_status::CANCELED.to_string();
            run.error = Some("canceled by user".to_string());
            run.finished_at = Some(now_secs());
            db.save_run_progress(run).await?;
            crate::metrics::RUNS_CANCELED_TOTAL.inc();
            crate::events::run_canceled(run).await;
            return Ok(());
        }

        let started = now_secs();
        let result = execute_step(step, &run.context, &run.tenant_id, &run.id, db).await;
        let finished = now_secs();

        // subworkflow pauses immediately without saving an output yet
        if step.kind == "subworkflow" && result.is_ok() {
            run.cursor = idx as i32;
            run.status = run_status::WAITING_SUBWORKFLOW.to_string();
            db.save_run_progress(run).await?;
            return Ok(());
        }

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
            obj.insert(step.id.clone(), output.clone());
        }
        
        if step.kind == "branch" && error.is_none() {
            if let Some(next_step_id) = output.get("next_step").and_then(|v| v.as_str()) {
                if let Some(next_idx) = steps.iter().position(|s| s.id == next_step_id) {
                    idx = next_idx;
                } else {
                    idx += 1;
                }
            } else {
                idx += 1;
            }
        } else {
            idx += 1;
        }
        
        run.cursor = idx as i32;
        db.save_run_progress(run).await?;
    }

    run.status = run_status::SUCCEEDED.to_string();
    run.finished_at = Some(now_secs());
    db.save_run_progress(run).await?;
    crate::events::run_succeeded(run).await;
    
    // Subworkflow resume: if we were triggered as a subworkflow, resume the parent.
    if let Some(parent) = run.context.get("_parent").and_then(|v| v.as_object()) {
        if let (Some(parent_run_id), Some(parent_step_id)) = (
            parent.get("run_id").and_then(|v| v.as_str()),
            parent.get("step_id").and_then(|v| v.as_str())
        ) {
            if let Ok(Some(mut p_run)) = db.get_run(&run.tenant_id, parent_run_id).await {
                if p_run.status == run_status::WAITING_SUBWORKFLOW {
                    if let Some(obj) = p_run.context.as_object_mut() {
                        obj.insert(parent_step_id.to_string(), run.context.clone());
                    }
                    p_run.cursor += 1;
                    p_run.status = run_status::QUEUED.to_string();
                    db.save_run_progress(&p_run).await?;
                    
                    db.insert_run_step(&RunStep {
                        id: format!("rs_{}", uuid::Uuid::new_v4().simple()),
                        run_id: p_run.id.clone(),
                        step_index: (p_run.cursor - 1) as i32,
                        step_id: parent_step_id.to_string(),
                        kind: "subworkflow".to_string(),
                        status: "succeeded".to_string(),
                        output: run.context.clone(),
                        error: None,
                        started_at: now_secs(),
                        finished_at: now_secs(),
                    }).await?;
                    
                    crate::worker::kick();
                }
            }
        }
    }
    
    Ok(())
}

/// Run one step. Returns its JSON output or an error string for the log.
async fn execute_step(step: &Step, ctx: &serde_json::Value, tenant_id: &str, run_id: &str, db: &Db) -> Result<serde_json::Value, String> {
    match step.kind.as_str() {
        "log" => {
            let msg = step.config.get("message").and_then(|v| v.as_str()).unwrap_or("");
            tracing::info!(step = %step.id, "log: {msg}");
            Ok(serde_json::json!({ "logged": msg }))
        }
        "script" => execute_script(step, ctx).await,
        "branch" => execute_branch(step, ctx).await,
        "foreach" => execute_foreach(step, ctx, tenant_id, run_id, db).await,
        "subworkflow" => execute_subworkflow(step, ctx, tenant_id, run_id, db).await,
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
    let url = format!("{}/internal/tintflow/step", monolith_url());
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
        .bearer_auth(internal_api_token())
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
// ... existing http logic handled before

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
    
    let content_length = resp.content_length().unwrap_or(0);
    if content_length > 5 * 1024 * 1024 {
        return Err(format!("http response too large: {} bytes (max 5MB)", content_length));
    }
    
    let bytes = resp.bytes().await.map_err(|e| format!("failed to read body: {e}"))?;
    if bytes.len() > 5 * 1024 * 1024 {
        return Err("http response too large (max 5MB)".to_string());
    }
    let text = String::from_utf8(bytes.to_vec()).unwrap_or_default();
    
    // Try to surface JSON bodies structurally; fall back to a string.
    let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
    if !(200..300).contains(&status) {
        return Err(format!("http {status}"));
    }
    Ok(serde_json::json!({ "status": status, "body": body }))
}

async fn execute_script(step: &Step, ctx: &serde_json::Value) -> Result<serde_json::Value, String> {
    let script = step.config.get("script").and_then(|v| v.as_str())
        .ok_or("script step requires a 'script' field")?;
    
    let engine = rhai::Engine::new();
    let mut scope = rhai::Scope::new();
    
    let dynamic_ctx: rhai::Dynamic = rhai::serde::to_dynamic(ctx.clone())
        .map_err(|e| format!("failed to convert context: {e}"))?;
        
    scope.push("ctx", dynamic_ctx);
    
    let result: rhai::Dynamic = engine.eval_with_scope(&mut scope, script)
        .map_err(|e| format!("script error: {e}"))?;
        
    rhai::serde::from_dynamic(&result).map_err(|e| format!("failed to convert script result: {e}"))
}

async fn execute_branch(step: &Step, ctx: &serde_json::Value) -> Result<serde_json::Value, String> {
    let condition = step.config.get("condition").and_then(|v| v.as_str())
        .ok_or("branch requires 'condition'")?;
    let true_step = step.config.get("true_step").and_then(|v| v.as_str());
    let false_step = step.config.get("false_step").and_then(|v| v.as_str());
    
    let engine = rhai::Engine::new();
    let mut scope = rhai::Scope::new();
    
    let dynamic_ctx: rhai::Dynamic = rhai::serde::to_dynamic(ctx.clone())
        .map_err(|e| format!("failed to convert context: {e}"))?;
    scope.push("ctx", dynamic_ctx);
    
    let is_true: bool = engine.eval_with_scope(&mut scope, condition)
        .map_err(|e| format!("branch condition error: {e}"))?;
        
    let next_step = if is_true { true_step } else { false_step };
    Ok(serde_json::json!({ "condition": condition, "evaluated_to": is_true, "next_step": next_step }))
}

async fn execute_subworkflow(step: &Step, _ctx: &serde_json::Value, tenant_id: &str, run_id: &str, db: &Db) -> Result<serde_json::Value, String> {
    let target_wf_id = step.config.get("workflow_id").and_then(|v| v.as_str())
        .ok_or("subworkflow requires 'workflow_id'")?;
    
    let empty_payload = serde_json::json!({});
    let payload = step.config.get("payload").unwrap_or(&empty_payload);
    
    let wf = db.get_workflow(tenant_id, target_wf_id).await.map_err(|e| e.to_string())?
        .ok_or("target workflow not found")?;
        
    if !wf.enabled {
        return Err("target workflow is disabled".to_string());
    }
    
    let mut child_run = crate::scheduler::new_run(&wf, "subworkflow");
    if let Some(obj) = child_run.context.as_object_mut() {
        obj.insert("trigger".into(), payload.clone());
        obj.insert("_parent".into(), serde_json::json!({
            "run_id": run_id,
            "step_id": step.id
        }));
    }
    
    // Add idempotency so retries don't spawn multiple
    child_run.idempotency_key = Some(format!("{}_{}_sub", run_id, step.id));
    
    db.create_run(&child_run).await.map_err(|e| e.to_string())?;
    crate::worker::kick();
    
    Ok(serde_json::json!({ "child_run_id": child_run.id }))
}

async fn execute_foreach(step: &Step, ctx: &serde_json::Value, tenant_id: &str, run_id: &str, db: &Db) -> Result<serde_json::Value, String> {
    let target_wf_id = step.config.get("workflow_id").and_then(|v| v.as_str())
        .ok_or("foreach requires 'workflow_id'")?;
        
    let items_expr = step.config.get("items").and_then(|v| v.as_str())
        .ok_or("foreach requires 'items' expression")?;
        
    let engine = rhai::Engine::new();
    let mut scope = rhai::Scope::new();
    let dynamic_ctx: rhai::Dynamic = rhai::serde::to_dynamic(ctx.clone())
        .map_err(|e| format!("failed to convert context: {e}"))?;
    scope.push("ctx", dynamic_ctx);
    
    let result: rhai::Dynamic = engine.eval_with_scope(&mut scope, items_expr)
        .map_err(|e| format!("foreach items error: {e}"))?;
        
    let items: Vec<serde_json::Value> = rhai::serde::from_dynamic(&result)
        .map_err(|e| format!("items must evaluate to an array: {e}"))?;
        
    let wf = db.get_workflow(tenant_id, target_wf_id).await.map_err(|e| e.to_string())?
        .ok_or("target workflow not found")?;
        
    if !wf.enabled {
        return Err("target workflow is disabled".to_string());
    }
    
    let mut child_run_ids = Vec::new();
    for (i, item) in items.into_iter().enumerate() {
        let mut child_run = crate::scheduler::new_run(&wf, "foreach");
        if let Some(obj) = child_run.context.as_object_mut() {
            obj.insert("item".into(), item);
            obj.insert("_parent_run".into(), serde_json::json!(run_id));
        }
        child_run.idempotency_key = Some(format!("{}_{}_foreach_{}", run_id, step.id, i));
        db.create_run(&child_run).await.map_err(|e| e.to_string())?;
        child_run_ids.push(child_run.id);
    }
    
    crate::worker::kick();
    
    Ok(serde_json::json!({ "spawned_runs": child_run_ids }))
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
