//! HTTP API for TintFlow. This is the service boundary the monolith (and, later,
//! the gateway) calls. Tenant identity arrives in the `X-Tenant-Id` header set by
//! the trusted caller — TintFlow runs on the internal network behind the gateway.

use crate::{db::{Db, DbError, Webhook}, metrics, model::*, scheduler, worker};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

fn e500(e: DbError) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

/// Tenant from the trusted `X-Tenant-Id` header (defaults to "default").
fn tenant(headers: &HeaderMap) -> String {
    headers.get("x-tenant-id").and_then(|v| v.to_str().ok()).unwrap_or("default").to_string()
}

pub fn router(db: Arc<Db>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/metrics", get(|| async { metrics::gather() }))
        .route("/workflows", post(create_workflow).get(list_workflows))
        .route("/workflows/{id}", get(get_workflow).put(update_workflow).delete(delete_workflow))
        .route("/workflows/{id}/run", post(run_workflow))
        .route("/workflows/{id}/webhooks", post(create_webhook))
        .route("/workflows/{id}/schedules", get(list_schedules).post(create_schedule))
        .route("/schedules/{id}", axum::routing::delete(delete_schedule))
        .route("/runs", get(list_runs))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/cancel", post(cancel_run))
        .route("/runs/{id}/retry", post(retry_run))
        .route("/runs/{id}/context", axum::routing::patch(patch_run_context))
        .route("/hooks/{token}", post(trigger_webhook))
        .route("/approvals", get(list_approvals))
        .route("/approvals/{id}", get(get_approval).post(resolve_approval))
        .route("/templates", get(list_templates).post(create_template))
        .with_state(db)
}

// ── Workflows ───────────────────────────────────────────────────────────────
#[derive(Deserialize)]
pub struct UpsertWorkflow {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub created_by: Option<String>,
}
fn yes() -> bool { true }

async fn create_workflow(State(db): State<Arc<Db>>, headers: HeaderMap, Json(b): Json<UpsertWorkflow>) -> ApiResult<Workflow> {
    let now = now_secs();
    let wf = Workflow {
        id: format!("wf_{}", uuid::Uuid::new_v4().simple()),
        tenant_id: tenant(&headers),
        name: b.name, description: b.description, steps: b.steps, enabled: b.enabled,
        version: 1,
        created_by: b.created_by, created_at: now, updated_at: now,
    };
    db.create_workflow(&wf).await.map_err(e500)?;
    Ok(Json(wf))
}

async fn list_workflows(State(db): State<Arc<Db>>, headers: HeaderMap) -> ApiResult<Vec<Workflow>> {
    Ok(Json(db.list_workflows(&tenant(&headers)).await.map_err(e500)?))
}

async fn get_workflow(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<Workflow> {
    db.get_workflow(&tenant(&headers), &id).await.map_err(e500)?
        .map(Json).ok_or((StatusCode::NOT_FOUND, "workflow not found".into()))
}

async fn update_workflow(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>, Json(b): Json<UpsertWorkflow>) -> ApiResult<Workflow> {
    let t = tenant(&headers);
    let mut wf = db.get_workflow(&t, &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "workflow not found".into()))?;
    wf.name = b.name; wf.description = b.description; wf.steps = b.steps; wf.enabled = b.enabled;
    db.update_workflow(&wf).await.map_err(e500)?;
    // The DB bumped the version; reflect it (and the new updated_at) here.
    wf.version += 1;
    wf.updated_at = now_secs();
    Ok(Json(wf))
}

async fn delete_workflow(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> impl IntoResponse {
    match db.delete_workflow(&tenant(&headers), &id).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── Runs ──────────────────────────────────────────────────────────────────
async fn run_workflow(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<Run> {
    let t = tenant(&headers);
    if let Some(key) = headers.get("Idempotency-Key").and_then(|v| v.to_str().ok()) {
        if let Ok(Some(existing)) = db.get_run_by_idempotency_key(&t, key).await {
            return Ok(Json(existing));
        }
    }

    let wf = db.get_workflow(&t, &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "workflow not found".into()))?;
    let mut run = scheduler::new_run(&wf, "manual");
    if let Some(key) = headers.get("Idempotency-Key").and_then(|v| v.to_str().ok()) {
        run.idempotency_key = Some(key.to_string());
    }
    db.create_run(&run).await.map_err(e500)?;
    metrics::RUNS_TOTAL.inc();
    // Enqueued durably; the worker executes it. Clients poll GET /runs/{id}.
    worker::kick();
    Ok(Json(run))
}

#[derive(Deserialize)]
pub struct RunQuery { pub workflow_id: Option<String>, pub status: Option<String>, pub limit: Option<i64> }

async fn list_runs(State(db): State<Arc<Db>>, headers: HeaderMap, Query(q): Query<RunQuery>) -> ApiResult<Vec<Run>> {
    let runs = db.list_runs(&tenant(&headers), q.workflow_id.as_deref(), q.status.as_deref(), q.limit.unwrap_or(50)).await.map_err(e500)?;
    Ok(Json(runs))
}

/// Cancel a run. Queued/retrying/waiting runs cancel immediately; running
/// runs cancel cooperatively at the next step boundary.
async fn cancel_run(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<Run> {
    let t = tenant(&headers);
    match db.request_cancel(&t, &id, now_secs()).await.map_err(e500)? {
        Some(run) => {
            if run.status == run_status::CANCELED {
                metrics::RUNS_CANCELED_TOTAL.inc();
                crate::events::run_canceled(&run).await;
            }
            Ok(Json(run))
        }
        None => {
            // Either unknown or already terminal — disambiguate for the client.
            match db.get_run(&t, &id).await.map_err(e500)? {
                Some(run) => Err((StatusCode::CONFLICT, format!("run is already {}", run.status))),
                None => Err((StatusCode::NOT_FOUND, "run not found".into())),
            }
        }
    }
}

#[derive(Deserialize, Default)]
pub struct RetryRun {
    /// true = re-run from step 0 with a fresh context; false (default) =
    /// resume at the failed step with the accumulated context.
    #[serde(default)]
    pub restart: bool,
}

/// Requeue a failed / dead-lettered / canceled run with a fresh retry budget.
async fn retry_run(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>, body: Option<Json<RetryRun>>) -> ApiResult<Run> {
    let t = tenant(&headers);
    let restart = body.map(|Json(b)| b.restart).unwrap_or(false);
    match db.requeue_run(&t, &id, restart).await.map_err(e500)? {
        Some(run) => {
            metrics::RUNS_TOTAL.inc();
            worker::kick();
            Ok(Json(run))
        }
        None => match db.get_run(&t, &id).await.map_err(e500)? {
            Some(run) => Err((StatusCode::CONFLICT, format!("run is {} — only failed, dead_letter or canceled runs can be retried", run.status))),
            None => Err((StatusCode::NOT_FOUND, "run not found".into())),
        },
    }
}

#[derive(serde::Serialize)]
pub struct RunDetail { #[serde(flatten)] pub run: Run, pub steps: Vec<RunStep> }

async fn get_run(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<RunDetail> {
    let run = db.get_run(&tenant(&headers), &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "run not found".into()))?;
    let steps = db.list_run_steps(&run.id).await.map_err(e500)?;
    Ok(Json(RunDetail { run, steps }))
}

#[derive(Deserialize)]
pub struct PatchContext {
    pub context: serde_json::Value,
}

async fn patch_run_context(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>, Json(b): Json<PatchContext>) -> ApiResult<Run> {
    let t = tenant(&headers);
    let mut run = db.get_run(&t, &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "run not found".into()))?;
        
    if run.status != run_status::DEAD_LETTER {
        return Err((StatusCode::CONFLICT, "only dead_letter runs can be edited".into()));
    }
    
    run.context = b.context;
    db.save_run_progress(&run).await.map_err(e500)?;
    Ok(Json(run))
}

// ── Webhooks ────────────────────────────────────────────────────────────────
#[derive(serde::Serialize)]
pub struct WebhookCreated { pub token: String, pub url_path: String }

async fn create_webhook(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<WebhookCreated> {
    let t = tenant(&headers);
    db.get_workflow(&t, &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "workflow not found".into()))?;
    let token = format!("whk_{}", uuid::Uuid::new_v4().simple());
    db.create_webhook(&Webhook {
        token: token.clone(), workflow_id: id, tenant_id: t, enabled: true, created_at: now_secs(),
    }).await.map_err(e500)?;
    Ok(Json(WebhookCreated { url_path: format!("/hooks/{token}"), token }))
}

async fn trigger_webhook(State(db): State<Arc<Db>>, Path(token): Path<String>, body: Option<Json<serde_json::Value>>) -> ApiResult<Run> {
    let hook = db.get_webhook(&token).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "unknown webhook".into()))?;
    let wf = db.get_workflow(&hook.tenant_id, &hook.workflow_id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "workflow not found".into()))?;
    if !wf.enabled {
        return Err((StatusCode::CONFLICT, "workflow is disabled".into()));
    }
    let mut run = scheduler::new_run(&wf, "webhook");
    // Seed the run context with the webhook payload under "trigger".
    if let (Some(Json(payload)), Some(obj)) = (body, run.context.as_object_mut()) {
        obj.insert("trigger".into(), payload);
    }
    db.create_run(&run).await.map_err(e500)?;
    metrics::RUNS_TOTAL.inc();
    metrics::WEBHOOK_RUNS_TOTAL.inc();
    worker::kick();
    Ok(Json(run))
}

// ── Schedules ───────────────────────────────────────────────────────────────
#[derive(Deserialize)]
pub struct CreateSchedule { pub cron: String }

async fn list_schedules(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<Vec<Schedule>> {
    let t = tenant(&headers);
    Ok(Json(db.list_schedules(&t, &id).await.map_err(e500)?))
}

async fn create_schedule(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>, Json(b): Json<CreateSchedule>) -> ApiResult<Schedule> {
    let t = tenant(&headers);
    db.get_workflow(&t, &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "workflow not found".into()))?;
    if !crate::cron::matches(&b.cron, now_secs()) && b.cron.split_whitespace().count() != 5 {
        return Err((StatusCode::UNPROCESSABLE_ENTITY, "cron must be 5 space-separated fields".into()));
    }
    let s = Schedule {
        id: format!("sch_{}", uuid::Uuid::new_v4().simple()),
        workflow_id: id, tenant_id: t, cron: b.cron, enabled: true, last_run: None, created_at: now_secs(),
    };
    db.create_schedule(&s).await.map_err(e500)?;
    Ok(Json(s))
}

async fn delete_schedule(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> impl IntoResponse {
    match db.delete_schedule(&tenant(&headers), &id).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── Approvals ───────────────────────────────────────────────────────────────
async fn list_approvals(State(db): State<Arc<Db>>, headers: HeaderMap) -> ApiResult<Vec<Approval>> {
    Ok(Json(db.list_pending_approvals(&tenant(&headers), 200).await.map_err(e500)?))
}

async fn get_approval(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>) -> ApiResult<Approval> {
    db.get_approval(&tenant(&headers), &id).await.map_err(e500)?
        .map(Json).ok_or((StatusCode::NOT_FOUND, "approval not found".into()))
}

#[derive(Deserialize)]
pub struct ResolveApproval { pub decision: String, pub by: Option<String>, pub note: Option<String> }

async fn resolve_approval(State(db): State<Arc<Db>>, headers: HeaderMap, Path(id): Path<String>, Json(b): Json<ResolveApproval>) -> ApiResult<Run> {
    let t = tenant(&headers);
    let approval = db.get_approval(&t, &id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "approval not found".into()))?;
    if approval.status != "pending" {
        return Err((StatusCode::CONFLICT, "approval already decided".into()));
    }
    let approved = b.decision == "approved" || b.decision == "approve";
    let status = if approved { "approved" } else { "rejected" };
    db.resolve_approval(&id, status, b.by.as_deref().unwrap_or("system"), b.note.as_deref())
        .await.map_err(e500)?;

    let mut run = db.get_run(&t, &approval.run_id).await.map_err(e500)?
        .ok_or((StatusCode::NOT_FOUND, "run not found".into()))?;

    // Guard: the run must still be waiting on this gate (it may have been
    // canceled while pending).
    if run.status != run_status::WAITING_APPROVAL {
        return Err((StatusCode::CONFLICT, format!("run is no longer waiting for approval (status: {})", run.status)));
    }

    if !approved {
        run.status = run_status::CANCELED.to_string();
        run.error = Some(format!("approval '{}' rejected", approval.step_id));
        run.finished_at = Some(now_secs());
        db.save_run_progress(&run).await.map_err(e500)?;
        crate::events::run_canceled(&run).await;
        return Ok(Json(run));
    }

    // Approved: skip past the approval step and requeue; the durable worker
    // resumes the run from the saved cursor and context.
    run.cursor += 1;
    run.status = run_status::QUEUED.to_string();
    run.next_attempt_at = 0;
    db.save_run_progress(&run).await.map_err(e500)?;
    worker::kick();
    Ok(Json(run))
}

// ── Templates ───────────────────────────────────────────────────────────────
#[derive(Deserialize)]
pub struct CreateTemplate { pub name: String, #[serde(default)] pub description: String, #[serde(default)] pub steps: Vec<Step>, #[serde(default)] pub global: bool }

async fn create_template(State(db): State<Arc<Db>>, headers: HeaderMap, Json(b): Json<CreateTemplate>) -> ApiResult<Template> {
    let t = Template {
        id: format!("tpl_{}", uuid::Uuid::new_v4().simple()),
        tenant_id: if b.global { None } else { Some(tenant(&headers)) },
        name: b.name, description: b.description, steps: b.steps, created_at: now_secs(),
    };
    db.create_template(&t).await.map_err(e500)?;
    Ok(Json(t))
}

async fn list_templates(State(db): State<Arc<Db>>, headers: HeaderMap) -> ApiResult<Vec<Template>> {
    Ok(Json(db.list_templates(&tenant(&headers)).await.map_err(e500)?))
}
