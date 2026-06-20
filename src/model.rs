//! TintFlow domain types. Steps are stored as JSON so workflows are editable
//! without migrations; each step is `{ id, kind, config }`.

use serde::{Deserialize, Serialize};

/// One unit of work in a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    /// Executor selector: "http" | "log" | "delay" | "approval".
    pub kind: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Bumped on every update; runs record the version they executed.
    #[serde(default = "default_version")]
    pub version: i32,
    pub created_by: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

fn default_true() -> bool { true }
fn default_version() -> i32 { 1 }

/// Run lifecycle states. Some are referenced only inside queue SQL (claim,
/// reap, requeue) — the constants stay as the documented state space.
#[allow(dead_code)]
pub mod run_status {
    pub const QUEUED: &str = "queued";
    pub const RUNNING: &str = "running";
    /// Run is waiting for a delay to elapse.
    pub const SLEEPING: &str = "sleeping";
    /// A step failed but attempts remain; requeued with backoff.
    pub const RETRYING: &str = "retrying";
    pub const WAITING_APPROVAL: &str = "waiting_approval";
    pub const WAITING_SUBWORKFLOW: &str = "waiting_subworkflow";
    pub const SUCCEEDED: &str = "succeeded";
    pub const FAILED: &str = "failed";
    /// Retries exhausted — parked for operator inspection / manual retry.
    pub const DEAD_LETTER: &str = "dead_letter";
    pub const CANCELED: &str = "canceled";

    /// Terminal states — the run will never execute again on its own.
    pub fn is_terminal(s: &str) -> bool {
        matches!(s, SUCCEEDED | FAILED | DEAD_LETTER | CANCELED)
    }
}

/// Exponential backoff before retry `attempt` (1-based): 5s, 15s, 45s… capped
/// at 5 minutes. Deterministic so tests and operators can predict requeues.
pub fn backoff_secs(attempt: i32) -> i64 {
    let a = attempt.clamp(1, 8) as u32;
    (5i64 * 3i64.pow(a - 1)).min(300)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub workflow_id: String,
    pub tenant_id: String,
    pub status: String,
    pub trigger: String,
    pub cursor: i32,
    pub context: serde_json::Value,
    pub error: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Retry bookkeeping: how many executions have been attempted, the cap,
    /// and when the run is next eligible for claiming.
    #[serde(default)]
    pub attempt: i32,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
    #[serde(default)]
    pub next_attempt_at: i64,
    #[serde(default)]
    pub cancel_requested: bool,
    /// Immutable snapshot of the steps this run executes (taken at enqueue).
    /// `None` only for runs created before snapshots existed.
    #[serde(default)]
    pub steps: Option<Vec<Step>>,
    #[serde(default = "default_version")]
    pub workflow_version: i32,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

fn default_max_attempts() -> i32 { 3 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStep {
    pub id: String,
    pub run_id: String,
    pub step_index: i32,
    pub step_id: String,
    pub kind: String,
    pub status: String,
    pub output: serde_json::Value,
    pub error: Option<String>,
    pub started_at: i64,
    pub finished_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub workflow_id: String,
    pub tenant_id: String,
    pub cron: String,
    pub enabled: bool,
    pub last_run: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Approval {
    pub id: String,
    pub run_id: String,
    pub tenant_id: String,
    pub step_id: String,
    pub status: String,
    pub note: Option<String>,
    pub decided_by: Option<String>,
    pub created_at: i64,
    pub decided_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Template {
    pub id: String,
    pub tenant_id: Option<String>,
    pub name: String,
    pub description: String,
    pub steps: Vec<Step>,
    pub created_at: i64,
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_secs(1), 5);
        assert_eq!(backoff_secs(2), 15);
        assert_eq!(backoff_secs(3), 45);
        assert_eq!(backoff_secs(4), 135);
        assert_eq!(backoff_secs(5), 300); // capped
        assert_eq!(backoff_secs(100), 300);
        assert_eq!(backoff_secs(0), 5); // clamped low
    }

    #[test]
    fn terminal_states() {
        assert!(run_status::is_terminal(run_status::SUCCEEDED));
        assert!(run_status::is_terminal(run_status::DEAD_LETTER));
        assert!(run_status::is_terminal(run_status::CANCELED));
        assert!(!run_status::is_terminal(run_status::RETRYING));
        assert!(!run_status::is_terminal(run_status::WAITING_APPROVAL));
        assert!(!run_status::is_terminal(run_status::RUNNING));
    }
}
