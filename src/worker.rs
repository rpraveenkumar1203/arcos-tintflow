//! Durable queue worker. The only place runs actually execute.
//!
//! Loop: claim due runs from Postgres (FOR UPDATE SKIP LOCKED, bounded batch),
//! execute each on its own task, then sleep until the next poll tick — or
//! earlier, when `kick()` signals that something was just enqueued. A second
//! loop reaps runs whose worker died (lease expired), requeueing or
//! dead-lettering them.
//!
//! Multiple service replicas are safe: claims are atomic, so each run is
//! executed by exactly one live worker at a time.

use crate::{db::Db, engine, metrics, model::now_secs};
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};

/// How often the worker polls when idle. Enqueues call `kick()` so the usual
/// dispatch latency is ~0, not this.
const POLL_SECS: u64 = 2;
/// How often expired leases are reaped.
const REAP_SECS: u64 = 30;
/// Max runs claimed per poll and executed concurrently per replica.
const MAX_CONCURRENT: usize = 8;

static WAKE: Notify = Notify::const_new();

/// Wake the worker immediately (called after enqueueing a run).
pub fn kick() {
    WAKE.notify_one();
}

pub fn spawn(db: Arc<Db>) {
    let worker_id = format!("wkr_{}", uuid::Uuid::new_v4().simple());
    tracing::info!(worker = %worker_id, "queue worker started");

    // Reaper: reclaim runs from dead workers.
    let reaper_db = Arc::clone(&db);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(REAP_SECS));
        loop {
            ticker.tick().await;
            match reaper_db.reap_stale_runs(now_secs()).await {
                Ok(0) => {}
                Ok(n) => {
                    metrics::RUNS_REAPED_TOTAL.inc_by(n);
                    tracing::warn!(count = n, "reaped runs with expired leases");
                }
                Err(e) => tracing::warn!(error = %e, "lease reaper failed"),
            }
        }
    });

    // Claim/execute loop. A permit is reserved BEFORE claiming so a run is
    // never claimed (lease ticking) without capacity to execute it.
    tokio::spawn(async move {
        let gate = Arc::new(Semaphore::new(MAX_CONCURRENT));
        loop {
            let permit = match Arc::clone(&gate).acquire_owned().await {
                Ok(p) => p,
                Err(_) => return, // semaphore closed — shutting down
            };

            let claimed = db
                .claim_due_runs(&worker_id, now_secs(), engine::LEASE_SECS, 1)
                .await;
            match claimed {
                Ok(mut runs) if !runs.is_empty() => {
                    let mut run = runs.remove(0);
                    let db = Arc::clone(&db);
                    tokio::spawn(async move {
                        let _permit = permit; // held for the run's lifetime
                        if let Err(e) = engine::execute_claimed(&db, &mut run).await {
                            tracing::error!(run = %run.id, error = %e, "run execution failed to persist");
                        }
                    });
                    // More work may be due — claim again right away.
                    continue;
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "claiming runs failed"),
            }
            drop(permit);
            tokio::select! {
                _ = WAKE.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(POLL_SECS)) => {}
            }
        }
    });
}
