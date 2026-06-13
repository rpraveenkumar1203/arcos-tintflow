//! TintFlow service entry point. Independent binary: connects to its OWN
//! PostgreSQL (TINTFLOW_DATABASE_URL), runs its migrations, starts the cron
//! scheduler, and serves the HTTP API.

mod api;
mod cron;
mod db;
mod engine;
mod events;
mod model;
mod metrics;
mod scheduler;
mod worker;

use std::sync::Arc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let db_url = std::env::var("TINTFLOW_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://tintflow:tintflow@localhost:5433/tintflow".to_string());
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8090);

    let db = match db::Db::connect(&db_url).await {
        Ok(db) => Arc::new(db),
        Err(e) => {
            tracing::error!(error = %e, "failed to connect/migrate TintFlow database");
            std::process::exit(1);
        }
    };
    tracing::info!("TintFlow database ready");

    // Optional NATS event bus — no-op if NATS_URL is unset/unreachable.
    events::init(std::env::var("NATS_URL").ok().as_deref()).await;

    scheduler::spawn(Arc::clone(&db));
    tracing::info!("scheduler started");

    // Durable queue worker: claims and executes runs; reaps expired leases.
    // Restart-safe — anything mid-flight when the process died is reclaimed.
    worker::spawn(Arc::clone(&db));

    let app = api::router(db).layer(tower_http::trace::TraceLayer::new_for_http());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    tracing::info!(%addr, "TintFlow listening");
    axum::serve(listener, app).await.expect("serve");
}
