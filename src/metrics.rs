use prometheus::{Encoder, IntCounter, TextEncoder};
use std::sync::LazyLock;

pub static RUNS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "tintflow_runs_total",
        "Total workflow runs accepted by TintFlow"
    )
    .expect("register TintFlow run metric")
});

pub static WEBHOOK_RUNS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "tintflow_webhook_runs_total",
        "Total workflow runs triggered by webhooks"
    )
    .expect("register TintFlow webhook metric")
});

pub static RUNS_RETRIED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "tintflow_runs_retried_total",
        "Run attempts that failed and were requeued with backoff"
    )
    .expect("register TintFlow retry metric")
});

pub static RUNS_DEAD_LETTER_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "tintflow_runs_dead_letter_total",
        "Runs that exhausted their retry budget and were dead-lettered"
    )
    .expect("register TintFlow dead-letter metric")
});

pub static RUNS_CANCELED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "tintflow_runs_canceled_total",
        "Runs canceled by user request"
    )
    .expect("register TintFlow cancel metric")
});

pub static RUNS_REAPED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "tintflow_runs_reaped_total",
        "Running runs reclaimed after their worker's lease expired"
    )
    .expect("register TintFlow reaper metric")
});

pub fn gather() -> String {
    let _ = (
        &*RUNS_TOTAL,
        &*WEBHOOK_RUNS_TOTAL,
        &*RUNS_RETRIED_TOTAL,
        &*RUNS_DEAD_LETTER_TOTAL,
        &*RUNS_CANCELED_TOTAL,
        &*RUNS_REAPED_TOTAL,
    );
    let encoder = TextEncoder::new();
    let mut output = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut output)
        .unwrap_or_default();
    String::from_utf8(output).unwrap_or_default()
}
