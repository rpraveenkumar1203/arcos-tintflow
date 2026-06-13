//! Minimal 5-field cron matcher: `minute hour day-of-month month day-of-week`.
//! Supports `*`, lists (`1,2,3`), ranges (`1-5`), and steps (`*/5`, `0-30/10`).
//! Evaluated against a UTC timestamp at minute granularity — enough to drive the
//! scheduler tick without pulling a cron dependency.

use chrono::{Datelike, TimeZone, Timelike, Utc};

/// Does `cron` fire at the given unix `ts` (UTC, minute granularity)?
pub fn matches(cron: &str, ts: i64) -> bool {
    let fields: Vec<&str> = cron.split_whitespace().collect();
    if fields.len() != 5 {
        return false;
    }
    let dt = match Utc.timestamp_opt(ts, 0).single() {
        Some(dt) => dt,
        None => return false,
    };
    let dow = dt.weekday().num_days_from_sunday(); // 0 = Sunday
    field_matches(fields[0], dt.minute())
        && field_matches(fields[1], dt.hour())
        && field_matches(fields[2], dt.day())
        && field_matches(fields[3], dt.month())
        && field_matches(fields[4], dow)
}

/// Match one cron field against `value`.
fn field_matches(field: &str, value: u32) -> bool {
    field.split(',').any(|part| part_matches(part, value))
}

fn part_matches(part: &str, value: u32) -> bool {
    // Step syntax: BASE/STEP where BASE is `*` or a range.
    let (base, step) = match part.split_once('/') {
        Some((b, s)) => (b, s.parse::<u32>().ok()),
        None => (part, None),
    };
    let step = match step {
        Some(0) | None if part.contains('/') => return false, // /0 or unparsable
        Some(s) => s,
        None => 1,
    };

    let (lo, hi) = if base == "*" {
        (u32::MIN, u32::MAX)
    } else if let Some((a, b)) = base.split_once('-') {
        match (a.parse::<u32>(), b.parse::<u32>()) {
            (Ok(a), Ok(b)) => (a, b),
            _ => return false,
        }
    } else {
        match base.parse::<u32>() {
            Ok(n) => return value == n && step == 1, // bare number ignores step
            Err(_) => return false,
        }
    };

    if value < lo || value > hi {
        return false;
    }
    // For `*` the step anchor is 0; for a range it's the range start.
    let anchor = if base == "*" { 0 } else { lo };
    (value - anchor) % step == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap().timestamp()
    }

    #[test]
    fn every_minute() {
        assert!(matches("* * * * *", ts(2026, 6, 6, 13, 37)));
    }

    #[test]
    fn specific_minute_hour() {
        assert!(matches("30 9 * * *", ts(2026, 6, 6, 9, 30)));
        assert!(!matches("30 9 * * *", ts(2026, 6, 6, 9, 31)));
        assert!(!matches("30 9 * * *", ts(2026, 6, 6, 10, 30)));
    }

    #[test]
    fn step_every_five_minutes() {
        assert!(matches("*/5 * * * *", ts(2026, 6, 6, 0, 0)));
        assert!(matches("*/5 * * * *", ts(2026, 6, 6, 0, 5)));
        assert!(!matches("*/5 * * * *", ts(2026, 6, 6, 0, 7)));
    }

    #[test]
    fn range_and_list() {
        // 9-17 business hours, on the hour.
        assert!(matches("0 9-17 * * *", ts(2026, 6, 6, 12, 0)));
        assert!(!matches("0 9-17 * * *", ts(2026, 6, 6, 18, 0)));
        // Monday or Friday (1,5). 2026-06-08 is a Monday.
        assert!(matches("0 0 * * 1,5", ts(2026, 6, 8, 0, 0)));
        assert!(!matches("0 0 * * 1,5", ts(2026, 6, 9, 0, 0))); // Tuesday
    }

    #[test]
    fn invalid_fields() {
        assert!(!matches("* * *", ts(2026, 6, 6, 0, 0)));        // too few
        assert!(!matches("bad * * * *", ts(2026, 6, 6, 0, 0)));
    }
}
