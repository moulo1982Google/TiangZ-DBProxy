//! Offline budget validation. A valid budget is not permission to run a formal matrix.
#[derive(Debug, PartialEq)]
pub struct Budget {
    pub warmup: u64,
    pub sample: u64,
    pub rows: u64,
    pub waves: u64,
    pub calls: u64,
    pub ready_per_publisher: u64,
    pub remaining_per_publisher: u64,
}

pub fn validate(warmup: u64, sample: u64, rows: u64, mixed: bool) -> Result<Budget, &'static str> {
    if !matches!((warmup, sample), (2, 5) | (120, 300)) {
        return Err("only 2/5 smoke or 120/300 candidate timing is supported");
    }
    // Multiples of 40 keep both publishers and both FIFO partitions balanced,
    // including the 90% blocked / 10% ready mixed distributions.
    if !(1000..=40000).contains(&rows) || !rows.is_multiple_of(40) {
        return Err("rows must be 1000..40000 and divisible by 40");
    }
    let waves = (warmup + sample) * 2;
    let calls = waves * 2; // Two workers together: four calls per second.
    let ready_per_publisher = rows / if mixed { 20 } else { 2 };
    let demand_per_publisher = calls / 2;
    // Retain at least half the initial eligible reserve, including warmup.
    if ready_per_publisher < demand_per_publisher * 2 {
        return Err("eligible reserve would fall below 50%; no refill is allowed");
    }
    Ok(Budget {
        warmup,
        sample,
        rows,
        waves,
        calls,
        ready_per_publisher,
        remaining_per_publisher: ready_per_publisher - demand_per_publisher,
    })
}

pub fn smoke_environment(get: impl Fn(&str) -> Option<String>) -> Result<Budget, &'static str> {
    // Fail before creating evidence or connecting to PostgreSQL. Older launchers
    // passed unused formal parameters, which must no longer be silently ignored.
    for (name, expected) in [
        ("P07_WARMUP_SECONDS", "2"),
        ("P07_SAMPLE_SECONDS", "5"),
        ("P07_ROWS", "1000"),
        ("P07_PUBLISHERS", "2"),
        ("P07_WORKERS", "2"),
        ("P07_ROUNDS", "1"),
        ("P07_CLAIMS_PER_SECOND", "4"),
        ("P07_STATS", "0"),
    ] {
        if get(name).is_some_and(|value| value != expected) {
            return Err(
                "unsupported execution parameters: only 1000-row 2/5 smoke, two workers/publishers, total 4/s, one round, no stats is enabled",
            );
        }
    }
    validate(2, 5, 1000, true)
}

#[test]
fn budgets_preserve_reserve_without_increasing_rate() {
    let smoke = validate(2, 5, 1000, true).unwrap();
    assert_eq!((smoke.calls, smoke.remaining_per_publisher), (28, 36));
    let formal = validate(120, 300, 33600, true).unwrap();
    assert_eq!((formal.waves, formal.calls), (840, 1680));
    assert_eq!(
        (formal.ready_per_publisher, formal.remaining_per_publisher),
        (1680, 840)
    );
    assert!(validate(120, 300, 33560, true).is_err());
    assert!(validate(120, 300, 1000, false).is_err());
    assert!(validate(120, 300, 1000, true).is_err());
    for rows in [0, 999, 1001, 40040, 100000, u64::MAX] {
        assert!(validate(120, 300, rows, true).is_err());
    }
    for timing in [(0, 5), (2, 0), (2, 300), (120, 301), (u64::MAX, u64::MAX)] {
        assert!(validate(timing.0, timing.1, 33600, true).is_err());
    }
}

#[test]
fn unsupported_environment_is_rejected_before_database_work() {
    assert!(smoke_environment(|_| None).is_ok());
    for (name, valid, invalid) in [
        ("P07_WARMUP_SECONDS", "2", "120"),
        ("P07_SAMPLE_SECONDS", "5", "300"),
        ("P07_ROWS", "1000", "33600"),
        ("P07_PUBLISHERS", "2", "1"),
        ("P07_WORKERS", "2", "4"),
        ("P07_ROUNDS", "1", "3"),
        ("P07_CLAIMS_PER_SECOND", "4", "8"),
        ("P07_STATS", "0", "1"),
    ] {
        assert!(smoke_environment(|key| (key == name).then(|| valid.into())).is_ok());
        for value in [invalid, "", "-1", "NaN", "18446744073709551616"] {
            assert!(smoke_environment(|key| (key == name).then(|| value.into())).is_err());
        }
    }
}
