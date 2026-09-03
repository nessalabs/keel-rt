//! 10k–200k ticker farm. FakeClock; NoopStore (no snapshot pile).
//!
//! ```text
//! cargo test -p keel-rt-schedule --test stress -- --nocapture --test-threads=1
//! ```

use bytes::Bytes;
use chrono::{TimeZone, Timelike, Utc};
use chrono_tz::America::Vancouver;
use keel_rt::{
    Event, ExecutionContext, FailingStore, FakeClock, FnSink, NodeOutcome, NoopStore, Runtime,
    Timestamp, WorkflowDefinition,
};
use keel_rt_schedule::{Schedule, ScheduleSpec};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PERIOD_MS: u64 = 60_000;
const SEQUENTIAL: u32 = 100_000;
const CATCH_UP: u64 = 200_000;
const ARMED_10K: u32 = 10_000;
const ARMED_100K: u32 = 100_000;

fn weekday_def() -> WorkflowDefinition {
    WorkflowDefinition::builder("weekday")
        .node("work", "work")
        .build()
        .unwrap()
}

fn rss_bytes() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            return kb.saturating_mul(1024);
        }
    }
    0
}

fn format_ms(d: Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

fn format_bytes(n: u64) -> String {
    if n >= 1_048_576 {
        format!("{:.1} MiB", n as f64 / 1_048_576.0)
    } else if n >= 1024 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn farm_runtime(clock: Arc<FakeClock>, starts: Arc<AtomicU32>) -> Arc<Runtime> {
    let sink_starts = starts;
    Arc::new(
        Runtime::builder()
            .clock(clock)
            .store(NoopStore)
            .sink(FnSink(move |e: &Event| {
                if matches!(e, Event::ExecutionStarted { .. }) {
                    sink_starts.fetch_add(1, Ordering::SeqCst);
                }
            }))
            .register_fn("work", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .register_fn("boom", |_ctx: ExecutionContext| async {
                panic!("farm boom");
            })
            .build(),
    )
}

async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

async fn wait_starts(starts: &AtomicU32, n: u32) {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if starts.load(Ordering::SeqCst) >= n {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {n} starts"));
}

/// Item 12 at farm size: 200k missed minutes → 1 start.
#[tokio::test(flavor = "current_thread")]
async fn missed_tick_storm_200k_periods_is_one_start() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(ScheduleSpec::new("* * * * *", "UTC", weekday_def()).unwrap())
        .build()
        .run();
    settle().await;
    let t0 = Instant::now();
    clock.set(Timestamp(CATCH_UP * PERIOD_MS));
    wait_starts(&starts, 1).await;
    settle().await;
    let elapsed = t0.elapsed();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    eprintln!(
        "schedule_stress missed_tick_storm periods={CATCH_UP} starts=1 elapsed={} rss={}",
        format_ms(elapsed),
        format_bytes(rss_bytes())
    );
    drop(running);
}

/// 100k one-period walks of one spec. Starts == N. RSS must not look like a fire log.
#[tokio::test(flavor = "current_thread")]
async fn sequential_100k_fires_one_spec() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(ScheduleSpec::new("* * * * *", "UTC", weekday_def()).unwrap())
        .build()
        .run();
    settle().await;
    let rss0 = rss_bytes();
    let t0 = Instant::now();
    let mut rss_10k = 0u64;
    for i in 1..=SEQUENTIAL {
        clock.set(Timestamp(u64::from(i) * PERIOD_MS));
        wait_starts(&starts, i).await;
        if i == 10_000 {
            rss_10k = rss_bytes();
        }
    }
    let elapsed = t0.elapsed();
    let rss1 = rss_bytes();
    assert_eq!(starts.load(Ordering::SeqCst), SEQUENTIAL);
    let growth = rss1.saturating_sub(rss_10k);
    eprintln!(
        "schedule_stress sequential_100k starts={SEQUENTIAL} elapsed={} rss_before={} rss_10k={} rss_after={} growth_10k_to_100k={}",
        format_ms(elapsed),
        format_bytes(rss0),
        format_bytes(rss_10k),
        format_bytes(rss1),
        format_bytes(growth)
    );
    assert!(
        growth < 512 * 1_048_576,
        "RSS grew {} from 10k→100k — looks like an O(N) fire log",
        format_bytes(growth)
    );
    drop(running);
}

fn arm_n(
    n: u32,
    clock: Arc<FakeClock>,
    starts: Arc<AtomicU32>,
) -> keel_rt_schedule::RunningSchedule {
    let rt = farm_runtime(clock.clone(), starts);
    let spec = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    Schedule::builder(rt)
        .clock(clock)
        .jobs((0..n).map(|_| spec.clone()))
        .build()
        .run()
}

/// 10k armed specs, one fire each after a jump (unbounded wake).
#[tokio::test(flavor = "current_thread")]
async fn armed_10k_specs_one_fire_each() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rss0 = rss_bytes();
    let running = arm_n(ARMED_10K, clock.clone(), starts.clone());
    settle().await;
    let rss_armed = rss_bytes();
    let t0 = Instant::now();
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_10K).await;
    let elapsed = t0.elapsed();
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), ARMED_10K);
    eprintln!(
        "schedule_stress armed_10k starts={ARMED_10K} elapsed={} rss_before={} rss_armed={} rss_after={}",
        format_ms(elapsed),
        format_bytes(rss0),
        format_bytes(rss_armed),
        format_bytes(rss_bytes())
    );
    drop(running);
}

/// 100k armed specs sharing one definition. Arm cost is ticker+cron, not N DAGs.
#[tokio::test(flavor = "current_thread")]
async fn armed_100k_specs_one_fire_each() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rss0 = rss_bytes();
    let running = arm_n(ARMED_100K, clock.clone(), starts.clone());
    settle().await;
    let rss_armed = rss_bytes();
    let t0 = Instant::now();
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_100K).await;
    let elapsed = t0.elapsed();
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), ARMED_100K);
    eprintln!(
        "schedule_stress armed_100k starts={ARMED_100K} elapsed={} rss_before={} rss_armed={} rss_after={} arm_delta={}",
        format_ms(elapsed),
        format_bytes(rss0),
        format_bytes(rss_armed),
        format_bytes(rss_bytes()),
        format_bytes(rss_armed.saturating_sub(rss0))
    );
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn drop_runner_after_arming_100k_stops_starts() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let running = arm_n(ARMED_100K, clock.clone(), starts.clone());
    settle().await;
    drop(running);
    clock.set(Timestamp(PERIOD_MS));
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn drop_runtime_arc_while_10k_armed_still_starts() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    let running = Schedule::builder(rt.clone())
        .clock(clock.clone())
        .jobs((0..ARMED_10K).map(|_| spec.clone()))
        .build()
        .run();
    settle().await;
    drop(rt);
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_10K).await;
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn start_err_mid_10k_farm_siblings_still_fire() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let good = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    let bad_def = WorkflowDefinition::builder("bad")
        .node("x", "missing")
        .build()
        .unwrap();
    let bad = ScheduleSpec::new("* * * * *", "UTC", bad_def).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(bad)
        .jobs((0..ARMED_10K).map(|_| good.clone()))
        .build()
        .run();
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_10K).await;
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), ARMED_10K);
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn store_put_fail_mid_farm_does_not_stick_firing() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let fires = Arc::new(AtomicU32::new(0));
    let f = fires.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(FailingStore::fail_on_nth_put(1))
            .register_fn("work", move |_ctx: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build(),
    );
    let spec = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    let n = 256u32;
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .jobs((0..n).map(|_| spec.clone()))
        .build()
        .run();
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&fires, n).await;
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn executor_panic_mid_farm_ticker_survives() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let boom = WorkflowDefinition::builder("boom")
        .node("x", "boom")
        .build()
        .unwrap();
    let good = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(ScheduleSpec::new("* * * * *", "UTC", boom).unwrap())
        .jobs((0..1_000).map(|_| good.clone()))
        .build()
        .run();
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, 1_001).await;
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn two_runners_10k_each_no_mutex() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    let a = Schedule::builder(rt.clone())
        .clock(clock.clone())
        .jobs((0..ARMED_10K).map(|_| spec.clone()))
        .build()
        .run();
    let b = Schedule::builder(rt)
        .clock(clock.clone())
        .jobs((0..ARMED_10K).map(|_| spec.clone()))
        .build()
        .run();
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_10K * 2).await;
    assert_eq!(starts.load(Ordering::SeqCst), ARMED_10K * 2);
    drop(a);
    drop(b);
}

#[tokio::test(flavor = "current_thread")]
async fn vancouver_dst_still_holds_with_10k_armed() {
    let def = Arc::new(weekday_def());
    let van = ScheduleSpec::with_shared("30 2 * * *", "America/Vancouver", def.clone()).unwrap();
    let utc = ScheduleSpec::with_shared("0 9 * * 1", "UTC", def).unwrap();
    let before = Vancouver
        .with_ymd_and_hms(2026, 3, 8, 1, 59, 0)
        .single()
        .unwrap();
    let now = Timestamp(before.timestamp_millis() as u64);
    let next = van.next_after(now).expect("gap landing");
    let local = Utc
        .timestamp_millis_opt(next.as_millis() as i64)
        .single()
        .unwrap()
        .with_timezone(&Vancouver);
    assert_ne!(
        local.hour(),
        2,
        "must not invent 02:xx with many specs armed"
    );
    let clock = Arc::new(FakeClock::new());
    clock.set(now);
    let starts = Arc::new(AtomicU32::new(0));
    let rt = farm_runtime(clock.clone(), starts.clone());
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(van)
        .jobs((0..ARMED_10K).map(|_| utc.clone()))
        .build()
        .run();
    settle().await;
    clock.set(next);
    wait_starts(&starts, 1).await;
    settle().await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "only the Vancouver spec is due at the gap landing"
    );
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn jump_backward_after_10k_fire_does_not_refire() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let running = arm_n(ARMED_10K, clock.clone(), starts.clone());
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_10K).await;
    clock.set(Timestamp(0));
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), ARMED_10K);
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn exact_t_then_same_window_10k_does_not_double_fire() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let running = arm_n(ARMED_10K, clock.clone(), starts.clone());
    settle().await;
    clock.set(Timestamp(PERIOD_MS));
    wait_starts(&starts, ARMED_10K).await;
    clock.set(Timestamp(PERIOD_MS));
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), ARMED_10K);
    drop(running);
}

#[test]
fn stress_required_names_exist() {
    let s = include_str!("stress.rs");
    for name in [
        "fn missed_tick_storm_200k_periods_is_one_start",
        "fn sequential_100k_fires_one_spec",
        "fn armed_10k_specs_one_fire_each",
        "fn armed_100k_specs_one_fire_each",
        "fn drop_runner_after_arming_100k_stops_starts",
        "fn drop_runtime_arc_while_10k_armed_still_starts",
        "fn start_err_mid_10k_farm_siblings_still_fire",
        "fn store_put_fail_mid_farm_does_not_stick_firing",
        "fn executor_panic_mid_farm_ticker_survives",
        "fn two_runners_10k_each_no_mutex",
        "fn vancouver_dst_still_holds_with_10k_armed",
        "fn jump_backward_after_10k_fire_does_not_refire",
        "fn exact_t_then_same_window_10k_does_not_double_fire",
    ] {
        assert!(s.contains(name), "stress.rs missing {name}");
    }
}
