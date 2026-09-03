//! FakeClock fires. `cargo test -p keel-rt-schedule --test schedule`

use bytes::Bytes;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::America::Vancouver;
use keel_rt::{
    Event, ExecutionContext, FailingStore, FakeClock, FnSink, MemoryStore, NodeOutcome, NodeState,
    Runtime, StateStore, Timestamp, WorkflowDefinition,
};
use keel_rt_schedule::{Schedule, ScheduleSpec, SpecError};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 1970-01-05 (Monday) 09:00:00 UTC.
const MON1_9: Timestamp = Timestamp(4 * 86_400_000 + 9 * 3_600_000);
/// 1970-01-12 (Monday) 09:00:00 UTC.
const MON2_9: Timestamp = Timestamp(11 * 86_400_000 + 9 * 3_600_000);

fn weekday_def() -> WorkflowDefinition {
    WorkflowDefinition::builder("weekday")
        .node("work", "work")
        .build()
        .unwrap()
}

fn counting_runtime(clock: Arc<FakeClock>, starts: Arc<AtomicU32>) -> Arc<Runtime> {
    let sink_starts = starts;
    Arc::new(
        Runtime::builder()
            .clock(clock)
            .sink(FnSink(move |e: &Event| {
                if matches!(e, Event::ExecutionStarted { .. }) {
                    sink_starts.fetch_add(1, Ordering::SeqCst);
                }
            }))
            .register_fn("work", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
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
    tokio::time::timeout(Duration::from_secs(2), async {
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

#[tokio::test(flavor = "current_thread")]
async fn monday_nine_fires_exactly_one_start() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 0, "before Monday 9:00");
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    assert_eq!(starts.load(Ordering::SeqCst), 1, "Monday 9:00 fires once");
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn same_window_does_not_fire_twice() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    clock.set(MON1_9);
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn pause_across_two_mondays_is_one_catch_up_start() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    // Jump past Monday 1 and Monday 2 without stopping on either.
    clock.set(Timestamp(MON2_9.as_millis() + 3_600_000));
    wait_starts(&starts, 1).await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "catch-up=1, not one start per missed Monday"
    );
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn drop_runner_stops_further_starts() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    drop(running);
    clock.set(MON2_9);
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 1, "drop stops the ticker");
}

#[tokio::test(flavor = "current_thread")]
async fn overlap_still_starts() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let sink_starts = starts.clone();
    let def = WorkflowDefinition::builder("hold")
        .node("hold", "wait")
        .build()
        .unwrap();
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .sink(FnSink(move |e: &Event| {
                if matches!(e, Event::ExecutionStarted { .. }) {
                    sink_starts.fetch_add(1, Ordering::SeqCst);
                }
            }))
            .build(),
    );
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", def).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    clock.set(MON2_9);
    wait_starts(&starts, 2).await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "live previous run does not skip the next start"
    );
    drop(running);
}

#[test]
fn spec_rejects_non_five_field_and_unknown_tz() {
    let def = weekday_def();
    match ScheduleSpec::new("0 9 * * 1 *", "UTC", def.clone()) {
        Err(SpecError::CronFields { found: 6 }) => {}
        other => panic!("{other:?}"),
    }
    match ScheduleSpec::new("0 9 * * 1", "Not/AZone", def) {
        Err(SpecError::Timezone(_)) => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn next_after_monday_nine_is_the_following_monday() {
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    assert_eq!(spec.next_after(MON1_9), Some(MON2_9));
    let before = Timestamp(MON1_9.as_millis() - 1);
    assert_eq!(spec.next_after(before), Some(MON1_9));
}

/// Arm `wait_until` before T, then land on exact T. This is the Monday 9:00
/// skip if the ticker samples `now` after the jump (next_after is exclusive).
#[tokio::test(flavor = "current_thread")]
async fn arm_before_exact_t_then_set_t_fires_once() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    drop(running);
}

/// Start the ticker at exact Monday 9:00: next_after is exclusive, so this
/// slot is skipped (the race if arming happens after the clock already hit T).
#[tokio::test(flavor = "current_thread")]
async fn start_at_exact_monday_nine_skips_this_slot() {
    let clock = Arc::new(FakeClock::new());
    clock.set(MON1_9);
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        0,
        "exact T at arm time is next_after exclusive — this Monday is skipped"
    );
    clock.set(MON2_9);
    wait_starts(&starts, 1).await;
    drop(running);
}

/// A fire is Runtime::start, not a parked Ready{T} cron fake.
#[tokio::test(flavor = "current_thread")]
async fn fire_is_start_not_ready_t() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let store = MemoryStore::new();
    let ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let ids_s = ids.clone();
    let sink_starts = starts.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(store.clone())
            .sink(FnSink(move |e: &Event| {
                if let Event::ExecutionStarted { execution_id, .. } = e {
                    sink_starts.fetch_add(1, Ordering::SeqCst);
                    ids_s.lock().unwrap().push(execution_id.clone());
                }
            }))
            .register_fn("work", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build(),
    );
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ids = ids.lock().unwrap().clone();
            if let Some(id) = ids.first() {
                if let Some(snap) = store.get(id).await.unwrap() {
                    if snap.state.is_terminal() {
                        for (_, n) in snap.iter_nodes() {
                            assert!(
                                !matches!(
                                    n.state,
                                    NodeState::Ready {
                                        runnable_at: Some(_)
                                    }
                                ),
                                "schedule must not park Ready{{T}} to fake cron"
                            );
                        }
                        return;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal snapshot");
    drop(running);
}

/// Drop the caller's Runtime Arc while the ticker is armed. Drive holds the
/// last Arc — next tick still starts, no panic, one driver.
#[tokio::test(flavor = "current_thread")]
async fn drop_runtime_arc_while_armed_next_tick_still_starts() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt.clone())
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    drop(rt);
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    drop(running);
}

/// start() Err (unregistered) must not hang the ticker or skip a sibling job.
#[tokio::test(flavor = "current_thread")]
async fn tick_start_err_does_not_hang_or_skip_sibling() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let bad = WorkflowDefinition::builder("bad")
        .node("x", "missing")
        .build()
        .unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(ScheduleSpec::new("0 9 * * 1", "UTC", bad).unwrap())
        .job(ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap())
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    clock.set(MON1_9);
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 1, "sibling starts; no storm");
    clock.set(MON2_9);
    wait_starts(&starts, 2).await;
    drop(running);
}

/// `Runtime::start` has no Store variant — put/persist Err is after Ok.
/// The ticker must still arm the next Monday (no silent death, no storm).
#[tokio::test(flavor = "current_thread")]
async fn tick_store_put_fail_does_not_kill_ticker() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let fires = Arc::new(AtomicU32::new(0));
    let store = FailingStore::fail_on_nth_put(1);
    let f = fires.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(store)
            .register_fn("work", move |_ctx: ExecutionContext| {
                f.fetch_add(1, Ordering::SeqCst);
                async { NodeOutcome::Succeeded(Bytes::from_static(b"ok")) }
            })
            .build(),
    );
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&fires, 1).await;
    clock.set(MON1_9);
    settle().await;
    assert_eq!(
        fires.load(Ordering::SeqCst),
        1,
        "put fail must not retry-storm this slot"
    );
    clock.set(MON2_9);
    wait_starts(&fires, 2).await;
    drop(running);
}

/// After a fire, jumping the clock backward must not refire that slot.
#[tokio::test(flavor = "current_thread")]
async fn clock_jump_backward_after_fire_does_not_refire() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let spec = ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap();
    let running = Schedule::builder(rt)
        .clock(clock.clone())
        .job(spec)
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 1).await;
    clock.set(Timestamp(MON1_9.as_millis() - 60_000));
    settle().await;
    clock.set(MON1_9);
    settle().await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "armed next stays the following Monday"
    );
    drop(running);
}

#[tokio::test(flavor = "current_thread")]
async fn two_running_schedules_on_one_runtime_each_start() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(MON1_9.as_millis() - 1_000));
    let starts = Arc::new(AtomicU32::new(0));
    let rt = counting_runtime(clock.clone(), starts.clone());
    let a = Schedule::builder(rt.clone())
        .clock(clock.clone())
        .job(ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap())
        .build()
        .run();
    let b = Schedule::builder(rt)
        .clock(clock.clone())
        .job(ScheduleSpec::new("0 9 * * 1", "UTC", weekday_def()).unwrap())
        .build()
        .run();
    settle().await;
    clock.set(MON1_9);
    wait_starts(&starts, 2).await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "two tickers are two starts; not a shared mutex"
    );
    drop(a);
    drop(b);
}

fn ts_local(y: i32, m: u32, d: u32, h: u32, min: u32) -> Timestamp {
    let dt = Vancouver
        .with_ymd_and_hms(y, m, d, h, min, 0)
        .single()
        .unwrap();
    Timestamp(dt.timestamp_millis() as u64)
}

/// America/Vancouver 2026-03-08 spring-forward: 02:00–02:59 does not exist.
/// croner does not invent the gap minute; it lands on the first valid
/// instant after the jump (03:00 PDT), not the next calendar day's 02:30.
#[test]
fn vancouver_spring_forward_skips_missing_local_minute() {
    let spec = ScheduleSpec::new("30 2 * * *", "America/Vancouver", weekday_def()).unwrap();
    let before = ts_local(2026, 3, 8, 1, 59);
    let next = spec.next_after(before).expect("croner finds a later fire");
    let local = Utc
        .timestamp_millis_opt(next.as_millis() as i64)
        .single()
        .unwrap()
        .with_timezone(&Vancouver);
    assert!(
        !(local.month() == 3 && local.day() == 8 && local.hour() == 2),
        "must not invent 02:30 on the spring-forward gap, got {local}"
    );
    assert!(next > before);
    let gap_landing = Vancouver
        .with_ymd_and_hms(2026, 3, 8, 3, 0, 0)
        .single()
        .expect("03:00 PDT exists after the jump");
    assert_eq!(
        next,
        Timestamp(gap_landing.timestamp_millis() as u64),
        "croner first valid instant after the 2026 Vancouver gap is 03:00 PDT"
    );
    let after_gap = spec.next_after(next).expect("next after gap landing");
    assert_eq!(
        after_gap,
        ts_local(2026, 3, 9, 2, 30),
        "the following 02:30 PDT is Monday 2026-03-09"
    );
}

/// Fall-back 2026-11-01: 01:30 occurs twice. next_after is exclusive of `now`
/// and returns the next croner occurrence (first 01:30 after 01:00 PDT).
#[test]
fn vancouver_fall_back_picks_next_occurrence_not_both() {
    let spec = ScheduleSpec::new("30 1 * * 0", "America/Vancouver", weekday_def()).unwrap();
    let first_1am = Vancouver
        .with_ymd_and_hms(2026, 11, 1, 1, 0, 0)
        .earliest()
        .unwrap();
    let now = Timestamp(first_1am.timestamp_millis() as u64);
    let next = spec.next_after(now).expect("a 01:30 exists");
    let again = spec.next_after(next).expect("later week");
    assert!(next > now);
    assert!(
        again > next,
        "must not return the same fall-back 01:30 twice"
    );
}
