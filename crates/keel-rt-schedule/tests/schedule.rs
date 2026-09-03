//! FakeClock fires. `cargo test -p keel-rt-schedule --test schedule`

use bytes::Bytes;
use keel_rt::{
    Event, ExecutionContext, FakeClock, FnSink, NodeOutcome, Runtime, Timestamp, WorkflowDefinition,
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
