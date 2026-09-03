//! Hot-path split on this SHA. `cargo test -p keel-rt-schedule --release --test profile -- --nocapture --test-threads=1`

use bytes::Bytes;
use keel_rt::{
    Event, ExecutionContext, FakeClock, FnSink, NodeOutcome, NoopStore, Runtime, Timestamp,
    WorkflowDefinition,
};
use keel_rt_schedule::ScheduleSpec;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

const N: u32 = 100_000;

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

fn ms(d: std::time::Duration) -> String {
    format!("{:.3}ms", d.as_secs_f64() * 1000.0)
}

#[test]
fn profile_next_after_and_clone_and_arm() {
    let spec = ScheduleSpec::new("* * * * *", "UTC", weekday_def()).unwrap();
    let mut now = Timestamp(0);
    let t0 = Instant::now();
    let mut last = Timestamp(0);
    for _ in 0..N {
        last = spec.next_after(now).expect("next");
        now = last;
    }
    let next_after = t0.elapsed();

    let def = spec.definition();
    let t1 = Instant::now();
    let mut keep = 0usize;
    for _ in 0..N {
        let c = def.clone();
        keep = keep.wrapping_add(c.nodes().len());
    }
    let def_clone = t1.elapsed();
    assert_eq!(keep, N as usize);

    let rss0 = rss_bytes();
    let template = ScheduleSpec::with_shared("* * * * *", "UTC", Arc::new(weekday_def())).unwrap();
    let t2 = Instant::now();
    let armed: Vec<_> = (0..N).map(|_| template.clone()).collect();
    let arm_clone = t2.elapsed();
    let rss1 = rss_bytes();
    let next = armed[0].next_after(Timestamp(0));
    eprintln!(
        "profile next_after_{N}={} last={:?} def_clone_{N}={} spec_clone_arm_{N}={} rss_before={} rss_armed={} spec_size={} next={next:?}",
        ms(next_after),
        last,
        ms(def_clone),
        ms(arm_clone),
        rss0,
        rss1,
        std::mem::size_of_val(&template),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn profile_start_and_spawn_wait() {
    let clock = Arc::new(FakeClock::new());
    clock.set(Timestamp(0));
    let starts = Arc::new(AtomicU32::new(0));
    let sink = starts.clone();
    let rt = Arc::new(
        Runtime::builder()
            .clock(clock.clone())
            .store(NoopStore)
            .sink(FnSink(move |e: &Event| {
                if matches!(e, Event::ExecutionStarted { .. }) {
                    sink.fetch_add(1, Ordering::SeqCst);
                }
            }))
            .register_fn("work", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build(),
    );
    let def = weekday_def();

    let t0 = Instant::now();
    for _ in 0..N {
        let h = rt.start(def.clone()).expect("start");
        tokio::spawn(async move {
            h.wait().await;
        });
    }
    let start_plus_spawn = t0.elapsed();
    while starts.load(Ordering::SeqCst) < N {
        tokio::task::yield_now().await;
    }
    let until_started = t0.elapsed();

    let t1 = Instant::now();
    for _ in 0..N {
        let h = rt.start(def.clone()).expect("start");
        drop(h);
    }
    let start_plus_drop = t1.elapsed();

    eprintln!(
        "profile start+spawn_wait_{N}={} until_started={} start+drop_handle_{N}={} rss={}",
        ms(start_plus_spawn),
        ms(until_started),
        ms(start_plus_drop),
        rss_bytes()
    );
}
