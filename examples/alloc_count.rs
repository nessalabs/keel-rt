//! Allocation-count probe (256-wide join). Not a CI gate.
//!
//! ```text
//! cargo run --release --example alloc_count
//! ```
//!
//! Uses a counting wrapper around the system allocator. One `#[global_allocator]`
//! per binary — do not run this together with `benches/jemalloc_compare`.
//! `keel-rt` never sets a process allocator.

use bytes::Bytes;
use keel_rt::{
    ExecutionContext, ExecutionState, NodeOutcome, Runtime, WorkflowDefinition,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static DEALLOC: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC.fetch_add(1, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn snapshot() -> (u64, u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        DEALLOC.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio");
    rt.block_on(async {
        let n = 256usize;
        let mut b = WorkflowDefinition::builder("wide")
            .node("src", "ok")
            .node("join", "ok");
        for i in 0..n {
            let id = format!("w{i}");
            b = b
                .node(id.as_str(), "ok")
                .edge("src", id.as_str())
                .edge(id.as_str(), "join");
        }
        let def = b.build().expect("wide");
        let runtime = Runtime::builder()
            .concurrency(32)
            .register_fn("ok", |_ctx: ExecutionContext| async {
                NodeOutcome::Succeeded(Bytes::from_static(b"ok"))
            })
            .build();
        let before = snapshot();
        let started = Instant::now();
        let state = tokio::time::timeout(Duration::from_secs(30), runtime.run(def))
            .await
            .expect("timeout")
            .expect("start");
        let elapsed = started.elapsed();
        assert_eq!(state, ExecutionState::Succeeded);
        let after = snapshot();
        println!(
            "alloc_count wide_256 allocs={} deallocs={} bytes={} elapsed={:.3}ms (system allocator, current_thread)",
            after.0.saturating_sub(before.0),
            after.1.saturating_sub(before.1),
            after.2.saturating_sub(before.2),
            elapsed.as_secs_f64() * 1000.0,
        );
    });
}
