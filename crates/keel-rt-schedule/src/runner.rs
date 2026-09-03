//! Drive loop: `Clock::wait_until` then `Runtime::start`. Not the kernel scheduler.
//!
//! One loop + a next-T min-heap. Not per-spec tasks. Not a second workflow engine.

use crate::spec::ScheduleSpec;
use keel_rt::{Clock, Runtime, Timestamp};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{AbortHandle, JoinHandle};

/// In-process cron ticker. Each due tick calls [`Runtime::start`] (new
/// `ExecutionId`). Overlap: a live previous run does **not** skip the next
/// start (kernel does not care). Crash of this loop loses the ticker; the
/// caller reconstructs specs. Catch-up is one start, then next from `now`.
pub struct Schedule {
    runtime: Arc<Runtime>,
    clock: Arc<dyn Clock>,
    jobs: Vec<ScheduleSpec>,
    max_starts_per_wake: usize,
}

pub struct ScheduleBuilder {
    runtime: Arc<Runtime>,
    clock: Option<Arc<dyn Clock>>,
    jobs: Vec<ScheduleSpec>,
    max_starts_per_wake: usize,
}

impl Schedule {
    pub fn builder(runtime: Arc<Runtime>) -> ScheduleBuilder {
        ScheduleBuilder {
            runtime,
            clock: None,
            jobs: Vec::new(),
            max_starts_per_wake: 64,
        }
    }

    /// Spawn the drive. Drop the returned runner to stop further starts.
    /// In-flight executions are not cancelled. A clock that never reaches the
    /// armed T stays in `wait_until`; Drop is the only hang-bound.
    pub fn run(self) -> RunningSchedule {
        let drive = tokio::spawn(drive(
            self.runtime,
            self.clock,
            self.jobs,
            self.max_starts_per_wake,
        ));
        RunningSchedule {
            abort: drive.abort_handle(),
            _join: drive,
        }
    }
}

impl ScheduleBuilder {
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn job(mut self, spec: ScheduleSpec) -> Self {
        self.jobs.push(spec);
        self
    }

    pub fn jobs(mut self, specs: impl IntoIterator<Item = ScheduleSpec>) -> Self {
        self.jobs.extend(specs);
        self
    }

    /// Cap `Runtime::start` calls per wake. Default is 64. Runtime concurrency
    /// is per execution, not across starts — an unbounded wake of 100k due
    /// specs is 100k live drives. Remaining due jobs after a cap still fire
    /// on the next loop (`wait_until` of a due T returns immediately).
    /// Fires are not dropped. Pass `usize::MAX` for an unbounded burst.
    pub fn max_starts_per_wake(mut self, n: usize) -> Self {
        self.max_starts_per_wake = n.max(1);
        self
    }

    pub fn build(self) -> Schedule {
        Schedule {
            runtime: self.runtime,
            clock: self.clock.unwrap_or_else(|| Arc::new(WallClock)),
            jobs: self.jobs,
            max_starts_per_wake: self.max_starts_per_wake,
        }
    }
}

/// Abort the drive on Drop. Does not cancel executions already started.
pub struct RunningSchedule {
    abort: AbortHandle,
    _join: JoinHandle<()>,
}

impl Drop for RunningSchedule {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

struct Armed {
    spec: ScheduleSpec,
    next: Timestamp,
}

async fn drive(
    runtime: Arc<Runtime>,
    clock: Arc<dyn Clock>,
    jobs: Vec<ScheduleSpec>,
    max_starts_per_wake: usize,
) {
    let now = clock.now();
    let mut armed: Vec<Armed> = jobs
        .into_iter()
        .filter_map(|spec| {
            let next = spec.next_after(now)?;
            Some(Armed { spec, next })
        })
        .collect();
    let mut heap: BinaryHeap<Reverse<(Timestamp, usize)>> = BinaryHeap::with_capacity(armed.len());
    for (i, job) in armed.iter().enumerate() {
        heap.push(Reverse((job.next, i)));
    }
    loop {
        let when = loop {
            let Some(Reverse((t, i))) = heap.peek().copied() else {
                return;
            };
            if armed[i].next != t {
                heap.pop();
                continue;
            }
            break t;
        };
        if when == Timestamp::MAX {
            return;
        }
        clock.wait_until(when).await;
        let now = clock.now();
        let mut started = 0usize;
        while started < max_starts_per_wake {
            let Some(Reverse((t, i))) = heap.peek().copied() else {
                break;
            };
            if armed[i].next != t {
                heap.pop();
                continue;
            }
            if t > now {
                break;
            }
            heap.pop();
            // Catch-up=1: one start, then next from now (not each missed slot).
            match runtime.start(armed[i].spec.definition().clone()) {
                Ok(handle) => {
                    tokio::spawn(async move {
                        handle.wait().await;
                    });
                }
                Err(_) => {}
            }
            let next = armed[i].spec.next_after(now).unwrap_or(Timestamp::MAX);
            armed[i].next = next;
            if next != Timestamp::MAX {
                heap.push(Reverse((next, i)));
            }
            started += 1;
        }
        // A jump can make every spec due at `now`. Yield so started
        // drives can finish before the next due batch (wait_until of a
        // past T returns immediately). This is not a live-execution
        // supervisor and does not drop remaining fires.
        if started == max_starts_per_wake {
            tokio::task::yield_now().await;
        }
    }
}

/// Local wall clock. Kernel `SystemClock` is not a public type.
struct WallClock;

#[async_trait::async_trait]
impl Clock for WallClock {
    fn now(&self) -> Timestamp {
        Timestamp::now_system()
    }

    async fn sleep(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        tokio::time::sleep(duration).await;
    }
}
