//! Drive loop: `Clock::wait_until` then `Runtime::start`. Not the kernel scheduler.

use crate::spec::ScheduleSpec;
use keel_rt::{Clock, Runtime, Timestamp};
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
}

pub struct ScheduleBuilder {
    runtime: Arc<Runtime>,
    clock: Option<Arc<dyn Clock>>,
    jobs: Vec<ScheduleSpec>,
}

impl Schedule {
    pub fn builder(runtime: Arc<Runtime>) -> ScheduleBuilder {
        ScheduleBuilder {
            runtime,
            clock: None,
            jobs: Vec::new(),
        }
    }

    /// Spawn the drive. Drop the returned runner to stop further starts.
    /// In-flight executions are not cancelled.
    pub fn run(self) -> RunningSchedule {
        let drive = tokio::spawn(drive(self.runtime, self.clock, self.jobs));
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

    pub fn build(self) -> Schedule {
        Schedule {
            runtime: self.runtime,
            clock: self.clock.unwrap_or_else(|| Arc::new(WallClock)),
            jobs: self.jobs,
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

async fn drive(runtime: Arc<Runtime>, clock: Arc<dyn Clock>, jobs: Vec<ScheduleSpec>) {
    let now = clock.now();
    let mut armed: Vec<Armed> = jobs
        .into_iter()
        .filter_map(|spec| {
            let next = spec.next_after(now)?;
            Some(Armed { spec, next })
        })
        .collect();
    loop {
        let Some(when) = armed.iter().map(|a| a.next).min() else {
            return;
        };
        if when == Timestamp::MAX {
            return;
        }
        clock.wait_until(when).await;
        let now = clock.now();
        for job in &mut armed {
            if job.next > now {
                continue;
            }
            // Catch-up=1: one start for any number of missed ticks, then
            // next is computed from now (not each skipped Monday).
            match runtime.start(job.spec.definition().clone()) {
                Ok(handle) => {
                    tokio::spawn(async move {
                        handle.wait().await;
                    });
                }
                Err(_) => {}
            }
            job.next = job.spec.next_after(now).unwrap_or(Timestamp::MAX);
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
