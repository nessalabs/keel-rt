//! In-process cron ticker. Each fire is [`Runtime::start`] — a new
//! `ExecutionId`. This is not a parked kernel deadline and not a
//! sqlite timer table. The kernel does not depend on this crate.
//!
//! The whole machine is [`ScheduleSpec`] → `next_after(now)` →
//! [`Clock::wait_until`](keel_rt::Clock::wait_until) → [`Runtime::start`]
//! → arm next. Drop [`RunningSchedule`] stops further starts. Durable
//! store, lease, and DAG execution stay in the kernel.
//!
//! Overlap default: still `start` if the previous run is live. Catch-up
//! after a paused ticker is **one** start, then next from `now`.

mod runner;
mod spec;

pub use runner::{RunningSchedule, Schedule, ScheduleBuilder};
pub use spec::{ScheduleSpec, SpecError};
