//! In-process cron ticker. Each fire is [`Runtime::start`] — a new
//! `ExecutionId`. This is not kernel `Ready { runnable_at }` and not a
//! sqlite timer table. The kernel does not depend on this crate.
//!
//! Overlap default: still `start` if the previous run is live. Catch-up
//! after a paused ticker is **one** start, then next from `now`.

mod runner;
mod spec;

pub use runner::{RunningSchedule, Schedule, ScheduleBuilder};
pub use spec::{ScheduleSpec, SpecError};
