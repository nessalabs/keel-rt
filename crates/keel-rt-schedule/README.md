# keel-rt-schedule

In-process cron ticker for `keel-rt`. Each tick calls [`Runtime::start`]
with a reusable [`WorkflowDefinition`] — a **new** `ExecutionId` every
fire. The kernel does not depend on this crate.

This is not Phase 5 `Ready { runnable_at }` (that parks a node inside one
run). It is not distributed workers, HTTP, HITL, or a sqlite timer table.

## Contract

- 5-field cron (`min hour dom month dow`) plus an explicit IANA timezone.
- Drive waits with `Clock::wait_until` (use `FakeClock` in tests).
- Missed ticks after a pause: **one** catch-up start, then next from now.
  Never replay every missed Monday as N starts.
- Overlap: if the previous run is still live, still `start`. There is no
  `SkipIfRunning`.
- Phase 1 is in-memory. Crash of the ticker loses the loop; the caller
  reconstructs specs. No schedule table.

```rust
use keel_rt::{Runtime, WorkflowDefinition};
use keel_rt_schedule::{Schedule, ScheduleSpec};
use std::sync::Arc;

let def = WorkflowDefinition::builder("weekday")
    .node("work", "work")
    .build()?;
let spec = ScheduleSpec::new("0 9 * * 1-5", "UTC", def)?;
let rt = Arc::new(Runtime::builder().register_fn("work", /* … */).build());
let running = Schedule::builder(rt).job(spec).build().run();
// Drop `running` to stop further starts. In-flight runs keep going.
```
