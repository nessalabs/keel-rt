# keel-rt-schedule

In-process cron ticker for `keel-rt`. Each tick calls [`Runtime::start`]
with a reusable [`WorkflowDefinition`] — a **new** `ExecutionId` every
fire. The kernel does not depend on this crate.

This is not Phase 5 `Ready { runnable_at }` (that parks a node inside one
run). It is not distributed workers, HTTP, HITL, or a sqlite timer table.

## Contract

- 5-field cron (`min hour dom month dow`) plus an explicit IANA timezone.
- Drive waits with `Clock::wait_until` (use `FakeClock` in tests). One
  loop + a next-T min-heap (not a linear scan, not a task per spec).
- Missed ticks after a pause: **one** catch-up start, then next from now.
  Never replay every missed Monday as N starts. A 200k-period jump is
  one `next_after(now)`, not a walk of intermediate slots.
- Overlap: if the previous run is still live, still `start`. There is no
  `SkipIfRunning`.
- Phase 1 is in-memory. Crash of the ticker loses the loop; the caller
  reconstructs specs. No schedule table.
- DST: croner's `find_next_occurrence` in the IANA zone. A spring-forward
  gap minute is not invented. For America/Vancouver 2026, `30 2 * * *`
  from 01:59 PST lands on the first valid instant after the gap (03:00
  PDT / `2026-03-08T10:00:00Z`), not a fabricated 02:30 and not the next
  calendar day's 02:30. A fall-back repeated minute is the next
  occurrence after `now`, not both copies. See `ScheduleSpec::next_after`.
- `start()` `Err` on a tick (`StartError` is unregistered executors
  only): that fire is skipped, the ticker arms the next slot, sibling
  jobs still start. No retry-storm. Store `put`/`persist` `Err` happens
  after `start` Ok and is the kernel drive, not this crate.
- Many specs share one definition (`ScheduleSpec::clone` / `with_shared`).
  There is no fire-history vec. `Runtime::start` still takes an owned
  `WorkflowDefinition` (kernel), so each fire clones that DAG once.
- A jump that makes N specs due issues N starts on that wake (Runtime
  concurrency is per execution, not across starts).
  `max_starts_per_wake` paces the burst; remaining due jobs still fire.
- Stuck Armed (clock never reaches T): `wait_until` does not return.
  Drop `RunningSchedule` is the hang-bound. There is no `ScheduleState`.

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
