# Changelog

## Cancel one execution (`sdk/cancel`)

[`KeelClient::cancel(execution_id)`] is `POST /cancel/:id` with the same
[`SECRET_HEADER`] / Bearer, [`HANG_BOUND`], `send`, and `uri` as start /
inspect / complete. No `CANCEL_HANG_BOUND` and no `cancel rejected`
Display. The server calls [`Runtime::cancel`] — the same inbox
[`Event::Cancel`] as [`ExecutionHandle::cancel`] / Drop. Stolen lease
is **423 Locked** [`KeelClientError::ClaimedElsewhere`]. Unknown id is
**404**. Already-terminal (Succeeded after approve) is **200 noop**
(cite `cancel_already_terminal_is_noop`; does not rewrite to Cancelled).
Duplicate cancel of Cancelled is the same noop. Cancel of one HTTP start
leaves the other park Waiting. FailSubtree still-Running on a
**different** execution is not cancelled. Cancel of a FailSubtree run
cancels its Running sibling. Later approve is **409** and does not
revive. Server-drop still Drop-cancels remaining parks
(`client_drop_http_server_after_start_cancels_wait`). SQLite proof lives
in `keel-rt-sqlite` (`http_sqlite_start_cancel_second_runtime_is_claimed_elsewhere`).
Kernel `src/` grew only [`Runtime::cancel`] / [`CancelError`] — proven
by `runtime_cancel_by_execution_id_cancels_parked_wait` (no cancel-by-id
existed). No Agent / HTTP / HITL / EventLog types in the kernel.

## Start then approve / reject (`sdk/start-approve`)

[`KeelClient::start`] is `POST /start` with kernel durable JSON
([`WorkflowDefinition::durable_bytes`] / [`from_durable_bytes`]):
`id`, `on_failure`, nodes `{id, executor_id, join}`, edges — not a
snapshot dump and not a second graph language. FailSubtree / AllDone
survive the wire (`client_start_fail_subtree_keeps_running_sibling`).
The server calls [`Runtime::start`] and **holds the handle iff the
snapshot is not terminal** (`hold_if_live` / `drop_held_if_terminal`).
No `wait` / `wait_stable` in the adapter — those are a second status
machine. The next start reaps held terminals via snapshot
`is_terminal()` only (`reap_held_terminals`); N instant-success
starts do not stay in the vec
(`reap_held_terminals_clears_n_finished_keeps_park`,
`client_n_instant_http_starts_are_reaped_on_next_start`).
FailSubtree reject returns while the sibling is still Running
and later server-drop still cancels it
(`client_fail_subtree_reject_returns_and_server_drop_cancels_sibling`).
Dropping the server Drop-cancels parks
(`client_drop_http_server_after_start_cancels_wait`) and does not
Cancel a terminal already dropped
(`client_start_terminal_survives_server_drop`). Duplicate approve
after Drop of a **terminal** handle is 200 noop (same bytes; no
second downstream). Drop-cancel of a **live park** stays 409
(`client_approve_after_terminal_handle_drop_is_200_noop`,
`client_approve_after_http_start_server_drop_is_409`,
`complete_after_drop_of_terminal_handle_is_duplicate_noop`).
The client returns the new [`ExecutionId`]. Each call is a new run
(`client_two_starts_are_distinct_ids`); a second Runtime's new id is
not a steal (`http_start_second_runtime_new_id_is_not_steal`).
HTTP start + inspect + complete on sqlite (two Runtimes, one file)
lives in `keel-rt-sqlite` — this crate does not import SqliteStore
(`http_sqlite_start_inspect_complete_two_runtimes_new_id_is_not_steal`).
Unregistered / empty definition is **400**; oversized **413**; missing
secret **401** (verb-neutral Display; no `START_*` names). Same
[`SECRET_HEADER`] / [`HANG_BOUND`] / `send` / `uri` as inspect and
complete. [`CompleteSecret`] stays named for historical
`x-keel-complete`; it is the secret on every route.

[`KeelClient::approve`] / [`KeelClient::reject`] POST the existing
`/complete` body: `Decision::Complete(bytes)` and `Decision::Fail`
(`NodeOutcome::failed("failed")` → execution Failed). There is no
`Decision` reject variant (PR #9). No `/approve` or `/reject` routes.
Proof: `client_start_inspect_approve_unblocks_wait`,
`client_start_inspect_reject_fails_execution` (cite
`client_decision_fail_fails_execution`). Approve of an issued
Running-node token leaves the wait parked
(`client_approve_issued_token_for_running_node_leaves_wait_parked`).
Kernel `src/` unchanged.

## Inspect then complete (`sdk/inspect`)

[`KeelClient::inspect`] is `GET /inspect/:id` with the same secret as
[`KeelClient::complete`] (`X-Keel-Complete` and `Authorization: Bearer`).
The response is [`InspectView`] (execution id, state, nodes with wait
token inside [`InspectNodeState::Waiting`]) — not an [`ExecutionHandle`]
and not a cloned kernel [`NodeState`]. Unknown id is 404; terminal and
Cancelled are 200 with state (complete of a cancelled token is still
409). Shared client errors are verb-neutral (`request rejected`, `hung`,
`unknown execution`): inspect 401 Display does not contain the word
`complete`. Inspect and complete share one send path ([`HANG_BOUND`],
[`SECRET_HEADER`]). A hung inspect is [`KeelClientError::Hung`] at
[`HANG_BOUND`]; the client does not follow redirects off
loopback. The other process reads the wait token from inspect, then
completes; the engine unblocks and downstream sees the bytes. Proof:
`client_inspect_then_complete_unblocks_wait`. `InspectView` JSON carries
the wait token **once**, inside `InspectNodeState::Waiting { token }`
(kernel snapshot serde is frozen; the HTTP DTO has no `resume_token`
field and does not clone `NodeState`). Running-node tokens stay on the
snapshot and do not appear on inspect JSON
(`inspect_view_json_running_pred_does_not_contain_running_token`). Two
Runtimes: inspect does not steal; complete on the non-owner is **423
Locked** [`KeelClientError::ClaimedElsewhere`] with body
`{"error":"claimed_elsewhere"}` — not 400, not 409 Cancelled.
Live inspect and the
store agree on the wait token after `NodeWaiting` is announced
(`inspect_after_node_waiting_matches_store_and_does_not_emit`); inspect
does not emit a public `Event`. Inspect while a predecessor is Running
has no wait token
(`client_inspect_while_running_has_no_token_then_wait_sees_token`).
A parked wait beside a Running sibling still exposes the wait token;
complete of the Running handle token does not unblock
(`client_inspect_wait_sibling_while_running_completes_only_wait`).
Completing a token issued for the Running node leaves the wait parked
(`client_complete_issued_token_for_running_node_leaves_wait_parked`).
Release numbers: [`benches/BASELINE.md`](benches/BASELINE.md). Thin
[`Runtime::inspect`] wraps the existing live [`Event::Inspect`] / store
get so the adapter can read by id. Start / resume / complete are
unchanged. No forms, no HITL names, no start HTTP, no schedule.

## Complete client (`sdk/complete`)

[`keel-rt-http::KeelClient`] is the out-of-process SDK client;
[`KeelClient::complete`] POSTs the same `CompleteBody` `{ token, resume }`
the server already accepts (`X-Keel-Complete` and
`Authorization: Bearer`). Optional
[`Decision::{Complete(Bytes), Fail, Reinvoke}`] maps onto existing
[`Resume`] (Succeeded / Failed / Reinvoke) — not a second token or
state machine. Missing/wrong secret is still 401; cancelled token 409
(complete does not revive); duplicate complete is 200 noop; body > 1
MiB is 413. A hung server is [`KeelClientError::Hung`] after
[`HANG_BOUND`] (5s); the client does not follow redirects
off the given URL. In-process complete stays [`Runtime::complete`].
Kernel `src/` is unchanged. The kernel does not depend on this crate.
Not sqlite, not a lease, not a schedule ticker, not forms or identity.

## Schedule ticker (`sdk/schedule`)

Sibling crate [`keel-rt-schedule`]: 5-field cron + IANA timezone, driven
by `Clock::wait_until`. Each fire is `Runtime::start` (new `ExecutionId`).
Catch-up after a paused ticker is one start, then next from now. Overlap
still starts. `start` `Err` (unregistered) skips that fire and arms the
next slot — no hang, no retry-storm. Store put/persist `Err` is after
`start` Ok (kernel drive). America/Vancouver DST is croner's next
occurrence: spring-forward `30 2 * * *` from 01:59 PST lands on 03:00
PDT, not an invented 02:30. Kernel `src/` has no cron types. The kernel
does not depend on this crate. Not a sqlite timer table. Not HTTP. Not
HITL. Drive is one loop + a next-T heap. Specs share a definition
`Arc` (cron/tz/strings interned; clone is a pointer). A 200k-period
catch-up is one start. Default `max_starts_per_wake` is 64 (Runtime
concurrency does not cap starts). The drive yields between batches;
remaining due jobs still fire. Drop of the runner is the hang-bound
for a `wait_until` that never completes. Heap entries are
`(Timestamp, index)` — not a spec per node.

## Wait / gate (`sdk/wait-gate`)

Builtin executor id `wait` ([`Wait`]) is auto-registered on
[`Runtime::builder`]. `execute` returns `Waiting { token: ctx.resume_token }`.
Override the id if a caller registers their own `"wait"`.

[`Runtime::complete(token, Resume)`] is the in-process hook: if this Runtime
already has the execution live, it injects Complete/Reinvoke (no second
drive). If not, it loads the snapshot, applies, persists, then drives so a
second process can complete after crash or engine-down. Drop handle still
cancels; complete does not revive Cancelled. Unknown token is
[`CompleteError::UnknownToken`]. Duplicate Complete remains a noop.
Failed Complete uses definition `OnFailure` (fail-fast default).

Cross-process HTTP is [`keel-rt-http`]: `POST /complete` with token + Resume.
A shared secret is required (`Authorization: Bearer …` or `X-Keel-Complete`).
Missing/wrong secret is 401. Default bind is `127.0.0.1` only (`serve_on`
is the explicit 0.0.0.0 path). Body larger than 1 MiB is 413 and does not
complete. [`ResumeToken`] nonce is a 128-bit mix (not a counter). The kernel
crate does not depend on the HTTP crate. No cron, no EventLog, no NodeReady.

Two Runtimes on one store are fenced by a store lease: `StateStore::claim`
/ `heartbeat` / `release`. Persist and complete carry a fencing `epoch`.
Default TTL is 30s (`DEFAULT_LEASE_TTL`, Clock `now`). sqlite columns
`owner`, `epoch`, `lease_until` on `executions` (`ALTER TABLE` on old
files; kernel `SCHEMA_VERSION` stays 1). Claim uses `BEGIN IMMEDIATE`
and never `INSERT OR REPLACE`. Drop Runtime releases; drop handle still
cancels. HTTP `POST /complete` stays on the owning process.

## Phase 4 RetryFailed (`phase-4/retry-failed`)

[`Runtime::resume`] is still Continue: Failed stay Failed. [`Runtime::resume_with`]
`Recover::RetryFailed` turns Failed/TimedOut/Cancelled into Pending through
`set_state` (attempt 0, drop token/output; keep `last_error`), rebuilds
remain, then Ready-now only when `remain == 0` (dispatch is attempt 1, a
fresh [`RetryPolicy`] budget). A Succeeded AllDone join whose fan-in
includes a retried pred goes Pending (output cleared) and re-runs after
the pred Succeeded. An AllDone join that itself Failed waits
(`remain > 0`) then re-runs. Leaves whose preds are still Succeeded
become Ready. Non-consumer Succeeded nodes keep Bytes. Waiting tokens
stay. Execution must be Failed or Completed-with-failures; Succeeded /
Waiting / Cancelled → [`ResumeError::NotFailed`] (`ApplyError::Illegal`
only). Persist the recovered snapshot before dispatch (Ready-now on the
store in that gap; `inspect` after resume_with is post-Restore). CAS
still applies. New attempts use the definition `OnFailure` (fail-fast
default unchanged). Token resume is still [`ExecutionHandle::resume`]
(`Resume::Complete` / `Reinvoke`). No cron, no per-node freshness, no
EventLog, no NodeReady. Wait is still the Runtime drive
(`next_drive_event`: inbox vs `Clock::wait_until`). Due T is `RetryDue`
then dispatch — not TimedOut.

## Phase 5 snapshot deadlines (`phase-5/timers`)

A node is not runnable until **T**. T is a [`Timestamp`] (u64 millis) on
`NodeState::Ready { runnable_at: Some(T) }` so crash-resume sees it. Policy
(how long, retry/backoff counts) stays on `RetryPolicy`. `timeout_after`
keeps the node Running (executor Delay) and is not snapshot T. Waiting stays
HITL. Kernel has no cron, no wall timezone, no sqlite timer table.
`Event::NodeTimedOut` already exists; do not emit `NodeReady`. Drop handle
still cancels the Runtime drive waiter (RAII). Fail-fast / AND-join defaults
unchanged. Domain and runtime
name [`Clock`] only; `FakeClock` lives in `src/testing` and `tests/`. sqlite
persists whatever the snapshot already has (`synchronous=FULL` default).
Waiting for T is the Runtime drive (`next_drive_event`: inbox vs
`Clock::wait_until`); domain and scheduler apply given `now` and do not sleep.
The drive prefers the apply inbox when T is already due (`try_recv`) so
Cancel/Shutdown at the same instant as a due deadline does not dispatch.
Due T is `RetryDue` then dispatch — not TimedOut. TimedOut is already on the
snapshot when policy Accepts at `FinishNode`. Constructed stuck-wait tests:
haywire `wait_until` (Pending forever) still loses to hang bound / inbox
cancel; `Timestamp::MAX` cancel returns without `thread::sleep`; due T does
not call `wait_until`. When a caller persist/resume parked nodes, T is
`nodes.runnable_at` INTEGER; parked JSON is compact Ready-now plus a short
`last_error` so live inspect and crash-resume inspect agree. Crash-resume
still restores `Ready { runnable_at: Some(T) }`. `SCHEMA_VERSION` is still 1:
new files write compact Ready + column; an old adapter that only reads JSON
would load `Ready { None }` and dispatch immediately (same-repo is OK, not
fail-closed). `Timestamp::saturating_add` saturates `Duration` millis that
do not fit in `u64` (`1<<61` seconds used to wrap to T==now). Node JSON
omits null optionals; a retry park drops the stale attempt token (Waiting
still carries the token). MemoryStore no-timer medians stay within 10% of
`main` (`da1e6fa`).

## Phase 3 events (`phase-3/events`)

Public surface is [`Event`] + [`EventSink`] only (no `EventLog`). Variants:
ExecutionStarted/Succeeded/Failed/Completed/Cancelled and
NodeStarted/Succeeded/Failed/TimedOut/Cancelled/Waiting. No `NodeReady`.
Each event carries execution id, workflow id, node id when it is a node
event, attempt, Clock time, and `schema_version`. persist_then_emit is
store Ok then sink; sink `Err` / panic does not un-persist or fail the
run. Resume is still the StateStore snapshot. sqlite may write event rows
in the same persist txn; those rows are never used to resume. At-least-once
re-invoke may emit the same node event twice. Absences are tests: no public
`EventLog`, frozen Event variants (no `NodeReady`), persist-before-announce,
`scripts/pr_body_gate.py` (mermaid + `When a caller` + base `main` unless
`[stack]`). persist `Err` no longer drops pending events: a later persist
`Ok` (including Shutdown retry) announces the transitions that became durable.
sqlite: COMMIT is persist Ok; equal-revision does not insert event rows;
checkpoint `Err` after COMMIT does not fail persist. `EventSink::try_emit` is
the required method; `emit` swallows `Err`. `Event::node_id` / `attempt` are
accessors. Resume seeds `last_persisted` from the snapshot so a no-op Waiting
restore does not open a new sqlite txn.

## Shutdown retries persist until the store recovers (`chaos/load-p2`)

A single extra persist on Shutdown left sqlite **Running** when the terminal
write failed twice (command + first Shutdown attempt). `wait()` had already
returned Succeeded. Resume re-invoked work the caller treated as done. Fix:
Shutdown retries persist up to eight times. Permanently failing persist is
still Phase 1 in-memory wins. Fail-fast / AND-join unchanged.

Seeded sqlite crash-inject (`crates/keel-rt-sqlite/tests/crash_inject.rs`)
runs 256 process-kill/resume seeds against small DAGs (FakeClock, no wall
sleep) plus constructed killers (clock jump, stale FinishNode, HITL
duplicate Complete, persisted cancel, AlreadyActive, uncommitted retry
delay, WAL after Succeeded COMMIT, definition mismatch, unicode ids, 64KiB
join inputs, FIFO-64, `open` vs `open_fast`). CI job `chaos-sqlite`.

## Shutdown flushes a transient last persist (`chaos/load-p2`)

`wait()` / Drop-cancel used to leave sqlite at **Running** when the terminal
or Cancel persist returned `Err` once (`last_persisted` advanced on failure;
Shutdown did not persist). Resume re-invoked work the caller already saw as
Succeeded or Cancelled. Fix: persist `Ok` only advances `last_persisted`;
Shutdown retries `persist_then_emit`. Fail-fast / AND-join unchanged.

## Standing sqlite chaos pack (`chaos/load-p2`)

`crates/keel-rt-sqlite/tests/chaos.rs` is the standing breaker: thousands of
short jobs, 256/2k-wide AND-join crash/resume, diamond farm + retry + HITL,
burst/idle/burst, fat vs 1-byte, start-crash-resume storms, two Runtimes on
one file, 1pm/4pm `FakeClock` AND-join, drop-handle mid-persist, policy/sink/
executor/persist panic. Nothing broke on the first pass. Numbers:
[`docs/CHAOS_LOG.md`](docs/CHAOS_LOG.md). CI job `chaos-sqlite` (not coverage).
256-wide resume under 32 concurrent starts is **sqlite-bound** (~474 ms idle
and under load) — the 50-diamond −32% loop is start/drop bound.

## Sqlite persist ≥50% (phase-2/resume)

`keel-rt-sqlite` no longer dumps the whole graph JSON on every persist.
WAL, one transaction per persist call, dirty node rows after the first write.
Default `SqliteStore::open` is `synchronous=FULL`. 256-wide resume used to
take **1.008 s** debug median; now **421 ms** (−58%) without donating
power-loss durability. `open_fast` (`NORMAL`) is **259 ms** (−74%) if the
caller accepts that power loss may drop the last WAL frames. Process kill
after COMMIT recovers on both. Crash-after-CAS, torn WAL, and `SQLITE_BUSY`
stay typed. MemoryStore benches unchanged (≤10% vs RAII). Kernel coverage
100%.

## Phase 2 snapshot resume

At-least-once resume from the last CAS snapshot. `Runtime::resume(&id)`.
Running-at-crash is re-invoked (attempt + 1). File adapter is
`keel-rt-sqlite` (sibling crate). ADR 0004. Kernel still has no Agent/HTTP
and does not pick an allocator.

## Phase 1 freeze

Local DAG kernel (`keel-rt`) is frozen on `main` at
`5ca1b971c0b21e9259881709b491c0bd7d58bebd`.

This changelog commit exists so Origin can open a review PR (`main` cannot
PR onto itself). **Kernel code stays on `main`**; this branch does not revert
or rewrite it.

- Fail-fast default: `OnFailure::FailExecution`. AND-join: `Join::AllSucceeded`.
- `FailSubtree` / `Join::AllDone` stay definition-only opt-in.
- No Agent, HTTP, or merge of `examples/studio`.
- Library does not set `#[global_allocator]` and has no `jemalloc` feature.
  Benches on `current_thread` were noise; jemalloc is **not recommended**.
- Review description (template filled): [`docs/PR_BASELINE.md`](docs/PR_BASELINE.md).
- Architecture baseline: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
