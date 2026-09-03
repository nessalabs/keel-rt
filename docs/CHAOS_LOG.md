# Sqlite chaos log (standing breaker)

Standing attack log against **one local sqlite file**. Fail-fast and AND-join
stay the library defaults. Two `Runtime`s on one file are fenced by store
lease + epoch (ADR 0004).

When an attack **breaks**: fix production + a named test. A green load pack
is not a hunt.

## Defect: persist `Err` treated as durable; Shutdown did not flush

**Broke.** Kernel `src/runtime/scheduler.rs`. Named tests (failing before the
fix, then kept):

- `transient_terminal_persist_err_shutdown_flushes_succeeded` (catalog / coverage)
- `transient_cancel_persist_err_shutdown_flushes_cancelled`
- `transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded` (`open` and `open_fast`)
- `transient_cancel_persist_err_shutdown_flushes_sqlite_cancelled`

### Mechanism

`persist_snapshot` on `Ok(Err(_))` / persist panic set `last_persisted =
exec.revision()` anyway, and `Event::Shutdown` returned without persisting.

`apply` sends the watch (`wait` / `inspect`) **before** persist. `persist_then_emit`
skips the sink on persist `Err` (that half was already correct). The last
apply of a run is terminal Succeeded/Failed/Cancelled, or Drop-cancel. There
is no later `NodeFinished` to carry dirty rows forward. Treating persist `Err`
as “already persisted” meant Shutdown saw `revision == last_persisted` and
skipped the retry.

Sqlite `SQLITE_BUSY` / a one-shot wrapper `Err` on the terminal write is the
same shape: COMMIT did not happen, watch already said Succeeded/Cancelled,
process then does a **clean** Drop (`wait` consumes the handle so Drop is
Shutdown only, not Cancel). File still Running. `Runtime::resume` restores
Running as Ready and re-invokes.

### Preconditions

1. At least one persist of the execution already succeeded (Running is in the
   file).
2. The **next** persist is the last apply (terminal or Cancel) and returns
   `Err` (or panics before COMMIT).
3. Caller observes in-memory terminal via `wait` / `wait_stable` / inspect.
4. Clean Drop of the handle/runtime (Shutdown), not `mem::forget` crash.

### Blast radius

- `wait()` returned **Succeeded**, resume re-ran the executor (duplicate
  side effects; at-least-once of a job the product already treated as done).
- Drop-cancel returned **Cancelled** in-memory, file stayed **Running**,
  resume resurrected work the handle cancelled.
- Fail-fast / AND-join unchanged. Live-process “in-memory wins” when persist
  **never** succeeds is still Phase 1
  (`store_error_on_terminal_write_keeps_in_memory_succeeded`).

### Fix

- Advance `last_persisted` only on persist `Ok(())`.
- `Event::Shutdown` calls `persist_then_emit` after aborting execute tasks.

Process kill (`mem::forget` + drop tokio) still does not flush — that is
crash-at-Running and stays at-least-once per ADR 0004 / the resume catalog.

## Defect: Shutdown persist was a single extra attempt

**Broke.** Same files. Named tests (failed with `left: Running, right: Succeeded`,
then kept):

- `transient_terminal_persist_err_twice_shutdown_retries_until_ok` (catalog)
- `transient_terminal_persist_err_twice_shutdown_retries_sqlite`

### Mechanism

The first fix retried persist **once** on Shutdown. Timeline: NodeFinished
persist fails (`wait()` already Succeeded) → Drop sends Shutdown → persist
fails again (`SQLITE_BUSY` / `SQLITE_FULL` still recovering) → file stays
Running. Resume re-invokes work the caller observed as done.

### Preconditions

Same as the first defect, plus the recovering store still returns `Err` on
the first Shutdown persist.

### Blast radius

Identical to the first defect whenever the backend needs more than one extra
attempt (a locked writer, a full disk that is being freed). Fail-fast /
AND-join unchanged. Permanently failing persist is still Phase 1 in-memory
wins (`store_error_on_terminal_write_keeps_in_memory_succeeded`).

### Fix

`Event::Shutdown` calls `persist_then_emit_n(8)`. Bounded so Drop cannot hang.
Apply / watch order unchanged (in-memory still wins live).

## Seeded crash-inject (`crates/keel-rt-sqlite/tests/crash_inject.rs`)

`randomized_crash_inject_sqlite`: xorshift seed, printed on failure. Small
DAGs (chain, diamond, wide join, hourglass, FailSubtree+AllDone, HITL
Waiting, retry delay). Crash at a legal moment after a persist (Running,
Waiting, retry Ready, Failed, cancel, or `wait()` + Shutdown). Reopen the
sqlite file, `resume`, assert: terminals never re-run, Waiting keeps the
token, Running-at-crash is attempt+1, AND-join once, fail-fast stays Failed,
file is not Running if `wait()` already returned Succeeded/Cancelled, no
permit leak, no hang. 256 seeds / 60s budget; default CI `test` job does not
run this pack (`chaos-sqlite` does).

If a seed fails: keep it as a named regression. The suite is not a 2000-job
loop that never crashes.

## Hunt refutations (from the code, not a passing stress table)

| Suspicion | Verdict | Why it cannot happen / what it is |
|---|---|---|
| Dirty set incomplete (`runnable_at`, token, attempt) | **Phase 4 RetryFailed is a `set_state` leave of terminals** | Failed/TimedOut/Cancelled (and a Succeeded AllDone consumer of a retried pred) go Pending through `set_state` → `dec_count` now leaves terminals; `mark_dirty` is included. Not “impossible without set_state skip”. Other mutations still go `set_state` or mutate then `set_state` on that slot. `remain[]` is **not** persisted; `from_snapshot` rebuilds it (`remain_for`). |
| `Join::AllDone` vs `AllSucceeded` restored wrong | **impossible if definition bytes match** | Join lives on `WorkflowDefinition`, stored beside the snapshot (`INSERT OR IGNORE` by `content_hash`). Restore uses `definition.join_at`, not the snapshot. Hash mismatch is `DefinitionHashMismatch`. Same hash ⇒ same `durable_bytes` (joins included). |
| Persist-before-announce lie (sink) | **held** | `persist_then_emit` takes events, persist, emit only on `Ok`. Catalog: `persist_succeeds_before_execution_succeeded_is_emitted`. **Watch** still updates in `apply_cmd_result` before persist (live `wait` can return before the file). After this fix, **clean Shutdown** retries so the file catches up when persist can succeed. |
| `AlreadyActive` TOCTOU two apply tasks | **impossible on one Runtime** | `claim_active` is `HashSet::insert` under the mutex **before** `store.get` / spawn. Second `resume` gets `AlreadyActive`. Failed `spawn_resume` drops `ActiveGuard` and unregisters. Two Runtimes on one store are fenced by `claim` (`ClaimedElsewhere`). |
| WAL TRUNCATE vs in-flight `put` | **same connection, after COMMIT** | `checkpoint_wal` runs only if `committed && terminal` on that connection, `debug_assert!(is_autocommit)`. Checkpoint `Err` (SQLITE_BUSY) is ignored — COMMIT already durable (`checkpoint_busy_after_commit_is_persist_ok_and_resume_sees_succeeded`). Another `SqliteStore` is another connection; tearing WAL frames is `truncated_wal_does_not_invent_a_terminal`. Equal-revision persist does not append event rows (`equal_revision_persist_does_not_grow_event_rows`). |
| Crash between Running persist and cancel persist | **documented at-least-once** | Catalog: crash while Running re-invokes. Drop is Cancel **if Shutdown/Cancel persist**. A **process kill** before cancel persist leaves Running — not a Drop-cancel. The bug above was clean Drop + persist `Err`, not crash. |
| Attempt not bumped on sqlite resume of Running | **impossible** | `from_snapshot` converts Running → Ready with `reinvoke: false`; `dispatch_node` does `attempt += 1`. Stale `FinishNode` for attempt 1 is a no-op (`finish_node` match). `crash_during_b_running_reinvokes_b_not_a` asserts attempt 2. |
| Resume with a different definition, same hash | **fail-closed or identical DAG** | Non-empty `definition_hash` must equal `content_hash()`. Collision of SHA body is identical `durable_bytes`. Empty hash is only `Default` / unhashed; sqlite persist always writes `content_hash()`. |
| Clock: SystemClock after FakeClock deadlines | **caller clock port** | `runnable_at` is an opaque `Timestamp`. Resume uses the **new** Runtime’s `Clock`. Mixing FakeClock(0) deadlines with `SystemClock` fires immediately (`at <= now`). Not a sqlite restore bug; do not mix clocks. |
| Id serde `/` `.` unicode | **round-trip as TEXT** | `NodeId` / `ExecutionId` serialize as strings; sqlite binds `?1` TEXT. `ExecutionId::parse` rejects only empty. No SQL concatenation. |
| First persist full, second dirty, execution-level Waiting/Failed lost | **meta always written** | Incremental persist `upsert_execution_meta` writes `exec.state()` whenever `found < exec.revision()`. Execution-level change always bumps revision (`apply` if `effect.changed`). |
| `open_fast` vs `open` hiding FULL bugs | **same `persist_exec`** | Only `PRAGMA synchronous` differs. Defect tests run both opens. |
| Permit leak after resume Waiting then Complete | **Waiting never holds a permit** | `NodeFinished` `release_permit_slot` before apply; Waiting is a finished execute. Restore `held` is all zeros. Complete dispatches successors and takes permits. `waiting_releases_permit_sibling_runs`. |
| Revision 0 reuse / u64 wrap | **impossible in practice** | `Execution::new` revision 0; first `apply` sets 1. Sqlite stores `revision as i64`; wrap would require `> i64::MAX` applies. Reopen loads stored revision; `from_snapshot` keeps it (or +1 if converting Running). Equal revision persist is a no-op, not a clobber. |
| Clock jump **backward** after persist of `runnable_at` | **`at <= now`, not elapsed** | Park and `is_ready_now` compare absolute `Timestamp`. `saturating_duration_since` is only the sleep length. `now.saturating_sub(runnable_at) == 0` is **not** used as due. Constructed: `clock_jump_backward_after_runnable_at_persist_does_not_fire`. |
| Clock jump **forward** over staggered retries | **Timer drain** | `Event::Timer` then `while at <= now { RetryDue }`. Sqlite: `clock_jump_forward_over_staggered_retries_sqlite`. |
| Stale `FinishNode` for attempt N after resume bumped N+1 | **domain no-op** | `finish_node` matches `Running { attempt }` exactly; otherwise `Ok` without bumping revision. Crash drops the old SpawnSet so the Runtime API cannot inject the stale finish. Constructed: `stale_finish_node_after_resume_attempt_bump_is_noop`. |
| Duplicate HITL Complete / mismatched payload after resume | **typed `ConflictingComplete`** | `apply` Resume on Succeeded compares `last_outcome` (restored from output bytes). Sqlite: `duplicate_hitl_complete_after_sqlite_resume`. |
| Cancel vs in-flight vs persist of Running | **persisted Cancelled stays Cancelled** | Crash after Cancelled COMMIT: `cancel_persisted_survives_crash_and_resume`. Crash before cancel persist is at-least-once Running (ADR 0004). Persist is inline; Cancel is the next event after the previous persist returns. |
| Two `resume` + one `start` same id / file | **AlreadyActive per Runtime; start mints a new id; two Runtimes `ClaimedElsewhere`** | `claim_active` insert-before-get. `two_resume_plus_start_same_runtime_already_active`. Store `claim` fences a second Runtime (`two_runtimes_same_file_are_not_fenced`). |
| Persist `Err` on **non-terminal** then crash | **last successful persist** | Watch/inspect can be ahead of disk. Sink does not emit on persist `Err`. Crash without Shutdown resumes the last `Ok` snapshot. A failed persist of `Ready { runnable_at }` leaves Running on disk; resume re-invokes immediately (delay was not durable). Contract: `non_terminal_ready_delay_persist_err_then_crash_skips_uncommitted_delay`. Not persist-before-announce (Phase 1 in-memory wins). |
| `SQLITE_FULL` / `SQLITE_BUSY` during terminal persist + Shutdown | **same `StoreError` as the Shutdown retry** | Wrapper `Err` is the shape. First two terminal persists fail, third succeeds: `transient_terminal_persist_err_twice_shutdown_retries_sqlite`. Permanent fail remains Phase 1. |
| WAL truncated after COMMIT of Succeeded (`FULL`) | **checkpoint after terminal COMMIT** | `persist_exec` `wal_checkpoint(TRUNCATE)` only if `committed && terminal`. Truncating an empty WAL afterwards still reads Succeeded: `wal_truncated_after_succeeded_commit_still_succeeded_full`. Truncating WAL **before** checkpoint would drop the txn — that is destroying committed frames, not process-kill. `open_fast` NORMAL may lose last frames on **power** loss; process-kill after COMMIT recovers on both (`open_and_open_fast_process_kill_after_running_commit_both_reinvoke`). |
| Definition mismatch on resume | **fail closed** | `from_snapshot` hash / missing / unknown node. Poisoning `definitions.body` under the stored hash: `definition_mismatch_on_resume_fail_closed`. |
| Unicode / long NodeIds | **TEXT bind** | `unicode_and_long_node_ids_survive_crash_resume`. No SQL concatenation. |
| Empty Bytes vs 64KiB join inputs after resume | **JSON round-trip; AND-join sees both** | `empty_and_64kib_join_inputs_after_resume`. `inputs_for_slot` includes `Some` empty Bytes. |
| FailSubtree sibling still Running when another page Failed, then crash | **failed page not re-run; reducer AllDone once** | `fail_subtree_sibling_running_crash_keeps_failed_page`. |
| FIFO 64 ready, concurrency 1, crash, resume | **all run; join once** | Resume re-enqueues in **definition order** (`enqueue_dispatchable`), not the original FIFO. Eligibility is unchanged: `fifo_64_ready_concurrency_1_crash_resume_all_run_join_once`. |

## Previous load pack (not a hunt)

Those tests still exist (`just chaos-sqlite`). They did not find this defect.

| Attack | Test |
|---|---|
| 2000 short jobs | `two_thousand_short_jobs_one_file` |
| 256-wide AND-join crash/resume | `wide_256_and_join_crash_resume` |
| 256-wide sqlite-bound resume + concurrent starts | `wide_256_resume_under_concurrent_starts_is_sqlite_bound` |
| 2k-wide Ready crash/resume | `wide_2k_and_join_crash_resume_of_ready` |
| Diamond farm retry+HITL | `diamond_farm_retry_hitl_shuffled_resume` |
| Burst/idle/burst | `burst_idle_burst_crash_resume_sqlite` |
| 1pm/4pm FakeClock AND-join | `and_join_1pm_waiting_4pm_delay_crash_resume` |
| Drop handle mid-persist | `drop_handle_mid_persist_cancels_not_succeed` |
| Two Runtimes one diamond | `two_runtimes_diamond_no_silent_wrong_terminal` |

## Rules

1. Construct the interleaving, write a test you expect to fail, then fix or
   refute from the code.
2. Do not weaken fail-fast or AND-join.
3. Kernel `src/` changes: coverage 100%. No HTTP/Agent. No persist queue.
