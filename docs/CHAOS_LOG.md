# Sqlite chaos log (standing breaker)

Standing attack log against **one local sqlite file**. Fail-fast and AND-join
stay the library defaults. Two `Runtime`s on one file are unfenced (ADR 0004).

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

## Hunt refutations (from the code, not a passing stress table)

| Suspicion | Verdict | Why it cannot happen / what it is |
|---|---|---|
| Dirty set incomplete (`runnable_at`, token, attempt) | **impossible without `set_state` skip** | Every node mutation that must hit the file goes through `set_state` → `mark_dirty`, or mutates fields then `set_state` on that slot (`dispatch_node` attempt/token, retry `last_error`, Waiting token). `remain[]` is **not** persisted; `from_snapshot` rebuilds it from definition join + node states (`remain_for`). |
| `Join::AllDone` vs `AllSucceeded` restored wrong | **impossible if definition bytes match** | Join lives on `WorkflowDefinition`, stored beside the snapshot (`INSERT OR IGNORE` by `content_hash`). Restore uses `definition.join_at`, not the snapshot. Hash mismatch is `DefinitionHashMismatch`. Same hash ⇒ same `durable_bytes` (joins included). |
| Persist-before-announce lie (sink) | **held** | `persist_then_emit` takes events, persist, emit only on `Ok`. Catalog: `persist_succeeds_before_execution_succeeded_is_emitted`. **Watch** still updates in `apply_cmd_result` before persist (live `wait` can return before the file). After this fix, **clean Shutdown** retries so the file catches up when persist can succeed. |
| `AlreadyActive` TOCTOU two apply tasks | **impossible on one Runtime** | `claim_active` is `HashSet::insert` under the mutex **before** `store.get` / spawn. Second `resume` gets `AlreadyActive`. Failed `spawn_resume` drops `ActiveGuard` and unregisters. Two Runtimes are the documented no-fence. |
| WAL TRUNCATE vs in-flight `put` | **same connection, after COMMIT** | `checkpoint_wal` runs only if `committed && terminal` on that connection, `debug_assert!(is_autocommit)`. Another `SqliteStore` is another connection; `TRUNCATE` of in-flight frames is sqlite’s busy/locked, mapped to `StoreError`, not an empty resume (`truncated_wal_does_not_invent_a_terminal`). |
| Crash between Running persist and cancel persist | **documented at-least-once** | Catalog: crash while Running re-invokes. Drop is Cancel **if Shutdown/Cancel persist**. A **process kill** before cancel persist leaves Running — not a Drop-cancel. The bug above was clean Drop + persist `Err`, not crash. |
| Attempt not bumped on sqlite resume of Running | **impossible** | `from_snapshot` converts Running → Ready with `reinvoke: false`; `dispatch_node` does `attempt += 1`. Stale `FinishNode` for attempt 1 is a no-op (`finish_node` match). `crash_during_b_running_reinvokes_b_not_a` asserts attempt 2. |
| Resume with a different definition, same hash | **fail-closed or identical DAG** | Non-empty `definition_hash` must equal `content_hash()`. Collision of SHA body is identical `durable_bytes`. Empty hash is only `Default` / unhashed; sqlite persist always writes `content_hash()`. |
| Clock: SystemClock after FakeClock deadlines | **caller clock port** | `runnable_at` is an opaque `Timestamp`. Resume uses the **new** Runtime’s `Clock`. Mixing FakeClock(0) deadlines with `SystemClock` fires immediately (`at <= now`). Not a sqlite restore bug; do not mix clocks. |
| Id serde `/` `.` unicode | **round-trip as TEXT** | `NodeId` / `ExecutionId` serialize as strings; sqlite binds `?1` TEXT. `ExecutionId::parse` rejects only empty. No SQL concatenation. |
| First persist full, second dirty, execution-level Waiting/Failed lost | **meta always written** | Incremental persist `upsert_execution_meta` writes `exec.state()` whenever `found < exec.revision()`. Execution-level change always bumps revision (`apply` if `effect.changed`). |
| `open_fast` vs `open` hiding FULL bugs | **same `persist_exec`** | Only `PRAGMA synchronous` differs. Defect tests run both opens. |
| Permit leak after resume Waiting then Complete | **Waiting never holds a permit** | `NodeFinished` `release_permit_slot` before apply; Waiting is a finished execute. Restore `held` is all zeros. Complete dispatches successors and takes permits. `waiting_releases_permit_sibling_runs`. |
| Revision 0 reuse / u64 wrap | **impossible in practice** | `Execution::new` revision 0; first `apply` sets 1. Sqlite stores `revision as i64`; wrap would require `> i64::MAX` applies. Reopen loads stored revision; `from_snapshot` keeps it (or +1 if converting Running). Equal revision persist is a no-op, not a clobber. |

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
