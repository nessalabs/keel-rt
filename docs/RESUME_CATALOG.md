# Phase 2 resume / persist catalog

Every crash, CAS, fence, file-error, and resume-error case. Rows are the
production contract. **Zero MISSING.**

Hunt: inventory existing packs first, then re-derive. A pack test that does
not prove the claim is not coverage. Recovery tests use a **real sqlite
file**. MemoryStore proves only that it does **not** survive process death.

Process resilience (crash / forget handle) is not graph resilience (drop
handle = cancel; fail-fast stays Failed). Do not reverse fail-fast.

| column | meaning |
|---|---|
| Trigger | How a caller or port produces the case |
| Observable | Execution / node state, `ResumeError` / `StoreError` |
| Test | Named regression that fails without the handling |
| Handling | typed error / re-invoke / no-op / documented no-fence |

Kernel `src/` line coverage stays 100% (`coverage/BASELINE` empty allowlist).
Sqlite adapter lines are not kernel `src/`.

## Hunt

| Production case | Verdict | Proof |
|---|---|---|
| Crash, 1 of N parallel nodes Running; resume; AND-join once | writer runs once; succeeded side skipped | `test: crash_diamond_join_runs_writer_once` `test: resume_diamond_join_waits_for_both_sides` |
| Crash during Waiting | same `ResumeToken`; node does not re-run | `test: crash_while_waiting_keeps_the_same_token` `test: resume_keeps_waiting_token` |
| Crash during retry delay | stays `Ready { runnable_at }`; FakeClock must advance | `test: crash_during_retry_delay_does_not_fire_early` `test: resume_retry_ready_does_not_fire_before_deadline` `test: start_timeout_retry_crash_during_backoff_resume_advance_succeeds` `test: crash_resume_full_file_keeps_deadline` |
| Crash after timeout persisted, before retry dispatch | T still on snapshot; resume does not double-run | `test: crash_after_timeout_persisted_before_dispatch_does_not_double_run` `test: crash_after_accept_timeout_persisted_resume_stays_timed_out` |
| Backoff retry survives crash | same attempt policy; no extra attempts | `test: backoff_retry_survives_crash_same_attempt_policy` |
| Cancel / Drop during parked deadline | Cancelled; FakeClock sleeper dropped | `test: cancel_during_parked_deadline_is_cancelled_sleeper_dropped` `test: drop_handle_during_parked_deadline_cancels_sleeper` `test: cancel_ready_with_future_deadline_does_not_start_later` `test: cancel_while_drive_waits_on_future_t_drops_waiter` |
| Hung `wait_until` / `Timestamp::MAX` | hang bound or inbox cancel still terminates; no `thread::sleep` | `test: hung_wait_until_hang_bound_still_cancels` `test: timestamp_max_deadline_cancel_returns_without_thread_sleep` |
| Persist of deadline drop / double-fire | T round-trips; `last_error` round-trips; due-on-resume dispatches once | `test: sqlite_deadline_persist_does_not_drop_or_double_fire` `test: persisted_deadline_already_due_on_resume_runs_once_not_twice` `test: incremental_persist_does_not_drop_runnable_at` `test: persist_resume_256_runnable_at_set_vs_unset` `test: parked_ready_t_uses_column_omits_nested_json_keeps_last_error` `test: dirty_persist_ready_t_to_t_prime_updates_only_runnable_at` |
| FailSubtree + parked sibling | sibling keeps `Ready { T }`; execution stays live | `test: fail_subtree_parked_sibling_keeps_deadline` `test: fail_subtree_parked_sibling_not_in_subtree_stays_parked` |
| StartNode on due T | `ApplyError::Illegal`; RetryDue first | `test: start_node_on_due_t_is_illegal_without_retry_due` |
| Cancel vs due T same instant | Cancel in inbox beats Timer | `test: cancel_when_deadline_already_due_does_not_dispatch` `test: due_t_hung_wait_until_inbox_cancel_does_not_dispatch` |
| 256 parked crash-resume | one advance, each fires once | `test: wide_256_parked_advance_once_each_fires_once` `test: crash_resume_256_parked_advance_once_each_once` |
| Two Runtimes, parked T | still unfenced | `test: two_runtimes_parked_deadline_are_not_fenced` |
| Crash during timeout (Running + FakeClock Delay) | re-invoke; advance FakeClock → TimedOut | `test: start_arm_timeout_crash_before_fire_resume_advance_is_timed_out` |
| Crash after terminal persist | terminal kept; succeeded/failed nodes do not re-run | `test: process_restart_is_new_runtime_same_file` `test: crash_after_fail_fast_stays_failed` |
| `resume` while first resume still live, concurrent | one `Ok`, one `AlreadyActive` | `test: concurrent_resume_same_id_one_already_active` `test: concurrent_resume_same_runtime_one_already_active` `test: resume_of_live_start_is_already_active` `test: resume_twice_live_is_already_active` |
| Two Runtimes, one sqlite file | **no process fence**; both may re-invoke. CAS: stale put loses | `test: two_runtimes_same_file_are_not_fenced` `test: stale_put_does_not_clobber` `test: stale_put_loses_on_memory_store` |
| Persist CAS then kill before event | terminal not lost; executor not re-run; event may be absent | `test: crash_after_terminal_cas_before_emit_keeps_terminal` `test: persist_cas_then_drop_runtime_resume_keeps_terminal` `test: persist_panic_after_write_keeps_terminal_and_does_not_emit` `test: persist_succeeds_before_execution_succeeded_is_emitted` |
| Persist `Err` then later persist `Ok` of the same snapshot | sink gets the events for that snapshot (Started not dropped); sqlite event rows on shutdown flush | `test: persist_err_then_ok_emits_events_for_the_durable_snapshot` `test: transient_terminal_persist_err_shutdown_still_emits_execution_succeeded` `test: cancel_persist_err_then_shutdown_emits_execution_cancelled` `test: transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded` |
| sqlite event rows in the snapshot txn | rows exist; resume still snapshot-only | `test: event_rows_in_snapshot_txn_are_not_used_for_resume` `test: resume_reinvoke_emits_node_started_again` |
| Equal-revision `persist_with_events` (COMMIT then checkpoint busy, or second writer) | event row count unchanged; snapshot stays | `test: equal_revision_persist_does_not_grow_event_rows` `test: checkpoint_busy_after_terminal_commit_does_not_duplicate_events` |
| Resume of Waiting / same snapshot revision | no extra persist; event rows unchanged | `test: waiting_resume_does_not_append_event_rows` `test: resume_waiting_does_not_repersist_same_revision` |
| Transient persist `Err` of last apply (terminal or Drop-cancel), then clean Shutdown | file matches in-memory terminal; resume does not re-invoke | `test: transient_terminal_persist_err_shutdown_flushes_succeeded` `test: transient_cancel_persist_err_shutdown_flushes_cancelled` `test: transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded` `test: transient_cancel_persist_err_shutdown_flushes_sqlite_cancelled` `test: transient_terminal_persist_err_twice_shutdown_retries_until_ok` `test: transient_terminal_persist_err_twice_shutdown_retries_sqlite` |
| Seeded process-kill crash-inject (small DAGs, FakeClock, sqlite reopen) | recovery contract; seed printed on fail | `test: randomized_crash_inject_sqlite` |
| Schema / definition mismatch | fail-closed `Snapshot` / `DefinitionMissing` | `test: resume_schema_mismatch_is_snapshot_error` `test: schema_version_in_json_fail_closed` `test: resume_without_definition_is_definition_missing` |
| FailSubtree + AllDone crash mid-fanout | failed pages stay failed; reducer AllDone once | `test: crash_fail_subtree_pages_stay_failed` `test: resume_fail_subtree_keeps_failed_pages_and_runs_reducer` `test: fail_subtree_all_done_resume_runs_reducer_once` |
| Fail-fast crash after Failed persist | resume stays Failed; no resurrect | `test: crash_after_fail_fast_stays_failed` `test: resume_failed_execution_stays_failed` |
| `resume_with(RetryFailed)` after fail-fast diamond | Failed node re-invoked; Succeeded not; Cancelled → Pending then Ready | `test: resume_stays_failed_resume_with_retry_failed_reruns_b_only` `test: retry_failed_diamond_reruns_b_uncancels_c_d_keeps_a` |
| `resume_with(RetryFailed)` FailSubtree + AllDone | failed page retried; succeeded page not; Succeeded AllDone join re-runs once and sees p1's new Bytes | `test: resume_with_retry_failed_fail_subtree_all_done_retries_failed_page` |
| `resume_with(RetryFailed)` FailSubtree + AllDone join itself Failed | join stays Pending (remain 1) until retried pred Succeeded; then runs once and sees new Bytes | `test: resume_with_retry_failed_failed_all_done_join_waits_for_retried_pred` `test: retry_failed_all_done_failed_join_remain_waits_for_retried_pred` |
| `resume_with(RetryFailed)` on Succeeded / Waiting / Cancelled | `ResumeError::NotFailed` (`ApplyError::Illegal` only) | `test: resume_with_retry_failed_on_succeeded_is_not_failed` `test: resume_with_retry_failed_on_waiting_is_not_failed` `test: resume_with_retry_failed_on_cancelled_is_not_failed` |
| `start` after Failed | new `ExecutionId`; all nodes run | `test: start_after_failed_is_new_id_and_reruns_all_nodes` |
| Live handle + RetryFailed | `AlreadyActive` | `test: hitl_live_handle_retry_failed_is_already_active` |
| Waiting after drop handle | RetryFailed is `NotFailed`; Continue keeps token; Complete works | `test: resume_with_retry_failed_on_waiting_then_continue_keeps_token` |
| RetryFailed recover persist then kill before StartNode | store get is Ready-now (not Failed); Continue re-invokes | `test: retry_failed_recover_persist_then_kill_before_startnode_continue_reinvokes` |
| RetryFailed FailSubtree AllDone map-reduce × many, kill before StartNode | each get() is p1 Ready-now / join Pending; Continue re-invokes p1 + join | `test: retry_failed_all_done_map_reduce_recover_persist_kill_before_startnode_many` |
| RetryFailed fail-fast diamond × N | Failed critic retried; Succeeded research not; Cancelled writer runs | `test: retry_failed_fail_fast_diamond_times_n` |
| RetryFailed persist `Err` after apply | on-disk stays Failed; next `resume_with(RetryFailed)` works | `test: resume_with_retry_failed_persist_err_leaves_failed_then_retry_works` |
| RetryFailed resets `RetryPolicy` budget | attempt 0; dispatch 1; max_attempts is a new budget | `test: resume_with_retry_failed_resets_retry_policy_budget` |
| Adversarial RetryFailed | failed leaf re-run; persist-then-emit | `test: resume_with_retry_failed_reruns_failed_leaf_not_succeeded` |
| Crash after Running persist | file reopens (no leaked lock); Running re-invoked | `test: crash_after_running_persist_releases_lock_and_reinvokes` `test: crash_during_b_running_reinvokes_b_not_a` |
| Drop handle after Running persist | **graph** cancel; resume stays Cancelled (not crash) | `test: drop_handle_after_running_persist_cancels_not_reinvoke` |
| Fat `Bytes` snapshot | MemoryStore refcount; sqlite JSON copy preserves bytes | `test: fat_bytes_resume_join_is_refcount` `test: fat_bytes_sqlite_round_trip_preserves_bytes` `test: fat_payloads_64kib_times_eight_persist_resume` `test: fat_payloads_64kib_times_32_persist_resume_within_bound` `test: fat_bytes_join_input_is_refcount_not_copy` |
| Sqlite busy / locked / truncated / empty | typed `StoreError` or empty db + unknown id; no panic | `test: locked_file_is_typed_error_not_panic` `test: persist_under_lock_returns_within_busy_bound` `test: truncated_file_is_typed_error` `test: empty_file_opens_as_new_store` `test: corrupt_snapshot_json_is_typed_error` |
| Torn WAL / crash mid-`put` (uncommitted BEGIN) | typed error or successful recover; never a silent wrong terminal | `test: truncated_wal_does_not_invent_a_terminal` `test: crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` |
| Incremental persist after first write | Pending nodes stay in the file (dirty list is not the whole graph) | `test: incremental_persist_keeps_pending_nodes` `test: incremental_persist_does_not_delete_unchanged_rows` `test: incremental_persist_256_wide_succeeded_does_not_drop_pending` |
| Two persist calls / kill before second COMMIT | first snapshot kept; second turn not merged into one txn | `test: second_uncommitted_persist_does_not_merge_into_first_commit` `test: crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` `test: crash_after_terminal_cas_before_emit_keeps_terminal` |
| WAL checkpoint TRUNCATE | after COMMIT of a terminal only; autocommit; checkpoint Err does not fail persist | `test: checkpoint_runs_only_after_commit_of_terminal` `test: two_hundred_start_crash_resume_wal_bounded` `test: checkpoint_busy_after_terminal_commit_does_not_duplicate_events` `test: checkpoint_busy_after_commit_is_persist_ok_and_resume_sees_succeeded` `test: equal_revision_persist_does_not_grow_event_rows` |
| CAS after incremental rows | stale put loses | `test: stale_put_after_incremental_dirty_rows_loses` `test: stale_put_does_not_clobber` |
| `synchronous=FULL` default vs `open_fast` NORMAL | FULL is default; both recover process-kill | `test: open_default_is_synchronous_full` `test: open_fast_is_synchronous_normal` `test: durable_is_synchronous_full` `test: resume_256_wide_full_vs_normal` |
| Two connections persist two executions, one file | no panic; `Ok` ⇒ row exists; `SQLITE_BUSY` is typed | `test: concurrent_persist_two_executions_same_file_no_panic` |
| Hourglass neck Running crash/resume | sources not re-run; sinks run after neck | `test: crash_hourglass_neck_running_resume_runs_sinks_not_sources` `test: crash_resume_hourglass_256_within_bound` |
| `resume` unknown id | `ResumeError::UnknownExecution` | `test: resume_unknown_id_is_unknown_execution` `test: resume_unknown_id_on_file` |
| MemoryStore after “process death” | new store has no history | `test: memory_store_does_not_survive_process_death` |
| 50–200 sequential start-crash-resume diamonds, one file | no leaked locks; WAL checkpointed (`TRUNCATE` on terminal) | `test: fifty_start_crash_resume_diamonds_one_file` `test: two_hundred_start_crash_resume_wal_bounded` |
| Crash/resume same diamond N times | attempt climbs only on the in-flight node | `test: crash_resume_same_diamond_attempt_climbs_only_on_inflight` `test: twenty_diamond_repeat_crash_resume_within_bound` |

## Stress (CI `stress-resume`)

| Shape | Bound | Proof |
|---|---|---|
| Resume 256-wide Ready snapshot (debug, n=3) | 8s per sample; FULL vs NORMAL recorded | `test: resume_256_wide_snapshot_within_bound` `test: resume_256_wide_full_vs_normal` |
| Persist 256-wide first + incremental Start | 8s | `test: persist_256_wide_apply_within_bound` |
| Hourglass-256 crash/resume | 20s | `test: crash_resume_hourglass_256_within_bound` |
| 1000 sequential 1-node DAGs, resume last | 30s | `test: one_thousand_sequential_dags_resume_last` |
| 50 start-crash-resume diamonds, one file | 10s; WAL < 8 MiB | `test: fifty_start_crash_resume_diamonds_one_file` |
| 200 start-crash-resume diamonds, one file | 40s; WAL bounded | `test: two_hundred_start_crash_resume_wal_bounded` |
| Same diamond crash/resume N=8 / N=20 | attempt climbs; src not re-run | `test: crash_resume_same_diamond_attempt_climbs_only_on_inflight` `test: twenty_diamond_repeat_crash_resume_within_bound` |
| Fat 64KiB × 32 persist+resume | 10s | `test: fat_payloads_64kib_times_32_persist_resume_within_bound` |
| 2k-wide snapshot resume (debug) | 120s | `test: resume_2k_wide_snapshot_debug_within_bound` |
| 10k-wide snapshot resume | release only (debug ~350 s extrapolated; 2k debug is 14 s) | `test: resume_10k_wide_snapshot_release_within_bound` |

Numbers: [`benches/BASELINE.md`](../benches/BASELINE.md) (sqlite vs MemoryStore, labeled).
Standing high-load / messy-user attacks (not coverage): [`docs/CHAOS_LOG.md`](CHAOS_LOG.md).

## Documented no-fence

`AlreadyActive` is **per Runtime**. Two processes (or two `Runtime`s) on one
sqlite file can both `resume` and both re-invoke a Running node. CAS rejects
a stale `put`; the loser’s in-memory apply is not rolled back (persist fail
skips emit). Callers that need a lease do it outside the kernel. ADR 0004.

- Default join is `Join::AllSucceeded`. Default `OnFailure` is `FailExecution`.
- Waiting is a node state. Retry delay is `Ready { runnable_at }` (snapshot `Timestamp` T).
- Persist succeeds, then the sink is told. No persist queue (ADR 0001).
- Kernel has no cron, no wall timezone, no sqlite timer table. Waiting for T is the Runtime drive (`Clock::wait_until`), not the scheduler.
- sqlite parked Ready{T} JSON keeps a short `last_error` so inspect agrees with MemoryStore. Old adapters that ignore `nodes.runnable_at` would see Ready-now.
