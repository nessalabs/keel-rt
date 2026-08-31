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
| Crash during retry delay | stays `Ready { runnable_at }`; FakeClock must advance | `test: crash_during_retry_delay_does_not_fire_early` `test: resume_retry_ready_does_not_fire_before_deadline` |
| Crash after terminal persist | terminal kept; succeeded/failed nodes do not re-run | `test: process_restart_is_new_runtime_same_file` `test: crash_after_fail_fast_stays_failed` |
| `resume` while first resume still live, concurrent | one `Ok`, one `AlreadyActive` | `test: concurrent_resume_same_id_one_already_active` `test: concurrent_resume_same_runtime_one_already_active` `test: resume_of_live_start_is_already_active` `test: resume_twice_live_is_already_active` |
| Two Runtimes, one sqlite file | **no process fence**; both may re-invoke. CAS: stale put loses | `test: two_runtimes_same_file_are_not_fenced` `test: stale_put_does_not_clobber` `test: stale_put_loses_on_memory_store` |
| Persist CAS then kill before event | terminal not lost; executor not re-run; event may be absent | `test: crash_after_terminal_cas_before_emit_keeps_terminal` `test: persist_cas_then_drop_runtime_resume_keeps_terminal` `test: persist_panic_after_write_keeps_terminal_and_does_not_emit` `test: persist_succeeds_before_execution_succeeded_is_emitted` |
| Schema / definition mismatch | fail-closed `Snapshot` / `DefinitionMissing` | `test: resume_schema_mismatch_is_snapshot_error` `test: schema_version_in_json_fail_closed` `test: resume_without_definition_is_definition_missing` |
| FailSubtree + AllDone crash mid-fanout | failed pages stay failed; reducer AllDone once | `test: crash_fail_subtree_pages_stay_failed` `test: resume_fail_subtree_keeps_failed_pages_and_runs_reducer` `test: fail_subtree_all_done_resume_runs_reducer_once` |
| Fail-fast crash after Failed persist | resume stays Failed; no resurrect | `test: crash_after_fail_fast_stays_failed` `test: resume_failed_execution_stays_failed` |
| Crash after Running persist | file reopens (no leaked lock); Running re-invoked | `test: crash_after_running_persist_releases_lock_and_reinvokes` `test: crash_during_b_running_reinvokes_b_not_a` |
| Drop handle after Running persist | **graph** cancel; resume stays Cancelled (not crash) | `test: drop_handle_after_running_persist_cancels_not_reinvoke` |
| Fat `Bytes` snapshot | MemoryStore refcount; sqlite JSON copy preserves bytes | `test: fat_bytes_resume_join_is_refcount` `test: fat_bytes_sqlite_round_trip_preserves_bytes` `test: fat_payloads_64kib_times_eight_persist_resume` `test: fat_payloads_64kib_times_32_persist_resume_within_bound` `test: fat_bytes_join_input_is_refcount_not_copy` |
| Sqlite busy / locked / truncated / empty | typed `StoreError` or empty db + unknown id; no panic | `test: locked_file_is_typed_error_not_panic` `test: persist_under_lock_returns_within_busy_bound` `test: truncated_file_is_typed_error` `test: empty_file_opens_as_new_store` `test: corrupt_snapshot_json_is_typed_error` |
| Torn WAL / crash mid-`put` (uncommitted BEGIN) | typed error or successful recover; never a silent wrong terminal | `test: truncated_wal_does_not_invent_a_terminal` `test: crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` |
| Incremental persist after first write | Pending nodes stay in the file (dirty list is not the whole graph) | `test: incremental_persist_keeps_pending_nodes` `test: incremental_persist_does_not_delete_unchanged_rows` `test: incremental_persist_256_wide_succeeded_does_not_drop_pending` |
| Two persist calls / kill before second COMMIT | first snapshot kept; second turn not merged into one txn | `test: second_uncommitted_persist_does_not_merge_into_first_commit` `test: crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` `test: crash_after_terminal_cas_before_emit_keeps_terminal` |
| WAL checkpoint TRUNCATE | after COMMIT of a terminal only; autocommit | `test: checkpoint_runs_only_after_commit_of_terminal` `test: two_hundred_start_crash_resume_wal_bounded` |
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

## Documented no-fence

`AlreadyActive` is **per Runtime**. Two processes (or two `Runtime`s) on one
sqlite file can both `resume` and both re-invoke a Running node. CAS rejects
a stale `put`; the loser’s in-memory apply is not rolled back (persist fail
skips emit). Callers that need a lease do it outside the kernel. ADR 0004.

- Default join is `Join::AllSucceeded`. Default `OnFailure` is `FailExecution`.
- Waiting is a node state. Retry delay is `Ready { runnable_at }`.
- Persist succeeds, then the sink is told. No persist queue (ADR 0001).
