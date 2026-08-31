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
| Fat `Bytes` snapshot | MemoryStore refcount; sqlite JSON copy preserves bytes | `test: fat_bytes_resume_join_is_refcount` `test: fat_bytes_sqlite_round_trip_preserves_bytes` `test: fat_bytes_join_input_is_refcount_not_copy` |
| Sqlite busy / locked / truncated / empty | typed `StoreError` or empty db + unknown id; no panic | `test: locked_file_is_typed_error_not_panic` `test: truncated_file_is_typed_error` `test: empty_file_opens_as_new_store` `test: corrupt_snapshot_json_is_typed_error` |
| `resume` unknown id | `ResumeError::UnknownExecution` | `test: resume_unknown_id_is_unknown_execution` `test: resume_unknown_id_on_file` |
| MemoryStore after “process death” | new store has no history | `test: memory_store_does_not_survive_process_death` |
| 50 sequential start-crash-resume diamonds, one file | no leaked handles / sqlite locks | `test: fifty_start_crash_resume_diamonds_one_file` |
| Crash/resume same diamond N times | attempt climbs only on the in-flight node | `test: crash_resume_same_diamond_attempt_climbs_only_on_inflight` |

## Stress (CI `stress-resume`)

| Shape | Bound | Proof |
|---|---|---|
| Resume 256-wide Ready snapshot (debug, n=3) | 30s per sample; median recorded | `test: resume_256_wide_snapshot_within_bound` |
| 1000 sequential 1-node DAGs, resume last | 90s | `test: one_thousand_sequential_dags_resume_last` |
| 50 start-crash-resume diamonds, one file | 60s | `test: fifty_start_crash_resume_diamonds_one_file` |
| Same diamond crash/resume N=8 | attempt climbs; src not re-run | `test: crash_resume_same_diamond_attempt_climbs_only_on_inflight` |
| 10k-node snapshot resume | not gated (debug budget); like `stress_100k` | not in default / `stress-resume` |

Numbers: [`benches/BASELINE.md`](../benches/BASELINE.md) (sqlite vs MemoryStore, labeled).

## Documented no-fence

`AlreadyActive` is **per Runtime**. Two processes (or two `Runtime`s) on one
sqlite file can both `resume` and both re-invoke a Running node. CAS rejects
a stale `put`; the loser’s in-memory apply is not rolled back (persist fail
skips emit). Callers that need a lease do it outside the kernel. ADR 0004.

- Default join is `Join::AllSucceeded`. Default `OnFailure` is `FailExecution`.
- Waiting is a node state. Retry delay is `Ready { runnable_at }`.
- Persist succeeds, then the sink is told. No persist queue (ADR 0001).
