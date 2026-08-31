# Sqlite chaos log (standing breaker)

Standing attack log against **one local sqlite file**. Fail-fast
(`OnFailure::FailExecution`) and AND-join (`Join::AllSucceeded`) stay the
library defaults. Two `Runtime`s on one file are **unfenced** (ADR 0004) —
hunt silent wrong terminals, not Phase 7 leases.

When an attack **breaks**: fix production + a named test. Do not weaken
fail-fast or AND-join. Kernel `src/` is not touched here (adapter tests only).

When it **holds**: this file, attack → test name, numbers.

Pack: `cargo test -p keel-rt-sqlite --test chaos -- --test-threads=1 --nocapture`
(CI job `chaos-sqlite`, `just chaos-sqlite`). Not coverage.

The 50-diamond start-crash-resume loop is start/drop bound (~−32% vs a naive
headline). Sqlite-bound proof is large-snapshot resume (idle and under
concurrent `start` on the same file).

## This pass (debug, this machine)

**Broke:** nothing. No production sqlite/kernel change.

**Held:** every named attack below. Join-once, fail-fast, and Cancelled-vs-Succeeded
invariants stayed honest.

| Attack | Result | Test | Number |
|---|---|---|---|
| 2000 sequential 1-node jobs, one file; resume last | held | `two_thousand_short_jobs_one_file` | **2.690 s** (bound 30 s) |
| 256-wide AND-join, hang one worker, crash, resume; join once | held | `wide_256_and_join_crash_resume` | join runs = 1 |
| 256-wide Ready resume idle vs under 32 concurrent starts (sqlite-bound) | held | `wide_256_resume_under_concurrent_starts_is_sqlite_bound` | idle **474 ms**; +32 starts **473 ms** (bound 15 s). Concurrent 1-node starts overlap the snapshot resume; this is not the 50-diamond start/drop loop. |
| 2k-wide Ready snapshot crash/reopen/resume; join once | held | `wide_2k_and_join_crash_resume_of_ready` | **15.993 s** (bound 120 s). Same path as `resume_2k_wide_snapshot_debug_within_bound`. Live hang-one-of-2k is start/drop bound; not used. |
| Two OS threads: 256-wide resume vs start storm; no panic; no silent wrong terminal | held | `two_threads_wide_resume_and_starts_no_wrong_terminal` | — |
| 8 diamonds, retry then HITL Waiting, crash, reverse-order token resume | held | `diamond_farm_retry_hitl_shuffled_resume` | writer runs = 8 |
| Burst 32-wide / idle Waiting gate / crash / resume / wave B | held | `burst_idle_burst_crash_resume_sqlite` | wave B = 32 after token; 0 before |
| 64 KiB + 1-byte payloads, hang, crash, AND-join | held | `mixed_fat_and_tiny_payloads_crash_resume` | bytes round-trip |
| Start-crash-resume storm (24 mixed 1-node/diamond), WAL after terminals | held | `start_crash_resume_storm_one_file` | **150 ms**, WAL **0** (TRUNCATE on terminal; bound 20 s / 8 MiB) |
| 1pm Waiting + 4pm delay (`FakeClock`), crash, token then clock; AND-join writer once | held | `and_join_1pm_waiting_4pm_delay_crash_resume` | writer = 1 after 4pm, 0 at 1pm |
| Drop handle mid persist of Cancel (gated yield) | held | `drop_handle_mid_persist_cancels_not_succeed` | file Cancelled; resume does not succeed |
| Executor panic; sqlite resume stays Failed | held | `executor_panic_sqlite_resume_stays_failed` | Failed |
| Sink panic after sqlite persist; snapshot still Succeeded | held | `sink_panic_during_sqlite_persist_still_durable` | Succeeded |
| Persist panics after COMMIT of terminal; file keeps Succeeded | held | `persist_panic_after_sqlite_commit_keeps_succeeded` | Succeeded; executor not re-run |
| Policy `decide` panic fail-fast; sqlite resume stays Failed | held | `policy_panic_during_sqlite_put_stays_failed` | Failed; no resurrect |
| Two Runtimes, one file, same diamond; unfenced; no writer Succeeded with live pred | held | `two_runtimes_diamond_no_silent_wrong_terminal` | snapshot restores; terminal |

## Already proven (catalog) — still un-weakened

These attacks were already in `docs/RESUME_CATALOG.md` / `keel-rt-sqlite` lib
tests. This pass did not drop them.

| Attack | Test |
|---|---|
| `SQLITE_BUSY` / locked file is typed, not a panic | `locked_file_is_typed_error_not_panic` `persist_under_lock_returns_within_busy_bound` `concurrent_persist_two_executions_same_file_no_panic` |
| Torn / truncated WAL does not invent a terminal | `truncated_wal_does_not_invent_a_terminal` `crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` |
| WAL `TRUNCATE` only after COMMIT of a terminal | `checkpoint_runs_only_after_commit_of_terminal` `two_hundred_start_crash_resume_wal_bounded` |
| Two Runtimes, one file, no process fence | `two_runtimes_same_file_are_not_fenced` |
| Default `synchronous=FULL`; `open_fast` is NORMAL | `open_default_is_synchronous_full` `resume_256_wide_full_vs_normal` |

## Rules for the next loop

1. Add a named attack, run it, record hold/break + a number.
2. If it breaks: production fix in `keel-rt-sqlite` (or kernel, with 100%
   `src/` coverage). Do not donate fail-fast or AND-join.
3. No HTTP/Agent in kernel. No persist queue (ADR 0001). Failpoints stay out
   of the scheduler (ADR 0003). `PersistGate` is test-only.
4. Crash recipe stays `mem::forget(handle)` + drop Runtime + drop tokio +
   reopen file. Ban `mem::forget` in kernel `src/`.
