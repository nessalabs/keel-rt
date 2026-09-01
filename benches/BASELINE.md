# Kernel microbench baseline (this run)

Machine: Cloud Agent VM (x86_64, 4× Intel Xeon). Profile: `cargo test` (debug), Tokio `current_thread`,
`--test-threads=1`. ScriptedExecutor succeed-immediately (zero user work).
Median of 7 iterations unless noted.

## Phase 5 snapshot deadlines (2026-09-01)

T is `Ready { runnable_at: Some(T) }` on the snapshot. FakeClock park; no
sqlite timer table. MemoryStore hot path **without timers** (this pack does
not change persist). Gate: no median >10% vs the persist-err hunt column.

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.328 ms | 4.360 ms | +0.7% |
| deep_chain_128 (debug median n=7) | 1.941 ms | 1.931 ms | −0.5% |
| diamond_10k (debug median n=7) | 154.715 ms | 154.976 ms | +0.2% |
| apply_only (debug median n=7) | 14.815 ms | 14.831 ms | +0.1% |

All four inside the 10% band. **No revert.**

## Phase 3 events re-measure (2026-08-31)

`Event` + `EventSink` (no EventLog). persist_then_emit is store Ok then sink.
MemoryStore still ignores the event slice (`persist_with_events` default →
`persist`). Gate: no MemoryStore median >10% vs the Phase 2 resume column.

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.367 ms | 4.383 ms | +0.4% |
| deep_chain_128 (debug median n=7) | 2.033 ms | 1.932 ms | −5.0% |
| diamond_10k (debug median n=7) | 162.417 ms | 154.809 ms | −4.7% |
| apply_only (debug median n=7) | 14.534 ms | 14.898 ms | +2.5% |

All four inside the 10% band. **No revert.** Sqlite event rows are adapter-only
and are not this gate.

## Persist-err event hunt (2026-09-01)

Keep `pending_events` until persist `Ok`. Success persist path unchanged.
Gate: no MemoryStore median >10% vs the Phase 3 events column.

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.383 ms | 4.328 ms | −1.3% |
| deep_chain_128 (debug median n=7) | 1.932 ms | 1.941 ms | +0.5% |
| diamond_10k (debug median n=7) | 154.809 ms | 154.715 ms | −0.1% |
| apply_only (debug median n=7) | 14.898 ms | 14.815 ms | −0.6% |

All four inside the 10% band. **No revert.**

Allocator experiment (not this debug gate): [`JEMALLOC.md`](JEMALLOC.md).
This crate does not pick an allocator and has no `jemalloc` feature.
current_thread sys vs jemalloc was noise — not recommended.

## Sqlite persist/resume ≥50% (2026-08-31)

WAL + one `BEGIN IMMEDIATE` per persist; after the first write only dirty
node rows are upserted (`Execution::dirty_nodes`). Default open is
`synchronous=FULL` (process kill **and** power loss of the last txn).
`open_fast` is `NORMAL` (process kill only). Terminal persist
`wal_checkpoint(TRUNCATE)` **after COMMIT**. Schema: `executions` + `nodes` +
`definitions`. Architecture after = before (sqlite stays a sibling crate).
MemoryStore still uses `dirty_slots` — not this path.

**Before** (this machine, commit before the sqlite rewrite; debug):

| bench | before | bound then |
|---|---:|---:|
| sqlite 256-wide resume (debug median n=3) | **1.008 s** (984 / 1008 / 1082 ms) | 30 s |
| sqlite 50 diamond crash-resume | **424 ms** | 60 s |
| sqlite 1000 sequential last-resume | ~201 µs (pack ~6 s) | 90 s |

Earlier reports on this branch: 963 ms / 1.689 s for 256-wide. The 50%
target uses **this machine, this commit** (1.008 s → ≤ ~504 ms).

**After** (same VM, `cargo test -p keel-rt-sqlite --test resume_stress -- --test-threads=1 --nocapture`):

| bench | after | vs before | CI bound |
|---|---:|---:|---:|
| sqlite 256-wide resume FULL default (debug median n=3) | **421 ms** (411 / 421 / 456 ms) | **−58%** | 8 s |
| sqlite 256-wide resume NORMAL `open_fast` (debug median n=3) | 259 ms (258 / 259 / 262 ms) | −74% | 8 s |
| sqlite 50 diamond crash-resume | 288 ms (WAL 0 B after terminals) | −32% | 10 s |
| sqlite persist 256-wide first snapshot | 3.8 ms | (new) | 8 s |
| sqlite persist 256-wide incremental Start | 2.4 ms | (new) | 8 s |
| sqlite hourglass-256 crash-resume | 770 ms | (new) | 20 s |
| sqlite 20 diamond-repeat crash-resume | 25 ms | (new) | 40 s |
| sqlite fat 64KiB × 32 persist+resume | 311 ms | (new) | 10 s |
| sqlite 200 start-crash-resume, one file | 1.25 s (WAL end/peak 0 B) | (new) | 40 s |
| sqlite 1000 sequential last-resume | 276 µs | | 30 s |
| sqlite 2k-wide snapshot resume (debug) | 14.0 s | (new; CI gated) | 120 s |
| sqlite 10k-wide snapshot resume (release) | 23.8 s | debug 10k would be ~350 s (quadratic load of all rows; not in 60 s) | 60 s release-only |

256-wide resume **meets ≥50%** under both `synchronous=FULL` (default) and
`NORMAL` (`open_fast`). 50-diamond is a smaller win because the loop is
mostly start/crash/runtime drop, not snapshot JSON.

**NORMAL vs FULL** (this machine, debug median n=3, 256-wide resume):

| sync | median | vs 1.008 s pre-opt | default? |
|---|---:|---:|---|
| FULL (`SqliteStore::open`) | **421 ms** | **−58%** (≤504 ms) | **yes** |
| NORMAL (`SqliteStore::open_fast`) | 259 ms | −74% | opt-in speed |

FULL still beats half of 1.008 s, so the default is FULL. Process kill after
COMMIT recovers on both. Power loss of the last txn: FULL keeps it; NORMAL
may drop last WAL frames.

MemoryStore hot path after this durability pass (n=7) vs RAII 4.237 / 1.940 /
157.509 / 14.366 ms: **4.300 / 1.894 / 154.253 / 14.534 ms** (+1.5% / −2.4% /
−2.1% / +1.2%). All ≤10%. **No revert.**

Crash pack still green: uncommitted mid-`put` rolls back (no invented
terminal), torn WAL does not invent a terminal, `SQLITE_BUSY` returns a
typed `StoreError` in <500 ms with a 50 ms busy timeout, two Runtimes on
one file stay unfenced, incremental persist keeps Pending nodes (including
256-wide one-Succeeded / 256-Pending kill-resume).

When a caller resumes a 256-wide sqlite snapshot with the default FULL
store, it used to take **1.008 s** debug median; now **421 ms**. `open_fast`
(NORMAL) is **259 ms** if the caller accepts power-loss of the last txn.

## Phase 2 resume re-measure (2026-08-31)

Persist-before-emit, CAS, `Runtime::resume`. Definition hash is lazy
(`OnceLock`); MemoryStore first persist does not serialize the DAG.
Gate: no MemoryStore median >10% vs RAII column.

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.237 ms | 4.367 ms | +3.1% |
| deep_chain_128 (debug median n=7) | 1.940 ms | 2.033 ms | +4.8% |
| diamond_10k (debug median n=7) | 157.509 ms | 162.417 ms | +3.1% |
| apply_only (debug median n=7) | 14.366 ms | 14.534 ms | +1.2% |

All four inside the 10% band. **No revert.** Same machine class as Phase 1
`main` (4.324 / 1.956 / 160.279 / 14.503 ms).

Sqlite resume of a 256-wide Ready snapshot (debug, n=3, file adapter, not
the MemoryStore gate): median **1.689 s**. Measured separately; do not tax
the in-memory path.

## Phase 2 resume stress re-measure (2026-08-31)

Same VM class. `cargo test -p keel-rt-sqlite --test resume_stress -- --test-threads=1 --nocapture`.
Not the MemoryStore 10% gate.

| bench | this run | bound |
|---|---:|---|
| sqlite resume 256-wide (debug median n=3) | 963 ms | 30 s |
| sqlite 50 start-crash-resume diamonds, one file | 362 ms | 60 s |
| sqlite 1000 sequential 1-node DAGs, resume last | last resume 168 µs (pack < 6 s) | 90 s |
| MemoryStore wide_fan_out_256 (from Phase 2 re-measure) | 4.367 ms | ≤10% vs RAII |

10k-node sqlite snapshot resume is **not** in `stress-resume` (debug budget;
same split as `stress_100k`).

## RAII re-measure (2026-08-31)

SpawnSet/cancel-bound Drop, `ctx.sleep` park-until-abort, `running_count`.
No scheduler algorithm change. Gate: no median >10% vs hunt column.

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.160 ms | 4.237 ms | +1.9% |
| deep_chain_128 (debug median n=7) | 1.839 ms | 1.940 ms | +5.5% |
| diamond_10k (debug median n=7) | 149.718 ms | 157.509 ms | +5.2% |
| apply_only (debug median n=7) | 15.080 ms | 14.366 ms | −4.7% |

All four inside the 10% band. **No revert.**

## Hunt re-measure (2026-08-31)

Tests + catalog only. No scheduler / apply / persist change. Gate: no median
>10% worse than the previous “Failure-catalog re-measure” column.

First pass ran beside `cargo llvm-cov` (`diamond_10k` 164.237 ms, +9.9%).
Repeat after coverage:

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.141 ms | 4.160 ms | +0.5% |
| deep_chain_128 (debug median n=7) | 1.851 ms | 1.839 ms | −0.6% |
| diamond_10k (debug median n=7) | 149.460 ms | 149.718 ms | +0.2% |
| apply_only (debug median n=7) | 15.062 ms | 15.080 ms | +0.1% |

All four inside the noise band. **No revert.**

## Failure-catalog re-measure (2026-08-31)

Empty-id rejection, MemoryStore poison recovery, Display impls, cancel-after-terminal
no-op. No scheduler hot-path algorithm change. Gate: no median >10% worse than
the previous “this run” column.

| bench | previous this run | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.403 ms | 4.141 ms | −6.0% |
| deep_chain_128 (debug median n=7) | 1.965 ms | 1.851 ms | −5.8% |
| diamond_10k (debug median n=7) | 157.950 ms | 149.460 ms | −5.4% |
| apply_only (debug median n=7) | 14.731 ms | 15.062 ms | +2.2% |

`apply_only` +2.2% is inside the 10% noise band. **No revert.**

## Architecture review re-measure (2026-08-31)

Same harness, same VM class. Structural work only (visibility, dead layers,
`apply.rs` split). No scheduler algorithm change. Gate: no hot-path median
worse than 10% vs the previous “after” column.

| bench | previous after | this run | change |
|---|---:|---:|---:|
| wide_fan_out_256 (debug median n=7) | 4.581 ms | 4.403 ms | −3.9% |
| deep_chain_128 (debug median n=7) | 1.870 ms | 1.965 ms | +5.1% |
| diamond_10k (debug median n=7) | 148.668 ms | 157.950 ms | +6.2% |
| apply_only (debug median n=7) | 14.479 ms | 14.731 ms | +1.7% |
| wide_100k debug | 57354 ms | 54646 ms | −4.7% |
| chain_100k debug | 1332 ms | 1348 ms | +1.2% |
| diamonds_100k debug | 1302 ms | 1309 ms | +0.5% |
| wide_100k release (repeat) | 4079 ms | 2939 ms | −28.0% |
| chain_100k release (repeat) | 329 ms | 317 ms | −3.6% |
| diamonds_100k release (repeat) | 288 ms | 277 ms | −3.8% |

Release 100k first pass was noisier (`chain_100k` 365 ms, `diamonds_100k`
327 ms); the repeat sits at or under the previous baseline. Debug diamond
+6.2% is inside the 10% noise band (same apply path). **No revert.**

Kernel release medians (this machine, n=7): wide 0.917 ms, chain 0.403 ms,
diamond_10k 38.980 ms, apply_only 3.834 ms (no prior release kernel row).

`diamond_10k` is 2500 sequential Research→{Sum,Crit}→Writer diamonds (10 000
nodes), concurrency 32. `apply_only` drives `Execution::apply` on the same
shape with no spawn.

| bench | baseline median | after median | change |
|---|---:|---:|---:|
| wide_fan_out_256 | 38.608 ms | 4.581 ms | **−88.1%** |
| deep_chain_128 | 10.386 ms | 1.870 ms | **−82.0%** |
| diamond_10k | >30 000 ms (timeout) | 148.668 ms | **−99.5%+** |
| apply_only (optional) | 900.569 ms | 14.479 ms | **−98.4%** |

Gate: ≥40% faster on at least two of {wide, chain, diamond_10k}; no other
bench regresses by more than 10%. All four improved.

## What moved the needle

1. **Incremental `MemoryStore::persist`** — after the first full snapshot, only
   dirty node slots are updated. Persist used to rebuild + clone a
   `HashMap<NodeId, _>` of the whole graph on every injected event.
2. **O(1) `derive_state`** — counters updated on each node transition instead
   of scanning every node on every `apply` (dominant on 10k-node DAGs).
3. **Slot ready queue** — `VecDeque<NodeSlot>` + `Vec<u8>` membership instead
   of `HashSet<NodeId>` / `VecDeque<NodeId>` clones on enqueue and dispatch.
4. **Executor slot cache** — `Arc<dyn Executor>` looked up once at scheduler
   start, not hashed by `ExecutorId` on every launch.
5. **`inputs_for` `with_capacity`** — pred-sized map, no grow-from-empty.
6. **Iterative Kahn cycle check** — replaces petgraph’s recursive DFS so a
   10k-node chained diamond can be built without blowing the stack.

FIFO current-thread ready queue, AND-join, fail-fast, Waiting ≠ retry, and
drop-handle-cancels are unchanged.

## 100k scale + uneven work (this run)

`current_thread`, `--test-threads=1`. Scale tests use a shared
`FunctionExecutor` and a silent sink (no event log). Uneven tests use
`FakeClock` + `ScriptedExecutor` Delay / `RetryPolicy` so wall time is
kernel work, not `sleep`.

Debug **did** finish 100k. `wide_100k` is the AND-join tax: ~57s debug /
4.1s release (timeout 90s). Chain and diamonds at 100k are ~1.3s debug.

Largest N that completed: **100 000** (wide, chain, and 25 000×4 diamonds).

| test | N nodes | debug | release |
|---|---:|---:|---:|
| wide_100k | 100 000 (1→99998→1, conc 64) | 57354 ms | 4079 ms |
| chain_10k | 10 000 | 174 ms | 25 ms |
| chain_25k | 25 000 | 297 ms | 56 ms |
| chain_100k | 100 000 | 1332 ms | 329 ms |
| diamonds_100k | 100 000 (25 000 diamonds) | 1302 ms | 288 ms |
| straggler_join | 66 | 3.4 ms | 0.83 ms |
| mixed_fanin | 79 | 2.1 ms | 0.44 ms |
| hourglass | 513 | 84 ms | 11 ms |
| uneven_payloads | 5 (2×64 KiB) | 0.23 ms | 0.09 ms |
| uneven_delays_fifo | 32+8+8 | 1.2 ms | 0.33 ms |
| skewed_retry | 4 | 0.35 ms | 0.07 ms |
| cancel_hanging_fanout_1k | 1000 | 7.0 ms | 2.3 ms |
| concurrency_1_mixed_delay_256 | 256 | 42 ms | 7.9 ms |

```bash
cargo test --test stress_100k -- --nocapture --test-threads=1
cargo test --release --test stress_100k -- --nocapture --test-threads=1
cargo test --test stress_uneven -- --nocapture --test-threads=1
```

### Bug found while adding the pack

`ready_successors` scanned every predecessor of the join on each incoming
success (`O(fan²)`). `wide_100k` timed out at 60s in debug. Fixed: per-slot
remaining-pred count, decremented once when a predecessor Succeeds. AND-join
semantics unchanged. No test was deleted.

## Production workloads (this run)

`tests/workloads.rs`. In-process ScriptedExecutor / FunctionExecutor.
`current_thread`, `--test-threads=1`, FakeClock for retries. Debug finished
every workload (none needed `--release`).

**Global fail-fast:** after policy Accepts Failed, the whole execution is
Failed and every non-terminal node is Cancelled. A production crawl that
must survive a failed page is **N executions**, not one giant DAG with
Accept-Failed. Encoded as `fail_fast_is_execution_wide` +
`crawl_as_many_executions` (500 isolated seed→child→3-down runs; 10 fail,
490 succeed; MemoryStore ids do not mix). Permanent 1% fail is the same
shape: `many_executions_1pct_fail` (200 one-node executions). The shared
`flaky_io_retry_storm` graph only retry-then-succeeds.

Phase 1 `Runtime::start` already spawns one scheduler loop per execution,
so 8 concurrent diamonds on one current_thread Runtime is legal
(`many_executions_8_concurrent`).

| test | N / mix | debug | release |
|---|---|---:|---:|
| fail_fast_is_execution_wide | 5 nodes, 1 Accept-Failed | 0.29 ms (median 0.11) | 0.09 ms |
| crawl_as_many_executions | 500 execs, 10 fail / 490 ok | 39.7 ms (12.6k execs/s) | 7.2 ms (69k execs/s) |
| agent_farm_1000 | 1000 diamonds (4000 nodes), 5% retry + 2% HITL | 276 ms | 76 ms |
| map_reduce_tree | 10 000 maps → 100 partials → 1 | 131 ms | 33 ms |
| hitl_drain | 200 Waiting, resume ×20 shuffled | 44 ms | 9.7 ms |
| flaky_io_retry_storm | 1000 nodes, 30% fail×1 + 5% fail×2 | 29 ms | 4.9 ms |
| many_executions_1pct_fail | 200 isolated, 2 Accept-Failed | 10 ms | 1.8 ms |
| burst_idle_burst | 2000 → Waiting gate → 2000, conc 64 | 107 ms | 19 ms |
| many_executions_1000_sequential | 1000 × 4-node diamond | 70 ms (14.3k execs/s) | 13 ms (78k execs/s) |
| many_executions_8_concurrent | 8 diamonds, one Runtime | 0.80 ms | 0.15 ms |

25k-diamond agent farm was not added: `diamonds_100k` already covers 100k
succeed-immediately diamonds; this pack’s farm is the mixed retry+HITL
product shape at N=1000 (fits debug easily).

```bash
cargo test --test workloads -- --nocapture --test-threads=1
cargo test --release --test workloads -- --nocapture --test-threads=1
```

No new kernel bugs. HITL duplicate-Complete must reuse the same payload
(`ConflictingComplete` otherwise). Retrying Research and Waiting Critic
must not share a diamond if the park probe waits for Waiting before the
clock advances.

## Resilience / I/O-fault harness (this run)

`tests/resilience.rs`. `NetFault` on `ScriptedExecutor` + `FakeClock` (no
sockets). Fail-fast stays **execution-wide**. `siblings_survive_timeout_same_dag`
is `#[ignore = "requires failure scopes"]`.

| fault | scheduler | this node | same-exec siblings | other executions |
|---|---|---|---|---|
| Timeout + Accept | alive | TimedOut | Cancelled | n/a |
| Timeout + Retry | alive | Ready then Succeeded | may run (permit released) | n/a |
| Reset + Retry | alive | Ready then Succeeded | may run | n/a |
| Delay + AND-join | alive | Running then Succeeded | join stays Pending | n/a |
| Timeout in 1 of 100 execs | alive | that exec Failed | n/a | other 99 Succeeded |
| mixed 32 / conc 8 | alive | retry-then-succeed | peak Running = 8 | n/a |

| test | debug |
|---|---:|
| timeout_accept_fail_fasts_execution | 0.14 ms |
| timeout_retry_releases_permit_then_succeeds | 0.26 ms |
| delay_and_join_waits_for_slow_pred | 0.26 ms |
| reset_retry_then_success | 0.21 ms |
| timeout_isolates_one_of_100_executions | 7.52 ms |
| mixed_faults_retry_then_succeed | 0.86 ms (peak Running 8) |
| timeout_after_clock_while_running | 0.30 ms |
| delay_aborts_on_cancel | 0.26 ms |

```bash
cargo test --test resilience -- --nocapture --test-threads=1
```

100 sequential diamonds: MemoryStore keys by `ExecutionId`. The TimedOut
execution stays Failed after the other 99 Succeeded. That is how a crawl
stays resilient today — N executions, not continue-on-error in one DAG.
