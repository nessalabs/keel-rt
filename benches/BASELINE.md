# Kernel microbench baseline (this run)

Machine: Cloud Agent VM. Profile: `cargo test` (debug), Tokio `current_thread`,
`--test-threads=1`. ScriptedExecutor succeed-immediately (zero user work).
Median of 7 iterations unless noted.

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
