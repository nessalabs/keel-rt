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
