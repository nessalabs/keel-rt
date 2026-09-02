# keel-rt-sqlite

File-backed `StateStore` adapter for `keel-rt`. The kernel never names this
crate. Delete it without editing `scheduler.rs`.

## Durability

| Open | `synchronous` | Process kill after COMMIT | Machine power loss of last txn |
|---|---|---|---|
| `SqliteStore::open` / `SqliteStore::durable` (default) | **FULL** | recovered | recovered |
| `SqliteStore::open_fast` | NORMAL | recovered | **may lose last WAL frames** |

Phase 2 crash tests kill the process after `persist` returns (`COMMIT`
completed). That path is green on both FULL and NORMAL.

256-wide snapshot resume on this machine (debug, n=3): NORMAL ~259 ms, FULL
~421 ms. Both beat half of the pre-opt **1.008 s** baseline, so the default
is FULL — the ≥50% speedup does not donate power-loss durability.
See [`benches/BASELINE.md`](../../benches/BASELINE.md).

```rust
use keel_rt_sqlite::SqliteStore;

let store = SqliteStore::open("/tmp/keel.db")?;          // FULL
let fast = SqliteStore::open_fast("/tmp/keel-fast.db")?; // NORMAL
```

## Persist contract

- One `BEGIN IMMEDIATE` … `COMMIT` per `persist`/`put` call.
- First persist writes every node row. Later persists upsert dirty slots
  only; unchanged Pending rows are not deleted. Parked Ready{T} stores T
  in `nodes.runnable_at`; compact JSON keeps a short `last_error` so
  inspect after persist matches live inspect.
- `wal_checkpoint(TRUNCATE)` is best-effort after COMMIT of a **terminal**
  snapshot, never inside the transaction. Checkpoint `Err` does not fail
  persist. Equal-revision persist does not insert event rows.

Standing load / messy-user attacks (not coverage):
`cargo test -p keel-rt-sqlite --test chaos -- --test-threads=1 --nocapture`
and [`docs/CHAOS_LOG.md`](../../docs/CHAOS_LOG.md).
