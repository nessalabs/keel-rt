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

- Lease: `executions.owner`, `epoch`, `lease_until`. `claim` is
  `BEGIN IMMEDIATE` and never `INSERT OR REPLACE`. Existing files get
  `ALTER TABLE` on open; kernel `SCHEMA_VERSION` stays 1 (adapter
  columns, not snapshot schema). Default TTL 30s.
- One `BEGIN IMMEDIATE` … `COMMIT` per `persist`/`put` call.
- First persist writes every node row. Later persists upsert dirty slots
  only; unchanged Pending rows are not deleted. Parked Ready{T} stores T
  in `nodes.runnable_at`; compact JSON keeps a short `last_error` so
  inspect after persist matches live inspect.
- `wal_checkpoint(TRUNCATE)` is best-effort after COMMIT of a **terminal**
  snapshot, never inside the transaction. Checkpoint `Err` does not fail
  persist. Equal-revision persist does not insert event rows.

## Startup and recovery

`Runtime::start_durable(definition).await` uses `StateStore::initialize` to commit
the new execution's lease, `Created` snapshot, every node, and definition in one
transaction before any executor launches. Both FULL and NORMAL support process
crash recovery; their power-loss guarantees remain those in the table above.
Temporary and in-memory databases cannot acknowledge durable initialization.
Existing IDs return `InitializeError::AlreadyExists` and are never overwritten.

The existing `Runtime::start` path can leave a lease reservation if interrupted
before its first snapshot. `get` and `workflow_definition` return `None` for that
specific record; `Runtime::resume` returns `UnknownExecution`. These reads do not
release ownership. Lease expiry/takeover and fencing still apply. Malformed rows
and missing definitions for real snapshots remain errors.

`persist` initializes a reservation even at revision zero. `put` only updates an
initialized execution; use `persist` first so the definition is stored too.
No snapshot does not prove no executor ran on the ordinary `start` path. Callers
retain submission inputs and stable business operation IDs for safe retries,
including replacement executions with new IDs. Keel does not deduplicate external
effects or automatically restart an unknown execution.

Startup regressions include actual subprocess death before the initial commit,
after commit without acknowledgement, and immediately after acknowledgement.
Run `cargo test -p keel-rt-sqlite --lib --test startup -- --test-threads=1`.

Standing load / messy-user attacks (not coverage):
`cargo test -p keel-rt-sqlite --test chaos -- --test-threads=1 --nocapture`
and [`docs/CHAOS_LOG.md`](../../docs/CHAOS_LOG.md).

Durable dispatch claims the lease and reloads the committed snapshot before
running nodes. A delayed starter therefore respects progress and cancellation
recorded by a recovering runtime. SQLite retains the lease generation across
release and owner cleanup; released tokens cannot heartbeat or persist, and a
subsequent claim receives a greater generation.
