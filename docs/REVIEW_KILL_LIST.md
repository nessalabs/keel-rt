# Review kill list (Pass A)

Adversarial read of `src/` against `skills/engineering/coding` +
`system-architect` (structure, review, patterns, rust) + `method`.
Frozen product contract is out of scope for deletion.

Severity: **block** (must fix or ADR) · **fix** (do this pass) · **nit** (may skip).

| # | File | Principle | Sev | Finding | Proposed |
|---|------|-----------|-----|---------|----------|
| 1 | `src/lib.rs` | rust.md: minimise `pub`; structure: contracts only | block | `pub mod domain` / `pub mod runtime` leak `NodeRuntime`, `ExecutorRegistry`, `RecordingSink`, module paths | `pub(crate) mod`; re-export the real surface at the crate root |
| 2 | `src/domain/definition.rs` | patterns: speculative abstraction | fix | `EdgePredicate::Always` is the only variant; unused policy hook | Delete the enum; `Edge` is `from`/`to` |
| 3 | `src/domain/snapshot.rs` | coding: forwarding layer | fix | `iter_nodes` only forwards to `iter_ordered` | Keep `iter_nodes`; delete `iter_ordered` |
| 4 | `src/runtime/park.rs` | patterns: trait for one impl | fix | `Park` has one impl; `FakePark` is a dead alias | Delete trait + alias; keep `ChannelPark` |
| 5 | `src/runtime/inject.rs` | rust.md: dead; coding: extract on second use | fix | `JoinKind::Cancelled` is `#[allow(dead_code)]` | Delete the variant and the scheduler branch |
| 6 | `src/runtime/scheduler.rs` | coding: leftover after DX fail-fast | fix | `launch_slot` still fabricates “no executor registered” | `expect` the slot cache; start already rejected missing ids |
| 7 | `src/runtime/handle.rs` | structure: product vocab out of kernel | fix | Docs say HITL | Say Waiting / resume |
| 8 | `src/runtime/sink.rs` | structure: test double in adapter | fix | `RecordingSink` is a test log in the production module | Move to `testing/` |
| 9 | `src/domain/definition.rs` | rust.md: invariant field private | fix | `WorkflowDefinition.id` is `pub` — post-build mutation | Private + `id()` |
| 10 | `src/runtime/inject.rs` | structure: unbounded without a documented bound | block | `mpsc::unbounded_channel` for apply events | Document + ADR 0001 (bound would deadlock handle ops) |
| 11 | `src/runtime/store.rs` | structure: port knows the aggregate | block | `StateStore::persist(&Execution)` | Keep: incremental MemoryStore is a measured win. ADR 0002 |
| 12 | `src/domain/state.rs` | structure: god file; local reasoning gone | block | ~1410 lines: types + apply + fail + join + tests | Split `apply.rs`; extract shared cancel loop |
| 13 | `Cargo.toml` | coding: dead code | fix | `serde_json` unused | Drop the dep |
| 14 | `src/domain/ids.rs` | rust.md: field with invariant private | fix | `ResumeToken` identity fields are `pub` | Private + getters |
| 15 | `src/` | method: artefact that holds absences | block | No structure test over import graph / product vocab | `tests/structure.rs` |
| 16 | `src/runtime/scheduler.rs` | rust.md: undocumented cancel | fix | `cancel_bound` uses wall `tokio::time::sleep`, not `Clock` | Document in ARCHITECTURE + rustdoc |
| 17 | `src/testing/failpoint.rs` | structure: ambient singleton | block | Process-wide `OnceLock<Mutex<…>>` | Accept in test-util only. ADR 0003 |
| 18 | `src/domain/ids.rs` | structure: global mutable | nit | `EXEC_SEQ` / `TOKEN_SEQ` process counters | Keep (id issuance). Note in ARCHITECTURE |
| 19 | `src/runtime/spawn.rs` | rust.md: unsafe needs a safety comment | fix | `CatchUnwind` `map_unchecked_mut` | Document the pin projection |
| 20 | `src/domain/state.rs` | coding: extract on second use | fix | `fail_fast` / `cancel_graph` duplicate the cancel scan | One `cancel_non_terminals` |
| 21 | `src/testing/faults.rs` + comments | structure: prefer zero product words in `src/` | fix | Comments name HTTP / Agent / HITL | Rephrase absences without those words |
| 22 | `benches/` | method: measurement as a record | fix | Gate is implicit (stress tests, no Criterion) | Document harness-as-gate; re-measure after refactors |

## Not a problem (evidence)

| Item | Why it stays |
|------|----------------|
| `Execution` / `ApplyCmd` public | Apply-only benches + stale/timer packs are public-API tests against the aggregate. Hiding them would invent a test door. |
| Policy consulted inside `apply` | Correct: mechanism (scheduler) does not own retry rules. |
| `Execution` holds `WorkflowDefinition` | Aggregate contains the definition; types stay separate (`NodeDef` vs `NodeRuntime`). |
| `store_arc` / `policy_arc` / `register_fn` | DX follow-up on `main`. Keep. |
| `#[must_use]` on `ExecutionHandle` | Already present; Drop cancels; not Clone. |
| No `utils` / `common` | Confirmed. |
| Domain has no `tokio` / `std::net` | Confirmed (Pass B will lock it). |
| Scheduler does not name node “kinds” | Dispatches by slot + `Executor` cache. |
| Workload test names (`agent_farm`, crawl) | `tests/` product-shaped graphs, not kernel types. Out of `src/`. |
| `examples/studio` | Other branch. Do not merge. |

## Leftovers that need an ADR if still true after Pass C

- Unbounded apply inbox (0001).
- `StateStore::persist(&Execution)` (0002).
- Test-util failpoint map (0003).

Update this table in Pass D: each row **fixed**, **deleted as not-a-problem**, or **ADR’d**.
