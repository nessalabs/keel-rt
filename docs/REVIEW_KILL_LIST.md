# Review kill list — Pass D

Pass A findings vs what landed. Frozen product contract unchanged.
DX APIs (`register_fn`, `Clock`, `builder(String)`, start fail-fast,
`iter_nodes`, `is_successful_finish`, `Runtime::run`) kept.

| # | After | Evidence |
|---|--------|----------|
| 1 | **fixed** | `pub(crate) mod domain` / `runtime`; root re-exports. `tests/structure.rs` `lib_does_not_export_module_trees` |
| 2 | **fixed** | `EdgePredicate` deleted; `Edge` is `from`/`to` |
| 3 | **fixed** | `iter_ordered` deleted; `iter_nodes` owns the iterator |
| 4 | **fixed** | `Park` / `FakePark` / `ChannelPark` deleted. Wait is Runtime `next_drive_event` (inbox vs `Clock::wait_until`). `tests/structure.rs` `apply_path_does_not_sleep` |
| 5 | **fixed** | `JoinKind` deleted; `Event::NodeFinished` is `Result<NodeOutcome, String>` |
| 6 | **fixed** | `launch_slot` `expect`s the executor cache |
| 7 | **fixed** | Handle rustdoc says Waiting / resume |
| 8 | **fixed** | `RecordingSink` lives in `src/testing/recording.rs` |
| 9 | **fixed** | `WorkflowDefinition::id()`; field private |
| 10 | **ADR’d** | [0001](adr/0001-unbounded-apply-inbox.md) |
| 11 | **ADR’d** | [0002](adr/0002-store-persist-live-aggregate.md) |
| 12 | **fixed** | `src/domain/state/{mod.rs,apply.rs}`; `cancel_non_terminals` |
| 13 | **fixed** | `serde_json` dropped from `Cargo.toml` |
| 14 | **fixed** | `ResumeToken` getters |
| 15 | **fixed** | `tests/structure.rs` |
| 16 | **fixed** | rustdoc on `DEFAULT_CANCEL_BOUND` + comment in `arm_cancel_bound`; pinned by `hang_ignore_cancel_ends_within_documented_bound` |
| 17 | **ADR’d** | [0003](adr/0003-test-util-failpoints.md) |
| 18 | **not-a-problem** | process id counters; noted in ARCHITECTURE |
| 19 | **fixed** | Safety comment on `CatchUnwind` pin projection |
| 20 | **fixed** | `cancel_non_terminals` |
| 21 | **fixed** | `src/` has no Agent/HTTP/HITL/Sql/crawl words (`src_has_no_product_resource_identifiers`) |
| 22 | **fixed** | Harness-as-gate documented; `benches/BASELINE.md` re-measured |

## Still accepted (not smells)

`Execution` / `ApplyCmd` public (apply-only + stale/timer packs). Policy
inside `apply`. DX builder methods. `#[must_use]` handle. No `utils/`.
`examples/studio` not merged.

## New smells introduced?

None that remain. `pub(crate)` fields on `Execution` after the split are
crate-visible only. Structure tests fail if the module arrow reverses.
