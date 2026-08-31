# Phase 1 failure catalog

Every interrupt, retry, race, empty, illegal, drop, panic, and I/O-of-ports
case the kernel handles. Rows are the production contract: if a test name is
here, the handling is pinned. **Zero MISSING.**

How to read a row:

| column | meaning |
|---|---|
| Trigger | How a caller or port produces the case |
| Observable | Execution / node state, events, handle `Result` |
| Test | Named regression that fails without the handling |
| Handling | typed error / cancel / no-op / default / documented panic |

Kernel `src/` line coverage (python gate, OR-merged lcov): **2735/2735 = 100.00%**.
Empty allowlist. Floor 100.

- Default join is `Join::AllSucceeded`. Default `OnFailure` is `FailExecution`.
- `FailSubtree` and `Join::AllDone` are definition-only opt-in.
- Retry delay is `Ready { runnable_at }`, never Waiting.
- Drop `ExecutionHandle` cancels the graph (JoinSet, not detach).
- Persist / sink / policy panics: process survives; in-memory apply does not roll back.
- Apply inbox stays unbounded ([ADR 0001](adr/0001-unbounded-apply-inbox.md)).

## Remaining documented panics

Library paths in `src/domain` and `src/runtime` were grepped for
`unwrap` / `expect` / `panic!` / `todo!` / `unimplemented!` / `unreachable!`.

| Site | Why it stays | Test that proves the public path cannot reach it |
|---|---|---|
| `scheduler.rs` `launch_slot` `.expect("Runtime::start rejected unregistered executor ids")` | Invariant after `StartError::UnregisteredExecutors`. Prefer this over a dead defensive branch. | `start_unknown_executor_errors_and_nothing_runs` |
| `scheduler.rs` `launch_slot` `.expect("dispatch issues a resume token")` | `dispatch_node` always issues a token before spawn. | `resume_on_running_node_is_error` (token present on Running) |

`src/domain` has none on library paths (only `#[cfg(test)]`).
`MemoryStore` mutex poison used to `expect` and kill the scheduler; it now
recovers with `into_inner` (`poisoned_mutex_recovers_on_next_persist`).

`#[must_use]` on `ExecutionHandle` is a rustc warning, not a runtime test.
Dropping the handle is pinned by `dropping_execution_handle_cancels_graph_not_detach`.
`wait` after `wait` is a compile error (`wait` takes `self`).

---

## Definition

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Empty graph | `DefinitionError::Empty` | `empty_graph_rejected` | typed error |
| Single node / 0 predecessors | source Ready at Start, Succeeded | `zero_predecessor_source_runs` | default |
| Self-edge | `DefinitionError::Cycle` | `self_edge_rejected` | typed error |
| Cycle | `DefinitionError::Cycle` | `cycle_is_rejected` / `cycle_rejected` | typed error |
| Two disconnected components | accepted; both chains run | `two_disconnected_components_are_accepted` | default |
| Duplicate node id | `DefinitionError::DuplicateNode` | `duplicate_node_id_rejected` | typed error |
| Edge to missing node | `DefinitionError::DisconnectedNode` | `disconnected_node_rejected` / `dangling_edge_rejected` | typed error |
| Duplicate edge | one pred (deduped) | `duplicate_edge_is_one_pred` | no-op extra edge |
| Empty node id | `DefinitionError::EmptyNodeId` | `empty_node_id_rejected` | typed error |
| Empty workflow id | `DefinitionError::EmptyWorkflowId` | `empty_workflow_id_rejected` | typed error |
| Empty executor id | `DefinitionError::EmptyExecutorId` | `empty_executor_id_rejected` | typed error |
| `Join::AllDone` on a node with no preds | source Ready, runs | `all_done_source_with_no_preds_runs` | default (remain=0) |
| FailSubtree with no children (0-successor fail) | node Failed, execution **Completed** (not Failed) | `fail_subtree_with_no_children_is_noop_on_descendants` | FailSubtree contract |
| 0-successor fail under FailExecution | node Failed, execution Failed | `zero_successor_fail_execution_is_failed` | fail-fast |
| Every `DefinitionError` Display | string names the variant | `every_definition_error_variant_has_display` | Display |

## Start

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Unknown executor id(s) | `StartError::UnregisteredExecutors`, nothing runs | `start_unknown_executor_errors_and_nothing_runs` / `unregistered_executor_fails_node_no_hang` | typed error |
| Start after `Runtime` dropped | in-flight handle still Succeeded | `start_after_runtime_dropped_execution_still_runs` | scheduler owns Arcs |
| Two starts of the same definition | distinct `ExecutionId`s, two Succeeded | `two_starts_same_definition_distinct_execution_ids` | two executions |
| Sequential starts, shared store | snapshots do not mix | `sequential_second_execution_does_not_mix_store` | keyed by ExecutionId |
| `concurrency(0)` | clamped to 1, Succeeded | `concurrency_zero_does_not_deadlock` | clamp |
| `concurrency(1)` | peak execute ≤ 1 | `concurrency_one_serializes_two_ready` | semaphore |
| `concurrency(N)` burst | Running ≤ N | `permit_cap_never_exceeded_during_burst` | semaphore |
| Omit store / policy / sink / clock | MemoryStore, AcceptPolicy, NoopSink, SystemClock, conc 8 | `builder_defaults_without_store_policy_sink_clock` | explicit defaults |
| Default MemoryStore persist | snapshot retrievable after run | `memory_store_is_the_builder_default` | default |
| `Runtime::run` vs `start`+`wait` | same Succeeded | `runtime_run_matches_start_then_wait` | `run` = start+wait |

## Run

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Executor panic | node Failed (`panic: …`), scheduler alive, execution Failed | `executor_panic_scheduler_survives` / `executor_panic_node_failed_scheduler_alive_execution_failed` / `executor_panic_string_and_unknown_payload_fail_the_node` | catch_unwind → Failed |
| Executor hang until cancel | Cancelled within `DEFAULT_CANCEL_BOUND` | `hang_ignore_cancel_ends_within_documented_bound` | abort after bound |
| Executor returns Waiting without / with garbage token | kernel-issued token on inspect; resume garbage → `TokenMismatch` | `waiting_executor_supplied_token_ignored_kernel_token_used` | ignore executor token |
| Succeeded empty `Bytes` vs fat `Bytes` | both stored; join sees both | `succeeded_empty_bytes_vs_fat_bytes` / `uneven_payloads` | opaque bytes |
| Failed + Accept | node Failed, execution Failed, no retry | `accept_policy_on_fail_no_retry` | Accept |
| Failed + Retry then Succeeded | attempts 1,2,… at-least-once | `retry_policy_max_3_fail_twice_then_succeed` / `retry_is_at_least_once_two_execute_invocations` | Retry; **not** exactly-once |
| Failed + Retry exhausted | node Failed, execution Failed | `retry_policy_max_2_always_fail` | Accept after max |
| TimedOut + Accept fail-fast | execution Failed | `timeout_accept_fail_fasts_execution` | Accept |
| TimedOut + Retry | Ready then Succeeded, permit released | `timeout_retry_releases_permit_then_succeeds` | Retry |
| Reject Waiting | node Failed, execution Failed | `never_wait_policy_rejects_waiting` | Reject |
| Reject Failed | node Failed (`policy rejected outcome`), execution Failed | `reject_policy_on_failed_fails_node` | Reject |
| Retry delay | `Ready { runnable_at }`, not Waiting | `retry_delay_is_ready_with_runnable_at_not_waiting` | Ready |
| Two timers same deadline | both fire (no lost-wake) | `two_nodes_same_retry_deadline_both_run` | drain due deadlines |
| Staggered retry deadlines | fast must not fire slow | `two_nodes_staggered_retry_deadlines_both_run` | per-node deadline |
| Policy panic | node Failed, execution Failed, scheduler alive | `policy_decide_panic_does_not_kill_scheduler` | catch_unwind |
| Sink panic | execution Succeeded, later emits continue | `event_sink_panic_kernel_survives_and_progresses` | catch_unwind |
| Store persist panic | in-memory Succeeded | `persist_panic_does_not_kill_execution` | catch_unwind; no rollback |
| Store persist `Err` | in-memory Succeeded | `failing_store_put_does_not_roll_back_in_memory` / `failing_store_every_put_diamond_still_succeeds` | log + skip |
| MemoryStore mutex poison | next persist recovers | `poisoned_mutex_recovers_on_next_persist` | `into_inner` |
| Clock `now` panic (scheduler death) | inspect stopped snapshot; wait `Cancelled` | `panicking_clock_inspect_is_stopped_and_wait_is_cancelled` | Cancelled |

Retry of a non-idempotent side effect is **at-least-once**. The kernel re-invokes
`execute` after Retry. It does not dedupe user I/O. Callers that need
exactly-once must make the executor idempotent.

## Join / failure scope

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| AND-join, one pred Failed, FailExecution | siblings+descendants Cancelled, execution Failed | `b_fails_d_depends_on_b_cancelled_never_started` / `diamond_c_fails_d_cancelled_b_stays_succeeded` / `diamond_fail_execution_still_fail_fasts` | fail-fast |
| Same diamond, FailSubtree | sibling continues; D Cancelled; execution Completed | `diamond_fail_subtree_all_succeeded_completes` | FailSubtree |
| AllDone reducer, one pred Failed, FailSubtree | reducer runs with succeeded preds only; Completed | `fanin_all_done_reducer_runs` | AllDone |
| AllSucceeded reducer, failed pred, FailSubtree | reducer Cancelled, never started; Completed (does not hang) | `fanin_all_succeeded_reducer_cancelled` | cancel descendant |
| AllSucceeded reducer, failed pred, FailExecution | join Cancelled; execution Failed; process terminates | `all_succeeded_reducer_with_failed_pred_terminates` | fail-fast |
| Mixed TimedOut + Failed + Succeeded fan-in, FailSubtree+AllDone | reducer runs with succeeded only; Completed | `mixed_timed_out_failed_succeeded_fanin_all_done` / `farm_50_fail_subtree_all_done` | AllDone |
| Nested FailSubtree | uncle/writer run; down Cancelled | `nested_fail_subtree_uncle_writer_runs` | subtree only |
| TimedOut + FailSubtree | descendants Cancelled, siblings live | `timeout_fail_subtree_siblings_live` | FailSubtree |
| User cancel under FailSubtree | whole graph Cancelled | `user_cancel_overrides_fail_subtree` | cancel wins |

## HITL / resume

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Resume Complete | node Succeeded; dependents run with output | `resume_complete_succeeded_dependents_run_with_output` | apply |
| Resume Reinvoke | same attempt, second execute | `resume_reinvoke_same_attempt` / `reinvoke_then_complete_success_path` | reinvoke |
| Resume after cancel | `ApplyError::ResumeAfterCancel` | `resume_after_cancel_is_error` / `resume_after_cancel_errors` | typed error |
| Wrong execution / node / nonce | `TokenMismatch` / `UnknownNode` | `resume_wrong_node_wrong_execution_wrong_nonce` | typed error |
| Stale FinishNode attempt | no-op, revision unchanged | `finish_wrong_attempt_ignored` | no-op |
| Duplicate Complete, same payload | Ok no-op; dependents not run twice | `duplicate_complete_same_success_ok_dependents_not_run_twice` | no-op |
| Duplicate Complete, mismatching payload | `ApplyError::ConflictingComplete`; first payload kept | `conflicting_complete_errors_second_payload_not_used` | typed error |
| Resume when Running (not Waiting) | `ApplyError::NotWaiting` | `resume_on_running_node_is_error` | typed error |
| Resume Complete on Failed after execution Failed | `ApplyError::ResumeAfterCancel` | `resume_complete_on_failed_node_after_execution_failed` | typed error |
| Waiting releases permit | sibling runs at conc 1 | `waiting_releases_permit_other_ready_node_runs` / `waiting_releases_permit_sibling_runs` | release |
| Drop handle while Waiting | execution Cancelled; sibling Succeeded kept; no leak | `drop_handle_while_waiting_cancels_and_releases_permit` | cancel |
| `wait()` on Waiting | does not return (timeout) | `wait_does_not_return_while_waiting` | wait is terminal-only |
| `wait_stable()` on Waiting | returns `Waiting` | `wait_stable_returns_on_waiting` | stable includes Waiting |

## Cancel / drop

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Cancel Running + Pending sibling | Running Cancelled; pending never starts | `cancel_mid_run_running_sees_token_pending_never_starts` / `cancel_running_pending_sibling_never_starts` | cancel |
| Cancel Waiting | Cancelled; later resume `ResumeAfterCancel` | `cancel_while_waiting_is_cancelled` | cancel |
| Cancel already terminal | no-op; still Succeeded / Failed | `cancel_already_terminal_is_noop` / `cancel_after_failed_stays_failed` / `double_cancel_is_noop_and_start_node_rejects_unknown_and_pending` | no-op |
| Drop handle | graph Cancelled, not detached | `dropping_execution_handle_cancels_graph_not_detach` / `drop_handle_cancels_unique_owner` | Drop = cancel |
| Cancel vs in-flight retry timer | Ready with future deadline does not start later | `cancel_ready_with_future_deadline_does_not_start_later` | cancel |
| Cancel bound | hang ignoring cancel ends within `DEFAULT_CANCEL_BOUND` | `hang_ignore_cancel_ends_within_documented_bound` / `cancel_twice_then_bound_still_cancels_hang` | abort |
| Inspect after cancel | Cancelled snapshot, not panic | `inspect_after_cancel_returns_cancelled_not_error` | snapshot |
| Resume after cancel | `ApplyError::ResumeAfterCancel`, not panic | `resume_after_cancel_is_error` | typed error |
| `#[must_use]` forgotten handle | rustc warning; Drop cancels | `dropping_execution_handle_cancels_graph_not_detach` | Drop |

## Stale / concurrent

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| FinishNode old attempt | no-op, not error | `finish_wrong_attempt_ignored` | no-op |
| Two completions same node/attempt after success | no-op; first payload kept | `second_finish_same_attempt_after_success_is_noop` | no-op |
| Burst of ready vs concurrency cap | CREATED/RUNNING counts ≤ cap | `permit_cap_never_exceeded_during_burst` | cap |
| FIFO ready queue | later Ready does not starve earlier | `uneven_delays_fifo` | FIFO |
| Timer after Succeeded | no-op | `timer_after_succeeded_is_noop` | no-op |

## Handle / API

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| `wait` after `wait` | compile error (`wait` takes `self`) | documented (no runtime test) | consume |
| Inspect during Running | live snapshot, Running nodes | `inspect_during_running_returns_live_snapshot` / `inspect_two_hung_running` | snapshot |
| `Runtime::run` vs start+wait | same terminal | `runtime_run_matches_start_then_wait` / `register_fn_tiny_diamond` | `run` |
| Apply Start twice | `ApplyError::Illegal` | `apply_start_twice_is_illegal_unknown_retry_is_noop` | typed error |

## Clock

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| FakeClock jump over deadline | retry still fires, second attempt runs | `fake_clock_jump_over_retry_deadline_still_fires` | park re-arms |
| `ctx.sleep` uses public Clock | FakeClock sleep, not wall | `ctx_sleep_uses_public_clock` | Clock port |
| Clock not advancing | retry stays attempt 1 across yields | `paused_clock_retry_does_not_busy_spin` | park on Notify |
| FakeClock lost-wake | sleep still completes | `sleep_does_not_lose_advance_notify` | subscribe-before-check |

## Ids / snapshots / Display

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Snapshot iter order | definition order | `snapshot_iter_nodes_matches_definition_order` | `iter_nodes` |
| `SCHEMA_VERSION` on live snapshot | `1` | `live_snapshot_carries_schema_version_1` | constant |
| Display every `DomainEvent` | each variant names itself | `domain_event_display_covers_every_variant` | Display |
| Display every `NodeState` / `ExecutionState` | each variant names itself | `node_state_and_execution_state_display_covers_every_variant` / `snapshot_display_names_every_execution_and_node_state` | Display |

## ADR 0001 / persist backpressure

| Trigger | Observable | Test | Handling |
|---|---|---|---|
| Inspect while persist blocks | inspect completes after persist; no deadlock | `inspect_during_blocking_persist_completes_after_persist` | unbounded inbox; backpressure |
| Persist `Err` | in-memory apply kept | `failing_store_put_does_not_roll_back_in_memory` | skip persist |

A bounded apply inbox would deadlock `resume`/`inspect` when apply is inside
`persist`. The inbox stays unbounded. A stuck persist stalls inspect until it
returns — that is wait-behind-I/O, not a lock cycle.
