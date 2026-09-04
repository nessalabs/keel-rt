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
| Crash during retry delay | stays `Ready { runnable_at }`; FakeClock must advance | `test: crash_during_retry_delay_does_not_fire_early` `test: resume_retry_ready_does_not_fire_before_deadline` `test: start_timeout_retry_crash_during_backoff_resume_advance_succeeds` `test: crash_resume_full_file_keeps_deadline` |
| Crash after timeout persisted, before retry dispatch | T still on snapshot; resume does not double-run | `test: crash_after_timeout_persisted_before_dispatch_does_not_double_run` `test: crash_after_accept_timeout_persisted_resume_stays_timed_out` |
| Backoff retry survives crash | same attempt policy; no extra attempts | `test: backoff_retry_survives_crash_same_attempt_policy` |
| Cancel / Drop during parked deadline | Cancelled; FakeClock sleeper dropped | `test: cancel_during_parked_deadline_is_cancelled_sleeper_dropped` `test: drop_handle_during_parked_deadline_cancels_sleeper` `test: cancel_ready_with_future_deadline_does_not_start_later` `test: cancel_while_drive_waits_on_future_t_drops_waiter` |
| Hung `wait_until` / `Timestamp::MAX` | hang bound or inbox cancel still terminates; no `thread::sleep` | `test: hung_wait_until_hang_bound_still_cancels` `test: timestamp_max_deadline_cancel_returns_without_thread_sleep` |
| Persist of deadline drop / double-fire | T round-trips; `last_error` round-trips; due-on-resume dispatches once | `test: sqlite_deadline_persist_does_not_drop_or_double_fire` `test: persisted_deadline_already_due_on_resume_runs_once_not_twice` `test: incremental_persist_does_not_drop_runnable_at` `test: persist_resume_256_runnable_at_set_vs_unset` `test: parked_ready_t_uses_column_omits_nested_json_keeps_last_error` `test: dirty_persist_ready_t_to_t_prime_updates_only_runnable_at` |
| FailSubtree + parked sibling | sibling keeps `Ready { T }`; execution stays live | `test: fail_subtree_parked_sibling_keeps_deadline` `test: fail_subtree_parked_sibling_not_in_subtree_stays_parked` |
| StartNode on due T | `ApplyError::Illegal`; RetryDue first | `test: start_node_on_due_t_is_illegal_without_retry_due` |
| Cancel vs due T same instant | Cancel in inbox beats Timer | `test: cancel_when_deadline_already_due_does_not_dispatch` `test: due_t_hung_wait_until_inbox_cancel_does_not_dispatch` |
| 256 parked crash-resume | one advance, each fires once | `test: wide_256_parked_advance_once_each_fires_once` `test: crash_resume_256_parked_advance_once_each_once` |
| Two Runtimes, parked T | second resume `ClaimedElsewhere` while lease live | `test: two_runtimes_parked_deadline_are_not_fenced` |
| Crash during timeout (Running + FakeClock Delay) | re-invoke; advance FakeClock → TimedOut | `test: start_arm_timeout_crash_before_fire_resume_advance_is_timed_out` |
| Crash after terminal persist | terminal kept; succeeded/failed nodes do not re-run | `test: process_restart_is_new_runtime_same_file` `test: crash_after_fail_fast_stays_failed` |
| `resume` while first resume still live, concurrent | one `Ok`, one `AlreadyActive` | `test: concurrent_resume_same_id_one_already_active` `test: concurrent_resume_same_runtime_one_already_active` `test: resume_of_live_start_is_already_active` `test: resume_twice_live_is_already_active` |
| Two Runtimes, one sqlite file | store lease + epoch; second resume `ClaimedElsewhere` while lease live. CAS: stale put loses | `test: two_runtimes_same_file_are_not_fenced` `test: stale_put_does_not_clobber` `test: stale_put_loses_on_memory_store` `test: two_runtimes_lease_ttl_then_second_claims` `test: stale_epoch_persist_is_rejected` |
| Persist CAS then kill before event | terminal not lost; executor not re-run; event may be absent | `test: crash_after_terminal_cas_before_emit_keeps_terminal` `test: persist_cas_then_drop_runtime_resume_keeps_terminal` `test: persist_panic_after_write_keeps_terminal_and_does_not_emit` `test: persist_succeeds_before_execution_succeeded_is_emitted` |
| Persist `Err` then later persist `Ok` of the same snapshot | sink gets the events for that snapshot (Started not dropped); sqlite event rows on shutdown flush | `test: persist_err_then_ok_emits_events_for_the_durable_snapshot` `test: transient_terminal_persist_err_shutdown_still_emits_execution_succeeded` `test: cancel_persist_err_then_shutdown_emits_execution_cancelled` `test: transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded` |
| sqlite event rows in the snapshot txn | rows exist; resume still snapshot-only | `test: event_rows_in_snapshot_txn_are_not_used_for_resume` `test: resume_reinvoke_emits_node_started_again` |
| Equal-revision `persist_with_events` (COMMIT then checkpoint busy, or second writer) | event row count unchanged; snapshot stays | `test: equal_revision_persist_does_not_grow_event_rows` `test: checkpoint_busy_after_terminal_commit_does_not_duplicate_events` |
| Resume of Waiting / same snapshot revision | no extra persist; event rows unchanged | `test: waiting_resume_does_not_append_event_rows` `test: resume_waiting_does_not_repersist_same_revision` |
| Transient persist `Err` of last apply (terminal or Drop-cancel), then clean Shutdown | file matches in-memory terminal; resume does not re-invoke | `test: transient_terminal_persist_err_shutdown_flushes_succeeded` `test: transient_cancel_persist_err_shutdown_flushes_cancelled` `test: transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded` `test: transient_cancel_persist_err_shutdown_flushes_sqlite_cancelled` `test: transient_terminal_persist_err_twice_shutdown_retries_until_ok` `test: transient_terminal_persist_err_twice_shutdown_retries_sqlite` |
| Seeded process-kill crash-inject (small DAGs, FakeClock, sqlite reopen) | recovery contract; seed printed on fail | `test: randomized_crash_inject_sqlite` |
| Schema / definition mismatch | fail-closed `Snapshot` / `DefinitionMissing` | `test: resume_schema_mismatch_is_snapshot_error` `test: schema_version_in_json_fail_closed` `test: resume_without_definition_is_definition_missing` |
| FailSubtree + AllDone crash mid-fanout | failed pages stay failed; reducer AllDone once | `test: crash_fail_subtree_pages_stay_failed` `test: resume_fail_subtree_keeps_failed_pages_and_runs_reducer` `test: fail_subtree_all_done_resume_runs_reducer_once` |
| Fail-fast crash after Failed persist | resume stays Failed; no resurrect | `test: crash_after_fail_fast_stays_failed` `test: resume_failed_execution_stays_failed` |
| `resume_with(RetryFailed)` after fail-fast diamond | Failed node re-invoked; Succeeded not; Cancelled → Pending then Ready | `test: resume_stays_failed_resume_with_retry_failed_reruns_b_only` `test: retry_failed_diamond_reruns_b_uncancels_c_d_keeps_a` |
| `resume_with(RetryFailed)` FailSubtree + AllDone | failed page retried; succeeded page not; Succeeded AllDone join re-runs once and sees p1's new Bytes | `test: resume_with_retry_failed_fail_subtree_all_done_retries_failed_page` |
| `resume_with(RetryFailed)` FailSubtree + AllDone join itself Failed | join stays Pending (remain 1) until retried pred Succeeded; then runs once and sees new Bytes | `test: resume_with_retry_failed_failed_all_done_join_waits_for_retried_pred` `test: retry_failed_all_done_failed_join_remain_waits_for_retried_pred` |
| `resume_with(RetryFailed)` on Succeeded / Waiting / Cancelled | `ResumeError::NotFailed` (`ApplyError::Illegal` only) | `test: resume_with_retry_failed_on_succeeded_is_not_failed` `test: resume_with_retry_failed_on_waiting_is_not_failed` `test: resume_with_retry_failed_on_cancelled_is_not_failed` |
| `start` after Failed | new `ExecutionId`; all nodes run | `test: start_after_failed_is_new_id_and_reruns_all_nodes` |
| Live handle + RetryFailed | `AlreadyActive` | `test: hitl_live_handle_retry_failed_is_already_active` |
| Waiting after drop handle | RetryFailed is `NotFailed`; Continue keeps token; Complete works | `test: resume_with_retry_failed_on_waiting_then_continue_keeps_token` |
| RetryFailed recover persist then kill before StartNode | store get is Ready-now (not Failed); Continue re-invokes | `test: retry_failed_recover_persist_then_kill_before_startnode_continue_reinvokes` |
| RetryFailed FailSubtree AllDone map-reduce × many, kill before StartNode | each get() is p1 Ready-now / join Pending; Continue re-invokes p1 + join | `test: retry_failed_all_done_map_reduce_recover_persist_kill_before_startnode_many` |
| RetryFailed fail-fast diamond × N | Failed critic retried; Succeeded research not; Cancelled writer runs | `test: retry_failed_fail_fast_diamond_times_n` |
| RetryFailed persist `Err` after apply | on-disk stays Failed; next `resume_with(RetryFailed)` works | `test: resume_with_retry_failed_persist_err_leaves_failed_then_retry_works` |
| RetryFailed resets `RetryPolicy` budget | attempt 0; dispatch 1; max_attempts is a new budget | `test: resume_with_retry_failed_resets_retry_policy_budget` |
| Adversarial RetryFailed | failed leaf re-run; persist-then-emit | `test: resume_with_retry_failed_reruns_failed_leaf_not_succeeded` |
| Builtin `wait` + in-process `Runtime::complete` | parks without register; second task Complete Bytes; next node sees them | `test: complete_from_second_task_unblocks_wait_and_downstream_sees_bytes` `test: wait_is_registered_without_manual_executor` |
| `complete` unknown token | `CompleteError::UnknownToken` | `test: complete_unknown_token_errors` `test: complete_unknown_token_is_unknown` |
| `complete` after Drop/cancel | `CompleteError::Cancelled`; run stays Cancelled | `test: complete_after_drop_handle_does_not_revive` |
| Engine-down complete (new Runtime, same store) | lease released or expired; new Runtime claims; apply + persist + drive | `test: complete_from_store_after_engine_down_unblocks_wait` `test: complete_after_sqlite_kill_new_runtime_unblocks_wait` |
| Shared MemoryStore, second Runtime resume | `ClaimedElsewhere` | `test: shared_memory_store_second_runtime_resume_is_claimed_elsewhere` |
| HTTP adapter `POST /start` | another process → `Runtime::start`; returns `ExecutionId`; live handle held so Drop does not cancel; terminals reaped on inspect of that id or on the next start; not idempotent | `test: client_start_inspect_complete_unblocks_wait` `test: client_start_then_inspect_is_waiting_not_cancelled` `test: client_two_starts_are_distinct_ids` `test: reap_held_terminals_clears_n_finished_keeps_park` `test: client_n_instant_http_starts_are_reaped_on_next_start` |
| HTTP start FailSubtree / AllDone | StartBody is durable_bytes; sibling stays Running; AllDone join still runs | `test: client_start_fail_subtree_keeps_running_sibling` `test: start_body_is_durable_bytes_not_snapshot` |
| HTTP FailSubtree reject + hold | reject returns immediately; sibling Running; server-drop still cancels | `test: client_fail_subtree_reject_returns_and_server_drop_cancels_sibling` |
| HTTP Reinvoke then old token | old approve/reject does not apply | `test: client_start_reinvoke_old_token_does_not_approve` `test: post_reinvoke_then_stale_complete_is_404` |
| HTTP start then drop server | App drop Drop-cancels remaining parks; terminal snapshot drops the handle | `test: client_drop_http_server_after_start_cancels_wait` `test: client_start_terminal_survives_server_drop` `test: client_approve_after_http_start_server_drop_is_409` `test: drop_held_if_terminal_keeps_waiting` |
| Duplicate approve after terminal handle Drop | 200 noop; node stays Succeeded; no second downstream. Live-park Drop stays 409 | `test: client_approve_after_terminal_handle_drop_is_200_noop` `test: client_duplicate_approve_is_noop` `test: complete_after_drop_of_terminal_handle_is_duplicate_noop` `test: complete_after_drop_handle_does_not_revive` |
| HTTP start + inspect + complete on sqlite | two Runtimes, one file; new id is not steal; other complete is ClaimedElsewhere | `test: http_sqlite_start_inspect_complete_two_runtimes_new_id_is_not_steal` |
| HTTP start two Runtimes one store | new id is not a steal; other complete is ClaimedElsewhere | `test: http_start_second_runtime_new_id_is_not_steal` `test: two_runtimes_inspect_does_not_steal_lease` `test: http_sqlite_start_inspect_complete_two_runtimes_new_id_is_not_steal` |
| `Runtime::cancel` by id | same inbox Cancel as handle; unknown is `UnknownExecution`; terminal is noop; stolen lease does not inject; engine-down store path persists Cancelled; persist Err is not Ok | `test: runtime_cancel_by_execution_id_cancels_parked_wait` `test: runtime_cancel_unknown_id_is_unknown_execution` `test: runtime_cancel_already_terminal_is_noop` `test: runtime_cancel_claimed_elsewhere_does_not_inject` `test: runtime_cancel_from_store_after_engine_down_is_cancelled` `test: runtime_cancel_and_handle_cancel_persist_same_cancelled_snapshot` `test: runtime_cancel_then_drop_handle_is_noop` `test: live_cancel_after_ttl_steal_is_claimed_elsewhere` `test: live_cancel_persist_err_is_not_ok` `test: runtime_cancel_store_persist_err_is_store` `test: runtime_cancel_at_T_does_not_dispatch_then_cancel` `test: cancel_already_terminal_is_noop` `test: cancel_when_deadline_already_due_does_not_dispatch` |
| HTTP `KeelClient::cancel` | start wait → cancel → Cancelled; later approve 409; one of two parks; unknown 404; Succeeded cancel is noop; reap then server-drop is noop; body dump is 413 | `test: client_start_wait_cancel_is_cancelled_approve_is_409` `test: client_cancel_one_of_two_http_starts_leaves_other_waiting` `test: client_cancel_unknown_id_is_404` `test: client_cancel_already_terminal_is_noop` `test: client_duplicate_cancel_is_noop` `test: client_http_cancel_reaps_handle_server_drop_is_noop` `test: client_cancel_in_process_waiting_without_http_hold` `test: client_cancel_with_body_is_413_does_not_cancel` |
| SDK loop example | engine `impl Executor` for `research`/`write`; client catalog → unregistered 400 names the id → inspect wait token → approve → write Succeeded with join bytes; second start cancel → 409 approve | `test: sdk_loop_approve_then_cancel_is_409` `example: crates/keel-rt-http/examples/sdk_loop.rs` |
| Custom node types | implement `Executor`, register before build, catalog lists ids + wait; subset missing is 400; empty id not in catalog; last register of same id wins; fail/panic inspect Failed; cancel/drop mid-Running custom cancels; empty Bytes inspect Succeeded; sqlite inspect Bytes without that adapter still reads | `test: custom_executor_types_catalog_start_approve_inspect` `test: custom_subset_registered_start_is_400_names_missing_only_nothing_runs` `test: register_empty_id_is_not_in_catalog` `test: register_same_id_twice_last_wins` `test: register_custom_executor_type_runs` `test: custom_adapter_failed_inspect_is_failed_not_succeeded` `test: custom_adapter_panic_inspect_is_failed` `test: client_cancel_mid_custom_running_is_cancelled` `test: client_drop_http_server_while_custom_running_cancels` `test: custom_empty_bytes_output_inspect_succeeded` `test: custom_double_register_last_wins_catalog_and_run` `test: custom_empty_register_id_is_not_in_catalog` `test: custom_catalog_without_secret_is_401` `test: http_sqlite_custom_executor_types_inspect_succeeded_bytes` |
| HTTP cancel secret / hung / no redirect | 401 verb-neutral; `Hung` at `HANG_BOUND`; 302 off loopback is Unexpected | `test: client_cancel_without_secret_is_401` `test: client_cancel_wrong_secret_is_401` `test: client_hung_cancel_is_hung_not_forever` `test: client_cancel_does_not_follow_redirect_off_loopback` `test: client_cancel_wire_sends_both_secret_headers` |
| HTTP cancel vs FailSubtree / in-flight approve | cancel of that run cancels Running sibling; other run's sibling stays Running; race lands Cancelled (no revive) or Succeeded (cancel noop) | `test: client_cancel_fail_subtree_cancels_running_sibling` `test: client_cancel_other_run_leaves_fail_subtree_sibling_running` `test: client_cancel_during_approve_does_not_revive` |
| HTTP start + cancel on sqlite | two Runtimes, one file; other cancel/complete is ClaimedElsewhere; new id is not steal | `test: http_sqlite_start_cancel_second_runtime_is_claimed_elsewhere` |
| `KeelClient::start` secret / hung / 400 / 413 | 401 verb-neutral; `Hung` at `HANG_BOUND`; unregistered 400 names ids (`KeelClientError::Unregistered`); empty 400 is `BadRequest`; oversized 413; no redirect | `test: client_start_without_secret_is_401` `test: client_start_wrong_secret_is_401` `test: client_hung_start_is_hung_not_forever` `test: client_start_unregistered_is_400_nothing_runs` `test: client_start_empty_definition_is_400` `test: client_start_oversized_body_is_413` `test: client_start_does_not_follow_redirect_off_loopback` |
| `KeelClient::executors` | `GET /executors` + same secret; ids from `Runtime::executor_ids` including builtin `wait` | `test: client_start_unregistered_is_400_nothing_runs` `test: client_executors_without_secret_is_401` |
| `KeelClient::start` wire | `POST /start` + `StartBody` + `X-Keel-Complete` and Bearer; not query | `test: client_start_wire_sends_both_secret_headers` `test: client_start_succeeds_against_bearer_only_server` |
| `KeelClient::approve` / `reject` | `Decision::Complete(bytes)` / `Decision::Fail` via `POST /approve` / `POST /reject` (aliases over complete apply); wait Succeeded + downstream / execution Failed; empty approve 400; 400KiB is base64 not array-413 | `test: client_start_inspect_approve_unblocks_wait` `test: client_start_inspect_reject_fails_execution` `test: client_decision_fail_fails_execution` `test: post_approve_empty_body_is_400_does_not_complete` `test: client_approve_400kib_is_200_not_json_array_413` `test: two_clients_approve_and_complete_one_token_downstream_runs_once` `test: client_live_approve_after_ttl_steal_is_claimed_elsewhere` |
| HTTP Succeeded bytes are one wire | complete / inspect / approve use `wire_bytes` base64; kernel Resume `[u8]` stays in-process; 400KiB complete is 200 | `test: client_complete_400kib_is_200_not_json_array_413` `test: complete_body_json_succeeded_is_base64_not_array` `test: client_approve_400kib_is_200_not_json_array_413` `test: inspect_view_json_1mib_succeeded_is_compact_base64` |
| approve of a Running-node token | wait stays Waiting | `test: client_approve_issued_token_for_running_node_leaves_wait_parked` |
| HTTP adapter `POST /complete` | another process → `Runtime::complete`; secret required; loopback default | `test: post_complete_unblocks_wait_node` `test: post_without_secret_is_401` `test: post_wrong_secret_is_401` (crate `keel-rt-http`) |
| HTTP adapter `GET /inspect/:id` | same secret as complete; unknown 404; terminal/Cancelled 200 with state | `test: client_inspect_unknown_id_is_404` `test: client_inspect_after_drop_handle_is_cancelled_complete_409` |
| `KeelClient::inspect` then `complete` | other process reads wait token, completes; engine unblocks; downstream sees bytes | `test: client_inspect_then_complete_unblocks_wait` |
| `KeelClient::inspect` while Running then wait | no wait token yet; Running-node tokens omitted from InspectView; after park, wait token is present | `test: client_inspect_while_running_has_no_token_then_wait_sees_token` `test: client_complete_of_running_handle_token_does_not_unblock_wait` |
| mixed Running sibling + parked wait | execution is Running; InspectView still exposes the wait token only; complete of the Running handle token does not unblock; complete of the wait token finishes only that node | `test: client_inspect_wait_sibling_while_running_completes_only_wait` |
| issued token for a Running node | inspect JSON has the wait token once and not the Running resume token; complete of `ResumeToken::issue` for the Running node leaves wait Waiting | `test: client_complete_issued_token_for_running_node_leaves_wait_parked` `test: inspect_view_json_running_pred_does_not_contain_running_token` |
| InspectView JSON is a DTO | parked wait JSON has the token once in `InspectNodeState::Waiting`; Succeeded owns base64 `output`; non-Succeeded omit output/last_error; 1MiB is not a u8 array bomb; HTTP inspect over `MAX_BODY` is 413 | `test: inspect_view_json_has_exactly_one_wait_token` `test: inspect_view_json_succeeded_owns_output` `test: inspect_view_json_non_succeeded_has_no_output_or_error` `test: inspect_view_json_1mib_succeeded_is_compact_base64` `test: client_inspect_over_max_body_is_413` `test: resume_token_helper_reads_waiting_state` `test: client_inspect_then_complete_unblocks_wait` `test: client_inspect_research_hold_write_outputs_are_per_node` |
| two Runtimes, inspect then complete | inspect is read-only (store, does not steal); complete on the non-owner is ClaimedElsewhere → **423 Locked** `{"error":"claimed_elsewhere"}`, not 400/401/404/409; owner still completes | `test: two_runtimes_inspect_does_not_steal_lease` `test: post_claimed_elsewhere_is_423_locked_not_409` |
| `KeelClient::inspect` missing/wrong secret / hung / no redirect | 401 Display is verb-neutral (no word `complete`); `Hung` at hang-bound; 302 off loopback is Unexpected | `test: client_inspect_without_secret_is_401` `test: client_inspect_wrong_secret_is_401` `test: client_inspect_401_error_text_does_not_say_complete` `test: client_hung_inspect_is_hung_not_forever` `test: client_inspect_does_not_follow_redirect_off_loopback` |
| `KeelClient::inspect` wire | `GET /inspect/:id` + `X-Keel-Complete` and Bearer; not query | `test: client_inspect_wire_sends_both_secret_headers` `test: client_inspect_succeeds_against_bearer_only_server` |
| `Runtime::inspect` live vs store | after `NodeWaiting` emit, live token == store token == event token; inspect does not emit | `test: inspect_after_node_waiting_matches_store_and_does_not_emit` |
| `KeelClient::complete` same JSON | other binary POSTs token + Resume; Decision maps onto Resume; Succeeded bytes are `wire_bytes` base64 (not kernel `[u8]`) | `test: client_complete_unblocks_wait_node` `test: client_decision_complete_unblocks_wait_node` `test: client_complete_400kib_is_200_not_json_array_413` `test: complete_body_json_succeeded_is_base64_not_array` |
| `KeelClient::complete` missing/wrong secret | 401 Unauthorized; does not complete | `test: client_without_secret_is_401` `test: client_wrong_secret_is_401` |
| `KeelClient::complete` after drop handle | 409 Cancelled; does not revive | `test: client_after_drop_handle_is_409_does_not_revive` |
| `KeelClient::complete` duplicate / oversized | 200 noop / 413; snapshot stays Waiting | `test: client_duplicate_complete_is_noop` `test: client_oversized_body_is_413_does_not_complete` |
| `KeelClient` wire protocol | `POST /complete` + `CompleteBody` + `X-Keel-Complete` and Bearer; not query | `test: client_wire_is_complete_body_and_secret_header` `test: client_complete_succeeds_against_bearer_only_server` |
| two `KeelClient` values one token | Decision vs Resume same bytes; downstream once | `test: two_keel_clients_one_token_downstream_runs_once` |
| `KeelClient` no redirect | 302 to `0.0.0.0` is Unexpected(302); trap is not hit | `test: client_does_not_follow_redirect_off_loopback` |
| `KeelClient` hung server | `Hung` at `HANG_BOUND`; paused time, no wall sleep | `test: client_hung_server_is_hung_not_forever` |
| `KeelClient` drop mid-POST | drop server → Transport, token unused; drop inflight → still Waiting | `test: client_drop_server_mid_post_is_transport_token_untouched` `test: client_drop_inflight_does_not_complete` |
| `Decision::Fail` via client | fail-fast Failed; maps onto Resume only | `test: client_decision_fail_fails_execution` |
| HTTP missing/wrong secret | 401; does not complete | `test: post_without_secret_is_401` `test: post_wrong_secret_is_401` `test: post_query_secret_is_still_401` |
| HTTP oversized body | 413/400; snapshot stays Waiting | `test: post_oversized_body_is_413_does_not_complete` |
| HTTP replay after success / cancel | 200 noop / 409 Cancelled | `test: post_duplicate_complete_is_200_noop` `test: post_after_cancel_is_409_does_not_revive` |
| Two HTTP completes one token | one Succeeded; downstream once | `test: two_http_completes_one_token_downstream_runs_once` |
| HTTP Reinvoke then stale Complete | new token; old token 404 | `test: post_reinvoke_then_stale_complete_is_404` |
| `ResumeToken` nonce | 128-bit mix; not sequential ints; not guessable from id | `test: resume_tokens_are_not_sequential_ints` `test: guessed_sequential_nonces_do_not_complete` |
| Token binds execution | A's token does not complete B | `test: complete_token_from_a_does_not_apply_to_b` |
| persist Err on complete | snapshot stays Waiting; retry works; complete Ok only after persist Ok | `test: complete_store_persist_err_is_store` `test: live_complete_persist_err_is_not_ok` |
| persist Err on cancel | store stays Waiting; live inspect is Waiting (not in-memory Cancelled); retry works | `test: live_cancel_persist_err_is_not_ok` `test: live_cancel_persist_err_inspect_is_waiting` `test: handle_cancel_persist_err_inspect_is_waiting` `test: runtime_cancel_store_persist_err_is_store` |
| Wait is not Ready{T} | clock advance does not auto-complete | `test: wait_is_waiting_not_ready_t_and_clock_does_not_complete` |
| complete vs fail-fast cancel | Cancelled; does not revive | `test: complete_while_fail_fast_already_cancelled_wait` |
| 256 concurrent waits then complete | hang bound still cancels | `test: complete_256_wait_nodes_then_hang_bound_cancels` |
| Two Runtimes one file both complete | A owns; B `ClaimedElsewhere`; drop A (or TTL) then B Ok | `test: two_runtimes_same_file_both_may_complete` |
| Live `complete` after TTL steal | A still has a handle; B claimed; A `complete` is `ClaimedElsewhere` (no inject, no downstream) | `test: live_complete_after_ttl_steal_is_claimed_elsewhere` |
| Live `cancel` after TTL steal | A still has live_tx; B claimed; A `cancel` is `ClaimedElsewhere` (no inject) | `test: live_cancel_after_ttl_steal_is_claimed_elsewhere` `test: client_live_cancel_after_ttl_steal_is_claimed_elsewhere` |
| Handle `resume` after TTL steal | A’s `ExecutionHandle::resume` is ClaimedElsewhere-equivalent; no inject, no downstream | `test: handle_resume_after_ttl_steal_is_claimed_elsewhere` |
| Crash after Running persist | file reopens (no leaked lock); Running re-invoked | `test: crash_after_running_persist_releases_lock_and_reinvokes` `test: crash_during_b_running_reinvokes_b_not_a` |
| Drop handle after Running persist | **graph** cancel; resume stays Cancelled (not crash) | `test: drop_handle_after_running_persist_cancels_not_reinvoke` |
| Fat `Bytes` snapshot | MemoryStore refcount; sqlite JSON copy preserves bytes | `test: fat_bytes_resume_join_is_refcount` `test: fat_bytes_sqlite_round_trip_preserves_bytes` `test: fat_payloads_64kib_times_eight_persist_resume` `test: fat_payloads_64kib_times_32_persist_resume_within_bound` `test: fat_bytes_join_input_is_refcount_not_copy` |
| Sqlite busy / locked / truncated / empty | typed `StoreError` or empty db + unknown id; no panic | `test: locked_file_is_typed_error_not_panic` `test: persist_under_lock_returns_within_busy_bound` `test: truncated_file_is_typed_error` `test: empty_file_opens_as_new_store` `test: corrupt_snapshot_json_is_typed_error` |
| Torn WAL / crash mid-`put` (uncommitted BEGIN) | typed error or successful recover; never a silent wrong terminal | `test: truncated_wal_does_not_invent_a_terminal` `test: crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` |
| Incremental persist after first write | Pending nodes stay in the file (dirty list is not the whole graph) | `test: incremental_persist_keeps_pending_nodes` `test: incremental_persist_does_not_delete_unchanged_rows` `test: incremental_persist_256_wide_succeeded_does_not_drop_pending` |
| Two persist calls / kill before second COMMIT | first snapshot kept; second turn not merged into one txn | `test: second_uncommitted_persist_does_not_merge_into_first_commit` `test: crash_mid_put_rolls_back_uncommitted_and_does_not_invent_terminal` `test: crash_after_terminal_cas_before_emit_keeps_terminal` |
| WAL checkpoint TRUNCATE | after COMMIT of a terminal only; autocommit; checkpoint Err does not fail persist | `test: checkpoint_runs_only_after_commit_of_terminal` `test: two_hundred_start_crash_resume_wal_bounded` `test: checkpoint_busy_after_terminal_commit_does_not_duplicate_events` `test: checkpoint_busy_after_commit_is_persist_ok_and_resume_sees_succeeded` `test: equal_revision_persist_does_not_grow_event_rows` |
| CAS after incremental rows | stale put loses | `test: stale_put_after_incremental_dirty_rows_loses` `test: stale_put_does_not_clobber` |
| `synchronous=FULL` default vs `open_fast` NORMAL | FULL is default; both recover process-kill | `test: open_default_is_synchronous_full` `test: open_fast_is_synchronous_normal` `test: durable_is_synchronous_full` `test: resume_256_wide_full_vs_normal` |
| Two connections persist two executions, one file | no panic; `Ok` ⇒ row exists; `SQLITE_BUSY` is typed | `test: concurrent_persist_two_executions_same_file_no_panic` |
| Hourglass neck Running crash/resume | sources not re-run; sinks run after neck | `test: crash_hourglass_neck_running_resume_runs_sinks_not_sources` `test: crash_resume_hourglass_256_within_bound` |
| `resume` unknown id | `ResumeError::UnknownExecution` | `test: resume_unknown_id_is_unknown_execution` `test: resume_unknown_id_on_file` |
| MemoryStore after “process death” | new store has no history | `test: memory_store_does_not_survive_process_death` |
| 50–200 sequential start-crash-resume diamonds, one file | no leaked locks; WAL checkpointed (`TRUNCATE` on terminal) | `test: fifty_start_crash_resume_diamonds_one_file` `test: two_hundred_start_crash_resume_wal_bounded` |
| Crash/resume same diamond N times | attempt climbs only on the in-flight node | `test: crash_resume_same_diamond_attempt_climbs_only_on_inflight` `test: twenty_diamond_repeat_crash_resume_within_bound` |

## Stress (CI `stress-resume`)

| Shape | Bound | Proof |
|---|---|---|
| Resume 256-wide Ready snapshot (debug, n=3) | 8s per sample; FULL vs NORMAL recorded | `test: resume_256_wide_snapshot_within_bound` `test: resume_256_wide_full_vs_normal` |
| Persist 256-wide first + incremental Start | 8s | `test: persist_256_wide_apply_within_bound` |
| Hourglass-256 crash/resume | 20s | `test: crash_resume_hourglass_256_within_bound` |
| 1000 sequential 1-node DAGs, resume last | 30s | `test: one_thousand_sequential_dags_resume_last` |
| 50 start-crash-resume diamonds, one file | 10s; WAL < 8 MiB | `test: fifty_start_crash_resume_diamonds_one_file` |
| 200 start-crash-resume diamonds, one file | 40s; WAL bounded | `test: two_hundred_start_crash_resume_wal_bounded` |
| Same diamond crash/resume N=8 / N=20 | attempt climbs; src not re-run | `test: crash_resume_same_diamond_attempt_climbs_only_on_inflight` `test: twenty_diamond_repeat_crash_resume_within_bound` |
| Fat 64KiB × 32 persist+resume | 10s | `test: fat_payloads_64kib_times_32_persist_resume_within_bound` |
| 2k-wide snapshot resume (debug) | 120s | `test: resume_2k_wide_snapshot_debug_within_bound` |
| 10k-wide snapshot resume | release only (debug ~350 s extrapolated; 2k debug is 14 s) | `test: resume_10k_wide_snapshot_release_within_bound` |

Numbers: [`benches/BASELINE.md`](../benches/BASELINE.md) (sqlite vs MemoryStore, labeled).
Standing high-load / messy-user attacks (not coverage): [`docs/CHAOS_LOG.md`](CHAOS_LOG.md).

## Store lease + epoch fence

`AlreadyActive` is still **per Runtime** (same process, same `Runtime`).
Two `Runtime`s sharing one store (one sqlite file, or one `MemoryStore`)
are fenced by `StateStore::claim` / `heartbeat` / `release`. A live lease
for another owner is `ClaimedElsewhere`. Persist / complete carry the
epoch; a stale epoch is `StoreError::StaleEpoch`. Default lease TTL is
30s (`DEFAULT_LEASE_TTL`); `Clock` `now`, not a wall sleep in apply.
Dead process: the other Runtime claims after TTL (or immediately after
`Drop` of the owning Runtime, which `release`s). Live A heartbeats so B
cannot steal. Two `MemoryStore`s are two worlds (no shared map). HTTP
`POST /complete` stays on the owning process (secret + loopback); the
other binary does not open the sqlite file while A lives. ADR 0004.

- Default join is `Join::AllSucceeded`. Default `OnFailure` is `FailExecution`.
- Waiting is a node state. Retry delay is `Ready { runnable_at }` (snapshot `Timestamp` T).
- Persist succeeds, then the sink is told. No persist queue (ADR 0001).
- Kernel has no cron, no wall timezone, no sqlite timer table. Waiting for T is the Runtime drive (`Clock::wait_until`), not the scheduler.
- sqlite parked Ready{T} JSON keeps a short `last_error` so inspect agrees with MemoryStore. Old adapters that ignore `nodes.runnable_at` would see Ready-now.
