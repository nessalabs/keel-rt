//! Adapter sits outside the kernel. Deleting this crate must not edit scheduler.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn kernel_package_does_not_depend_on_this_adapter() {
    let cargo = fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let pkg = cargo.split("[workspace]").next().unwrap_or(&cargo);
    assert!(
        !pkg.contains("keel-rt-sqlite") && !pkg.contains("rusqlite"),
        "keel-rt must not depend on the adapter or rusqlite"
    );
}

#[test]
fn scheduler_does_not_name_the_file_store() {
    let sched = fs::read_to_string(root().join("src/runtime/scheduler.rs")).unwrap();
    assert!(!sched.contains("sqlite"), "scheduler.rs names a file store");
    assert!(!sched.contains("rusqlite"), "scheduler.rs names rusqlite");
    assert!(!sched.contains("postgres"), "scheduler.rs names postgres");
}

#[test]
fn adapter_depends_on_keel_rt() {
    let cargo = fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/Cargo.toml").unwrap();
    assert!(cargo.contains("keel-rt"), "adapter must depend on keel-rt");
}

#[test]
fn chaos_log_names_the_standing_pack() {
    let log = fs::read_to_string(root().join("docs/CHAOS_LOG.md")).unwrap();
    for name in [
        "two_thousand_short_jobs_one_file",
        "wide_256_and_join_crash_resume",
        "wide_256_resume_under_concurrent_starts_is_sqlite_bound",
        "wide_2k_and_join_crash_resume_of_ready",
        "and_join_1pm_waiting_4pm_delay_crash_resume",
        "drop_handle_mid_persist_cancels_not_succeed",
        "two_runtimes_diamond_no_silent_wrong_terminal",
        "transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded",
        "transient_terminal_persist_err_twice_shutdown_retries_sqlite",
        "randomized_crash_inject_sqlite",
    ] {
        assert!(log.contains(name), "CHAOS_LOG missing {name}");
    }
    let chaos =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/chaos.rs").unwrap();
    for name in [
        "fn two_thousand_short_jobs_one_file",
        "fn wide_256_resume_under_concurrent_starts_is_sqlite_bound",
        "fn wide_2k_and_join_crash_resume_of_ready",
    ] {
        assert!(chaos.contains(name), "chaos.rs missing {name}");
    }
    let inject =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/crash_inject.rs")
            .unwrap();
    for name in [
        "fn randomized_crash_inject_sqlite",
        "fn clock_jump_backward_after_runnable_at_persist_does_not_fire",
        "fn non_terminal_ready_delay_persist_err_then_crash_skips_uncommitted_delay",
    ] {
        assert!(inject.contains(name), "crash_inject.rs missing {name}");
    }
}

#[test]
fn adapter_has_no_timer_table_and_persists_snapshot_deadline() {
    let lib = fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/src/lib.rs").unwrap();
    assert!(
        !lib.contains("CREATE TABLE") || !lib.to_lowercase().contains("create table timers"),
        "sqlite must not grow a timer table; T lives on the snapshot"
    );
    assert!(!lib.contains("CREATE TABLE IF NOT EXISTS timers"));
    assert!(
        lib.contains("runnable_at INTEGER"),
        "T must be a nodes column, not a timer table"
    );
    for col in ["owner TEXT", "epoch INTEGER", "lease_until INTEGER"] {
        assert!(
            lib.contains(col),
            "lease column {col} must live on executions"
        );
    }
    assert!(
        lib.contains("fn claim_conn") && lib.contains("BEGIN IMMEDIATE"),
        "claim SQL lives in keel-rt-sqlite"
    );
    let claim = lib
        .split("fn claim_conn")
        .nth(1)
        .and_then(|s| s.split("fn heartbeat_conn").next())
        .unwrap_or("");
    assert!(
        !claim.contains("INSERT OR REPLACE"),
        "claim must not use INSERT OR REPLACE"
    );
    for table in ["executions", "nodes", "definitions", "events"] {
        assert!(
            lib.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")),
            "expected table {table}"
        );
    }
    let resume =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/resume.rs").unwrap();
    for name in [
        "fn crash_resume_full_file_keeps_deadline",
        "fn sqlite_deadline_persist_does_not_drop_or_double_fire",
        "fn crash_after_timeout_persisted_before_dispatch_does_not_double_run",
        "fn incremental_persist_does_not_drop_runnable_at",
        "fn crash_resume_256_parked_advance_once_each_once",
        "fn persist_resume_256_runnable_at_set_vs_unset",
        "fn incremental_fire_does_not_overwrite_sibling_runnable_at",
        "fn busy_on_park_persist_rolls_back_then_t_lands",
        "fn reader_lock_does_not_block_park_persist",
        "fn many_park_wakes_wal_stays_bounded",
        "fn retry_failed_recover_persist_then_kill_before_startnode_continue_reinvokes",
        "fn retry_failed_all_done_map_reduce_recover_persist_kill_before_startnode_many",
        "fn complete_after_sqlite_kill_new_runtime_unblocks_wait",
        "fn two_runtimes_same_file_both_may_complete",
        "fn resume_running_custom_without_adapter_is_unregistered",
    ] {
        assert!(resume.contains(name), "resume.rs missing {name}");
    }
    let http =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/http.rs").unwrap();
    assert!(
        http.contains("fn http_sqlite_start_inspect_complete_two_runtimes_new_id_is_not_steal"),
        "http.rs must lock HTTP start+inspect+complete on sqlite (two Runtimes)"
    );
    assert!(
        http.contains("fn http_sqlite_custom_executor_types_inspect_succeeded_bytes"),
        "http.rs must lock custom Executor types on sqlite (inspect Bytes, subset 400)"
    );
    assert!(
        http.contains("fn http_sqlite_start_cancel_second_runtime_is_claimed_elsewhere"),
        "http.rs must lock HTTP start+cancel on sqlite (ClaimedElsewhere)"
    );
    let cargo = fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/Cargo.toml").unwrap();
    assert!(
        cargo.contains("keel-rt-http"),
        "sqlite tests-only dep on keel-rt-http; HTTP src must not name SqliteStore"
    );
    assert!(
        lib.contains("fn dirty_persist_ready_t_to_t_prime_updates_only_runnable_at"),
        "lib.rs must prove persist() dirty T→T' updates only the column"
    );
    assert!(
        lib.contains("fn parked_ready_t_uses_column_omits_nested_json_keeps_last_error"),
        "lib.rs must lock last_error round-trip on sqlite park get"
    );
}
