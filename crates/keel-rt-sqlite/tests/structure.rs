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
    ] {
        assert!(resume.contains(name), "resume.rs missing {name}");
    }
}
