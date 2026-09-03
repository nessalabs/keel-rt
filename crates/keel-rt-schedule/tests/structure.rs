//! Adapter sits outside the kernel. Deleting this crate must not edit scheduler.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn crate_src() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

#[test]
fn kernel_package_does_not_depend_on_this_adapter() {
    let cargo = fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let pkg = cargo.split("[workspace]").next().unwrap_or(&cargo);
    assert!(
        !pkg.contains("keel-rt-schedule") && !pkg.contains("croner") && !pkg.contains("cron"),
        "keel-rt must not depend on the schedule crate or a cron parser"
    );
}

#[test]
fn scheduler_does_not_name_cron() {
    let sched = fs::read_to_string(root().join("src/runtime/scheduler.rs")).unwrap();
    assert!(
        !sched.to_lowercase().contains("cron"),
        "scheduler.rs names cron"
    );
    assert!(!sched.contains("crontab"), "scheduler.rs names crontab");
}

#[test]
fn adapter_depends_on_keel_rt() {
    let cargo = fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/Cargo.toml").unwrap();
    assert!(cargo.contains("keel-rt"), "adapter must depend on keel-rt");
    assert!(
        !cargo.contains("rusqlite") && !cargo.contains("axum") && !cargo.contains("hyper"),
        "schedule crate must not pull sqlite or HTTP"
    );
}

#[test]
fn schedule_src_has_no_http_sqlite_or_hitl() {
    let mut stack = vec![crate_src()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|s| s.to_str()) != Some("rs") {
                continue;
            }
            let s = fs::read_to_string(&p).unwrap();
            for banned in [
                "rusqlite",
                "axum",
                "hyper",
                "reqwest",
                "Agent",
                "HITL",
                "HumanApproval",
                "Human in the loop",
            ] {
                assert!(!s.contains(banned), "{} contains {banned}", p.display());
            }
        }
    }
}

#[test]
fn spec_has_no_sleep_or_ready_t() {
    let spec = fs::read_to_string(crate_src().join("spec.rs")).unwrap();
    assert!(!spec.contains("sleep"), "spec.rs must not sleep");
    assert!(!spec.contains("wait_until"), "wait lives in runner.rs");
    assert!(!spec.contains("runnable_at"), "must not write Ready{{T}}");
    assert!(!spec.contains("CREATE TABLE"), "no timer / schedule table");
    let runner = fs::read_to_string(crate_src().join("runner.rs")).unwrap();
    assert!(
        runner.contains("clock.wait_until"),
        "drive must wait with Clock::wait_until"
    );
    assert!(!runner.contains("CREATE TABLE"));
}

#[test]
fn required_schedule_tests_exist() {
    let tests =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/schedule.rs").unwrap();
    for name in [
        "fn monday_nine_fires_exactly_one_start",
        "fn same_window_does_not_fire_twice",
        "fn pause_across_two_mondays_is_one_catch_up_start",
        "fn drop_runner_stops_further_starts",
        "fn arm_before_exact_t_then_set_t_fires_once",
        "fn start_at_exact_monday_nine_skips_this_slot",
        "fn fire_is_start_not_ready_t",
        "fn drop_runtime_arc_while_armed_next_tick_still_starts",
        "fn tick_start_err_does_not_hang_or_skip_sibling",
        "fn tick_store_put_fail_does_not_kill_ticker",
        "fn clock_jump_backward_after_fire_does_not_refire",
        "fn two_running_schedules_on_one_runtime_each_start",
        "fn vancouver_spring_forward_skips_missing_local_minute",
        "fn vancouver_fall_back_picks_next_occurrence_not_both",
        "fn next_after_max_is_none_not_due_now",
        "fn catch_up_200k_periods_is_one_start",
        "fn jump_to_timestamp_max_is_one_start_not_due_now",
        "fn drop_without_clock_advance_exits_stuck_armed",
        "fn executor_panic_ticker_survives",
        "fn max_starts_per_wake_does_not_drop_fires",
        "fn clock_jump_backward_while_armed_does_not_fire",
        "fn same_cron_different_definitions_both_start",
        "fn drop_runner_at_exact_t_is_one_start_not_two",
        "fn two_runtimes_two_schedules_each_start",
        "fn jump_to_max_two_jobs_each_one_start",
    ] {
        assert!(tests.contains(name), "schedule.rs missing {name}");
    }
}

/// The crate is spec.rs + runner.rs. No second workflow engine.
#[test]
fn crate_is_spec_and_runner_only() {
    let src = crate_src();
    let mut files = Vec::new();
    for e in fs::read_dir(&src).unwrap() {
        let p = e.unwrap().path();
        assert!(p.is_file(), "no src subdirs: {}", p.display());
        files.push(p.file_name().unwrap().to_string_lossy().into_owned());
    }
    files.sort();
    assert_eq!(files, vec!["lib.rs", "runner.rs", "spec.rs"]);
}

#[test]
fn public_types_are_exactly_the_ticker() {
    let lib = fs::read_to_string(crate_src().join("lib.rs")).unwrap();
    assert!(lib.contains("pub use runner::{RunningSchedule, Schedule, ScheduleBuilder}"));
    assert!(lib.contains("pub use spec::{ScheduleSpec, SpecError}"));
    let mut structs = Vec::new();
    let mut enums = Vec::new();
    for name in ["lib.rs", "runner.rs", "spec.rs"] {
        let s = fs::read_to_string(crate_src().join(name)).unwrap();
        for line in s.lines() {
            let t = line.trim();
            if let Some(rest) = t.strip_prefix("pub struct ") {
                structs.push(rest.split_whitespace().next().unwrap().to_string());
            }
            if let Some(rest) = t.strip_prefix("pub enum ") {
                enums.push(rest.split_whitespace().next().unwrap().to_string());
            }
        }
    }
    structs.sort();
    enums.sort();
    assert_eq!(
        structs,
        vec![
            "RunningSchedule",
            "Schedule",
            "ScheduleBuilder",
            "ScheduleSpec"
        ]
    );
    assert_eq!(enums, vec!["SpecError"]);
}

#[test]
fn src_has_no_second_engine_or_schedule_state() {
    let mut stack = vec![crate_src()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|s| s.to_str()) != Some("rs") {
                continue;
            }
            let s = fs::read_to_string(&p).unwrap();
            for banned in [
                "ScheduleState",
                "ExecutionState",
                "EventSink",
                "LeaseEpoch",
                "ClaimError",
                "RetryPolicy",
                "ResumeToken",
                "remain_pred",
                "EventLog",
                "runnable_at",
                "CREATE TABLE",
                "rusqlite",
                "Heartbeat",
                "Supervisor",
                "JobRegistry",
            ] {
                assert!(
                    !s.contains(banned),
                    "{} contains banned {banned}",
                    p.display()
                );
            }
        }
    }
}

#[test]
fn runner_is_one_drive_loop_with_a_heap() {
    let runner = fs::read_to_string(crate_src().join("runner.rs")).unwrap();
    assert!(
        runner.contains("BinaryHeap"),
        "100k armed specs need a next-T heap, not a linear min() each tick"
    );
    assert!(
        runner.matches("async fn drive").count() == 1,
        "exactly one drive loop"
    );
    assert_eq!(
        runner.matches("tokio::spawn(drive").count(),
        1,
        "one drive task, not a spawn per spec"
    );
    let lines = runner.lines().count();
    assert!(
        lines <= 240,
        "runner.rs is {lines} lines — a second scheduler, cut it"
    );
    let spec = fs::read_to_string(crate_src().join("spec.rs")).unwrap();
    let spec_lines = spec.lines().count();
    assert!(spec_lines <= 180, "spec.rs is {spec_lines} lines");
    assert!(
        !runner.contains("struct Armed"),
        "heap entries are (T, index); do not store a spec per heap node"
    );
}

#[test]
fn no_fire_history_vec() {
    let runner = fs::read_to_string(crate_src().join("runner.rs")).unwrap();
    for banned in ["last_fired", "fire_log", "fired_at", "history"] {
        assert!(!runner.contains(banned), "runner stores {banned}");
    }
}

#[test]
fn architecture_mermaid_names_schedule_crate() {
    let arch = fs::read_to_string(root().join("docs/ARCHITECTURE.md")).unwrap();
    assert!(arch.contains("```mermaid"), "ARCHITECTURE missing mermaid");
    for name in [
        "keel-rt-schedule",
        "ScheduleSpec",
        "SpecError",
        "ScheduleBuilder",
        "RunningSchedule",
        "Runtime::start",
    ] {
        assert!(arch.contains(name), "ARCHITECTURE missing {name}");
    }
    assert!(
        arch.contains("sched --> crate"),
        "schedule must depend on keel-rt, not reverse"
    );
}

#[test]
fn baseline_has_numbered_schedule_farm_release_row() {
    let base = fs::read_to_string(root().join("benches/BASELINE.md")).unwrap();
    assert!(
        base.contains("## Schedule ticker"),
        "BASELINE missing schedule section"
    );
    assert!(
        base.contains("release after"),
        "BASELINE must number release farm RSS/time"
    );
    assert!(base.contains("100 000") || base.contains("100000"));
}

#[test]
fn runner_has_no_persist_emit_or_node_ready() {
    let runner = fs::read_to_string(crate_src().join("runner.rs")).unwrap();
    let spec = fs::read_to_string(crate_src().join("spec.rs")).unwrap();
    for s in [&runner, &spec] {
        for banned in ["persist", "emit", "announce", "NodeReady", "EventLog"] {
            assert!(!s.contains(banned), "schedule src names {banned}");
        }
    }
    assert!(
        !runner.contains("fn wait_until"),
        "WallClock must use Clock default wait_until (sleep), not override it"
    );
    assert!(runner.contains("clock.wait_until"));
}

#[test]
fn kernel_src_has_no_cron_types() {
    let mut stack = vec![root().join("src")];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|s| s.to_str()) != Some("rs") {
                continue;
            }
            let s = fs::read_to_string(&p).unwrap();
            for banned in ["croner", "crontab", "keel-rt-schedule", "chrono_tz"] {
                assert!(
                    !s.contains(banned),
                    "kernel {} contains {banned}",
                    p.display()
                );
            }
        }
    }
}
