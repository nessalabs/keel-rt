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
    ] {
        assert!(tests.contains(name), "schedule.rs missing {name}");
    }
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
