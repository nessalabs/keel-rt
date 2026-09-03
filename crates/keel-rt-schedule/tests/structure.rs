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
