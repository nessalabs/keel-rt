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
