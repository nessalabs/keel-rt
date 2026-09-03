//! Absences for the HTTP adapter + CompleteClient. Not AGENTS.md.

use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn crate_src() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: PathBuf) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

fn contains_word(src: &str, word: &str) -> bool {
    let b = src.as_bytes();
    let w = word.as_bytes();
    let mut i = 0;
    while i + w.len() <= b.len() {
        if &b[i..i + w.len()] == w {
            let before = i == 0 || !is_ident(b[i - 1]);
            let after = i + w.len() == b.len() || !is_ident(b[i + w.len()]);
            if before && after {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

#[test]
fn kernel_package_does_not_depend_on_this_adapter() {
    let cargo = fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let pkg = cargo.split("[workspace]").next().unwrap_or(&cargo);
    assert!(
        !pkg.contains("keel-rt-http") && !pkg.contains("reqwest") && !pkg.contains("axum"),
        "keel-rt must not depend on the HTTP adapter"
    );
}

#[test]
fn adapter_depends_on_keel_rt_and_not_sqlite_or_schedule() {
    let cargo = fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/Cargo.toml").unwrap();
    assert!(cargo.contains("keel-rt"), "adapter must depend on keel-rt");
    for banned in ["rusqlite", "keel-rt-sqlite", "keel-rt-schedule", "croner"] {
        assert!(
            !cargo.contains(banned),
            "http crate Cargo.toml names {banned}"
        );
    }
}

#[test]
fn client_src_has_no_sqlite_schedule_or_hitl_names() {
    for p in rust_files(crate_src()) {
        let s = fs::read_to_string(&p).unwrap();
        for banned in [
            "rusqlite",
            "ScheduleState",
            "ScheduleSpec",
            "schedule_due",
            "next_wake_at",
            "HITL",
            "hitl",
            "Human",
            "Approve",
            "Reject",
            "claim",
            "lease",
            "SqliteStore",
            "MemoryStore",
            "StateStore",
        ] {
            assert!(
                !contains_word(&s, banned),
                "{} contains banned {banned}",
                p.display()
            );
        }
    }
}

#[test]
fn kernel_src_still_has_no_http_agent_or_hitl() {
    for p in rust_files(root().join("src")) {
        let s = fs::read_to_string(&p).unwrap();
        for banned in [
            "keel-rt-http",
            "CompleteClient",
            "reqwest",
            "axum",
            "hyper",
            "HITL",
            "hitl",
            "Agent",
        ] {
            assert!(
                !contains_word(&s, banned),
                "kernel {} contains {banned}",
                p.display()
            );
        }
    }
}

#[test]
fn public_surface_is_complete_resume_token() {
    let lib = fs::read_to_string(crate_src().join("lib.rs")).unwrap();
    assert!(lib.contains(
        "pub use client::{CompleteClient, CompleteClientError, Decision, COMPLETE_HANG_BOUND}"
    ));
    assert!(lib.contains("pub struct CompleteBody"));
    assert!(lib.contains("pub struct CompleteSecret"));
    let client = fs::read_to_string(crate_src().join("client.rs")).unwrap();
    assert!(client.contains("pub struct CompleteClient"));
    assert!(client.contains("pub enum Decision"));
    assert!(client.contains("pub async fn complete"));
    assert!(!client.contains("pub async fn approve"));
    assert!(!client.contains("pub async fn reject"));
}

#[test]
fn required_client_tests_exist() {
    let tests =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/client.rs").unwrap();
    for name in [
        "fn client_complete_unblocks_wait_node",
        "fn client_decision_complete_unblocks_wait_node",
        "fn client_without_secret_is_401",
        "fn client_wrong_secret_is_401",
        "fn client_after_drop_handle_is_409_does_not_revive",
        "fn client_duplicate_complete_is_noop",
        "fn client_oversized_body_is_413_does_not_complete",
        "fn two_client_completes_one_token_downstream_runs_once",
        "fn client_wire_is_complete_body_and_secret_header",
        "fn client_does_not_follow_redirect_off_loopback",
        "fn client_hung_server_is_hung_not_forever",
        "fn client_drop_server_mid_post_is_transport_token_untouched",
        "fn client_drop_inflight_does_not_complete",
        "fn client_decision_fail_fails_execution",
    ] {
        assert!(tests.contains(name), "client.rs missing {name}");
    }
    assert!(
        tests.contains("FakeClock"),
        "client tests must use FakeClock, not wall sleep"
    );
    assert!(
        !tests.contains("std::thread::sleep") && !tests.contains("tokio::time::sleep"),
        "client tests must not wall-sleep"
    );
    let client = fs::read_to_string(crate_src().join("client.rs")).unwrap();
    assert!(
        client.contains("COMPLETE_HANG_BOUND") && client.contains("tokio::time::timeout"),
        "CompleteClient must bound a hung server"
    );
    assert!(
        !client.contains("redirect::Policy") && !client.contains("follow_redirect"),
        "client must not install a redirect follower"
    );
}

#[test]
fn architecture_mermaid_names_complete_client() {
    let arch = fs::read_to_string(root().join("docs/ARCHITECTURE.md")).unwrap();
    assert!(arch.contains("```mermaid"), "ARCHITECTURE missing mermaid");
    for name in [
        "CompleteClient",
        "POST /complete",
        "Runtime::complete",
        "Decision",
        "CompleteBody",
    ] {
        assert!(arch.contains(name), "ARCHITECTURE missing {name}");
    }
}

#[test]
fn src_is_lib_and_client_only() {
    let mut files = Vec::new();
    for e in fs::read_dir(crate_src()).unwrap() {
        let p = e.unwrap().path();
        assert!(p.is_file(), "no src subdirs: {}", p.display());
        files.push(p.file_name().unwrap().to_string_lossy().into_owned());
    }
    files.sort();
    assert_eq!(files, vec!["client.rs", "lib.rs"]);
}
