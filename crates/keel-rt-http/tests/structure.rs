//! Absences for the HTTP adapter + KeelClient. Not AGENTS.md.

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
            "CompleteClient",
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
            "KeelClient",
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
    assert!(lib.contains("pub use client::{Decision, KeelClient, KeelClientError, HANG_BOUND}"));
    assert!(lib.contains("pub struct CompleteBody"));
    assert!(lib.contains("pub struct CompleteSecret"));
    assert!(lib.contains("pub const SECRET_HEADER"));
    let client = fs::read_to_string(crate_src().join("client.rs")).unwrap();
    assert!(client.contains("pub struct KeelClient"));
    assert!(client.contains("pub enum KeelClientError"));
    assert!(client.contains("pub enum Decision"));
    assert!(client.contains("pub async fn complete"));
    assert!(client.contains("pub async fn inspect"));
    assert!(client.contains("pub const HANG_BOUND"));
    assert!(
        !lib.contains("COMPLETE_HANG_BOUND")
            && !client.contains("COMPLETE_HANG_BOUND")
            && !lib.contains("COMPLETE_SECRET_HEADER")
            && !client.contains("COMPLETE_SECRET_HEADER"),
        "inspect shares SECRET_HEADER / HANG_BOUND; names must not say complete-only"
    );
    assert!(lib.contains("pub struct InspectView"));
    assert!(lib.contains("pub struct InspectNode"));
    assert!(lib.contains("pub enum InspectNodeState"));
    assert!(
        !lib.contains("pub resume_token: Option"),
        "InspectNode must not have a ghost resume_token field"
    );
    assert!(lib.contains("pub const CLAIMED_ELSEWHERE"));
    assert!(
        client.contains("ClaimedElsewhere") && client.contains("CLAIMED_ELSEWHERE"),
        "KeelClient must map 423 Locked to ClaimedElsewhere"
    );
    let inspect_fn = client
        .split("pub async fn inspect(")
        .nth(1)
        .expect("inspect")
        .split("pub async fn complete(")
        .next()
        .expect("complete after inspect");
    assert!(
        !inspect_fn.contains("CLAIMED_ELSEWHERE"),
        "inspect must not invent a steal / ClaimedElsewhere map"
    );
    assert_eq!(
        client.matches("tokio::time::timeout").count(),
        1,
        "one send-path timeout, not a copy-pasted pair"
    );
    assert!(
        client.contains("async fn send(") && client.contains("fn with_secret("),
        "inspect and complete must share one send path"
    );
    assert!(
        lib.contains("StatusCode::LOCKED") && lib.contains("claimed_elsewhere"),
        "complete must return 423 Locked with claimed_elsewhere body"
    );
    assert!(
        lib.contains("fn inspect_view_json_has_exactly_one_wait_token"),
        "InspectView JSON must lock exactly one wait token"
    );
    assert!(
        !lib.contains("impl Drop for InspectView") && !lib.contains("pub async fn cancel"),
        "InspectView must not be an ExecutionHandle (no cancel / Drop-cancel)"
    );
    assert!(!client.contains("CompleteClient"));
    assert!(client.contains("pub async fn start"));
    assert!(lib.contains("pub struct StartBody"));
    assert!(lib.contains("pub struct StartView"));
    assert!(lib.contains(".route(\"/start\""));
    let start_fn = client
        .split("pub async fn start(")
        .nth(1)
        .expect("start")
        .split("    pub async fn ")
        .next()
        .expect("start body");
    assert!(
        start_fn.contains(".send(") && !start_fn.contains("CLAIMED_ELSEWHERE"),
        "start must reuse send and must not invent a steal"
    );
    assert!(client.contains("pub async fn approve"));
    assert!(client.contains("pub async fn reject"));
    assert!(
        !lib.contains(".route(\"/approve\"") && !lib.contains(".route(\"/reject\""),
        "approve/reject must reuse POST /complete, not alias routes"
    );
    assert!(!client.contains("pub async fn schedule"));
    assert!(!client.contains("pub async fn claim"));
    assert!(
        !client.contains("START_HANG_BOUND")
            && !client.contains("START_SECRET_HEADER")
            && !client.contains("start rejected"),
        "start must not add verb-specific hang/secret/Display names"
    );
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
        "fn two_keel_clients_one_token_downstream_runs_once",
        "fn client_complete_succeeds_against_bearer_only_server",
        "fn client_wire_is_complete_body_and_secret_header",
        "fn client_does_not_follow_redirect_off_loopback",
        "fn client_hung_server_is_hung_not_forever",
        "fn client_drop_server_mid_post_is_transport_token_untouched",
        "fn client_drop_inflight_does_not_complete",
        "fn client_decision_fail_fails_execution",
        "fn client_inspect_then_complete_unblocks_wait",
        "fn client_inspect_without_secret_is_401",
        "fn client_inspect_wrong_secret_is_401",
        "fn client_inspect_unknown_id_is_404",
        "fn client_inspect_after_drop_handle_is_cancelled_complete_409",
        "fn client_inspect_while_running_has_no_token_then_wait_sees_token",
        "fn two_clients_inspect_same_token",
        "fn two_runtimes_inspect_does_not_steal_lease",
        "fn client_complete_of_running_handle_token_does_not_unblock_wait",
        "fn client_inspect_wait_sibling_while_running_completes_only_wait",
        "fn client_complete_issued_token_for_running_node_leaves_wait_parked",
        "fn client_hung_inspect_is_hung_not_forever",
        "fn client_inspect_does_not_follow_redirect_off_loopback",
        "fn client_inspect_wire_sends_both_secret_headers",
        "fn client_inspect_succeeds_against_bearer_only_server",
        "fn client_inspect_401_error_text_does_not_say_complete",
        "fn client_start_inspect_complete_unblocks_wait",
        "fn client_start_then_inspect_is_waiting_not_cancelled",
        "fn client_two_starts_are_distinct_ids",
        "fn client_start_without_secret_is_401",
        "fn client_start_wrong_secret_is_401",
        "fn client_start_unregistered_is_400_nothing_runs",
        "fn client_start_empty_definition_is_400",
        "fn client_start_oversized_body_is_413",
        "fn client_hung_start_is_hung_not_forever",
        "fn client_start_does_not_follow_redirect_off_loopback",
        "fn client_start_wire_sends_both_secret_headers",
        "fn client_start_succeeds_against_bearer_only_server",
        "fn client_start_inspect_approve_unblocks_wait",
        "fn client_start_inspect_reject_fails_execution",
        "fn client_approve_issued_token_for_running_node_leaves_wait_parked",
    ] {
        assert!(tests.contains(name), "client.rs missing {name}");
    }
    let lib = fs::read_to_string(crate_src().join("lib.rs")).unwrap();
    assert!(
        lib.contains("fn inspect_view_json_running_pred_does_not_contain_running_token"),
        "InspectView JSON must omit Running-node tokens"
    );
    let complete =
        fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/tests/complete.rs").unwrap();
    assert!(
        complete.contains("fn post_claimed_elsewhere_is_423_locked_not_409"),
        "complete.rs must lock 423 Locked vs 409 Cancelled"
    );
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
        client.contains("HANG_BOUND") && client.contains("tokio::time::timeout"),
        "KeelClient must bound a hung server"
    );
    assert!(
        !client.contains("redirect::Policy") && !client.contains("follow_redirect"),
        "client must not install a redirect follower"
    );
}

#[test]
fn architecture_mermaid_names_keel_client() {
    let arch = fs::read_to_string(root().join("docs/ARCHITECTURE.md")).unwrap();
    assert!(arch.contains("```mermaid"), "ARCHITECTURE missing mermaid");
    for name in [
        "KeelClient",
        "POST /complete",
        "POST /start",
        "GET /inspect",
        "Runtime::complete",
        "Runtime::inspect",
        "Runtime::start",
        "StartBody",
        "StartView",
        "InspectView",
        "InspectNodeState",
        "Decision",
        "CompleteBody",
    ] {
        assert!(arch.contains(name), "ARCHITECTURE missing {name}");
    }
    assert!(
        !arch.contains("CompleteClient"),
        "ARCHITECTURE still names CompleteClient"
    );
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

#[test]
fn baseline_has_numbered_inspect_release_row() {
    let base = fs::read_to_string(root().join("benches/BASELINE.md")).unwrap();
    assert!(
        base.contains("## Inspect then complete"),
        "BASELINE missing inspect section"
    );
    assert!(
        base.contains("KeelClient::inspect"),
        "BASELINE must number KeelClient::inspect"
    );
    assert!(
        base.contains("## Start then approve") && base.contains("KeelClient::start"),
        "BASELINE must number KeelClient::start"
    );
    assert!(
        base.contains("release after"),
        "BASELINE must number release before/after"
    );
    assert!(
        !base.contains("thread-per-call pile") || base.contains("No thread-per-call pile"),
        "BASELINE must record hang-bound thread count"
    );
}

#[test]
fn profile_harness_exists() {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/profile.rs");
    let s = fs::read_to_string(&p).unwrap();
    assert!(
        s.contains("fn profile_inspect_complete_release"),
        "profile.rs must measure inspect+complete"
    );
    assert!(
        s.contains("fn inspect_view_json_is_not_full_snapshot"),
        "profile.rs must lock InspectView JSON vs fat snapshot"
    );
    assert!(
        s.contains("fn profile_start_approve_release"),
        "profile.rs must measure start+approve"
    );
}
