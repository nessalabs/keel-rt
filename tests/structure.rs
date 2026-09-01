//! Holds absences: domain imports nothing outward; `src/` has no product
//! resource identifiers; crate modules are acyclic.
//!
//! `cargo test --test structure -- --test-threads=1`

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let p = e.path();
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

fn src_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rel(p: &Path) -> String {
    p.strip_prefix(src_root())
        .unwrap_or(p)
        .display()
        .to_string()
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

/// First crate-level module after `crate::` (`domain`, `runtime`, `testing`).
fn crate_mod_refs(src: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut rest = src;
    while let Some(idx) = rest.find("crate::") {
        rest = &rest[idx + "crate::".len()..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            out.insert(name);
        }
    }
    out
}

#[test]
fn domain_imports_nothing_outward() {
    for p in rust_files(&src_root().join("domain")) {
        let s = fs::read_to_string(&p).unwrap();
        let r = rel(&p);
        assert!(
            !s.contains("tokio::") && !s.contains("use tokio"),
            "{r} must not import tokio"
        );
        assert!(!s.contains("std::net"), "{r} must not import std::net");
        assert!(!s.contains("crate::runtime"), "{r} must not import runtime");
        assert!(!s.contains("crate::testing"), "{r} must not import testing");
    }
}

#[test]
fn kernel_src_has_no_storage_engine() {
    let cargo =
        fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    let deps = cargo.split("[dev-dependencies]").next().unwrap_or(&cargo);
    for word in ["rusqlite", "postgres", "tokio_postgres", "sqlx"] {
        assert!(
            !deps.contains(word),
            "keel-rt package deps must not name {word}"
        );
        for p in rust_files(&src_root()) {
            let s = fs::read_to_string(&p).unwrap();
            assert!(
                !s.contains(word),
                "{} contains banned storage engine {word}",
                rel(&p)
            );
        }
    }
}

#[test]
fn src_has_no_product_resource_identifiers() {
    for word in ["Agent", "HTTP", "HITL", "Sql", "crawl"] {
        for p in rust_files(&src_root()) {
            let s = fs::read_to_string(&p).unwrap();
            assert!(
                !contains_word(&s, word),
                "{} contains banned identifier {word}",
                rel(&p)
            );
        }
    }
}

#[test]
fn src_has_no_mem_forget() {
    for p in rust_files(&src_root()) {
        let s = fs::read_to_string(&p).unwrap();
        assert!(
            !s.contains("mem::forget"),
            "{} uses mem::forget; RAII Drop must release tasks/permits",
            rel(&p)
        );
    }
}

#[test]
fn crate_modules_are_acyclic() {
    let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
    for p in rust_files(&src_root()) {
        let rel_path = rel(&p);
        let from = if rel_path.starts_with("domain") {
            "domain"
        } else if rel_path.starts_with("runtime") {
            "runtime"
        } else if rel_path.starts_with("testing") {
            "testing"
        } else {
            "root"
        };
        let s = fs::read_to_string(&p).unwrap();
        for to in crate_mod_refs(&s) {
            if matches!(to.as_str(), "domain" | "runtime" | "testing") && to != from {
                edges.entry(from.to_string()).or_default().insert(to);
            }
        }
    }

    if let Some(tos) = edges.get("domain") {
        assert!(
            !tos.contains("runtime") && !tos.contains("testing"),
            "domain imports {tos:?}"
        );
    }
    if let Some(tos) = edges.get("runtime") {
        assert!(!tos.contains("testing"), "runtime imports testing");
    }

    // No 2-cycles among domain/runtime/testing/root that reverse the arrow.
    assert!(
        !edges
            .get("domain")
            .map(|t| t.contains("runtime"))
            .unwrap_or(false),
        "domain → runtime"
    );
}

#[test]
fn lib_does_not_export_module_trees() {
    let lib = fs::read_to_string(src_root().join("lib.rs")).unwrap();
    assert!(
        lib.contains("pub(crate) mod domain"),
        "domain must be crate-private"
    );
    assert!(
        lib.contains("pub(crate) mod runtime"),
        "runtime must be crate-private"
    );
    assert!(
        !lib.contains("pub mod domain"),
        "domain must not be a public module"
    );
    assert!(
        !lib.contains("pub mod runtime"),
        "runtime must not be a public module"
    );
}

#[test]
fn pr_template_and_agents_require_architecture_and_behavior() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let agents = fs::read_to_string(root.join("AGENTS.md")).unwrap();
    assert!(
        agents.contains("The tests define the absences."),
        "AGENTS must point at tests, not repeat the PR checklist"
    );
    assert!(
        !agents.contains("When a caller runs X, it used to Y. Now it Z."),
        "PR behavior sentence lives in pr_body_gate.py, not AGENTS prose"
    );

    let tmpl = fs::read_to_string(root.join(".github/pull_request_template.md")).unwrap();
    assert!(tmpl.contains("## Architecture (before)"));
    assert!(tmpl.contains("## Architecture (after)"));
    assert!(tmpl.contains("## User behavior (when X, used to Y, now Z)"));
    for verb in [
        "start", "wait", "cancel", "resume", "fail", "retry", "inspect",
    ] {
        assert!(tmpl.contains(verb), "template missing {verb}");
    }

    let arch = fs::read_to_string(root.join("docs/ARCHITECTURE.md")).unwrap();
    assert!(arch.contains("```mermaid"));
    assert!(arch.contains("flowchart TB"));
    assert!(arch.contains("classDiagram"));
    let lib = fs::read_to_string(src_root().join("lib.rs")).unwrap();
    for name in [
        "RuntimeBuilder",
        "Runtime",
        "ExecutionHandle",
        "Execution",
        "WorkflowDefinition",
        "Executor",
        "Policy",
        "StateStore",
        "EventSink",
        "Event",
        "NodeOutcome",
        "ExecutionState",
    ] {
        assert!(lib.contains(name), "lib.rs missing {name}");
        assert!(arch.contains(name), "ARCHITECTURE mermaid missing {name}");
    }
    assert!(arch.contains("src/domain/"));
    assert!(arch.contains("src/runtime/"));
    assert!(arch.contains("src/testing/"));
}

#[test]
fn ci_and_agents_name_phase2_review_jobs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ci = fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    for job in [
        "test:",
        "adversarial:",
        "coverage:",
        "pr-body:",
        "stress-resume:",
        "stress-100k:",
        "chaos-sqlite:",
    ] {
        assert!(ci.contains(job), "ci.yml missing job {job}");
    }
    assert!(
        !ci.contains("continue-on-error"),
        "ci.yml must not skip a red job"
    );
    assert!(
        ci.contains("--test chaos"),
        "chaos-sqlite job must run the sqlite chaos pack"
    );
    assert!(
        ci.contains("--test crash_inject"),
        "chaos-sqlite job must run the sqlite crash-inject pack"
    );
    assert!(
        ci.contains("--test resume_stress"),
        "stress-resume job must run the sqlite resume stress pack"
    );
    assert!(
        ci.contains("--test adversarial"),
        "adversarial job must run the kernel pack"
    );
    assert!(
        ci.contains("--test events"),
        "test job must run the events pack"
    );
    assert!(
        ci.contains("--test timers"),
        "test job must run the Phase 5 timers pack"
    );
    assert!(
        ci.contains("scripts/pr_body_gate.py"),
        "pr-body job must run the description gate"
    );

    let agents = fs::read_to_string(root.join("AGENTS.md")).unwrap();
    assert!(agents.contains("The tests define the absences."));
    assert!(agents.contains("stress-resume"));
    assert!(agents.contains("chaos-sqlite"));

    let tmpl = fs::read_to_string(root.join(".github/pull_request_template.md")).unwrap();
    assert!(tmpl.contains("## Phase 2+ review gate"));
    assert!(tmpl.contains("stress-resume"));
    assert!(tmpl.contains("chaos-sqlite"));

    let rule = fs::read_to_string(root.join(".cursor/rules/pr-architecture.mdc")).unwrap();
    assert!(rule.contains("pr_body_gate.py"));
    assert!(rule.contains("The tests define the absences."));

    let catalog = fs::read_to_string(root.join("docs/RESUME_CATALOG.md")).unwrap();
    assert!(catalog.contains("Zero MISSING"));
    assert!(
        !catalog.contains("| MISSING") && !catalog.contains("**MISSING**"),
        "RESUME_CATALOG must not leave a hunt row MISSING"
    );
    for name in [
        "crash_diamond_join_runs_writer_once",
        "two_runtimes_same_file_are_not_fenced",
        "crash_after_terminal_cas_before_emit_keeps_terminal",
        "resume_256_wide_snapshot_within_bound",
        "transient_terminal_persist_err_shutdown_flushes_sqlite_succeeded",
        "transient_terminal_persist_err_twice_shutdown_retries_until_ok",
        "persist_err_then_ok_emits_events_for_the_durable_snapshot",
        "equal_revision_persist_does_not_grow_event_rows",
        "checkpoint_busy_after_terminal_commit_does_not_duplicate_events",
        "randomized_crash_inject_sqlite",
        "crash_resume_full_file_keeps_deadline",
        "persisted_deadline_already_due_on_resume_runs_once_not_twice",
        "cancel_during_parked_deadline_is_cancelled_sleeper_dropped",
    ] {
        assert!(catalog.contains(name), "RESUME_CATALOG missing {name}");
    }

    let chaos = fs::read_to_string(root.join("docs/CHAOS_LOG.md")).unwrap();
    assert!(chaos.contains("two_thousand_short_jobs_one_file"));
    assert!(chaos.contains("wide_256_resume_under_concurrent_starts_is_sqlite_bound"));
    assert!(chaos.contains("wide_2k_and_join_crash_resume_of_ready"));
}

/// Phase 3 architect: kernel public surface is Event + EventSink. No EventLog type.
#[test]
fn kernel_src_has_no_public_event_log() {
    for p in rust_files(&src_root()) {
        let s = fs::read_to_string(&p).unwrap();
        for raw in s.lines() {
            let t = raw.trim();
            if t.starts_with("//") || t.starts_with("///") || t.starts_with("//!") {
                continue;
            }
            let pub_item = t.starts_with("pub ")
                && (t.contains("trait EventLog")
                    || t.contains("enum EventLog")
                    || t.contains("struct EventLog")
                    || t.contains("type EventLog")
                    || (t.starts_with("pub use") && contains_word(t, "EventLog")));
            assert!(
                !pub_item,
                "{} declares public EventLog (Phase 3: Event + EventSink only)",
                rel(&p)
            );
        }
    }
}

const PUBLIC_EVENT_VARIANTS: &[&str] = &[
    "ExecutionStarted",
    "ExecutionSucceeded",
    "ExecutionFailed",
    "ExecutionCompleted",
    "ExecutionCancelled",
    "NodeStarted",
    "NodeSucceeded",
    "NodeFailed",
    "NodeTimedOut",
    "NodeCancelled",
    "NodeWaiting",
];

fn event_enum_variants(src: &str) -> Vec<String> {
    let start = src
        .find("pub enum Event {")
        .expect("src/domain/events.rs must declare pub enum Event");
    let rest = &src[start..];
    let end = rest.find("\n}").expect("Event enum must close");
    let mut names = Vec::new();
    for line in rest[..end].lines() {
        let t = line.trim();
        if t.starts_with("pub ") || t.starts_with("//") || t.is_empty() {
            continue;
        }
        let name: String = t
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
            names.push(name);
        }
    }
    names
}

/// Adding NodeReady (or any unlisted variant) fails CI.
#[test]
fn public_event_variants_are_frozen_without_node_ready() {
    let src = fs::read_to_string(src_root().join("domain/events.rs")).unwrap();
    let names = event_enum_variants(&src);
    assert_eq!(
        names, PUBLIC_EVENT_VARIANTS,
        "Event variants must stay this exact set (no NodeReady, no EventLog fold)"
    );
    assert!(
        !names.iter().any(|n| n == "NodeReady"),
        "NodeReady is not a public Event"
    );
}

#[test]
fn pr_body_gate_script_enforces_mermaid_behavior_and_main_base() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = root.join("scripts/pr_body_gate.py");
    assert!(script.is_file(), "scripts/pr_body_gate.py must exist");
    let src = fs::read_to_string(&script).unwrap();
    assert!(
        src.contains("chaos pack missed main because #3 targeted a feature branch"),
        "script must encode the #3-missed-main failure mode"
    );
    let st = std::process::Command::new("python3")
        .arg(&script)
        .arg("--self-test")
        .status()
        .expect("python3 pr_body_gate.py --self-test");
    assert!(st.success(), "pr_body_gate.py --self-test failed");

    let readme = fs::read_to_string(root.join("README.md")).unwrap();
    assert!(
        readme.contains("nessalabs/keel-rt"),
        "README must name Origin nessalabs/keel-rt"
    );
    assert!(
        readme.contains("GitHub is not the kernel"),
        "README must say GitHub is not the kernel"
    );
}

/// Phase 5: HITL resume is Complete/Reinvoke. RetryFailed is not a kernel command.
#[test]
fn resume_enum_has_no_retry_failed() {
    let src = fs::read_to_string(src_root().join("domain/outcome.rs")).unwrap();
    let start = src.find("pub enum Resume {").expect("Resume enum");
    let rest = &src[start..];
    let end = rest.find("\n}").expect("Resume enum close");
    let body = &rest[..end];
    assert!(body.contains("Complete"));
    assert!(body.contains("Reinvoke"));
    assert!(
        !body.contains("RetryFailed"),
        "Do not implement Recover::RetryFailed"
    );
}

/// Apply is a tick: given `Clock::now()` (or `now: Timestamp`), a node
/// with `runnable_at: Some(T)` is dispatchable iff `now >= T`. Domain and
/// scheduler must not wait. Waiting for T is the Runtime drive loop
/// (`next_drive_event` selects inbox vs `Clock::wait_until(T)`). That is
/// the one allowed kernel waiter — Drop of the handle cancels the shell
/// future (RAII). `ctx.sleep` stays on ExecutionContext for executor
/// bodies (`executor.rs`); it is not used for timeout/backoff.
#[test]
fn apply_path_does_not_sleep() {
    assert!(
        !src_root().join("runtime/park.rs").exists(),
        "park.rs was the sleeper; wait lives in runtime.rs drive"
    );
    for p in rust_files(&src_root().join("domain")) {
        let s = fs::read_to_string(&p).unwrap();
        let r = rel(&p);
        assert!(
            !contains_word(&s, "sleep"),
            "{r} must not name sleep; apply is given now"
        );
        assert!(
            !s.contains("Clock::sleep"),
            "{r} must not call Clock::sleep"
        );
        assert!(
            !s.contains("tokio::time"),
            "{r} must not import tokio::time"
        );
    }
    let sched = fs::read_to_string(src_root().join("runtime/scheduler.rs")).unwrap();
    assert!(
        !contains_word(&sched, "sleep"),
        "scheduler.rs must not sleep; Runtime drive waits, apply ticks"
    );
    assert!(
        !sched.contains("Clock::sleep"),
        "scheduler.rs must not call Clock::sleep"
    );
    assert!(
        !sched.contains("tokio::time"),
        "scheduler.rs must not use tokio::time (cancel-bound wall sleep is Runtime)"
    );

    let rt = fs::read_to_string(src_root().join("runtime/runtime.rs")).unwrap();
    assert!(
        rt.contains("clock.wait_until(when)"),
        "Runtime drive is the allowed waiter: inbox vs Clock::wait_until(T)"
    );
    assert!(
        rt.contains("async fn next_drive_event"),
        "wait loop is next_drive_event in runtime.rs, not the scheduler"
    );
}

/// FakeClock is test harness (`src/testing`, `tests/`). Domain and runtime
/// depend on the Clock trait only. A use in scheduler.rs fails this test.
#[test]
fn kernel_has_no_fake_clock() {
    for dir in ["domain", "runtime"] {
        for p in rust_files(&src_root().join(dir)) {
            let s = fs::read_to_string(&p).unwrap();
            assert!(
                !contains_word(&s, "FakeClock"),
                "{} names FakeClock; kernel uses Clock, harness owns FakeClock",
                rel(&p)
            );
        }
    }
}

#[test]
fn coverage_script_runs_timers_pack() {
    let sh =
        fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/coverage.sh"))
            .unwrap();
    assert!(
        sh.contains("--test timers"),
        "coverage.sh must instrument timers.rs"
    );
}
