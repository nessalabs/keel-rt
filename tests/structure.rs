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
        assert!(
            !s.contains("crate::runtime"),
            "{r} must not import runtime"
        );
        assert!(
            !s.contains("crate::testing"),
            "{r} must not import testing"
        );
    }
}

#[test]
fn kernel_src_has_no_storage_engine() {
    let cargo = fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .unwrap();
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
    assert!(agents.contains("When a caller runs X, it used to Y. Now it Z."));
    assert!(agents.contains("RuntimeBuilder"));

    let tmpl = fs::read_to_string(root.join(".github/pull_request_template.md")).unwrap();
    assert!(tmpl.contains("## Architecture (before)"));
    assert!(tmpl.contains("## Architecture (after)"));
    assert!(tmpl.contains("## User behavior (when X, used to Y, now Z)"));
    for verb in ["start", "wait", "cancel", "resume", "fail", "retry", "inspect"] {
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
        ci.contains("--test resume_stress"),
        "stress-resume job must run the sqlite resume stress pack"
    );
    assert!(
        ci.contains("--test adversarial"),
        "adversarial job must run the kernel pack"
    );

    let agents = fs::read_to_string(root.join("AGENTS.md")).unwrap();
    assert!(agents.contains("Phase 2+ review gate"));
    assert!(agents.contains("stress-resume"));
    assert!(agents.contains("chaos-sqlite"));
    assert!(agents.contains("docs/RESUME_CATALOG.md"));
    assert!(agents.contains("docs/CHAOS_LOG.md"));

    let tmpl = fs::read_to_string(root.join(".github/pull_request_template.md")).unwrap();
    assert!(tmpl.contains("## Phase 2+ review gate"));
    assert!(tmpl.contains("stress-resume"));
    assert!(tmpl.contains("chaos-sqlite"));

    let rule = fs::read_to_string(root.join(".cursor/rules/pr-architecture.mdc")).unwrap();
    assert!(rule.contains("Phase 2+ review gate"));
    assert!(rule.contains("stress-resume"));
    assert!(rule.contains("chaos-sqlite"));

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
    ] {
        assert!(catalog.contains(name), "RESUME_CATALOG missing {name}");
    }

    let chaos = fs::read_to_string(root.join("docs/CHAOS_LOG.md")).unwrap();
    assert!(chaos.contains("two_thousand_short_jobs_one_file"));
    assert!(chaos.contains("wide_256_resume_under_concurrent_starts_is_sqlite_bound"));
    assert!(chaos.contains("wide_2k_and_join_crash_resume_of_ready"));
}
