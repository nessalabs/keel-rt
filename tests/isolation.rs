//! Isolation: the scheduler must not name Agent / Postgres / HTTP types.

#[test]
fn scheduler_has_no_foreign_resource_types() {
    let src = include_str!("../src/runtime/scheduler.rs");
    assert!(
        !src.contains("Agent"),
        "scheduler.rs must not mention Agent"
    );
    assert!(
        !src.contains("Postgres"),
        "scheduler.rs must not mention Postgres"
    );
    assert!(
        !src.contains("HTTP") && !src.contains("Http"),
        "scheduler.rs must not mention HTTP"
    );
}
