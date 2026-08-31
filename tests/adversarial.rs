//! Adversarial regression pack: concurrency, join, stale, timers, panic, definition, inspect.
//!
//! `cargo test --test adversarial -- --test-threads=1`
//!
//! `#[path]` keeps Cargo’s `tests/foo.rs` + `tests/foo/` layout: a sibling
//! directory of the same name is not automatically on the module search path
//! for an integration-test crate root.

#[path = "adversarial/common.rs"]
mod common;
#[path = "adversarial/concurrency.rs"]
mod concurrency;
#[path = "adversarial/join.rs"]
mod join;
#[path = "adversarial/stale.rs"]
mod stale;
#[path = "adversarial/timers.rs"]
mod timers;
#[path = "adversarial/panic.rs"]
mod panic;
#[path = "adversarial/definition.rs"]
mod definition;
#[path = "adversarial/inspect.rs"]
mod inspect;
#[path = "adversarial/diamond_repeat.rs"]
mod diamond_repeat;
#[path = "adversarial/resume.rs"]
mod resume;
