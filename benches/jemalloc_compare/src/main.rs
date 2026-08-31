//! Jemalloc-backed hot-path benches. Not published; not a `keel-rt` feature.
//!
//! `keel-rt` never sets `#[global_allocator]`. This **binary** does, so we can
//! compare against `cargo run --release --example kernel_benches` (system alloc).

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[path = "../../../examples/kernel_benches.rs"]
mod kernel_benches;

fn main() {
    std::env::set_var("KEEL_BENCH_ALLOC", "jemalloc");
    kernel_benches::main();
}
