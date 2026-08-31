# jemalloc vs system allocator

Allocator choice belongs to the **consumer binary**, not `keel-rt`.
Default `keel-rt` never sets `#[global_allocator]` (`lib.rs` has none;
the default feature set is empty of allocators).

Production binaries that want jemalloc should depend on `tikv-jemallocator`
themselves and put `#[global_allocator]` in *their* `main.rs`. Do not
expect `keel-rt` to pick one.

The optional crate feature `jemalloc` exists only so **this repo’s**
examples/benches (`examples/kernel_benches.rs`) can install jemalloc when
you pass `--features jemalloc`. If a consumer enables
`keel-rt = { features = ["jemalloc"] }` in *their* `Cargo.toml`, that only
pulls `tikv-jemallocator` as a dependency — it still does **not** set the
process allocator.

Tokio remains **current_thread** (Phase 1 FIFO apply loop). A multi-thread
Tokio runtime is a labelled experiment below; it does not change the
scheduler.

## Reproduce

Linux x86_64 (this machine). `tikv-jemallocator` 0.6 built cleanly. If a
platform cannot compile jemalloc, skip this feature — default CI does not
enable it.

```bash
cargo run --release --example kernel_benches
cargo run --release --example kernel_benches --features jemalloc
cargo run --release --example kernel_benches -- --multi-thread
cargo run --release --example kernel_benches --features jemalloc -- --multi-thread
cargo run --release --example alloc_count
```

## current_thread (Phase 1 default)

Release, median of 7 except `wide_100k` (n=1). Instant-succeed executors.

| scenario | sys | jemalloc | delta |
|---|---:|---:|---:|
| wide_256 | 0.493 ms | 0.458 ms | −7.1% |
| chain_128 | 0.227 ms | 0.218 ms | −4.0% |
| diamond_10k | 19.289 ms | 17.964 ms | −6.9% |
| apply_only | 6.318 ms | 6.119 ms | −3.2% |
| wide_100k | 239.853 ms | 224.326 ms | −6.5% |

jemalloc is a **small** win on current_thread (single-digit). The hot path is
mostly the apply loop and slot vectors, not cross-thread allocator
contention. **Do not switch anything in the library** — there is no library
allocator to switch.

## multi_thread Tokio (experiment)

`Builder::new_multi_thread().worker_threads(4)`. One apply task per
execution still; execute tasks may run on other workers. **Not** the Phase 1
scheduler contract.

| scenario | sys | jemalloc | delta |
|---|---:|---:|---:|
| wide_256 | 1.234 ms | 0.945 ms | −23.4% |
| chain_128 | 0.441 ms | 0.429 ms | −2.7% |

Wide fan-out under multi-thread shows more jemalloc benefit (thread-local
caches). Chain is almost unchanged. This does **not** justify changing
current_thread or FIFO, and it does **not** belong in `keel-rt`.

## Alloc count (system allocator, not a CI gate)

`examples/alloc_count.rs` wrapping `System` on current_thread, one 256-wide
join (258 nodes):

| metric | value |
|---|---:|
| allocs | 3732 |
| deallocs | 3193 |
| bytes requested | 685_100 |
| elapsed | 0.605 ms |

Not a fail-under. Re-measure if a hot-path change explodes this (e.g. 10×).
