//! Compile-time regression: generated bindings must work in a real consumer
//! whose only dependency is the adapter, without a direct Wasmtime dependency.

keel_rt_wasm::wasmtime::component::bindgen!({
    path: "../../examples/echo.wit",
    world: "echo",
    wasmtime_crate: keel_rt_wasm::wasmtime,
    exports: { default: async },
});
