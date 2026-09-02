# keel-rt-http

Thin adapter. Another binary `POST`s a resume token and payload; this
process calls `Runtime::complete`. Not the kernel. No forms, no identity.

A **shared secret is required**. Send `Authorization: Bearer <secret>` or
`X-Keel-Complete: <secret>`. Missing or wrong secret is **401**. Query-string
secrets are ignored.

Default bind is **`127.0.0.1`** (`DEFAULT_BIND` / `serve` / `serve_ephemeral`).
`0.0.0.0` only via explicit `serve_on`. Body larger than **1 MiB**
(`MAX_COMPLETE_BODY`) is **413** and does not call `complete`.

```
POST /complete
Authorization: Bearer <secret>
Content-Type: application/json

{ "token": { ... }, "resume": { "Complete": { "Succeeded": "<bytes>" } } }
```

```rust
let secret = keel_rt_http::CompleteSecret::new(std::env::var("KEEL_COMPLETE_SECRET")?)?;
keel_rt_http::serve(runtime, secret).await?;
```

Serve `keel_rt_http::router(Arc<Runtime>, secret)` or `serve` / `serve_on` /
`serve_ephemeral`. Delete this crate without editing `scheduler.rs`.
