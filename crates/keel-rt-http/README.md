# keel-rt-http

Thin adapter. Another binary `POST`s a resume token and payload; this
process calls `Runtime::complete`. Not the kernel. No forms, no identity.

```
POST /complete
Content-Type: application/json

{ "token": { ... }, "resume": { "Complete": { "Succeeded": "<bytes>" } } }
```

Serve `keel_rt_http::router(Arc<Runtime>)` or `serve` / `serve_ephemeral`.
Delete this crate without editing `scheduler.rs`.
