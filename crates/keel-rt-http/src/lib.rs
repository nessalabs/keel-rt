//! Thin HTTP adapter. Another process POSTs a token + [`Resume`]; this
//! process calls [`Runtime::complete`]. No forms, no identity.
//!
//! Kernel `keel-rt` does not depend on this crate.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Json;
use axum::Router;
use keel_rt::{CompleteError, Resume, ResumeToken, Runtime};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

/// `POST /complete` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteBody {
    pub token: ResumeToken,
    pub resume: Resume,
}

/// Router a caller can nest or serve. Path is `/complete`.
pub fn router(runtime: Arc<Runtime>) -> Router {
    Router::new()
        .route("/complete", post(complete_handler))
        .with_state(runtime)
}

async fn complete_handler(
    State(rt): State<Arc<Runtime>>,
    Json(body): Json<CompleteBody>,
) -> StatusCode {
    match rt.complete(body.token, body.resume).await {
        Ok(()) => StatusCode::OK,
        Err(CompleteError::UnknownToken) => StatusCode::NOT_FOUND,
        Err(CompleteError::Cancelled) => StatusCode::CONFLICT,
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

/// Bind and serve until the listener is dropped. Used by tests and a host binary.
pub async fn serve(runtime: Arc<Runtime>, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(runtime)).await
}

/// Bind `127.0.0.1:0` and return the local address plus the server task.
pub async fn serve_ephemeral(
    runtime: Arc<Runtime>,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move { axum::serve(listener, router(runtime)).await });
    Ok((addr, handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_round_trips() {
        let token = ResumeToken::issue(
            keel_rt::ExecutionId::parse("exec-1").unwrap(),
            keel_rt::NodeId::new("hold"),
            1,
        );
        let body = CompleteBody {
            token,
            resume: Resume::Reinvoke,
        };
        let v = serde_json::to_value(&body).unwrap();
        let back: CompleteBody = serde_json::from_value(v).unwrap();
        assert!(matches!(back.resume, Resume::Reinvoke));
    }
}
