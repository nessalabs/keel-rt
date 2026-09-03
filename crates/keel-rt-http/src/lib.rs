//! Thin HTTP adapter. Another process POSTs a token + [`Resume`]; this
//! process calls [`Runtime::complete`]. No forms, no identity.
//!
//! A shared secret is required. Default bind is `127.0.0.1` only.
//! [`CompleteClient`] POSTs the same JSON from the other process.
//! Kernel `keel-rt` does not depend on this crate.

mod client;

pub use client::{CompleteClient, CompleteClientError, Decision};

use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Json;
use axum::Router;
use keel_rt::{CompleteError, Resume, ResumeToken, Runtime};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use thiserror::Error;

/// Default listen address: loopback, ephemeral port. Never `0.0.0.0`.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// Reject JSON larger than this. Oversized body does not call `complete`.
pub const MAX_COMPLETE_BODY: usize = 1024 * 1024;

/// Shared-secret header (alternative to `Authorization: Bearer …`).
pub const COMPLETE_SECRET_HEADER: &str = "x-keel-complete";

/// Shared secret for `POST /complete`. Not identity. Empty is rejected.
#[derive(Clone)]
pub struct CompleteSecret(Arc<str>);

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SecretError {
    #[error("complete secret must not be empty")]
    Empty,
}

impl CompleteSecret {
    pub fn new(secret: impl Into<String>) -> Result<Self, SecretError> {
        let s = secret.into();
        if s.is_empty() {
            return Err(SecretError::Empty);
        }
        Ok(Self(Arc::from(s)))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CompleteSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompleteSecret(..)")
    }
}

/// `POST /complete` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteBody {
    pub token: ResumeToken,
    pub resume: Resume,
}

#[derive(Clone)]
struct App {
    runtime: Arc<Runtime>,
    secret: CompleteSecret,
}

/// Router a caller can nest or serve. Path is `/complete`.
pub fn router(runtime: Arc<Runtime>, secret: CompleteSecret) -> Router {
    Router::new()
        .route("/complete", post(complete_handler))
        .layer(DefaultBodyLimit::max(MAX_COMPLETE_BODY))
        .with_state(App { runtime, secret })
}

fn provided_secret(headers: &HeaderMap) -> Option<&[u8]> {
    if let Some(v) = headers.get(COMPLETE_SECRET_HEADER) {
        return Some(v.as_bytes());
    }
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "))
}

fn secrets_equal(expected: &str, got: &[u8]) -> bool {
    let exp = expected.as_bytes();
    if exp.len() != got.len() {
        return false;
    }
    exp.iter()
        .zip(got.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

async fn complete_handler(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<CompleteBody>,
) -> StatusCode {
    match provided_secret(&headers) {
        Some(got) if secrets_equal(app.secret.as_str(), got) => {}
        _ => return StatusCode::UNAUTHORIZED,
    }
    match app.runtime.complete(body.token, body.resume).await {
        Ok(()) => StatusCode::OK,
        Err(CompleteError::UnknownToken) => StatusCode::NOT_FOUND,
        Err(CompleteError::Cancelled) => StatusCode::CONFLICT,
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

/// Bind [`DEFAULT_BIND`] (`127.0.0.1`) and serve. Never `0.0.0.0`.
pub async fn serve(runtime: Arc<Runtime>, secret: CompleteSecret) -> std::io::Result<()> {
    serve_on(runtime, secret, DEFAULT_BIND).await
}

/// Explicit address. `0.0.0.0` only if the caller passes it.
pub async fn serve_on(
    runtime: Arc<Runtime>,
    secret: CompleteSecret,
    addr: SocketAddr,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(runtime, secret)).await
}

/// Bind `127.0.0.1:0` and return the local address plus the server task.
pub async fn serve_ephemeral(
    runtime: Arc<Runtime>,
    secret: CompleteSecret,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = tokio::net::TcpListener::bind(DEFAULT_BIND).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move { axum::serve(listener, router(runtime, secret)).await });
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

    #[test]
    fn default_bind_is_loopback_not_unspecified() {
        assert!(DEFAULT_BIND.ip().is_loopback());
        assert!(!DEFAULT_BIND.ip().is_unspecified());
        assert_eq!(DEFAULT_BIND.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(DEFAULT_BIND.port(), 0);
    }

    #[test]
    fn empty_secret_is_rejected() {
        assert_eq!(CompleteSecret::new("").unwrap_err(), SecretError::Empty);
        assert!(CompleteSecret::new("s").is_ok());
        assert_eq!(
            format!("{:?}", CompleteSecret::new("s").unwrap()),
            "CompleteSecret(..)"
        );
    }

    #[test]
    fn secrets_equal_rejects_wrong_and_empty() {
        assert!(secrets_equal("abc", b"abc"));
        assert!(!secrets_equal("abc", b"ab"));
        assert!(!secrets_equal("abc", b"abd"));
        assert!(!secrets_equal("abc", b""));
    }

    #[test]
    fn provided_secret_reads_bearer_or_header() {
        let mut h = HeaderMap::new();
        assert!(provided_secret(&h).is_none());
        h.insert(AUTHORIZATION, "Basic nope".parse().unwrap());
        assert!(provided_secret(&h).is_none());
        h.insert(AUTHORIZATION, "Bearer tok".parse().unwrap());
        assert_eq!(provided_secret(&h), Some(&b"tok"[..]));
        h.insert(COMPLETE_SECRET_HEADER, "hdr".parse().unwrap());
        assert_eq!(provided_secret(&h), Some(&b"hdr"[..]));
    }

    #[test]
    fn serve_source_does_not_default_to_unspecified() {
        let src = include_str!("lib.rs");
        let serve_fn = src.split("pub async fn serve(").nth(1).unwrap();
        let serve_body = serve_fn.split("pub async fn serve_on").next().unwrap();
        assert!(
            serve_body.contains("DEFAULT_BIND") && serve_body.contains("serve_on"),
            "serve must use DEFAULT_BIND via serve_on, not an implicit unspecified bind"
        );
    }
}
