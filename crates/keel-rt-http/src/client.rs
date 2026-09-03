//! Out-of-process start, inspect, and complete. POSTs the same JSON
//! [`CompleteBody`] the server already accepts. Not a second token type.

use crate::{
    CompleteBody, CompleteSecret, InspectView, StartBody, StartView, CLAIMED_ELSEWHERE,
    SECRET_HEADER,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use keel_rt::{ExecutionId, NodeOutcome, Resume, ResumeToken};
use std::fmt;
use std::time::Duration;
use thiserror::Error;

/// Bound for one start, inspect, or complete request. A hung server is
/// [`KeelClientError::Hung`], not a forever wait. Tokio time.
pub const HANG_BOUND: Duration = Duration::from_secs(5);

/// Thin mapping onto [`Resume`]. Not a second state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Complete(Bytes),
    Fail,
    Reinvoke,
}

impl From<Decision> for Resume {
    fn from(decision: Decision) -> Self {
        match decision {
            Decision::Complete(bytes) => Resume::Complete(NodeOutcome::Succeeded(bytes)),
            Decision::Fail => Resume::Complete(NodeOutcome::failed("failed")),
            Decision::Reinvoke => Resume::Reinvoke,
        }
    }
}

/// Errors from [`KeelClient::start`], [`KeelClient::inspect`], and
/// [`KeelClient::complete`]. Status map matches the server. Display is
/// verb-neutral for shared codes.
#[derive(Debug, Error)]
pub enum KeelClientError {
    #[error("request rejected: missing or wrong secret")]
    Unauthorized,
    #[error("unknown resume token")]
    UnknownToken,
    #[error("unknown execution")]
    UnknownExecution,
    #[error("token belongs to a cancelled execution")]
    Cancelled,
    #[error("request body exceeds server limit")]
    PayloadTooLarge,
    #[error("request rejected")]
    BadRequest,
    #[error("execution claimed elsewhere")]
    ClaimedElsewhere,
    #[error("unexpected status {0}")]
    Unexpected(u16),
    #[error("request hung: no response within hang-bound")]
    Hung,
    #[error("transport: {0}")]
    Transport(String),
}

/// Out-of-process SDK client. This crate implements [`Self::start`],
/// [`Self::inspect`], and [`Self::complete`]. No schedule HTTP.
///
/// Sends [`SECRET_HEADER`] and `Authorization: Bearer` when constructed
/// with a secret. Duplicate complete is Ok (server 200 noop).
/// Does not revive Cancelled. Does not follow redirects.
#[derive(Clone)]
pub struct KeelClient {
    http: Client<HttpConnector, Full<Bytes>>,
    base: String,
    secret: Option<CompleteSecret>,
    hang_bound: Duration,
}

impl KeelClient {
    /// `base_url` is the server root (e.g. `http://127.0.0.1:port`), not a route.
    pub fn new(base_url: impl AsRef<str>, secret: CompleteSecret) -> Result<Self, KeelClientError> {
        Self::build(base_url.as_ref(), Some(secret))
    }

    /// Omits the secret headers. A protected server answers 401.
    pub fn without_secret(base_url: impl AsRef<str>) -> Result<Self, KeelClientError> {
        Self::build(base_url.as_ref(), None)
    }

    fn build(base_url: &str, secret: Option<CompleteSecret>) -> Result<Self, KeelClientError> {
        Ok(Self {
            http: Client::builder(TokioExecutor::new()).build_http(),
            base: base_url.trim_end_matches('/').to_string(),
            secret,
            hang_bound: HANG_BOUND,
        })
    }

    /// One URL builder for start / inspect / complete. No complete-only field.
    fn uri(&self, path: &str) -> Result<Uri, KeelClientError> {
        path_url(&self.base, path)
            .parse::<Uri>()
            .map_err(|e| KeelClientError::Transport(e.to_string()))
    }

    /// Override [`HANG_BOUND`] for this client.
    pub fn hang_bound(mut self, bound: Duration) -> Self {
        self.hang_bound = bound;
        self
    }

    fn with_secret(
        &self,
        mut builder: hyper::http::request::Builder,
    ) -> hyper::http::request::Builder {
        if let Some(secret) = &self.secret {
            builder = builder
                .header(SECRET_HEADER, secret.as_str())
                .header(AUTHORIZATION, format!("Bearer {}", secret.as_str()));
        }
        builder
    }

    /// Headers + hang-bound. Callers map status.
    async fn send(
        &self,
        builder: hyper::http::request::Builder,
        body: Bytes,
    ) -> Result<hyper::Response<hyper::body::Incoming>, KeelClientError> {
        let req = self
            .with_secret(builder)
            .body(Full::new(body))
            .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        match tokio::time::timeout(self.hang_bound, self.http.request(req)).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(KeelClientError::Transport(e.to_string())),
            Err(_) => Err(KeelClientError::Hung),
        }
    }

    /// `POST /start` — same secret and hang-bound as inspect / complete.
    /// Returns the new [`ExecutionId`]. Each call is a new run.
    pub async fn start(
        &self,
        definition: impl Into<StartBody>,
    ) -> Result<ExecutionId, KeelClientError> {
        let json = serde_json::to_vec(&definition.into())
            .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        let resp = self
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri(self.uri("start")?)
                    .header(CONTENT_TYPE, "application/json")
                    .header(CONTENT_LENGTH, json.len()),
                Bytes::from(json),
            )
            .await?;
        match resp.status().as_u16() {
            200 => {
                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?
                    .to_bytes();
                let view: StartView = serde_json::from_slice(&bytes)
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?;
                Ok(view.execution_id)
            }
            401 => Err(KeelClientError::Unauthorized),
            413 => Err(KeelClientError::PayloadTooLarge),
            400 => Err(KeelClientError::BadRequest),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// `GET /inspect/:id` — same secret as complete. Hang-bound applies.
    pub async fn inspect(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<InspectView, KeelClientError> {
        let resp = self
            .send(
                Request::builder()
                    .method(Method::GET)
                    .uri(self.uri(&format!("inspect/{}", execution_id.as_str()))?),
                Bytes::new(),
            )
            .await?;
        match resp.status().as_u16() {
            200 => {
                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| KeelClientError::Transport(e.to_string()))?
                    .to_bytes();
                serde_json::from_slice(&bytes)
                    .map_err(|e| KeelClientError::Transport(e.to_string()))
            }
            401 => Err(KeelClientError::Unauthorized),
            404 => Err(KeelClientError::UnknownExecution),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// POST the same [`CompleteBody`] the server already accepts.
    ///
    /// `resume` is [`Resume`] or [`Decision`].
    pub async fn complete(
        &self,
        token: ResumeToken,
        resume: impl Into<Resume>,
    ) -> Result<(), KeelClientError> {
        let json = serde_json::to_vec(&CompleteBody {
            token,
            resume: resume.into(),
        })
        .map_err(|e| KeelClientError::Transport(e.to_string()))?;
        let resp = self
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri(self.uri("complete")?)
                    .header(CONTENT_TYPE, "application/json")
                    .header(CONTENT_LENGTH, json.len()),
                Bytes::from(json),
            )
            .await?;
        match resp.status().as_u16() {
            200 => Ok(()),
            401 => Err(KeelClientError::Unauthorized),
            404 => Err(KeelClientError::UnknownToken),
            409 => Err(KeelClientError::Cancelled),
            413 => Err(KeelClientError::PayloadTooLarge),
            400 => Err(KeelClientError::BadRequest),
            CLAIMED_ELSEWHERE => Err(KeelClientError::ClaimedElsewhere),
            other => Err(KeelClientError::Unexpected(other)),
        }
    }

    /// `Decision::Complete(bytes)` through [`Self::complete`]. Same
    /// `POST /complete` — not a second endpoint.
    pub async fn approve(
        &self,
        token: ResumeToken,
        output: impl Into<Bytes>,
    ) -> Result<(), KeelClientError> {
        self.complete(token, Decision::Complete(output.into()))
            .await
    }

    /// `Decision::Fail` through [`Self::complete`] — `NodeOutcome::failed("failed")`,
    /// execution [`keel_rt::ExecutionState::Failed`]. There is no `Decision`
    /// reject variant (PR #9 mapping).
    pub async fn reject(&self, token: ResumeToken) -> Result<(), KeelClientError> {
        self.complete(token, Decision::Fail).await
    }
}

impl fmt::Debug for KeelClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeelClient")
            .field("base", &self.base)
            .field("secret", &self.secret)
            .field("hang_bound", &self.hang_bound)
            .finish()
    }
}

fn path_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_maps_onto_resume() {
        let bytes = Bytes::from_static(b"ok");
        assert_eq!(
            Resume::from(Decision::Complete(bytes.clone())),
            Resume::Complete(NodeOutcome::Succeeded(bytes))
        );
        assert_eq!(
            Resume::from(Decision::Fail),
            Resume::Complete(NodeOutcome::failed("failed"))
        );
        assert_eq!(Resume::from(Decision::Reinvoke), Resume::Reinvoke);
        assert_eq!(
            Resume::from(Decision::Fail),
            Resume::Complete(NodeOutcome::failed("failed")),
            "reject is Decision::Fail; no second fail contract"
        );
    }

    #[test]
    fn path_url_trims_trailing_slash() {
        assert_eq!(
            path_url("http://127.0.0.1:9", "complete"),
            "http://127.0.0.1:9/complete"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "/start"),
            "http://127.0.0.1:9/start"
        );
        assert_eq!(
            path_url("http://127.0.0.1:9/", "inspect/exec-1"),
            "http://127.0.0.1:9/inspect/exec-1"
        );
    }

    #[test]
    fn debug_redacts_secret() {
        let secret = CompleteSecret::new("s3cret").unwrap();
        let c = KeelClient::new("http://127.0.0.1:1", secret).unwrap();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("CompleteSecret(..)"), "{dbg}");
        assert!(!dbg.contains("s3cret"), "{dbg}");
    }

    #[test]
    fn hang_bound_default_is_five_seconds() {
        assert_eq!(HANG_BOUND, Duration::from_secs(5));
        assert_eq!(crate::CLAIMED_ELSEWHERE, 423);
    }

    #[test]
    fn inspect_shared_errors_display_does_not_say_complete() {
        for err in [
            KeelClientError::Unauthorized,
            KeelClientError::UnknownExecution,
            KeelClientError::Hung,
        ] {
            let text = err.to_string();
            assert!(
                !text.to_ascii_lowercase().contains("complete"),
                "inspect-shared error must be verb-neutral: {text}"
            );
        }
    }
}
