//! Out-of-process complete. POSTs the same JSON [`CompleteBody`] the server
//! already accepts. Not a second token type.

use crate::{CompleteBody, CompleteSecret, COMPLETE_SECRET_HEADER};
use bytes::Bytes;
use keel_rt::{NodeOutcome, Resume, ResumeToken};
use std::fmt;
use thiserror::Error;

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

/// Errors from [`CompleteClient::complete`]. Status map matches the server.
#[derive(Debug, Error)]
pub enum CompleteClientError {
    #[error("complete rejected: missing or wrong secret")]
    Unauthorized,
    #[error("unknown resume token")]
    UnknownToken,
    #[error("token belongs to a cancelled execution")]
    Cancelled,
    #[error("complete body exceeds server limit")]
    PayloadTooLarge,
    #[error("complete request rejected")]
    BadRequest,
    #[error("unexpected complete status {0}")]
    Unexpected(u16),
    #[error("complete transport: {0}")]
    Transport(#[from] reqwest::Error),
}

/// POSTs `token` + [`Resume`] to `{base}/complete`.
///
/// Sends [`COMPLETE_SECRET_HEADER`] when constructed with a secret.
/// Duplicate complete is Ok (server 200 noop). Does not revive Cancelled.
#[derive(Clone)]
pub struct CompleteClient {
    http: reqwest::Client,
    complete_url: String,
    secret: Option<CompleteSecret>,
}

impl CompleteClient {
    /// `base_url` is the server root (e.g. `http://127.0.0.1:port`), not `/complete`.
    pub fn new(
        base_url: impl AsRef<str>,
        secret: CompleteSecret,
    ) -> Result<Self, CompleteClientError> {
        Self::build(base_url.as_ref(), Some(secret))
    }

    /// Omits the secret headers. A protected server answers 401.
    pub fn without_secret(base_url: impl AsRef<str>) -> Result<Self, CompleteClientError> {
        Self::build(base_url.as_ref(), None)
    }

    fn build(base_url: &str, secret: Option<CompleteSecret>) -> Result<Self, CompleteClientError> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            complete_url: complete_url(base_url),
            secret,
        })
    }

    /// POST the same [`CompleteBody`] the server already accepts.
    ///
    /// `resume` is [`Resume`] or [`Decision`].
    pub async fn complete(
        &self,
        token: ResumeToken,
        resume: impl Into<Resume>,
    ) -> Result<(), CompleteClientError> {
        let mut req = self.http.post(&self.complete_url).json(&CompleteBody {
            token,
            resume: resume.into(),
        });
        if let Some(secret) = &self.secret {
            req = req.header(COMPLETE_SECRET_HEADER, secret.as_str());
        }
        let resp = req.send().await?;
        match resp.status().as_u16() {
            200 => Ok(()),
            401 => Err(CompleteClientError::Unauthorized),
            404 => Err(CompleteClientError::UnknownToken),
            409 => Err(CompleteClientError::Cancelled),
            413 => Err(CompleteClientError::PayloadTooLarge),
            400 => Err(CompleteClientError::BadRequest),
            other => Err(CompleteClientError::Unexpected(other)),
        }
    }
}

impl fmt::Debug for CompleteClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompleteClient")
            .field("complete_url", &self.complete_url)
            .field("secret", &self.secret)
            .finish()
    }
}

fn complete_url(base: &str) -> String {
    format!("{}/complete", base.trim_end_matches('/'))
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
    }

    #[test]
    fn complete_url_trims_trailing_slash() {
        assert_eq!(
            complete_url("http://127.0.0.1:9"),
            "http://127.0.0.1:9/complete"
        );
        assert_eq!(
            complete_url("http://127.0.0.1:9/"),
            "http://127.0.0.1:9/complete"
        );
    }

    #[test]
    fn debug_redacts_secret() {
        let secret = CompleteSecret::new("s3cret").unwrap();
        let c = CompleteClient::new("http://127.0.0.1:1", secret).unwrap();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("CompleteSecret(..)"), "{dbg}");
        assert!(!dbg.contains("s3cret"), "{dbg}");
    }
}
