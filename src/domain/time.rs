use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Milliseconds since Unix epoch. Snapshot-friendly value object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn from_millis(ms: u64) -> Self {
        Self(ms)
    }

    pub fn as_millis(self) -> u64 {
        self.0
    }

    pub fn saturating_add(self, d: Duration) -> Self {
        Self(self.0.saturating_add(d.as_millis() as u64))
    }

    pub fn saturating_duration_since(self, earlier: Timestamp) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }

    pub fn now_system() -> Self {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self(ms)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
