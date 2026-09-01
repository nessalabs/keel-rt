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
        // `as_millis() as u64` wraps: 2^61 seconds is a multiple of 2^64 ms.
        let ms = d.as_millis();
        let add = if ms > u128::from(u64::MAX) {
            u64::MAX
        } else {
            ms as u64
        };
        Self(self.0.saturating_add(add))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `as_millis() as u64` wraps: `1<<61` seconds is 2^64 * 125 ms, which
    /// truncated to u64 is 0. Park would treat Duration::from_secs(1<<61) as
    /// due immediately. Saturate instead.
    #[test]
    fn saturating_add_huge_duration_does_not_wrap_to_now() {
        let now = Timestamp(100);
        let wrapped = Duration::from_secs(1 << 61).as_millis() as u64;
        assert_eq!(wrapped, 0, "precondition: the old cast wraps to 0");
        let at = now.saturating_add(Duration::from_secs(1 << 61));
        assert_eq!(
            at,
            Timestamp(u64::MAX),
            "huge backoff must saturate, not become T==now (got {at})"
        );
        let max = now.saturating_add(Duration::MAX);
        assert_eq!(max, Timestamp(u64::MAX));
        let small = Timestamp(5).saturating_add(Duration::from_millis(7));
        assert_eq!(small, Timestamp(12));
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
