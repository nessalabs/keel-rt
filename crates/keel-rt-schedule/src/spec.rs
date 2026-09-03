//! Cron + timezone + definition. No tokio. Next fire is a function of `now`.

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use keel_rt::{Timestamp, WorkflowDefinition};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

/// One reusable definition on a 5-field cron in an IANA timezone.
/// The definition is not scheduled on [`WorkflowDefinition`].
///
/// [`Clone`] is an `Arc` bump: cron, tz, and strings are interned once at
/// build (not copied per armed spec or per tick).
#[derive(Clone, Debug)]
pub struct ScheduleSpec {
    inner: Arc<SpecInner>,
}

#[derive(Debug)]
struct SpecInner {
    cron: Cron,
    tz: Tz,
    definition: Arc<WorkflowDefinition>,
    expr: Arc<str>,
    tz_name: Arc<str>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SpecError {
    #[error("cron must be 5 fields (min hour dom month dow), found {found}")]
    CronFields { found: usize },
    #[error("invalid cron: {0}")]
    Cron(String),
    #[error("unknown IANA timezone: {0}")]
    Timezone(String),
}

impl ScheduleSpec {
    /// `cron` is 5-field (`min hour dom month dow`). `tz` is IANA (e.g. `UTC`).
    pub fn new(
        cron: impl Into<String>,
        tz: impl Into<String>,
        definition: WorkflowDefinition,
    ) -> Result<Self, SpecError> {
        Self::with_shared(cron, tz, Arc::new(definition))
    }

    /// Same as [`Self::new`] with a shared definition (N specs, one DAG).
    pub fn with_shared(
        cron: impl Into<String>,
        tz: impl Into<String>,
        definition: Arc<WorkflowDefinition>,
    ) -> Result<Self, SpecError> {
        let expr: Arc<str> = cron.into().into();
        let tz_name: Arc<str> = tz.into().into();
        let found = expr.split_whitespace().count();
        if found != 5 {
            return Err(SpecError::CronFields { found });
        }
        let parsed = Cron::new(expr.as_ref())
            .with_seconds_optional()
            .parse()
            .map_err(|e| SpecError::Cron(e.to_string()))?;
        let tz =
            Tz::from_str(tz_name.as_ref()).map_err(|_| SpecError::Timezone(tz_name.to_string()))?;
        Ok(Self {
            inner: Arc::new(SpecInner {
                cron: parsed,
                tz,
                definition,
                expr,
                tz_name,
            }),
        })
    }

    pub fn cron_expr(&self) -> &str {
        &self.inner.expr
    }

    pub fn timezone(&self) -> &str {
        &self.inner.tz_name
    }

    pub fn definition(&self) -> &WorkflowDefinition {
        &self.inner.definition
    }

    /// Next fire strictly after `now`. None if the expression never fires again
    /// (including when `now` does not fit a chrono instant — not due-now).
    ///
    /// DST is croner's `find_next_occurrence` in this timezone, not a
    /// keel-rt wall-time interpreter:
    ///
    /// - Spring-forward gap: a missing local minute is not invented. From
    ///   just before the America/Vancouver 2026 jump, `30 2 * * *` lands
    ///   on the first valid instant after the gap (03:00 PDT /
    ///   `2026-03-08T10:00:00Z`), not a fabricated 02:30 on the missing
    ///   hour and not the next calendar day's 02:30.
    /// - Fall-back overlap: the next occurrence after `now` is one fire,
    ///   not both copies of the same local minute.
    pub fn next_after(&self, now: Timestamp) -> Option<Timestamp> {
        let utc = DateTime::<Utc>::from_timestamp_millis(i64::try_from(now.as_millis()).ok()?)?;
        let local = utc.with_timezone(&self.inner.tz);
        let next = self.inner.cron.find_next_occurrence(&local, false).ok()?;
        let ms = next.with_timezone(&Utc).timestamp_millis();
        if ms < 0 {
            return None;
        }
        Some(Timestamp(ms as u64))
    }
}
