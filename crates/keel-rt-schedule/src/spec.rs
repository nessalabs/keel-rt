//! Cron + timezone + definition. No tokio. Next fire is a function of `now`.

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use keel_rt::{Timestamp, WorkflowDefinition};
use std::str::FromStr;
use thiserror::Error;

/// One reusable definition on a 5-field cron in an IANA timezone.
/// The definition is not scheduled on [`WorkflowDefinition`].
#[derive(Clone, Debug)]
pub struct ScheduleSpec {
    cron: Cron,
    tz: Tz,
    definition: WorkflowDefinition,
    expr: String,
    tz_name: String,
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
        let expr = cron.into();
        let tz_name = tz.into();
        let found = expr.split_whitespace().count();
        if found != 5 {
            return Err(SpecError::CronFields { found });
        }
        let parsed = Cron::new(&expr)
            .with_seconds_optional()
            .parse()
            .map_err(|e| SpecError::Cron(e.to_string()))?;
        let tz = Tz::from_str(&tz_name).map_err(|_| SpecError::Timezone(tz_name.clone()))?;
        Ok(Self {
            cron: parsed,
            tz,
            definition,
            expr,
            tz_name,
        })
    }

    pub fn cron_expr(&self) -> &str {
        &self.expr
    }

    pub fn timezone(&self) -> &str {
        &self.tz_name
    }

    pub fn definition(&self) -> &WorkflowDefinition {
        &self.definition
    }

    /// Next fire strictly after `now`. None if the expression never fires again.
    pub fn next_after(&self, now: Timestamp) -> Option<Timestamp> {
        let utc = DateTime::<Utc>::from_timestamp_millis(i64::try_from(now.as_millis()).ok()?)?;
        let local = utc.with_timezone(&self.tz);
        let next = self.cron.find_next_occurrence(&local, false).ok()?;
        let ms = next.with_timezone(&Utc).timestamp_millis();
        if ms < 0 {
            return None;
        }
        Some(Timestamp(ms as u64))
    }
}
