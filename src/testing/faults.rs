//! I/O-fault harness (Tokio `test-util` analog). No sockets or product
//! resource types — faults are [`NodeOutcome`]s driven by [`FakeClock`](crate::testing::FakeClock).
//!
//! [`NetFault`] composes onto [`ScriptedExecutor`]. `Delay` sleeps on the
//! execution clock and is aborted if [`CancellationToken`] fires first.

use crate::domain::events::Event;
use crate::runtime::sink::EventSink;
use crate::testing::scripted::{ScriptedAction, ScriptedExecutor};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What a flaky node does after `fail_times` Failed attempts.
#[derive(Clone, Debug)]
pub enum FlakyThen {
    Succeed(Bytes),
    Timeout,
}

/// Network-shaped faults. The kernel still sees only [`crate::NodeOutcome`].
#[derive(Clone, Debug)]
pub enum NetFault {
    /// Running, then [`NodeOutcome::Succeeded`] after `FakeClock::advance`.
    Delay(Duration),
    /// Once Running, [`NodeOutcome::TimedOut`] (not a Failed string).
    Timeout,
    /// Connection-reset analog: [`NodeOutcome::Failed`] with `"reset"`.
    Reset,
    /// First attempt TimedOut, second Succeeded (pair with [`crate::RetryPolicy`]).
    TimeoutThenSucceed,
    /// `fail_times` Failed(`"reset"`) outcomes, then Succeed or Timeout.
    Flaky { fail_times: u32, then: FlakyThen },
}

impl NetFault {
    /// TimedOut after a FakeClock delay (socket-timeout analog: stays Running
    /// until the clock hits the deadline).
    pub fn timeout_after(delay: Duration) -> ScriptedAction {
        ScriptedAction::Delay {
            delay,
            then: Box::new(ScriptedAction::TimedOut),
        }
    }

    /// Failed(`"reset"`) after a FakeClock delay (reset mid-flight).
    pub fn reset_after(delay: Duration) -> ScriptedAction {
        ScriptedAction::Delay {
            delay,
            then: Box::new(ScriptedAction::Fail("reset".into())),
        }
    }

    fn push(self, mut exec: ScriptedExecutor) -> ScriptedExecutor {
        match self {
            Self::Delay(d) => exec.delay_succeed(d, Bytes::from_static(b"ok")),
            Self::Timeout => exec.then(ScriptedAction::TimedOut),
            Self::Reset => exec.fail("reset"),
            Self::TimeoutThenSucceed => exec
                .then(ScriptedAction::TimedOut)
                .succeed(Bytes::from_static(b"ok")),
            Self::Flaky { fail_times, then } => {
                for _ in 0..fail_times {
                    exec = exec.fail("reset");
                }
                match then {
                    FlakyThen::Succeed(b) => exec.succeed(b),
                    FlakyThen::Timeout => exec.then(ScriptedAction::TimedOut),
                }
            }
        }
    }
}

impl ScriptedExecutor {
    /// Append a [`NetFault`] script (composes with `succeed` / `fail` / …).
    pub fn fault(self, fault: NetFault) -> Self {
        fault.push(self)
    }

    pub fn timeout(self) -> Self {
        self.then(ScriptedAction::TimedOut)
    }

    pub fn timeout_after(self, delay: Duration) -> Self {
        self.then(NetFault::timeout_after(delay))
    }

    pub fn reset(self) -> Self {
        self.fail("reset")
    }

    pub fn reset_after(self, delay: Duration) -> Self {
        self.then(NetFault::reset_after(delay))
    }
}

/// [`EventSink`] that panics on the Nth `try_emit` (1-based). The scheduler
/// already `catch_unwind`s. Use [`FailingStore`] for store errors.
///
/// [`FailingStore`]: crate::testing::FailingStore
pub struct FaultySink {
    events: Arc<Mutex<Vec<Event>>>,
    panic_on_nth: Option<usize>,
    panic_all: bool,
    hits: AtomicUsize,
}

impl FaultySink {
    pub fn panic_on_nth(n: usize) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            panic_on_nth: Some(n),
            panic_all: false,
            hits: AtomicUsize::new(0),
        }
    }

    pub fn panic_all() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            panic_on_nth: None,
            panic_all: true,
            hits: AtomicUsize::new(0),
        }
    }

    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    pub fn events(&self) -> Vec<Event> {
        self.events.lock().expect("faulty sink").clone()
    }
}

impl EventSink for FaultySink {
    fn try_emit(&self, event: &Event) -> Result<(), crate::runtime::sink::SinkError> {
        let n = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
        if self.panic_all || self.panic_on_nth == Some(n) {
            panic!("FaultySink emit #{n}");
        }
        self.events.lock().expect("faulty sink").push(event.clone());
        Ok(())
    }
}
