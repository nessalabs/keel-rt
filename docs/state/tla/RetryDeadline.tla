---- MODULE RetryDeadline ----
EXTENDS Naturals, FiniteSets
VARIABLES now, ready, cancelled, launched, launchedAt, acceptedCancelled
vars == <<now,ready,cancelled,launched,launchedAt,acceptedCancelled>>
Init == /\ now = 0 /\ ready = FALSE /\ cancelled = FALSE /\ launched = FALSE
        /\ launchedAt = 0 /\ acceptedCancelled = FALSE
Tick == /\ now < 3 /\ now' = now+1 /\ UNCHANGED <<ready,cancelled,launched,launchedAt,acceptedCancelled>>
Due == /\ ~ready /\ ~cancelled /\ now >= 2 /\ ready' = TRUE
       /\ UNCHANGED <<now,cancelled,launched,launchedAt,acceptedCancelled>>
Cancel == /\ ~launched /\ ~cancelled /\ cancelled' = TRUE
          /\ UNCHANGED <<now,ready,launched,launchedAt,acceptedCancelled>>
Dispatch == /\ ready /\ ~cancelled /\ ~launched /\ launched' = TRUE
            /\ launchedAt' = now /\ acceptedCancelled' = cancelled /\ UNCHANGED <<now,ready,cancelled>>
Next == Tick \/ Due \/ Cancel \/ Dispatch
NotBeforeDeadline == launched => launchedAt >= 2
NoCancelledDispatch == ~acceptedCancelled
====
