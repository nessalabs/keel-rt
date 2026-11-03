---- MODULE DurableStart ----
EXTENDS Naturals, FiniteSets
VARIABLES committed, acknowledged, launched, abandoned
vars == <<committed,acknowledged,launched,abandoned>>
Init == /\ committed = FALSE /\ acknowledged = FALSE /\ launched = FALSE /\ abandoned = FALSE
Commit == /\ ~committed /\ ~abandoned /\ committed' = TRUE /\ UNCHANGED <<acknowledged,launched,abandoned>>
Acknowledge == /\ committed /\ ~abandoned /\ ~acknowledged /\ acknowledged' = TRUE
               /\ UNCHANGED <<committed,launched,abandoned>>
Launch == /\ committed /\ ~abandoned /\ ~launched /\ launched' = TRUE
          /\ UNCHANGED <<committed,acknowledged,abandoned>>
LoseCaller == /\ ~acknowledged /\ ~abandoned /\ abandoned' = TRUE
              /\ UNCHANGED <<committed,acknowledged,launched>>
Next == Commit \/ Acknowledge \/ Launch \/ LoseCaller
AcknowledgeAfterCommit == acknowledged => committed
LaunchAfterCommit == launched => committed
====
