---- MODULE DependencyJoin ----
EXTENDS Naturals, FiniteSets
VARIABLES pred, join, dependent, peer
vars == <<pred,join,dependent,peer>>
P == {1,2}
Init == /\ pred = [p \in P |-> "Pending"] /\ join \in {"AllSucceeded","AllDone"}
        /\ dependent = FALSE /\ peer = FALSE
Finish(p,o) == /\ pred[p] = "Pending" /\ pred' = [pred EXCEPT ![p] = o]
               /\ UNCHANGED <<join,dependent,peer>>
Satisfied == IF join = "AllSucceeded" THEN (\A p \in P : pred[p] = "Succeeded")
             ELSE (\A p \in P : pred[p] # "Pending")
StartDependent == /\ ~dependent /\ Satisfied /\ ~peer /\ dependent' = TRUE
                  /\ UNCHANGED <<pred,join,peer>>
StartPeer == /\ ~peer /\ ~dependent /\ peer' = TRUE /\ UNCHANGED <<pred,join,dependent>>
FinishPeer == /\ peer /\ peer' = FALSE /\ UNCHANGED <<pred,join,dependent>>
Next == StartDependent \/ StartPeer \/ FinishPeer
        \/ (\E p \in P, o \in {"Succeeded","Failed"}: Finish(p,o))
JoinBeforeStart == dependent => Satisfied
PermitBound == ~(dependent /\ peer)
====
