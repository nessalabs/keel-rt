---- MODULE WaitToken ----
EXTENDS Naturals, FiniteSets
VARIABLES current, offered, cancelled, accepted, acceptedToken, acceptedCancelled
vars == <<current,offered,cancelled,accepted,acceptedToken,acceptedCancelled>>
Init == /\ current \in {1,2} /\ offered \in {1,2} /\ cancelled = FALSE
        /\ accepted = FALSE /\ acceptedToken = 0 /\ acceptedCancelled = FALSE
Cancel == /\ ~accepted /\ ~cancelled /\ cancelled' = TRUE
          /\ UNCHANGED <<current,offered,accepted,acceptedToken,acceptedCancelled>>
Complete == /\ ~accepted /\ ~cancelled /\ offered = current
            /\ accepted' = TRUE /\ acceptedToken' = offered /\ acceptedCancelled' = cancelled
            /\ UNCHANGED <<current,offered,cancelled>>
Next == Cancel \/ Complete
CurrentTokenOnly == accepted => acceptedToken = current
NoAcceptAfterCancel == ~acceptedCancelled
====
