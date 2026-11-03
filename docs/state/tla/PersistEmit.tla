---- MODULE PersistEmit ----
EXTENDS Naturals, FiniteSets
VARIABLES persisted, everPersisted, emitted, sinkFailed
vars == <<persisted,everPersisted,emitted,sinkFailed>>
Init == /\ persisted = FALSE /\ everPersisted = FALSE /\ emitted = FALSE /\ sinkFailed = FALSE
Persist == /\ ~persisted /\ persisted' = TRUE /\ everPersisted' = TRUE /\ UNCHANGED <<emitted,sinkFailed>>
Emit == /\ persisted /\ ~emitted /\ emitted' = TRUE /\ UNCHANGED <<persisted,everPersisted,sinkFailed>>
SinkFailure == /\ emitted /\ ~sinkFailed /\ sinkFailed' = TRUE /\ UNCHANGED <<persisted,everPersisted,emitted>>
Next == Persist \/ Emit \/ SinkFailure
PersistBeforeEmit == emitted => everPersisted
SinkCannotUndoPersist == everPersisted => persisted
====
