---- MODULE LeaseFence ----
EXTENDS Naturals, FiniteSets
VARIABLES epoch, offered, wrote, launchChecked, writeEpoch, writeCurrent, launchEpoch, launchCurrent
vars == <<epoch,offered,wrote,launchChecked,writeEpoch,writeCurrent,launchEpoch,launchCurrent>>
Init == /\ epoch = 1 /\ offered \in {1,2} /\ wrote = FALSE /\ launchChecked = FALSE
        /\ writeEpoch = 0 /\ writeCurrent = 0 /\ launchEpoch = 0 /\ launchCurrent = 0
Takeover == /\ epoch = 1 /\ epoch' = 2
            /\ UNCHANGED <<offered,wrote,launchChecked,writeEpoch,writeCurrent,launchEpoch,launchCurrent>>
Write == /\ ~wrote /\ offered = epoch /\ offered <= epoch /\ wrote' = TRUE
         /\ writeEpoch' = offered /\ writeCurrent' = epoch
         /\ UNCHANGED <<epoch,offered,launchChecked,launchEpoch,launchCurrent>>
CheckLaunch == /\ ~launchChecked /\ offered = epoch /\ offered <= epoch /\ launchChecked' = TRUE
          /\ launchEpoch' = offered /\ launchCurrent' = epoch
          /\ UNCHANGED <<epoch,offered,wrote,writeEpoch,writeCurrent>>
Next == Takeover \/ Write \/ CheckLaunch
CurrentWriteOnly == wrote => writeEpoch = writeCurrent
CurrentLaunchCheckOnly == launchChecked => launchEpoch = launchCurrent
====
