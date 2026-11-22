----------------------------- MODULE MCRecovery -----------------------------
(***************************************************************************)
(* Constants and invariants for model checking Recovery.                   *)
(***************************************************************************)
EXTENDS Recovery, TLC

\* The LogServers of one epoch are interchangeable.
Symm == Permutations(Logs)

\* Reachability. TLC must find a state that violates each of these. A
\* violation shows that the model reaches the state.

\* The third epoch acknowledges a transaction.
NeverThirdEpoch ==
    ~(\E v \in Versions : <<3, v>> \in acked)

\* A recovery discards a record that a LogServer of the old epoch persisted.
NeverDiscard ==
    ~(\E e \in csEnded, l \in Logs : dv[e][l] > csEnd[e])

\* A recovery copies a record from the old epoch to the new epoch.
NeverCopy ==
    ~(\E e \in Epochs, l \in Logs : phase[e] = "accepting" /\ e > 1 /\ data[e][l] # {} /\ last[e] = rv[e])

=============================================================================
