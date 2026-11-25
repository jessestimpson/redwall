------------------------------ MODULE MCWatch ------------------------------
(***************************************************************************)
(* Constants and invariants for model checking Watch.                      *)
(***************************************************************************)
EXTENDS Watch, TLC

\* Reachability. TLC must find a state that violates each of these. A
\* violation shows that the model reaches the state.

\* The future is ready.
NeverReady == watch # "ready"

\* The client sends the watch again after a read that returned the value
\* of the watch.
NeverResend == "resend" \notin events

\* The StorageServer answers a watch after a rollback.
NeverRollbackAnswer == "rollback_answer" \notin events

\* The future is ready after a request without an answer.
NeverReadyAfterTimeout == ~(watch = "ready" /\ "timeout" \in events)

=============================================================================
