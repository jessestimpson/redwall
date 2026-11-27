---------------------------- MODULE TraceRecovery ----------------------------
(***************************************************************************)
(* Trace validation. This module checks whether one run of the code is a   *)
(* behaviour of Recovery.                                                  *)
(*                                                                         *)
(* dst/redwall_dst_trace.erl writes `TraceData` from a simulated run, and  *)
(* describes how the changes of the run are converted into the actions     *)
(* below. `Trace` is a sequence of actions. Each element names an action   *)
(* of Recovery and gives the arguments of the action. This spec has one    *)
(* behaviour, which takes the actions in order. If an action is not        *)
(* enabled when it is the next action of the sequence, the behaviour ends. *)
(* The run is therefore a behaviour of the model exactly when TLC reaches  *)
(* the end of `Trace`. In that case the depth of the search is             *)
(* Len(Trace) + 1. check_trace.sh compares the depth that TLC reports with *)
(* Len(Trace) + 1.                                                         *)
(*                                                                         *)
(* An action also carries `chk`: values that the code has after the        *)
(* action. The variables of the model must equal these values. It is       *)
(* therefore not sufficient that the model permits each action of the      *)
(* code. The code must also reach the same state as the model.             *)
(*                                                                         *)
(* An "Invalid" action is a change for which the derivation found no       *)
(* action. An "Invalid" action is never enabled.                           *)
(***************************************************************************)
EXTENDS Recovery, Sequences, TraceData

VARIABLE i      \* the position in Trace of the next action to take

Step(t) ==
    CASE t.act = "LockCState"  -> LockCState(t.e)
      [] t.act = "LockLog"     -> LockLog(t.e, t.l)
      [] t.act = "Recruit"     -> Recruit(t.e)
      [] t.act = "WriteCState" -> WriteCState(t.e)
      [] t.act = "RecoveryTxn" -> RecoveryTxn(t.e, t.r)
      [] t.act = "Accept"      -> Accept(t.e)
      [] t.act = "Assign"      -> Assign(t.e, t.r)
      [] t.act = "Persist"     -> Persist(t.e, t.l, t.v)
      [] t.act = "Ack"         -> Ack(t.e, t.v)
      [] OTHER                 -> FALSE

\* c = <<variable, a, b, c, value>>. An argument that the variable does not
\* have is 0.
Holds(c) ==
    CASE c[1] = "csCount" -> csCount' = c[5]
      [] c[1] = "csCur"   -> csCur' = c[5]
      [] c[1] = "csEnd"   -> csEnd'[c[2]] = c[5]
      [] c[1] = "rv"      -> rv'[c[2]] = c[5]
      [] c[1] = "pev"     -> pev'[c[2]] = c[5]
      [] c[1] = "first"   -> first'[c[2]] = c[5]
      [] c[1] = "last"    -> last'[c[2]] = c[5]
      [] c[1] = "live"    -> live'[c[2]] = c[5]
      [] c[1] = "pkcv"    -> pkcv'[c[2]] = c[5]
      [] c[1] = "dv"      -> dv'[c[2]][c[3]] = c[5]
      [] c[1] = "kcv"     -> kcv'[c[2]][c[3]] = c[5]
      [] c[1] = "data"    -> (c[4] \in data'[c[2]][c[3]]) = c[5]
      [] OTHER            -> FALSE

TraceInit == Init /\ i = 1

TraceNext ==
    /\ i <= Len(Trace)
    /\ Step(Trace[i])
    /\ \A k \in 1..Len(Trace[i].chk) : Holds(Trace[i].chk[k])
    /\ i' = i + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, i>>

=============================================================================
