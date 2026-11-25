------------------------------- MODULE Watch -------------------------------
(***************************************************************************)
(* The watch of FoundationDB, as redwall implements it.                    *)
(*                                                                         *)
(* Sources:                                                                *)
(*   - fdbclient/NativeAPI.actor.cpp (watchValue, setupWatches) and        *)
(*     fdbserver/storageserver.actor.cpp (watchWaitForValueChange) of      *)
(*     FoundationDB 7.3. The paper does not describe watches.              *)
(*   - "Watches" in redwall.erl and in redwall_storage.erl.                *)
(*                                                                         *)
(* The model contains:                                                     *)
(*   - one key, and a log of records. A record has an LSN and the value    *)
(*     of the key after the record. A record of a client writes the key.   *)
(*   - the acknowledged records, which are a prefix of the log.            *)
(*   - a recovery, which discards the records that are not acknowledged    *)
(*     and adds the recovery transaction. The recovery transaction has an  *)
(*     LSN above every discarded record and does not write the key.        *)
(*   - one StorageServer. It applies the records of the log, also the      *)
(*     records that are not acknowledged, and removes the discarded        *)
(*     records after a recovery (the rollback).                            *)
(*   - one watch with a value and a version, the request of the client     *)
(*     to the StorageServer, the answer, the read of the client after the  *)
(*     answer, and the time limit of the request.                          *)
(*                                                                         *)
(* The model does not contain:                                             *)
(*   - LogServers, epochs and the Resolver. Recovery.tla and Resolver.tla  *)
(*     model them. A read at a read version returns the value of the last  *)
(*     acknowledged record, which Recovery.tla states as StorageSafe and   *)
(*     StorageComplete.                                                    *)
(*   - more than one key, more than one watch, or more than one            *)
(*     StorageServer.                                                      *)
(*   - the watches of a transaction before its commit.                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Values,         \* the values that a record writes
    None,           \* the value of a key without a value
    MaxLsn,         \* bound: the highest LSN
    MaxRecoveries,  \* bound: the number of recoveries
    Trigger,        \* TRUE: the StorageServer reads the key of the watch
                    \*   when it applies a record. FALSE is a defect.
    Recheck,        \* TRUE: the StorageServer reads the key of the watch
                    \*   after a rollback. FALSE is a defect.
    Verify          \* TRUE: the client reads the key after the answer of
                    \*   the StorageServer. FALSE is a defect.

VARIABLES
    log,         \* the records of the log, in the order of their LSNs
    next,        \* the next LSN
    acked,       \* the number of records of the log that are acknowledged
    recoveries,  \* the number of recoveries
    ss,          \* the records that the StorageServer has applied
    watch,       \* the state of the watch: "none", "sent", "kept",
                 \*   "answered", "retry" or "ready"
    wValue,      \* the value of the watch
    wVersion,    \* the version of the request
    wFirst,      \* the version of the first request
    aVersion,    \* the version of the answer of the StorageServer
    events       \* the events that have happened, for reachability

vars == <<log, next, acked, recoveries, ss, watch, wValue, wVersion, wFirst, aVersion, events>>

\* `val' is the value of the key after the record. `write' is TRUE for a
\* record that writes the key, and FALSE for the recovery transaction.
Record == [lsn : 1..MaxLsn, val : Values \cup {None}, write : BOOLEAN]

\* The value of the key and the version after the records `s'.
Val(s) == IF Len(s) = 0 THEN None ELSE s[Len(s)].val
Ver(s) == IF Len(s) = 0 THEN 0 ELSE s[Len(s)].lsn

\* The acknowledged records. A read version is the version of the last one,
\* and a read at a read version returns its value.
Acked == SubSeq(log, 1, acked)

IsPrefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a

Min(a, b) == IF a <= b THEN a ELSE b

\* The number of records at the start of `a' that are also at the start of
\* `b'.
Common(a, b) ==
    CHOOSE n \in 0..Min(Len(a), Len(b)) :
        /\ SubSeq(a, 1, n) = SubSeq(b, 1, n)
        /\ n = Min(Len(a), Len(b)) \/ a[n + 1] # b[n + 1]

TypeOK ==
    /\ log \in Seq(Record)
    /\ next \in 1..(MaxLsn + 1)
    /\ acked \in 0..Len(log)
    /\ recoveries \in 0..MaxRecoveries
    /\ ss \in Seq(Record)
    /\ watch \in {"none", "sent", "kept", "answered", "retry", "ready"}
    /\ wValue \in Values \cup {None}
    /\ wVersion \in 0..MaxLsn
    /\ wFirst \in 0..MaxLsn
    /\ aVersion \in 0..MaxLsn
    /\ events \subseteq {"resend", "rollback_answer", "timeout"}

Init ==
    /\ log = <<>>
    /\ next = 1
    /\ acked = 0
    /\ recoveries = 0
    /\ ss = <<>>
    /\ watch = "none"
    /\ wValue = None
    /\ wVersion = 0
    /\ wFirst = 0
    /\ aVersion = 0
    /\ events = {}

(***************************************************************************)
(* The log.                                                                *)
(***************************************************************************)

\* A Proxy sends a record that sets the key to `v'.
Write(v) ==
    /\ next <= MaxLsn
    /\ log' = Append(log, [lsn |-> next, val |-> v, write |-> TRUE])
    /\ next' = next + 1
    /\ UNCHANGED <<acked, recoveries, ss, watch, wValue, wVersion, wFirst, aVersion, events>>

\* Every LogServer has stored the next record, and the Proxy answers the
\* client.
Ack ==
    /\ acked < Len(log)
    /\ acked' = acked + 1
    /\ UNCHANGED <<log, next, recoveries, ss, watch, wValue, wVersion, wFirst, aVersion, events>>

\* A recovery discards the records that are not acknowledged. The recovery
\* transaction of the new epoch has an LSN above every discarded record,
\* and does not write the key.
Recover ==
    /\ recoveries < MaxRecoveries
    /\ next <= MaxLsn
    /\ log' = Append(Acked, [lsn |-> next, val |-> Val(Acked), write |-> FALSE])
    /\ acked' = acked + 1
    /\ next' = next + 1
    /\ recoveries' = recoveries + 1
    /\ UNCHANGED <<ss, watch, wValue, wVersion, wFirst, aVersion, events>>

(***************************************************************************)
(* The StorageServer.                                                      *)
(***************************************************************************)

\* The StorageServer answers a kept watch whose value differs.
Answer(applied, read, event) ==
    IF watch = "kept" /\ read /\ Val(applied) # wValue
    THEN /\ watch' = "answered"
         /\ aVersion' = Ver(applied)
         /\ events' = events \cup event
    ELSE UNCHANGED <<watch, aVersion, events>>

\* The StorageServer applies the next record of the log. The record can be
\* a record that is not acknowledged. The StorageServer reads the key of
\* the watch only after a record that writes the key.
SSApply ==
    /\ IsPrefix(ss, log)
    /\ Len(ss) < Len(log)
    /\ ss' = Append(ss, log[Len(ss) + 1])
    /\ Answer(ss', Trigger /\ log[Len(ss) + 1].write, {})
    /\ UNCHANGED <<log, next, acked, recoveries, wValue, wVersion, wFirst>>

\* The StorageServer removes the records that a recovery discarded.
SSRollback ==
    /\ ~IsPrefix(ss, log)
    /\ ss' = SubSeq(ss, 1, Common(ss, log))
    /\ Answer(ss', Recheck, {"rollback_answer"})
    /\ UNCHANGED <<log, next, acked, recoveries, wValue, wVersion, wFirst>>

\* The StorageServer receives the request of the watch. It handles the
\* request when its version is at or above the version of the request.
SSReceive ==
    /\ watch = "sent"
    /\ Ver(ss) >= wVersion
    /\ IF Val(ss) # wValue
       THEN /\ watch' = "answered"
            /\ aVersion' = Ver(ss)
       ELSE /\ watch' = "kept"
            /\ UNCHANGED aVersion
    /\ UNCHANGED <<log, next, acked, recoveries, ss, wValue, wVersion, wFirst, events>>

\* The time limit of the request passes, or the StorageServer answers
\* `timed_out'.
Timeout ==
    /\ watch \in {"sent", "kept"}
    /\ watch' = "retry"
    /\ events' = events \cup {"timeout"}
    /\ UNCHANGED <<log, next, acked, recoveries, ss, wValue, wVersion, wFirst, aVersion>>

(***************************************************************************)
(* The client.                                                             *)
(***************************************************************************)

\* A transaction reads the key at a read version and creates the watch, or
\* a transaction writes the key and creates the watch, and its record is
\* acknowledged. The value of the watch is the value of the last
\* acknowledged record, and the version of the watch is its LSN.
Create ==
    /\ watch = "none"
    /\ watch' = "sent"
    /\ wValue' = Val(Acked)
    /\ wVersion' = Ver(Acked)
    /\ wFirst' = Ver(Acked)
    /\ UNCHANGED <<log, next, acked, recoveries, ss, aVersion, events>>

\* The client reads the key at a read version. If the value differs from
\* the value of the watch, the future is ready. Otherwise the client sends
\* the watch again with the read version.
Read ==
    IF Val(Acked) # wValue
    THEN /\ watch' = "ready"
         /\ UNCHANGED <<wVersion, events>>
    ELSE /\ watch' = "sent"
         /\ wVersion' = Ver(Acked)
         /\ events' = events \cup {"resend"}

\* The client has the answer of the StorageServer. It waits for a read
\* version at or above the version of the answer, and reads the key.
\* Without `Verify', the future is ready at once.
ClientAnswer ==
    /\ watch = "answered"
    /\ IF Verify
       THEN /\ Ver(Acked) >= aVersion
            /\ Read
       ELSE /\ watch' = "ready"
            /\ UNCHANGED <<wVersion, events>>
    /\ UNCHANGED <<log, next, acked, recoveries, ss, wValue, wFirst, aVersion>>

\* The client reads the key after a request without an answer.
ClientRetry ==
    /\ watch = "retry"
    /\ Read
    /\ UNCHANGED <<log, next, acked, recoveries, ss, wValue, wFirst, aVersion>>

Next ==
    \/ \E v \in Values : Write(v)
    \/ Ack
    \/ Recover
    \/ SSApply
    \/ SSRollback
    \/ SSReceive
    \/ Timeout
    \/ Create
    \/ ClientAnswer
    \/ ClientRetry

\* The fairness conditions for `Fires'. A record is acknowledged unless a
\* recovery discards it. The StorageServer and the client take their
\* steps. `Timeout' has no fairness condition, so `Fires' does not depend
\* on the time limit.
Fairness ==
    /\ WF_vars(Ack)
    /\ WF_vars(SSApply)
    /\ WF_vars(SSRollback)
    /\ WF_vars(SSReceive)
    /\ WF_vars(ClientAnswer)
    /\ WF_vars(ClientRetry)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* When the future is ready, an acknowledged record above the version of
\* the watch has written another value than the value of the watch.
ReadySound ==
    watch = "ready" =>
        \E i \in 1..acked : log[i].lsn > wFirst /\ log[i].val # wValue

\* If the value of the last acknowledged record differs from the value of
\* the watch from some time on, the future becomes ready.
Fires ==
    <>[](watch # "none" /\ Val(Acked) # wValue) => <>(watch = "ready")

=============================================================================
