------------------------------ MODULE Resolver ------------------------------
(***************************************************************************)
(* The Resolver of FoundationDB, and the versions that protect a Resolver  *)
(* without history.                                                        *)
(*                                                                         *)
(* Sources:                                                                *)
(*   - Zhou et al., "FoundationDB: A Distributed Unbundled Transactional   *)
(*     Key Value Store", SIGMOD 2021, section 2.4.2 and Algorithm 1. The   *)
(*     comments below call it "the paper".                                 *)
(*   - fdbserver/Resolver.actor.cpp, fdbserver/SkipList.cpp and            *)
(*     fdbserver/ClusterRecovery.actor.cpp of FoundationDB 7.3, for the    *)
(*     MVCC window and for the first version of an epoch. The paper does   *)
(*     not state these rules.                                              *)
(*                                                                         *)
(* The model contains:                                                     *)
(*   - one Resolver for each epoch, with `lastCommit' of Algorithm 1 and   *)
(*     the oldest version of the MVCC window.                              *)
(*   - transactions with a read version, a set of read keys and a set of   *)
(*     write keys.                                                         *)
(*   - commit versions that rise by 1 to MaxStep, as the clock does in     *)
(*     deadlift.                                                           *)
(*   - the start of a new epoch: a Resolver without history, and a first   *)
(*     version that is Gap above the last version.                         *)
(*                                                                         *)
(* The model does not contain:                                             *)
(*   - LogServers, the recovery and the StorageServer. Recovery.tla models *)
(*     them. This model assumes that a recovery keeps every record.        *)
(*   - key ranges. The Resolver of deadlift checks keys.                   *)
(*   - more than one Resolver in an epoch.                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,        \* the keys
    Txns,        \* the ids of the transactions
    MaxVersion,  \* bound: the highest version
    MaxEpoch,    \* bound: the highest epoch
    MaxStep,     \* a commit version is 1 to MaxStep above the last one
    Window,      \* the MVCC window, in versions
    Gap,         \* the first version of an epoch is the last version of
                 \*   the previous epoch plus Gap. A Gap below Window - 1
                 \*   is unsafe.
    Check        \* TRUE: the Resolver checks the read keys. FALSE is
                 \*   unsafe.

Versions == 0..MaxVersion

VARIABLES
    epoch,       \* the current epoch
    last,        \* the last commit version that the Sequencer assigned
    lastCommit,  \* lastCommit[k]: the commit version of the last
                 \*   transaction that wrote k, or 0. Algorithm 1.
    oldest,      \* the oldest version of the MVCC window
    status,      \* status[t]: "idle", "open", "committed", "conflict" or
                 \*   "too_old"
    readVersion, \* readVersion[t]
    reads,       \* reads[t]: the read keys
    writes,      \* writes[t]: the write keys
    commitVersion, \* commitVersion[t], or 0
    commitEpoch  \* commitEpoch[t]: the epoch of the commit, or 0

vars == <<epoch, last, lastCommit, oldest, status, readVersion, reads,
          writes, commitVersion, commitEpoch>>

TypeOK ==
    /\ epoch \in 1..MaxEpoch
    /\ last \in Versions
    /\ lastCommit \in [Keys -> Versions]
    /\ oldest \in Versions
    /\ status \in [Txns -> {"idle", "open", "committed", "conflict", "too_old"}]
    /\ readVersion \in [Txns -> Versions]
    /\ reads \in [Txns -> SUBSET Keys]
    /\ writes \in [Txns -> SUBSET Keys]
    /\ commitVersion \in [Txns -> Versions]
    /\ commitEpoch \in [Txns -> 0..MaxEpoch]

\* Version 1 is the recovery transaction of epoch 1.
Init ==
    /\ epoch = 1
    /\ last = 1
    /\ lastCommit = [k \in Keys |-> 0]
    /\ oldest = 0
    /\ status = [t \in Txns |-> "idle"]
    /\ readVersion = [t \in Txns |-> 0]
    /\ reads = [t \in Txns |-> {}]
    /\ writes = [t \in Txns |-> {}]
    /\ commitVersion = [t \in Txns |-> 0]
    /\ commitEpoch = [t \in Txns |-> 0]

\* A client starts transaction t with the read keys R and the write keys W.
\* The read version is the last commit version. In deadlift, a Proxy
\* reports every stored record to the Sequencer, so the read version
\* reaches the last commit version. A client that waits before the commit
\* has an older read version at the time of the commit.
Begin(t, R, W) ==
    /\ status[t] = "idle"
    /\ W # {}
    /\ status' = [status EXCEPT ![t] = "open"]
    /\ readVersion' = [readVersion EXCEPT ![t] = last]
    /\ reads' = [reads EXCEPT ![t] = R]
    /\ writes' = [writes EXCEPT ![t] = W]
    /\ UNCHANGED <<epoch, last, lastCommit, oldest, commitVersion, commitEpoch>>

\* The Sequencer assigns the commit version last + d to transaction t, and
\* the Resolver of the epoch checks the transaction. Algorithm 1, with the
\* MVCC window of the FoundationDB source:
\*   - the oldest version rises to the commit version minus Window;
\*   - a transaction with read keys and a read version below the oldest
\*     version is too old;
\*   - the Resolver removes the entries of lastCommit below the oldest
\*     version.
Commit(t, d) ==
    /\ status[t] = "open"
    /\ last + d <= MaxVersion
    /\ LET v == last + d
           newOldest == IF v > Window /\ v - Window > oldest THEN v - Window ELSE oldest
           kept == [k \in Keys |-> IF lastCommit[k] < newOldest THEN 0 ELSE lastCommit[k]]
           result ==
               IF readVersion[t] < newOldest /\ reads[t] # {} THEN "too_old"
               ELSE IF Check /\ \E k \in reads[t] : kept[k] > readVersion[t] THEN "conflict"
               ELSE "committed"
       IN
        /\ last' = v
        /\ oldest' = newOldest
        /\ lastCommit' = IF result = "committed"
                         THEN [k \in Keys |-> IF k \in writes[t] THEN v ELSE kept[k]]
                         ELSE kept
        /\ status' = [status EXCEPT ![t] = result]
        /\ commitVersion' = [commitVersion EXCEPT ![t] = v]
        /\ commitEpoch' = [commitEpoch EXCEPT ![t] = epoch]
    /\ UNCHANGED <<epoch, readVersion, reads, writes>>

\* A recovery starts a new epoch. The Resolver of the new epoch has no
\* history. The recovery transaction has the version last + Gap, and the
\* Resolver handles it.
NewEpoch ==
    /\ epoch < MaxEpoch
    /\ last + Gap <= MaxVersion
    /\ LET v == last + Gap IN
        /\ last' = v
        /\ oldest' = IF v > Window THEN v - Window ELSE 0
    /\ epoch' = epoch + 1
    /\ lastCommit' = [k \in Keys |-> 0]
    /\ UNCHANGED <<status, readVersion, reads, writes, commitVersion, commitEpoch>>

Next ==
    \/ \E t \in Txns, R \in SUBSET Keys, W \in SUBSET Keys : Begin(t, R, W)
    \/ \E t \in Txns, d \in 1..MaxStep : Commit(t, d)
    \/ NewEpoch

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties.                                                             *)

\* No committed transaction read a key that another committed transaction
\* wrote at a commit version above the read version of the first
\* transaction and below its commit version. With this property, the
\* committed transactions are serializable in the order of the commit
\* versions (paper, section 2.4.2).
NoConflict ==
    \A t, u \in Txns :
        (/\ t # u
         /\ status[t] = "committed"
         /\ status[u] = "committed"
         /\ commitVersion[u] > readVersion[t]
         /\ commitVersion[u] < commitVersion[t])
        => reads[t] \cap writes[u] = {}

\* A transaction without read keys is never rejected.
BlindWritesCommit ==
    \A t \in Txns : reads[t] = {} => status[t] \in {"idle", "open", "committed"}

=============================================================================
