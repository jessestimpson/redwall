------------------------------ MODULE Recovery ------------------------------
(***************************************************************************)
(* The logging protocol and the transaction system recovery of             *)
(* FoundationDB.                                                           *)
(*                                                                         *)
(* Sources:                                                                *)
(*   - Zhou et al., "FoundationDB: A Distributed Unbundled Transactional   *)
(*     Key Value Store", SIGMOD 2021, sections 2.4.3, 2.4.4 and 2.5. The   *)
(*     comments below call it "the paper".                                 *)
(*   - design/recovery-internals.md in the FoundationDB repository. The    *)
(*     comments below call it "the recovery document".                     *)
(*                                                                         *)
(* The model uses the terms of the paper: epoch, Sequencer, Proxy,         *)
(* LogServer, LSN, Known Committed Version (KCV), Durable Version (DV),    *)
(* Recovery Version (RV), previous epoch's end version (PEV) and           *)
(* coordinated state.                                                      *)
(*                                                                         *)
(* The model contains:                                                     *)
(*   - the coordinated state, as one register with atomic operations. In   *)
(*     deadlift a peace cluster holds this register.                       *)
(*   - one Sequencer and one Proxy for each epoch.                         *)
(*   - one LogServer for each element of Logs, in each epoch.              *)
(*                                                                         *)
(* The model does not contain:                                             *)
(*   - Resolvers. The model treats every transaction as admitted.          *)
(*   - StorageServers and tags. A log record has a set of K LogServers     *)
(*     that store the body of the record.                                  *)
(*   - the recovery transaction and the transaction state store.           *)
(*   - the election of a ClusterController.                                *)
(*   - the removal of old LogServers.                                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Logs,        \* the ids of the LogServers of one epoch (m LogServers)
    K,           \* the replication degree of a log record (k in the paper)
    MaxEpoch,    \* bound: the highest epoch
    MaxVersion,  \* bound: the highest LSN
    AckAll,      \* TRUE: a Proxy answers a client after every LogServer of
                 \*   the epoch has persisted the LSN.
                 \* FALSE: a Proxy answers after the K LogServers that store
                 \*   the body have persisted the LSN. This is unsafe.
    MinReplies,  \* the number of old LogServers that must answer a recovery.
                 \*   The paper requires more than m - k replies.
    CasWrite     \* TRUE: a Sequencer writes the coordinated state only if
                 \*   the recovery count is still the count that the
                 \*   Sequencer wrote. FALSE is unsafe.

ASSUME K \in 1..Cardinality(Logs)
ASSUME MinReplies \in 1..Cardinality(Logs)

Epochs   == 1..MaxEpoch
Versions == 1..MaxVersion

VARIABLES
    \* The coordinated state.
    csCount,   \* the recovery count
    csCur,     \* the epoch whose LogServers are the current log system
    csEnded,   \* the epochs that a later epoch has ended
    csEnd,     \* csEnd[e]: the Recovery Version that ended epoch e
    \* The Sequencer of each epoch.
    phase,     \* "none", "locking", "recruited" or "accepting"
    old,       \* old[e]: the epoch that the Sequencer of e recovers
    replied,   \* replied[e]: the old LogServers that answered the Sequencer
    pev,       \* pev[e]: the previous epoch's end version
    rv,        \* rv[e]: the Recovery Version
    last,      \* last[e]: the last LSN that the Sequencer assigned
    \* The log records of each epoch.
    rep,       \* rep[e][v]: the LogServers that store the body of LSN v.
               \*   The empty set means that v is not assigned in e.
    prevOf,    \* prevOf[e][v]: the previous LSN, sent with LSN v
    kcvOf,     \* kcvOf[e][v]: the KCV of the Proxy, sent with LSN v
    \* The Proxy of each epoch.
    pkcv,      \* pkcv[e]: the KCV of the Proxy
    \* The LogServers of each epoch.
    dv,        \* dv[e][l]: the Durable Version
    kcv,       \* kcv[e][l]: the maximum KCV received
    data,      \* data[e][l]: the LSNs whose body the LogServer stores
    stop,      \* stop[e][l]: the epoch that stopped the LogServer, or 0
    \* History variables.
    acked,     \* <<e, v>>: the Proxy of e answered "committed" for LSN v
    lost       \* <<e, v>>: a recovery kept LSN v of epoch e and found no body

vars == <<csCount, csCur, csEnded, csEnd, phase, old, replied, pev, rv, last,
          rep, prevOf, kcvOf, pkcv, dv, kcv, data, stop, acked, lost>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

TypeOK ==
    /\ csCount \in Epochs
    /\ csCur \in Epochs
    /\ csEnded \subseteq Epochs
    /\ csEnd \in [Epochs -> 0..MaxVersion]
    /\ phase \in [Epochs -> {"none", "locking", "recruited", "accepting"}]
    /\ old \in [Epochs -> 0..MaxEpoch]
    /\ replied \in [Epochs -> SUBSET Logs]
    /\ pev \in [Epochs -> 0..MaxVersion]
    /\ rv \in [Epochs -> 0..MaxVersion]
    /\ last \in [Epochs -> 0..MaxVersion]
    /\ rep \in [Epochs -> [Versions -> SUBSET Logs]]
    /\ prevOf \in [Epochs -> [Versions -> 0..MaxVersion]]
    /\ kcvOf \in [Epochs -> [Versions -> 0..MaxVersion]]
    /\ pkcv \in [Epochs -> 0..MaxVersion]
    /\ dv \in [Epochs -> [Logs -> 0..MaxVersion]]
    /\ kcv \in [Epochs -> [Logs -> 0..MaxVersion]]
    /\ data \in [Epochs -> [Logs -> SUBSET Versions]]
    /\ stop \in [Epochs -> [Logs -> 0..MaxEpoch]]
    /\ acked \subseteq Epochs \X Versions
    /\ lost \subseteq Epochs \X Versions

\* Epoch 1 is the first transaction system. It accepts commits in the
\* initial state.
Init ==
    /\ csCount = 1
    /\ csCur = 1
    /\ csEnded = {}
    /\ csEnd = [e \in Epochs |-> 0]
    /\ phase = [e \in Epochs |-> IF e = 1 THEN "accepting" ELSE "none"]
    /\ old = [e \in Epochs |-> 0]
    /\ replied = [e \in Epochs |-> {}]
    /\ pev = [e \in Epochs |-> 0]
    /\ rv = [e \in Epochs |-> 0]
    /\ last = [e \in Epochs |-> 0]
    /\ rep = [e \in Epochs |-> [v \in Versions |-> {}]]
    /\ prevOf = [e \in Epochs |-> [v \in Versions |-> 0]]
    /\ kcvOf = [e \in Epochs |-> [v \in Versions |-> 0]]
    /\ pkcv = [e \in Epochs |-> 0]
    /\ dv = [e \in Epochs |-> [l \in Logs |-> 0]]
    /\ kcv = [e \in Epochs |-> [l \in Logs |-> 0]]
    /\ data = [e \in Epochs |-> [l \in Logs |-> {}]]
    /\ stop = [e \in Epochs |-> [l \in Logs |-> 0]]
    /\ acked = {}
    /\ lost = {}

-----------------------------------------------------------------------------
(* The commit path. Paper, sections 2.4.1 and 2.4.3.                       *)

\* The Sequencer of epoch e assigns the next LSN. The Proxy sends the record
\* to every LogServer of the epoch. The LogServers in R receive the body. The
\* other LogServers receive an empty body. The record carries the previous
\* LSN and the KCV of the Proxy.
Assign(e, R) ==
    /\ phase[e] = "accepting"
    /\ last[e] < MaxVersion
    /\ Cardinality(R) = K
    /\ LET v == last[e] + 1 IN
        /\ rep' = [rep EXCEPT ![e][v] = R]
        /\ prevOf' = [prevOf EXCEPT ![e][v] = last[e]]
        /\ kcvOf' = [kcvOf EXCEPT ![e][v] = pkcv[e]]
        /\ last' = [last EXCEPT ![e] = v]
    /\ UNCHANGED <<csCount, csCur, csEnded, csEnd, phase, old, replied, pev,
                   rv, pkcv, dv, kcv, data, stop, acked, lost>>

\* LogServer l of epoch e persists LSN v. A LogServer processes the records
\* in the order of the LSNs, so the previous LSN of the record must be the
\* Durable Version of the LogServer. A stopped LogServer accepts no record.
Persist(e, l, v) ==
    /\ rep[e][v] # {}
    /\ stop[e][l] = 0
    /\ dv[e][l] = prevOf[e][v]
    /\ dv' = [dv EXCEPT ![e][l] = v]
    /\ kcv' = [kcv EXCEPT ![e][l] = IF kcvOf[e][v] > @ THEN kcvOf[e][v] ELSE @]
    /\ data' = [data EXCEPT ![e][l] = IF l \in rep[e][v] THEN @ \cup {v} ELSE @]
    /\ UNCHANGED <<csCount, csCur, csEnded, csEnd, phase, old, replied, pev,
                   rv, last, rep, prevOf, kcvOf, pkcv, stop, acked, lost>>

\* The LogServers that must persist LSN v before the Proxy answers.
Required(e, v) == IF AckAll THEN Logs ELSE rep[e][v]

\* The Proxy of epoch e answers "committed" for LSN v, and raises its KCV.
\* The Proxy does not know whether a later epoch exists.
Ack(e, v) ==
    /\ rep[e][v] # {}
    /\ <<e, v>> \notin acked
    /\ \A l \in Required(e, v) : dv[e][l] >= v
    /\ acked' = acked \cup {<<e, v>>}
    /\ pkcv' = [pkcv EXCEPT ![e] = IF v > @ THEN v ELSE @]
    /\ UNCHANGED <<csCount, csCur, csEnded, csEnd, phase, old, replied, pev,
                   rv, last, rep, prevOf, kcvOf, dv, kcv, data, stop, lost>>

-----------------------------------------------------------------------------
(* Recovery. Paper, section 2.4.4. The phase names in the comments are the *)
(* names of the recovery document.                                         *)

\* READING_CSTATE and LOCKING_CSTATE. The Sequencer of epoch n reads the
\* coordinated state and raises the recovery count from n - 1 to n. This is
\* one compare-and-set on the register. A Sequencer whose compare-and-set
\* fails has no effect and is not modelled.
LockCState(n) ==
    /\ phase[n] = "none"
    /\ csCount = n - 1
    /\ csCount' = n
    /\ phase' = [phase EXCEPT ![n] = "locking"]
    /\ old' = [old EXCEPT ![n] = csCur]
    /\ UNCHANGED <<csCur, csEnded, csEnd, replied, pev, rv, last, rep, prevOf,
                   kcvOf, pkcv, dv, kcv, data, stop, acked, lost>>

\* The Sequencer of epoch n stops LogServer l of the old epoch. The
\* LogServer answers with its DV and its KCV. A LogServer that a higher
\* epoch has stopped does not answer.
LockLog(n, l) ==
    /\ phase[n] = "locking"
    /\ stop[old[n]][l] < n
    /\ stop' = [stop EXCEPT ![old[n]][l] = n]
    /\ replied' = [replied EXCEPT ![n] = @ \cup {l}]
    /\ UNCHANGED <<csCount, csCur, csEnded, csEnd, phase, old, pev, rv, last,
                   rep, prevOf, kcvOf, pkcv, dv, kcv, data, acked, lost>>

\* RECRUITING. With enough answers, the Sequencer computes the PEV as the
\* maximum of the KCVs and the RV as the minimum of the DVs.
Recruit(n) ==
    /\ phase[n] = "locking"
    /\ Cardinality(replied[n]) >= MinReplies
    /\ pev' = [pev EXCEPT ![n] = Max({kcv[old[n]][l] : l \in replied[n]})]
    /\ rv' = [rv EXCEPT ![n] = Min({dv[old[n]][l] : l \in replied[n]})]
    /\ phase' = [phase EXCEPT ![n] = "recruited"]
    /\ UNCHANGED <<csCount, csCur, csEnded, csEnd, old, replied, last, rep,
                   prevOf, kcvOf, pkcv, dv, kcv, data, stop, acked, lost>>

\* WRITING_CSTATE and ACCEPTING_COMMITS. The Sequencer of epoch n writes the
\* new log system and the end of the old epoch to the coordinated state, and
\* starts to accept commits. The write is a compare-and-set on the recovery
\* count.
\*
\* The records in PEV + 1 .. RV are copied from the old LogServers that
\* answered to the new LogServers. The model copies each record to every new
\* LogServer. A record in that range whose body is on no LogServer that
\* answered is added to `lost'.
WriteCState(n) ==
    /\ phase[n] = "recruited"
    /\ CasWrite => csCount = n
    /\ LET o == old[n]
           range == (pev[n] + 1)..rv[n]
           have == {v \in range : \E l \in replied[n] : v \in data[o][l]}
       IN
        /\ csCur' = n
        /\ csEnded' = csEnded \cup {o}
        /\ csEnd' = [csEnd EXCEPT ![o] = rv[n]]
        /\ data' = [data EXCEPT ![n] = [l \in Logs |-> have]]
        /\ lost' = lost \cup {<<o, v>> : v \in range \ have}
        /\ dv' = [dv EXCEPT ![n] = [l \in Logs |-> rv[n]]]
        /\ kcv' = [kcv EXCEPT ![n] = [l \in Logs |-> pev[n]]]
        /\ last' = [last EXCEPT ![n] = rv[n]]
        /\ pkcv' = [pkcv EXCEPT ![n] = pev[n]]
    /\ phase' = [phase EXCEPT ![n] = "accepting"]
    /\ UNCHANGED <<csCount, old, replied, pev, rv, rep, prevOf, kcvOf, stop,
                   acked>>

Next ==
    \/ \E e \in Epochs, R \in SUBSET Logs : Assign(e, R)
    \/ \E e \in Epochs, l \in Logs, v \in Versions : Persist(e, l, v)
    \/ \E e \in Epochs, v \in Versions : Ack(e, v)
    \/ \E n \in Epochs \ {1} : LockCState(n) \/ Recruit(n) \/ WriteCState(n)
    \/ \E n \in Epochs \ {1}, l \in Logs : LockLog(n, l)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties.                                                             *)

\* A transaction that a Proxy answered as committed is not discarded: when
\* an epoch has ended, every acknowledged LSN of the epoch is at or below
\* the Recovery Version that ended the epoch.
Durable ==
    \A a \in acked : a[1] \in csEnded => a[2] <= csEnd[a[1]]

\* Every record that a recovery keeps and must copy has its body on an old
\* LogServer that answered the recovery.
NoLoss == lost = {}

\* An LSN is acknowledged in one epoch only. Two epochs that accept commits
\* at the same time violate this property.
UniqueVersions ==
    \A a, b \in acked : a[2] = b[2] => a[1] = b[1]

\* The KCV of a LogServer is never above an LSN that the epoch has not
\* acknowledged or received from the previous epoch.
KcvSound ==
    \A e \in Epochs, l \in Logs :
        kcv[e][l] > pev[e] => <<e, kcv[e][l]>> \in acked

=============================================================================
