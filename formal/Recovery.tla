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
(*   - the recovery transaction: the first record of an epoch, which the   *)
(*     Sequencer sends.                                                    *)
(*   - the gap between the Recovery Version and the first LSN of an epoch, *)
(*     and the limit on the versions in flight.                            *)
(*   - the read version of the Sequencer, and the requests of clients for  *)
(*     a read version, with the confirmation that the epoch is live.       *)
(*   - one StorageServer, which applies records and rolls back.            *)
(*                                                                         *)
(* The rules for the gap and for the confirmation are the rules of         *)
(* FoundationDB 7.3. The paper does not state them. The limit on the       *)
(* versions in flight is a rule of deadlift.                               *)
(*                                                                         *)
(* The model does not contain:                                             *)
(*   - Resolvers. The model treats every transaction as admitted.          *)
(*     Resolver.tla models the Resolver.                                   *)
(*   - keys and values. The StorageServer of the model has a set of        *)
(*     applied records.                                                    *)
(*   - tags. A log record has a set of K LogServers that store the body    *)
(*     of the record.                                                      *)
(*   - the content of the recovery transaction and the transaction state   *)
(*     store.                                                              *)
(*   - the clock. An LSN is the previous LSN plus 1.                       *)
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
    CasWrite,    \* TRUE: a Sequencer writes the coordinated state only if
                 \*   the recovery count is still the count that the
                 \*   Sequencer wrote. FALSE is unsafe.
    Gap,         \* the first LSN of an epoch is the Recovery Version plus
                 \*   Gap (MAX_VERSIONS_IN_FLIGHT in FoundationDB). It is 1
                 \*   if the Recovery Version is 0.
    LimitInFlight, \* TRUE: a Sequencer assigns an LSN only if the LSN is
                 \*   less than Gap above its read version. FALSE is unsafe
                 \*   for the StorageServer.
    Report,      \* TRUE: the Sequencer learns that an LSN is committed
                 \*   before the client does. FALSE is unsafe for read
                 \*   versions.
    Confirms,    \* the number of LogServers that must confirm that the
                 \*   epoch is live before a Proxy gives a read version.
                 \*   The rule is K. A lower number is unsafe.
    Reads,       \* the ids of the requests for a read version
    Storage,     \* TRUE: the model has the StorageServer
    Rollback     \* TRUE: the StorageServer removes the records above the
                 \*   end version of an epoch. FALSE is unsafe.

ASSUME K \in 1..Cardinality(Logs)
ASSUME MinReplies \in 1..Cardinality(Logs)
ASSUME Gap \in Nat \ {0}
ASSUME Confirms \in 0..Cardinality(Logs)

Epochs   == 1..MaxEpoch
Versions == 1..MaxVersion

VARIABLES
    \* The coordinated state.
    csCount,   \* the recovery count
    csCur,     \* the epoch whose LogServers are the current log system
    csEnded,   \* the epochs that a later epoch has ended
    csEnd,     \* csEnd[e]: the Recovery Version that ended epoch e
    \* The Sequencer of each epoch.
    phase,     \* "none", "locking", "recruited", "written" or "accepting"
    old,       \* old[e]: the epoch that the Sequencer of e recovers
    replied,   \* replied[e]: the old LogServers that answered the Sequencer
    pev,       \* pev[e]: the previous epoch's end version
    rv,        \* rv[e]: the Recovery Version
    last,      \* last[e]: the last LSN that the Sequencer assigned
    first,     \* first[e]: the LSN of the recovery transaction, or 0
    live,      \* live[e]: the read version of the Sequencer
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
    \* The requests of clients for a read version.
    rEpoch,    \* rEpoch[r]: the epoch of the Proxy that r asked, or 0
    rConf,     \* rConf[r]: the LogServers that confirmed the epoch
    rSeq,      \* rSeq[r]: the read version from the Sequencer, or 0
    rVer,      \* rVer[r]: the read version that the client received, or 0
    \* The StorageServer.
    ssEpoch,   \* the epoch whose LogServers the StorageServer reads
    ssVersion, \* the LSN of the last record that it applied
    ssApplied, \* <<e, v>>: it applied LSN v of epoch e and has not removed it
    ssRolled,  \* history: the StorageServer has removed a record
    \* History variables.
    succ,      \* succ[e]: the epoch that ended epoch e, or 0
    acked,     \* <<e, v>>: the Proxy of e answered "committed" for LSN v
    lost,      \* <<e, v>>: a recovery kept LSN v of epoch e and found no body
    given,     \* the read versions that a Sequencer has had
    rSeen      \* rSeen[r]: the highest acknowledged LSN when r started

logVars  == <<rep, prevOf, kcvOf, dv, kcv, data, stop>>
csVars   == <<csCount, csCur, csEnded, csEnd, succ>>
seqVars  == <<phase, old, replied, pev, rv, last, first, live, given>>
readVars == <<rEpoch, rConf, rSeq, rVer, rSeen>>
ssVars   == <<ssEpoch, ssVersion, ssApplied, ssRolled>>

vars == <<logVars, csVars, seqVars, readVars, ssVars, pkcv, acked, lost>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

TypeOK ==
    /\ csCount \in Epochs
    /\ csCur \in Epochs
    /\ csEnded \subseteq Epochs
    /\ csEnd \in [Epochs -> 0..MaxVersion]
    /\ phase \in [Epochs -> {"none", "locking", "recruited", "written", "accepting"}]
    /\ old \in [Epochs -> 0..MaxEpoch]
    /\ replied \in [Epochs -> SUBSET Logs]
    /\ pev \in [Epochs -> 0..MaxVersion]
    /\ rv \in [Epochs -> 0..MaxVersion]
    /\ last \in [Epochs -> 0..MaxVersion]
    /\ first \in [Epochs -> 0..MaxVersion]
    /\ live \in [Epochs -> 0..MaxVersion]
    /\ given \subseteq Versions
    /\ succ \in [Epochs -> 0..MaxEpoch]
    /\ rEpoch \in [Reads -> 0..MaxEpoch]
    /\ rConf \in [Reads -> SUBSET Logs]
    /\ rSeq \in [Reads -> 0..MaxVersion]
    /\ rVer \in [Reads -> 0..MaxVersion]
    /\ rSeen \in [Reads -> 0..MaxVersion]
    /\ ssEpoch \in Epochs
    /\ ssVersion \in 0..MaxVersion
    /\ ssApplied \subseteq Epochs \X Versions
    /\ ssRolled \in BOOLEAN
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

\* Epoch 1 is the first transaction system. In the initial state, its
\* Sequencer has written the coordinated state and has not sent the recovery
\* transaction.
Init ==
    /\ csCount = 1
    /\ csCur = 1
    /\ csEnded = {}
    /\ csEnd = [e \in Epochs |-> 0]
    /\ phase = [e \in Epochs |-> IF e = 1 THEN "written" ELSE "none"]
    /\ old = [e \in Epochs |-> 0]
    /\ replied = [e \in Epochs |-> {}]
    /\ pev = [e \in Epochs |-> 0]
    /\ rv = [e \in Epochs |-> 0]
    /\ last = [e \in Epochs |-> 0]
    /\ first = [e \in Epochs |-> 0]
    /\ live = [e \in Epochs |-> 0]
    /\ given = {}
    /\ succ = [e \in Epochs |-> 0]
    /\ rEpoch = [r \in Reads |-> 0]
    /\ rConf = [r \in Reads |-> {}]
    /\ rSeq = [r \in Reads |-> 0]
    /\ rVer = [r \in Reads |-> 0]
    /\ rSeen = [r \in Reads |-> 0]
    /\ ssEpoch = 1
    /\ ssVersion = 0
    /\ ssApplied = {}
    /\ ssRolled = FALSE
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

\* The first LSN of epoch n: the LSN of the recovery transaction. The rule
\* is the rule of readTransactionSystemState in FoundationDB 7.3.
First(n) == IF rv[n] = 0 THEN 1 ELSE rv[n] + Gap

\* The Sequencer of epoch n sends the recovery transaction to every
\* LogServer of the epoch. Its previous LSN is the Recovery Version, and its
\* KCV is the previous epoch's end version. Paper, section 2.4.4.
RecoveryTxn(n, R) ==
    /\ phase[n] = "written"
    /\ first[n] = 0
    /\ Cardinality(R) = K
    /\ First(n) <= MaxVersion
    /\ LET v == First(n) IN
        /\ rep' = [rep EXCEPT ![n][v] = R]
        /\ prevOf' = [prevOf EXCEPT ![n][v] = rv[n]]
        /\ kcvOf' = [kcvOf EXCEPT ![n][v] = pev[n]]
        /\ last' = [last EXCEPT ![n] = v]
        /\ first' = [first EXCEPT ![n] = v]
    /\ UNCHANGED <<csVars, phase, old, replied, pev, rv, live, given, pkcv,
                   dv, kcv, data, stop, acked, lost, readVars, ssVars>>

\* ACCEPTING_COMMITS. Every LogServer of epoch n has persisted the recovery
\* transaction. The Sequencer starts to assign LSNs. Its read version is
\* the LSN of the recovery transaction.
Accept(n) ==
    /\ phase[n] = "written"
    /\ first[n] # 0
    /\ \A l \in Logs : dv[n][l] >= first[n]
    /\ phase' = [phase EXCEPT ![n] = "accepting"]
    /\ live' = [live EXCEPT ![n] = first[n]]
    /\ given' = given \cup {first[n]}
    /\ UNCHANGED <<csVars, logVars, old, replied, pev, rv, last, first, pkcv,
                   acked, lost, readVars, ssVars>>

\* The Sequencer of epoch e assigns the next LSN. The Proxy sends the record
\* to every LogServer of the epoch. The LogServers in R receive the body. The
\* other LogServers receive an empty body. The record carries the previous
\* LSN and the KCV of the Proxy.
\*
\* With LimitInFlight, the Sequencer assigns an LSN only if the LSN is less
\* than Gap above its read version.
Assign(e, R) ==
    /\ phase[e] = "accepting"
    /\ last[e] < MaxVersion
    /\ LimitInFlight => last[e] + 1 < live[e] + Gap
    /\ Cardinality(R) = K
    /\ LET v == last[e] + 1 IN
        /\ rep' = [rep EXCEPT ![e][v] = R]
        /\ prevOf' = [prevOf EXCEPT ![e][v] = last[e]]
        /\ kcvOf' = [kcvOf EXCEPT ![e][v] = pkcv[e]]
        /\ last' = [last EXCEPT ![e] = v]
    /\ UNCHANGED <<csVars, phase, old, replied, pev, rv, first, live, given,
                   pkcv, dv, kcv, data, stop, acked, lost, readVars, ssVars>>

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
    /\ UNCHANGED <<csVars, seqVars, rep, prevOf, kcvOf, pkcv, stop, acked,
                   lost, readVars, ssVars>>

\* The LogServers that must persist LSN v before the Proxy answers.
Required(e, v) == IF AckAll THEN Logs ELSE rep[e][v]

\* The Proxy of epoch e raises its KCV to LSN v, reports the LSN to the
\* Sequencer, and answers "committed". The model joins the three steps.
\* With Report, the Sequencer raises its read version to the LSN. The Proxy
\* does not know whether a later epoch exists.
\*
\* The recovery transaction has no client, and is not acknowledged.
Ack(e, v) ==
    /\ phase[e] = "accepting"
    /\ rep[e][v] # {}
    /\ v > first[e]
    /\ <<e, v>> \notin acked
    /\ \A l \in Required(e, v) : dv[e][l] >= v
    /\ acked' = acked \cup {<<e, v>>}
    /\ pkcv' = [pkcv EXCEPT ![e] = IF v > @ THEN v ELSE @]
    /\ live' = [live EXCEPT ![e] = IF Report /\ v > @ THEN v ELSE @]
    /\ given' = IF Report THEN given \cup {v} ELSE given
    /\ UNCHANGED <<csVars, logVars, phase, old, replied, pev, rv, last, first,
                   lost, readVars, ssVars>>

-----------------------------------------------------------------------------
(* Read versions. The rule is the rule of getLiveCommittedVersion and      *)
(* confirmEpochLive in FoundationDB 7.3. The paper does not state it.      *)

MaxAcked == IF acked = {} THEN 0 ELSE Max({a[2] : a \in acked})

\* A client asks the Proxy of epoch e for a read version. The history
\* variable rSeen keeps the highest LSN that is acknowledged at this time.
ReadStart(r, e) ==
    /\ rEpoch[r] = 0
    /\ phase[e] = "accepting"
    /\ rEpoch' = [rEpoch EXCEPT ![r] = e]
    /\ rSeen' = [rSeen EXCEPT ![r] = MaxAcked]
    /\ UNCHANGED <<csVars, logVars, seqVars, pkcv, acked, lost, rConf, rSeq,
                   rVer, ssVars>>

\* LogServer l confirms that the epoch is live: no Sequencer has stopped
\* it.
ReadConfirm(r, l) ==
    /\ rEpoch[r] # 0
    /\ rVer[r] = 0
    /\ l \notin rConf[r]
    /\ stop[rEpoch[r]][l] = 0
    /\ rConf' = [rConf EXCEPT ![r] = @ \cup {l}]
    /\ UNCHANGED <<csVars, logVars, seqVars, pkcv, acked, lost, rEpoch, rSeq,
                   rVer, rSeen, ssVars>>

\* The Sequencer answers with its read version.
ReadSeq(r) ==
    /\ rEpoch[r] # 0
    /\ rSeq[r] = 0
    /\ rSeq' = [rSeq EXCEPT ![r] = live[rEpoch[r]]]
    /\ UNCHANGED <<csVars, logVars, seqVars, pkcv, acked, lost, rEpoch, rConf,
                   rVer, rSeen, ssVars>>

\* The Proxy answers the client.
ReadDone(r) ==
    /\ rSeq[r] # 0
    /\ rVer[r] = 0
    /\ Cardinality(rConf[r]) >= Confirms
    /\ rVer' = [rVer EXCEPT ![r] = rSeq[r]]
    /\ UNCHANGED <<csVars, logVars, seqVars, pkcv, acked, lost, rEpoch, rConf,
                   rSeq, rSeen, ssVars>>

-----------------------------------------------------------------------------
(* The StorageServer. Paper, sections 2.4.3 and 2.4.4.                     *)

\* The StorageServer applies LSN v of the epoch that it reads. It has
\* received the body from LogServer l. The previous LSN of the record is
\* the version of the StorageServer. The StorageServer does not wait until
\* every LogServer has persisted the record.
SSApply(l, v) ==
    /\ Storage
    /\ rep[ssEpoch][v] # {}
    /\ v \in data[ssEpoch][l]
    /\ prevOf[ssEpoch][v] = ssVersion
    /\ ssVersion' = v
    /\ ssApplied' = ssApplied \cup {<<ssEpoch, v>>}
    /\ UNCHANGED <<csVars, logVars, seqVars, pkcv, acked, lost, readVars,
                   ssEpoch, ssRolled>>

\* The StorageServer learns from the coordinated state that the epoch that
\* it reads has ended. If its version is above the end version, it removes
\* the records above the end version (the rollback). If its version is the
\* end version, it continues with the next epoch.
SSFollow ==
    /\ Storage
    /\ ssEpoch \in csEnded
    /\ LET end == csEnd[ssEpoch] IN
        \/ /\ ssVersion > end
           /\ ssVersion' = end
           /\ ssApplied' = IF Rollback
                            THEN {a \in ssApplied : ~(a[1] = ssEpoch /\ a[2] > end)}
                            ELSE ssApplied
           /\ ssRolled' = (ssRolled \/ \E a \in ssApplied : a[1] = ssEpoch /\ a[2] > end)
           /\ UNCHANGED ssEpoch
        \/ /\ ssVersion = end
           /\ ssEpoch' = succ[ssEpoch]
           /\ UNCHANGED <<ssVersion, ssApplied, ssRolled>>
    /\ UNCHANGED <<csVars, logVars, seqVars, pkcv, acked, lost, readVars>>

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
    /\ UNCHANGED <<csCur, csEnded, csEnd, succ, logVars, replied, pev, rv,
                   last, first, live, given, pkcv, acked, lost, readVars,
                   ssVars>>

\* The Sequencer of epoch n stops LogServer l of the old epoch. The
\* LogServer answers with its DV and its KCV. A LogServer that a higher
\* epoch has stopped does not answer.
LockLog(n, l) ==
    /\ phase[n] = "locking"
    /\ stop[old[n]][l] < n
    /\ stop' = [stop EXCEPT ![old[n]][l] = n]
    /\ replied' = [replied EXCEPT ![n] = @ \cup {l}]
    /\ UNCHANGED <<csVars, phase, old, pev, rv, last, first, live, given,
                   rep, prevOf, kcvOf, pkcv, dv, kcv, data, acked, lost,
                   readVars, ssVars>>

\* RECRUITING. With enough answers, the Sequencer computes the PEV as the
\* maximum of the KCVs and the RV as the minimum of the DVs.
Recruit(n) ==
    /\ phase[n] = "locking"
    /\ Cardinality(replied[n]) >= MinReplies
    /\ pev' = [pev EXCEPT ![n] = Max({kcv[old[n]][l] : l \in replied[n]})]
    /\ rv' = [rv EXCEPT ![n] = Min({dv[old[n]][l] : l \in replied[n]})]
    /\ phase' = [phase EXCEPT ![n] = "recruited"]
    /\ UNCHANGED <<csVars, logVars, old, replied, last, first, live, given,
                   pkcv, acked, lost, readVars, ssVars>>

\* WRITING_CSTATE. The Sequencer of epoch n writes the new log system and
\* the end of the old epoch to the coordinated state. The write is a
\* compare-and-set on the recovery count. The Sequencer then sends the
\* recovery transaction (RecoveryTxn) and accepts commits (Accept).
\*
\* The records of the old epoch in PEV + 1 .. RV are copied from the old
\* LogServers that answered to the new LogServers. The model copies each record to every new
\* LogServer. A record in that range whose body is on no LogServer that
\* answered is added to `lost'.
WriteCState(n) ==
    /\ phase[n] = "recruited"
    /\ CasWrite => csCount = n
    /\ LET o == old[n]
           \* The LSNs between the PEV and the RV that are records of the
           \* old epoch. With the gap between epochs, not every number in
           \* that range is the LSN of a record.
           range == {v \in (pev[n] + 1)..rv[n] :
                        rep[o][v] # {} \/ \E l \in Logs : v \in data[o][l]}
           have == {v \in range : \E l \in replied[n] : v \in data[o][l]}
       IN
        /\ csCur' = n
        /\ csEnded' = csEnded \cup {o}
        /\ csEnd' = [csEnd EXCEPT ![o] = rv[n]]
        /\ succ' = [succ EXCEPT ![o] = n]
        /\ data' = [data EXCEPT ![n] = [l \in Logs |-> have]]
        /\ lost' = lost \cup {<<o, v>> : v \in range \ have}
        /\ dv' = [dv EXCEPT ![n] = [l \in Logs |-> rv[n]]]
        /\ kcv' = [kcv EXCEPT ![n] = [l \in Logs |-> pev[n]]]
        /\ last' = [last EXCEPT ![n] = rv[n]]
        /\ pkcv' = [pkcv EXCEPT ![n] = pev[n]]
    /\ phase' = [phase EXCEPT ![n] = "written"]
    /\ UNCHANGED <<csCount, old, replied, pev, rv, first, live, given, rep,
                   prevOf, kcvOf, stop, acked, readVars, ssVars>>

Next ==
    \/ \E e \in Epochs, R \in SUBSET Logs : RecoveryTxn(e, R) \/ Assign(e, R)
    \/ \E e \in Epochs : Accept(e)
    \/ \E r \in Reads, e \in Epochs : ReadStart(r, e)
    \/ \E r \in Reads, l \in Logs : ReadConfirm(r, l)
    \/ \E r \in Reads : ReadSeq(r) \/ ReadDone(r)
    \/ \E l \in Logs, v \in Versions : SSApply(l, v)
    \/ SSFollow
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

\* A read version is at or above every LSN that was acknowledged, in any
\* epoch, before the client asked for the read version.
ReadVersionOrder ==
    \A r \in Reads : rVer[r] # 0 => rVer[r] >= rSeen[r]

\* A recovery discarded LSN v of epoch e.
Discarded(e, v) == e \in csEnded /\ v > csEnd[e]

\* The StorageServer answers a read at a read version R when its version
\* is at or above R. At that time it has no record at or below R that a
\* recovery discarded. A recovery transaction has no content, so the
\* property does not apply to it.
StorageSafe ==
    \A R \in given :
        ssVersion >= R =>
            \A a \in ssApplied :
                (a[2] <= R /\ a[2] # first[a[1]]) => ~Discarded(a[1], a[2])

\* At that time the StorageServer has every acknowledged record at or
\* below R.
StorageComplete ==
    \A R \in given :
        ssVersion >= R => \A a \in acked : a[2] <= R => a \in ssApplied

\* No LogServer persists a record of a client at or above the first LSN of
\* the next epoch. The StorageServer depends on this property. It holds
\* with LimitInFlight.
\*
\* The property does not hold for the recovery transaction. If epoch e
\* never accepts commits, the next epoch can have the Recovery Version of
\* epoch e, and then its recovery transaction has the LSN of the recovery
\* transaction of epoch e. A recovery transaction has no content.
BelowNextEpoch ==
    \A e \in csEnded, l \in Logs :
        (succ[e] # 0 /\ rv[succ[e]] # 0 /\ dv[e][l] # first[e])
            => dv[e][l] < First(succ[e])

=============================================================================
