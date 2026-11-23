----------------------------- MODULE MCResolver -----------------------------
(***************************************************************************)
(* Constants and invariants for model checking Resolver.                   *)
(***************************************************************************)
EXTENDS Resolver, TLC

\* The transactions are interchangeable, and the keys are interchangeable.
Symm == Permutations(Txns) \cup Permutations(Keys)

\* Reachability. TLC must find a state that violates each of these. A
\* violation shows that the model reaches the state.

\* The Resolver rejects a transaction for a conflict.
NeverConflict == ~(\E t \in Txns : status[t] = "conflict")

\* The Resolver rejects a transaction as too old in the epoch of its read
\* version.
NeverTooOldInEpoch ==
    ~(\E t \in Txns : status[t] = "too_old" /\ epoch = 1)

\* The Resolver of a later epoch rejects a transaction with a read version
\* of an earlier epoch as too old.
NeverTooOldAcrossEpochs ==
    ~(\E t \in Txns : status[t] = "too_old" /\ commitEpoch[t] > 1 /\ readVersion[t] = 1)

\* Two transactions of one batch commit.
NeverTwoInBatch ==
    ~(\E t, u \in Txns :
        /\ t # u
        /\ status[t] = "committed"
        /\ status[u] = "committed"
        /\ commitVersion[t] = commitVersion[u])

\* The Resolver rejects a transaction for a conflict with an earlier
\* transaction of its batch.
NeverConflictInBatch ==
    ~(\E t, u \in Txns :
        /\ status[t] = "conflict"
        /\ status[u] = "committed"
        /\ commitVersion[t] = commitVersion[u]
        /\ order[u] < order[t]
        /\ \A k \in reads[t] : base[k] <= readVersion[t])

\* A transaction with read keys commits in the second epoch.
NeverCommitSecondEpoch ==
    ~(\E t \in Txns : status[t] = "committed" /\ commitEpoch[t] = 2 /\ reads[t] # {})

=============================================================================
