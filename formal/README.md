# Formal model

The directory has two models:

| Model | Contents | Module for model checking |
|---|---|---|
| [Recovery.tla](Recovery.tla) | The logging protocol, the recovery, read versions and the StorageServer (paper, sections 2.4.3, 2.4.4 and 2.5) | [MCRecovery.tla](MCRecovery.tla) |
| [Resolver.tla](Resolver.tla) | The Resolver and the versions that protect a Resolver without history (paper, section 2.4.2 and Algorithm 1) | [MCResolver.tla](MCResolver.tla) |

The two models are separate because the Resolver depends on versions and
epochs only, and not on the LogServers. One model with both parts has a
state space that TLC does not finish.

## Contents of Recovery.tla

| Part | In the model |
|---|---|
| Coordinated state | One register with two atomic operations: `LockCState` and `WriteCState` |
| Sequencer | One for each epoch. It performs the recovery, sends the recovery transaction, assigns LSNs and keeps the read version |
| Proxy | One for each epoch. It keeps a Known Committed Version, answers clients and gives read versions |
| LogServer | One for each element of `Logs`, in each epoch. It keeps a Durable Version, a Known Committed Version and the bodies of records |
| Log record | An LSN, the previous LSN, the Known Committed Version of the Proxy, and the set of `K` LogServers that store the body |
| Request for a read version | One for each element of `Reads`. The Sequencer answers, and LogServers confirm that the epoch is live, in any order |
| StorageServer | One. It applies the records of one epoch at a time, and removes the records above the end version of an ended epoch |

The model does not contain Resolvers, keys and values, tags, the content
of the recovery transaction, the clock, the election of a
ClusterController or the removal of old LogServers.

### Recovery

1. `LockCState(n)`: the Sequencer of epoch `n` raises the recovery count
   from `n - 1` to `n`, and reads the current log system.
2. `LockLog(n, l)`: the Sequencer stops LogServer `l` of the old epoch. The
   LogServer answers with its Durable Version and its Known Committed
   Version.
3. `Recruit(n)`: with `MinReplies` answers, the Sequencer computes the
   previous epoch's end version (PEV) as the maximum of the Known Committed
   Versions, and the Recovery Version (RV) as the minimum of the Durable
   Versions.
4. `WriteCState(n)`: the Sequencer writes the new log system, if the
   recovery count is still `n`. The records in `PEV + 1 .. RV` are copied to
   the new LogServers.
5. `RecoveryTxn(n, R)`: the Sequencer sends the recovery transaction. Its
   LSN is `RV + Gap`, or 1 if the RV is 0. Its previous LSN is the RV.
6. `Accept(n)`: every LogServer of the epoch has persisted the recovery
   transaction. The Sequencer assigns LSNs. Its read version is the LSN of
   the recovery transaction.

### Commit

- `Assign(e, R)`: the Sequencer assigns the next LSN, if the LSN is less
  than `Gap` above the read version (`LimitInFlight`).
- `Persist(e, l, v)`: a LogServer persists a record.
- `Ack(e, v)`: the Proxy raises its Known Committed Version, the Sequencer
  raises its read version (`Report`), and the Proxy answers the client. The
  model joins the three steps.

### Read version

`ReadStart(r, e)`, `ReadConfirm(r, l)`, `ReadSeq(r)` and `ReadDone(r)`
model one request. The Proxy answers when the Sequencer has answered and
`Confirms` LogServers have confirmed that no Sequencer has stopped them.

### StorageServer

- `SSApply(l, v)`: the StorageServer applies a record whose body is on
  LogServer `l`, if the previous LSN of the record is the version of the
  StorageServer.
- `SSFollow`: the StorageServer learns that its epoch has ended. It removes
  the records above the end version (`Rollback`), or continues with the
  next epoch.

## Contents of Resolver.tla

| Part | In the model |
|---|---|
| Resolver | One for each epoch, with `lastCommit` of Algorithm 1 and the oldest version of the MVCC window |
| Transaction | A read version, a set of read keys and a set of write keys |
| Commit version | 1 to `MaxStep` above the last one, as with the clock of deadlift |
| Batch | A transaction can join the batch of the last commit version. The Resolver checks the transactions of a batch in their order |
| New epoch | A Resolver without history. The first version is the last version plus `Gap` |

The model assumes that a recovery keeps every record. It does not contain
LogServers, the StorageServer or more than one Resolver in an epoch.

A read range and a write range of deadlift are sets of keys in the model.
The model does not have the ordered set of bounds that
`deadlift_resolver` keeps. A unit test compares that set of bounds with one
version for each key, for 3,000 random transactions.

## Invariants

| Model | Invariant | Meaning |
|---|---|---|
| Recovery | `Durable` | When an epoch has ended, every LSN that the Proxy of the epoch acknowledged is at or below the Recovery Version |
| Recovery | `NoLoss` | Every record that a recovery must copy has its body on an old LogServer that answered |
| Recovery | `UniqueVersions` | An LSN is acknowledged in one epoch only |
| Recovery | `KcvSound` | The Known Committed Version of a LogServer is an acknowledged LSN, or the PEV of its epoch |
| Recovery | `BelowNextEpoch` | No LogServer persists a record of a client at or above the first LSN of the next epoch |
| Recovery | `ReadVersionOrder` | A read version is at or above every LSN that was acknowledged, in any epoch, before the client asked |
| Recovery | `StorageSafe` | When the StorageServer has the version of a read version, it has no record of a client at or below that version that a recovery discarded |
| Recovery | `StorageComplete` | At that time the StorageServer has every acknowledged record at or below that version |
| Resolver | `NoConflict` | No committed transaction read a key that another committed transaction wrote between the read version and the commit version of the first one, or at that commit version and earlier in the batch |
| Resolver | `BlindWritesCommit` | A transaction without read keys is never rejected |

## Configs and results

All Recovery configs have 3 LogServers, a replication degree of 2 and a
`Gap` of 2. All Resolver configs have 2 keys, 3 transactions, a window of 3
versions and a `MaxStep` of 2. The results are from one machine with 10
processors, on 2026-11-22.

### Configs that must pass

| Config | Bounds | Invariants | Distinct states | Time |
|---|---|---|---|---|
| `RecoverySafe` | 2 epochs, 7 versions | `Durable`, `NoLoss`, `UniqueVersions`, `KcvSound`, `BelowNextEpoch` | 1,107,907 | 9 s |
| `RecoveryReads` | 2 epochs, 5 versions, 1 request | `ReadVersionOrder` | 10,135,698 | 1 min 23 s |
| `RecoveryStorage` | 2 epochs, 6 versions | `StorageSafe`, `StorageComplete`, `BelowNextEpoch` | 3,580,158 | 37 s |
| `RecoverySafeThreeEpochs` | 3 epochs, 6 versions | As `RecoverySafe` | 48,258,908 | 19 min 5 s |
| `RecoveryStorageThreeEpochs` | 3 epochs, 6 versions | As `RecoveryStorage` | 713,242,009 | 3 h 34 min |
| `ResolverSafe` | 2 epochs, 11 versions, `Gap` 3 | `NoConflict`, `BlindWritesCommit` | 970,557 | 3 s |
| `ResolverSafeThreeEpochs` | 3 epochs, 15 versions, `Gap` 3 | As `ResolverSafe` | 4,445,361 | 14 s |
| `ResolverSmallestGap` | 2 epochs, 10 versions, `Gap` 2 | As `ResolverSafe` | 970,557 | 3 s |

Every config also checks `TypeOK`.

### Configs with a planted bug

Each config must produce a counterexample.

| Config | Planted bug | Violated invariant | Time |
|---|---|---|---|
| `RecoveryAckReplicasOnly` | `AckAll = FALSE` | `Durable` | under 1 s |
| `RecoveryFewReplies` | `MinReplies = 1` | `NoLoss` | under 1 s |
| `RecoveryNoCas` | `CasWrite = FALSE` | `UniqueVersions` | 1 min 45 s |
| `RecoveryNoReport` | `Report = FALSE` | `ReadVersionOrder` | under 1 s |
| `RecoveryFewConfirms` | `Confirms = 1` | `ReadVersionOrder` | 5 s |
| `RecoveryNoRollback` | `Rollback = FALSE` | `StorageSafe` | 1 s |
| `RecoveryNoInFlightLimit` | `LimitInFlight = FALSE` | `StorageSafe` | 5 s |
| `ResolverNoCheck` | `Check = FALSE` | `NoConflict` | under 1 s |
| `ResolverNoBatchCheck` | `CheckBatch = FALSE` | `NoConflict` | under 1 s |
| `ResolverNoGap` | `Gap = 1` with a window of 3 | `NoConflict` | under 1 s |

- **`RecoveryAckReplicasOnly`**. A Proxy answers a client after the `K`
  LogServers that store the body have persisted the LSN. A LogServer that
  received the empty body has a lower Durable Version. A recovery that
  hears from that LogServer chooses a Recovery Version below the
  acknowledged LSN. A Proxy must therefore wait for every LogServer of the
  epoch. The recovery document defines the Known Committed Version in the
  same way: the version that is durable on all LogServers.
- **`RecoveryFewReplies`**. The paper requires more than `m - k` answers
  from `m` LogServers. With fewer answers, the LogServers that answered can
  all lack the body of a record.
- **`RecoveryNoCas`**. Two Sequencers recover the same epoch, and both
  write the coordinated state. Both new epochs acknowledge the same LSN.
- **`RecoveryNoReport`**. The Proxy answers a client and the Sequencer
  does not learn the LSN. The next read version is below the acknowledged
  LSN.
- **`RecoveryFewConfirms`**. One LogServer confirms the epoch. A recovery
  locks the 2 other LogServers, and the next epoch acknowledges an LSN. The
  Proxy of the ended epoch then gives a read version below that LSN. The
  number of confirmations must be `K`, because a recovery needs more than
  `m - k` locks.
- **`RecoveryNoRollback`**. The StorageServer keeps a record above the end
  version of an epoch, and has it when it reaches a read version of the
  next epoch.
- **`RecoveryNoInFlightLimit`**. The Sequencer of epoch 1 assigns the LSNs
  2 and 3 while its read version is 1. One LogServer persists both. A
  recovery chooses the Recovery Version 1, so the first LSN of epoch 2 is
  3. The StorageServer has the discarded record 3 of epoch 1 at the read
  version 3 of epoch 2. The unit tests of the StorageServer found this
  defect before the model had a StorageServer.
- **`ResolverNoCheck`**. The Resolver admits a transaction that read a
  key below a later write.
- **`ResolverNoBatchCheck`**. Two transactions of one batch read and write
  one key. The Resolver checks both against the versions before the batch,
  and admits both.
- **`ResolverNoGap`**. The Resolver of the new epoch has no history, and
  the read version of a transaction of the previous epoch is inside the
  MVCC window. The Resolver admits the transaction.

### The smallest gap

`ResolverSmallestGap` passes with a `Gap` of 2 and a window of 3, and
`ResolverNoGap` fails with a `Gap` of 1. In the model, the gap must be at
least the window minus 1: the first commit of a client in the new epoch is
at least `Gap + 1` above the last version of the previous epoch.
FoundationDB and deadlift use a gap of 100,000,000 versions and a window
of 5,000,000 versions.

### Reachability configs

Each config must reach the state that its invariant says is never reached.

| Config | State |
|---|---|
| `RecoveryReachThirdEpoch` | The third epoch acknowledges a transaction |
| `RecoveryReachDiscard` | A recovery discards a persisted record |
| `RecoveryReachCopy` | A recovery copies a record to the new epoch |
| `RecoveryReachRollback` | The StorageServer removes a record |
| `RecoveryReachReadSecondEpoch` | A client receives a read version of the second epoch |
| `RecoveryReachStoreSecondEpoch` | The StorageServer applies a record of a client of the second epoch |
| `ResolverReachConflict` | The Resolver rejects a transaction for a conflict |
| `ResolverReachTooOldInEpoch` | The Resolver rejects a transaction as too old in the epoch of its read version |
| `ResolverReachTooOldAcrossEpochs` | The Resolver of a later epoch rejects a transaction with a read version of an earlier epoch as too old |
| `ResolverReachCommitSecondEpoch` | A transaction with read keys commits in the second epoch |
| `ResolverReachTwoInBatch` | Two transactions of one batch commit |
| `ResolverReachConflictInBatch` | The Resolver rejects a transaction for a conflict with an earlier transaction of its batch, and for no other reason |

All 12 configs reach their state in 1 s or less.

### A finding of the model

The first version of `BelowNextEpoch` applied to every record. TLC found a
counterexample with 3 epochs: the recovery transaction of epoch 2 reaches
one LogServer, epoch 2 never accepts commits, and epoch 3 has the Recovery
Version of epoch 2. The two recovery transactions then have the same LSN.
A recovery transaction has no content, so no read is affected. The
invariant now applies to the records of clients, and the comments in
`deadlift_sequencer` and `deadlift_storage` state the exception.

## Commands

```
./check.sh RecoverySafe            # one config that must pass
./check.sh RecoveryNoCas fail      # one config that must find a violation
./check_all.sh resolver            # the Resolver configs, about 30 seconds
./check_all.sh recovery            # the quick Recovery configs, about 5 minutes
./check_all.sh quick               # resolver and recovery
./check_all.sh long                # the Recovery configs with 3 epochs, about 4 hours
```

## When to run TLC

The configs with 3 epochs take about 4 hours, so a change does not run
every config. The rule is:

| Change | Configs to run | Time |
|---|---|---|
| Code, tests or documents, and no file in `formal/` | None | 0 |
| A comment in a model | None | 0 |
| `Resolver.tla` or `MCResolver.tla` | `./check_all.sh resolver` | about 30 seconds |
| `Recovery.tla` or `MCRecovery.tla` | `./check_all.sh recovery` | about 5 minutes |
| `Recovery.tla`, before the end of a milestone or before a release | `./check_all.sh long`, as a background job | about 4 hours |

A change of the code that changes a rule of a model changes the model in
the same step. The configs with 3 epochs last passed for the commit
`1b56582`. A later change of `Recovery.tla` is recorded in the table of
results as not checked with 3 epochs until the long configs have passed
again.

## Limitations

- The bounds are small: 3 LogServers, at most 3 epochs and 7 versions in
  Recovery; 2 keys, 3 transactions and at most 3 epochs in Resolver.
- The two models are not composed. Resolver assumes that a recovery keeps
  every record, and Recovery treats every transaction as admitted.
- Recovery has no keys and no values. `StorageSafe` and `StorageComplete`
  are statements about sets of records, not about values that a client
  reads.
- Recovery has no clock. An LSN is the previous LSN plus 1, and the gap is
  2 versions.
- `Ack` joins the report to the Sequencer and the answer to the client in
  one step. The model does not have the state between the two steps.
- The StorageServer of the model can apply a record above the end version
  of an epoch at any time before `SSFollow`. The StorageServer of deadlift
  applies no such record after it has read the coordinated state.
- The model copies each record in `PEV + 1 .. RV` to every new LogServer.
  FoundationDB copies a record to the LogServers of its tags.
- The model has one Proxy for each epoch, and one request for a read
  version.
- A Sequencer whose lock fails is not modelled, because that Sequencer
  changes nothing.
- The models check safety only. They have no liveness property.
