# Formal model

The directory has three models:

| Model | Contents | Module for model checking |
|---|---|---|
| [Recovery.tla](Recovery.tla) | The logging protocol, the recovery, read versions and the StorageServer (paper, sections 2.4.3, 2.4.4 and 2.5) | [MCRecovery.tla](MCRecovery.tla) |
| [Resolver.tla](Resolver.tla) | The Resolver and the versions that protect a Resolver without history (paper, section 2.4.2 and Algorithm 1) | [MCResolver.tla](MCResolver.tla) |
| [Watch.tla](Watch.tla) | The watch: the request to the StorageServer, the answer, and the read of the client after the answer (FoundationDB 7.3, `watchValue` and `watchWaitForValueChange`). The paper does not describe watches | [MCWatch.tla](MCWatch.tla) |

The models are separate:

- The Resolver depends on versions and epochs only, and not on the
  LogServers. One model with the recovery and the Resolver has a state
  space that TLC does not finish.
- The watch depends on the order of the records, on the acknowledged
  records and on the discarded records only. `Watch.tla` has a log
  without LogServers and without epochs.

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
| Commit version | 1 to `MaxStep` above the last one, as with the clock of redwall |
| Batch | A transaction can join the batch of the last commit version. The Resolver checks the transactions of a batch in their order |
| New epoch | A Resolver without history. The first version is the last version plus `Gap` |

The model assumes that a recovery keeps every record. It does not contain
LogServers, the StorageServer or more than one Resolver in an epoch.

A read range and a write range of redwall are sets of keys in the model.
The model does not have the ordered set of bounds that
`redwall_resolver` keeps. A unit test compares that set of bounds with one
version for each key, for 3,000 random transactions.

## Contents of Watch.tla

| Part | In the model |
|---|---|
| Log | A sequence of records. A record has an LSN and the value of one key after the record |
| Acknowledged records | A prefix of the log. A read at a read version returns the value of the last acknowledged record |
| Recovery | `Recover` discards the records that are not acknowledged, and adds the recovery transaction, which has a higher LSN and does not write the key |
| StorageServer | One. `SSApply` applies the next record of the log, also a record that is not acknowledged. `SSRollback` removes the discarded records |
| Watch | One, with a value and a version. `Create` takes them from the last acknowledged record |
| Request | `SSReceive`: the StorageServer handles the request when its version is at or above the version of the request. It answers if the value differs, and keeps the request otherwise. It reads the key again after a record that writes the key and after a rollback |
| Client | `ClientAnswer`: after the answer, the client waits for a read version at or above the version of the answer, and reads the key. The future is ready if the value differs. Otherwise the client sends the watch again with the read version |
| Time limit | `Timeout`: the request ends without an answer, and the client reads the key (`ClientRetry`) |

The model does not contain LogServers, epochs, the Resolver, more than
one key, more than one watch, more than one StorageServer, or the
watches of a transaction before its commit.

`Fires` is a liveness property. The fairness conditions of the model
are: a record is acknowledged unless a recovery discards it, and the
StorageServer and the client take their steps. `Timeout` has no fairness
condition, so `Fires` does not depend on the time limit of a request.

## Trace validation

Trace validation checks that a run of the code is a behaviour of
`Recovery.tla`.

1. The simulation records the state of the coordinated state, of each
   LogServer and of the process of each epoch after every step.
2. [`dst/redwall_dst_trace.erl`](../dst/redwall_dst_trace.erl) converts
   the changes into a sequence of model actions with their arguments, and
   writes the sequence to `TraceData.tla`. The header of that module
   describes the derivation.
3. `TraceRecovery.tla` has one behaviour, which takes the actions in
   order. After a change of the code, the variables of the model must
   equal the state of the code.
4. `check_trace.sh <dir>` runs TLC. If TLC cannot take every action, the
   script prints the first action that the model did not permit.

`make trace` validates 220 runs. Results of 2026-11-26:

| Runs | Seeds | Actions |
|---|---|---|
| No faults | 40 | 11,714 |
| Faults | 100 | 13,020 |
| ClusterControllers and faults | 40 | 4,143 |
| A peace cluster and faults | 40 | 4,889 |

TLC takes about 0.3 seconds for one run. `make trace` also runs 4 planted
defects, which TLC must reject:

| Planted defect | First seed that TLC rejects | Rejected action |
|---|---|---|
| `ack_replicas` | 1 | `Ack`: not every LogServer of the epoch has persisted the LSN |
| `no_cas` | 1 | A write of the coordinated state by an epoch that is not the recovery count |
| `no_report` | 1 | `Ack`: the read version of the code is not the read version of the model |
| `no_push_order` | 48 | `Persist`: the previous LSN is not the Durable Version of the LogServer |

Because trace validation checks the code against the model, the long TLC
configs do not need a new run when only the code changes.

Limitations:

- The trace spec has `Reads = {}` and `Storage = FALSE`. The requests for
  a read version and the StorageServer are not validated.
- In the model, an LSN is the previous LSN plus 1. Each LSN of the code
  gets a number of the model that keeps the order of the LSNs. The values
  of the LSNs and the limit on the versions in flight
  (`LimitInFlight = FALSE`) are not validated.
- A `LockLog` action is placed directly before `Recruit`, for a set of
  stopped LogServers whose versions give the Recovery Version and the
  previous epoch's end version of the code.
- The model copies each kept record to every new LogServer. The bodies of
  the copied records are not compared. TLC accepted the traces of 6 seeds
  with the planted defect `no_copy`.
- The process of an epoch can assign an LSN and stop before the harness
  reads its state. The record of a LogServer then gives the `Assign`
  action.
- Only `Recovery.tla` is covered. `Resolver.tla` and `Watch.tla` have no
  trace validation.

## Invariants and properties

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
| Watch | `ReadySound` | When the future of the watch is ready, an acknowledged record above the version of the watch has written another value than the value of the watch |
| Watch | `Fires` (temporal property) | If the value of the last acknowledged record differs from the value of the watch from some time on, the future becomes ready |

## Configs and results

All Recovery configs have 3 LogServers, a replication degree of 2 and a
`Gap` of 2. All Resolver configs have 2 keys, 3 transactions, a window of 3
versions and a `MaxStep` of 2. All Watch configs have 2 values. The
results are from one machine with 10 processors, on 2026-11-22 for
Recovery and Resolver, and on 2026-11-24 for Watch.

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
| `WatchSafe` | 6 LSNs, 2 recoveries | `ReadySound`, and the temporal property `Fires` | 1,591,146 | 22 s |

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
| `WatchNoVerify` | `Verify = FALSE` | `ReadySound` | under 1 s |
| `WatchNoTrigger` | `Trigger = FALSE` | `Fires` | under 1 s |
| `WatchNoRecheck` | `Recheck = FALSE` | `Fires` | 3 s |

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
- **`WatchNoVerify`**. The StorageServer applies a record that is not
  acknowledged and that writes another value. It answers the watch, and
  the future is ready without a read. A recovery discards the record, so
  no acknowledged record has written another value.
- **`WatchNoTrigger`**. The StorageServer keeps the request, and applies
  a record that writes another value without a read of the key. The
  record is acknowledged, and the future never becomes ready.
- **`WatchNoRecheck`**. The watch has the value `v1`. An acknowledged
  record writes `v2`, and a record that is not acknowledged writes `v1`
  again. The StorageServer has applied both, so it keeps the request. A
  recovery discards the second record. After the rollback the
  StorageServer has `v2`. Without a read of the key after the rollback,
  the future never becomes ready. The recovery transaction does not
  write the key, so it causes no read.

### The smallest gap

`ResolverSmallestGap` passes with a `Gap` of 2 and a window of 3, and
`ResolverNoGap` fails with a `Gap` of 1. In the model, the gap must be at
least the window minus 1: the first commit of a client in the new epoch is
at least `Gap + 1` above the last version of the previous epoch.
FoundationDB and redwall use a gap of 100,000,000 versions and a window
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
| `WatchReachReady` | The future of the watch is ready |
| `WatchReachResend` | The client sends the watch again after a read that returned the value of the watch |
| `WatchReachRollbackAnswer` | The StorageServer answers the watch after a rollback |
| `WatchReachReadyAfterTimeout` | The future is ready after a request without an answer |

All 16 configs reach their state in 1 s or less.

### A finding of the model

The first version of `BelowNextEpoch` applied to every record. TLC found a
counterexample with 3 epochs: the recovery transaction of epoch 2 reaches
one LogServer, epoch 2 never accepts commits, and epoch 3 has the Recovery
Version of epoch 2. The two recovery transactions then have the same LSN.
A recovery transaction has no content, so no read is affected. The
invariant now applies to the records of clients, and the comments in
`redwall_sequencer` and `redwall_storage` state the exception.

## Commands

```
./check.sh RecoverySafe            # one config that must pass
./check.sh RecoveryNoCas fail      # one config that must find a violation
./check_all.sh resolver            # the Resolver configs, about 30 seconds
./check_all.sh watch               # the Watch configs, about 40 seconds
./check_all.sh recovery            # the quick Recovery configs, about 5 minutes
./check_all.sh quick               # resolver, watch and recovery
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
| `Watch.tla` or `MCWatch.tla` | `./check_all.sh watch` | about 40 seconds |
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
- The three models are not composed. Resolver assumes that a recovery keeps
  every record, and Recovery treats every transaction as admitted. Watch
  assumes that a read at a read version returns the value of the last
  acknowledged record, which Recovery states as `StorageSafe` and
  `StorageComplete`.
- Watch has one key, one watch, one StorageServer, 2 values, at most 6
  LSNs and at most 2 recoveries. It has no LogServers and no epochs.
- Recovery has no keys and no values. `StorageSafe` and `StorageComplete`
  are statements about sets of records, not about values that a client
  reads.
- Recovery has no clock. An LSN is the previous LSN plus 1, and the gap is
  2 versions.
- `Ack` joins the report to the Sequencer and the answer to the client in
  one step. The model does not have the state between the two steps.
- The StorageServer of the model can apply a record above the end version
  of an epoch at any time before `SSFollow`. The StorageServer of redwall
  applies no such record after it has read the coordinated state.
- The model copies each record in `PEV + 1 .. RV` to every new LogServer.
  FoundationDB copies a record to the LogServers of its tags.
- The model has one Proxy for each epoch, and one request for a read
  version.
- A Sequencer whose lock fails is not modelled, because that Sequencer
  changes nothing.
- Recovery and Resolver check safety only. Watch has one liveness
  property, `Fires`.
