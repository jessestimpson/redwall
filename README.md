# deadlift

deadlift is an implementation of the transaction system of FoundationDB in
Erlang. A [peace](https://github.com/jessestimpson/peace) cluster has the
role of the Coordinators.

deadlift follows this paper, and uses its terms:

> Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
> Value Store", SIGMOD 2021.

This document calls it "the paper". The second source is
`design/recovery-internals.md` in the FoundationDB repository. This document
calls it "the recovery document".

## Use cases

deadlift is intended to support these use cases. deadlift supports none of
them yet.

- A general transactional key-value store, as FoundationDB is.
- A storage backend that keeps the data in memory, with low latency and
  high throughput. An application uses this backend as a table that is
  shared by every node of the cluster, in the way that an application uses
  an ETS table on one node.

## Status

deadlift is at the start. The repository contains:

- two TLA+ models, checked with TLC: one of the logging protocol, the
  recovery, read versions and the StorageServer, and one of the
  Resolver;
- the coordinated state, as a state machine for peace, with tests;
- the LogServer, with tests;
- the recovery, and a Sequencer that assigns read versions and commit
  versions and commits the recovery transaction, with tests;
- the Resolver, with tests;
- a Proxy that gives read versions and commits transactions, with tests;
- a StorageServer that reads the records from the LogServers and answers
  reads at a version, with tests;
- a deterministic simulation of these parts with
  [eta](https://github.com/jessestimpson/eta), with kills, partitions,
  and lost and delayed messages.

A caller can run a complete transaction with three calls: get a read
version from a Proxy, read keys from the StorageServer at the read
version, and commit the read keys and the writes through a Proxy. deadlift
has no client library that makes these calls and repeats a transaction
after a conflict. Every part keeps its data in memory. See
[ROADMAP.md](ROADMAP.md).

## Design

In FoundationDB, consensus is not on the commit path. The Coordinators store
a small record, the coordinated state. Every other part of the transaction
system is created again in each epoch (paper, sections 2.3.4 and 2.3.5).
deadlift keeps this design.

| Role in the paper | Function | In deadlift |
|---|---|---|
| Coordinators | Store the coordinated state | A peace cluster that runs [deadlift_cstate](src/deadlift_cstate.erl) |
| ClusterController | Recruits the Sequencer | Not implemented |
| Sequencer | Assigns read versions and commit versions, and performs the recovery | [deadlift_sequencer](src/deadlift_sequencer.erl) and [deadlift_recovery](src/deadlift_recovery.erl). The recovery, the recovery transaction and the versions are modelled in [formal/Recovery.tla](formal/Recovery.tla) |
| Proxy | Gives read versions to clients and commits transactions | [deadlift_proxy](src/deadlift_proxy.erl). The commit to the LogServers and the read version are modelled |
| Resolver | Checks a transaction for conflicts | [deadlift_resolver](src/deadlift_resolver.erl). Modelled in [formal/Resolver.tla](formal/Resolver.tla) |
| LogServer | Stores the log records of an epoch | [deadlift_log](src/deadlift_log.erl). Modelled |
| StorageServer | Stores data and answers reads at a version | [deadlift_storage](src/deadlift_storage.erl). Modelled, without keys and values |

### The coordinated state

The coordinated state holds:

- the recovery count;
- the log system of the current epoch;
- the log systems of the epochs that have ended, each with the Recovery
  Version that ended the epoch.

A Sequencer performs two operations on the coordinated state during a
recovery (paper, section 2.4.4):

1. The Sequencer locks the coordinated state. The lock raises the recovery
   count by 1. The new count is the epoch of the Sequencer.
2. After the Sequencer has stopped the old LogServers and has computed the
   Recovery Version, the Sequencer writes the new log system. The write
   succeeds only if the recovery count is still the epoch of the Sequencer.

Both operations are compare-and-set operations on the recovery count. If two
Sequencers recover at the same time, at most one of them writes.

### The LogServer

A LogServer stores the log records of one epoch (paper, section 2.4.3).

- A Proxy sends every record to every LogServer of the epoch. A record
  carries its LSN, the previous LSN and the Known Committed Version (KCV)
  of the Proxy. The LogServers that replicate the record receive the body.
  The other LogServers receive an empty body.
- A LogServer stores a record only when the previous LSN of the record is
  the Durable Version (DV) of the LogServer. A record that arrives early
  waits.
- The Sequencer of a later epoch locks the LogServer. A locked LogServer
  stores no more records. The answer to the lock contains the DV and the
  KCV.
- A locked LogServer does not answer the lock of a lower epoch.

These rules are the actions `Persist` and `LockLog` of the model.

### The recovery

A new Sequencer performs the recovery before it assigns a version (paper,
section 2.4.4):

1. The Sequencer reads the coordinated state and locks it. The new
   recovery count is the epoch of the Sequencer.
2. The Sequencer locks every LogServer of the previous epoch. The recovery
   needs more than m - k answers, where m is the number of LogServers of
   the previous epoch and k is the replication degree.
3. The Sequencer computes the previous epoch's end version (PEV) as the
   maximum of the KCVs, and the Recovery Version (RV) as the minimum of the
   DVs.
4. The Sequencer reads the records above the PEV, up to the RV, from the
   LogServers that answered.
5. The Sequencer recruits the LogServers of the new epoch. Each new
   LogServer starts with the DV RV, the KCV PEV and the records of step 4.
6. The Sequencer writes the new log system to the coordinated state.

The Sequencer then starts the Resolver of the epoch and commits the
recovery transaction (paper, section 2.4.4). The recovery transaction is
the first record of the epoch. Its LSN is the first commit version of the
epoch, and its previous LSN is the RV. The Sequencer sends it to the
Resolver and to every LogServer of the epoch. The read version at the
start of an epoch is therefore the LSN of a record that exists, and a
StorageServer reaches it. The recovery transaction of deadlift has no
content.

The Sequencer then assigns the LSNs above the recovery transaction. If a
step fails, the Sequencer stops. A recovery that fails after step 1 leaves a higher
recovery count. A recovery that fails after step 2 leaves stopped
LogServers. The next recovery handles both.

### Versions

The paper does not state the rules below. They are the rules of
FoundationDB 7.3, from `fdbserver/ClusterRecovery.actor.cpp`,
`fdbserver/masterserver.actor.cpp`, `fdbserver/Resolver.actor.cpp`,
`fdbserver/SkipList.cpp` and `fdbclient/ServerKnobs.cpp`.

| Rule | Value in deadlift and in FoundationDB |
|---|---|
| Versions for each second | 1,000,000 |
| A commit version | The last commit version plus the versions of the time since then; at least 1 more, at most 5,000,000 more |
| The first commit version of an epoch | The Recovery Version plus 100,000,000; 1 if the Recovery Version is 0 |
| The read version | The highest LSN that a Proxy has reported as stored on every LogServer; at the start of an epoch, the LSN of the recovery transaction |
| The MVCC window of the Resolver | 5,000,000 versions |

The gap at the start of an epoch is larger than the MVCC window. The
Resolver of a new epoch has no history. Because of the gap, it treats a
transaction with a read version of the previous epoch as too old.

The Sequencer of deadlift assigns a commit version only if the version is
less than 100,000,000 above the read version. Otherwise the Sequencer
answers `{error, versions_in_flight}` and the Proxy stops. With this
limit, no LogServer stores a record of a client at or above the first
commit version of the next epoch. A record of a client that a recovery
discards therefore has an LSN below every version of the next epoch. A
discarded recovery transaction can have the first LSN of the next epoch;
it has no content. The StorageServer depends on this
rule. FoundationDB uses the constant `MAX_VERSIONS_IN_FLIGHT` for the gap.
The limit in the Sequencer of deadlift is not taken from the FoundationDB
source.

### The Resolver

The Resolver follows Algorithm 1 of the paper (section 2.4.2). It keeps,
for each key range, the commit version of the last transaction that wrote
the range.

- A transaction has read keys and read ranges. The read range
  `{Begin, End}` has every key `K` with `Begin =< K` and `K < End`, in the
  order of Erlang terms.
- A transaction has a conflict if a key that it read, alone or in a read
  range, has a commit version above the read version of the transaction.
- A read range has the keys without a value. A transaction that reads a
  range gets a conflict when another transaction writes a new key in the
  range.
- A transaction with read keys or read ranges and a read version below
  the MVCC window is too old.
- A request has the transactions of one batch of the Proxy. The Resolver
  checks them in the order of the batch. A transaction has a conflict
  with an earlier transaction of the batch that the Resolver admitted.
- `deadlift_storage:read_range/4` reads the keys of a range at a read
  version.
- The Resolver handles the commit versions in their order. A request that
  arrives before the request with the previous version waits.

### The Proxy

A Proxy commits a batch of transactions in five steps (paper, sections
2.4.1 and 2.4.3):

1. When the Proxy closes a batch, it asks the Sequencer for an LSN and
   the previous LSN.
2. The Proxy sends the read version, the read keys, the read ranges and
   the write keys of each transaction of the batch to the Resolver. If
   the Resolver rejects a transaction, the Proxy answers the client of
   that transaction with an error.
3. The Proxy sends one record to every LogServer of the epoch. The record
   has the writes of the admitted transactions, in the order of the
   batch. The k LogServers that replicate the record receive the body.
4. When every LogServer of the epoch has answered, the Proxy raises its
   KCV to the LSN and reports the LSN to the Sequencer as committed.
5. When the Sequencer has answered, the Proxy answers the clients of the
   admitted transactions.

The Proxy also sends the record when the Resolver admitted no transaction
of the batch. A LogServer stores a record only after the record with the
previous LSN, so every LSN must reach the LogServers.

#### Batches

The Proxy collects the transactions of a short time into one batch
(paper, section 2.4.1). A batch has one commit version and is one log
record.

| Rule | In deadlift | In the FoundationDB source |
|---|---|---|
| A batch closes after a time | `batch_ms`, default 1 ms, fixed | A time that changes with the load (`commitBatcher` in `fdbserver/CommitProxyServer.actor.cpp`) |
| A batch closes at a number of transactions | `batch_max`, default 1,000 | `COMMIT_TRANSACTION_BATCH_COUNT_MAX` |
| A batch closes at a number of bytes | No limit | A limit that depends on the number of CommitProxies |
| One commit version for each batch | Yes | Yes (`preresolutionProcessing`) |
| The Resolver checks a transaction against the earlier admitted transactions of its batch | Yes | Yes (`checkIntraBatchConflicts` in `fdbserver/SkipList.cpp`) |
| The writes of a rejected transaction give no conflict | Yes | Yes |

- The index of a transaction is its position in the batch, from 0.
  `deadlift_proxy:commit_with_index/3` returns the LSN and the index. The
  pair of the LSN and the index gives the order of all committed
  transactions.
- When two transactions of a batch write one key, the StorageServer keeps
  the value of the transaction with the higher index.
- The rules in the table are taken from the named functions of
  FoundationDB 7.3. The default values of the knobs of FoundationDB are
  not taken from the source.

Because of step 4, the Sequencer knows that an LSN is committed before any
client does. A client that starts after an answer gets a read version at
or above that LSN.

The Proxy waits for every LogServer, and not only for the k LogServers
with the body. The config `RecoveryAckReplicasOnly` of the model shows
that a Proxy that waits for k LogServers lets a recovery discard an
acknowledged record.

If the Resolver, a LogServer or the Sequencer answers with an error or is
not alive, the Proxy answers `{error, commit_unknown_result}` to every
client that waits, and stops. The Proxy of an ended epoch stops in this
way at its next commit.

### Read versions

A Proxy gives a read version in three steps. The rule is from
`getLiveCommittedVersion` in `fdbserver/GrvProxyServer.actor.cpp` and
`confirmEpochLive` in `fdbserver/TagPartitionedLogSystem.actor.cpp` of
FoundationDB 7.3. The paper does not state it.

1. The Proxy asks the Sequencer for the read version.
2. At the same time, the Proxy asks every LogServer of the epoch that runs
   whether the epoch is live. A LogServer confirms until a Sequencer of a
   later epoch locks it.
3. When the Sequencer and k LogServers have answered, the Proxy answers
   the client. k is the replication degree.

A recovery needs answers to its lock from more than m - k of the m
LogServers of the epoch, so at least one LogServer that confirmed is among
them. That LogServer answered the lock after the client asked. A commit of
a later epoch is therefore acknowledged after the client asked. The read
version is at or above the commit version of every transaction that was
acknowledged before the client asked, in any epoch.

The Proxy stops in two cases:

- A LogServer answers that it is stopped. The epoch has ended.
- More than m - k LogServers of the epoch do not run. The Proxy cannot
  confirm the epoch, and the epoch cannot commit.

### The StorageServer

A StorageServer reads the records from the LogServers and applies their
writes to a store with versions (paper, sections 2.4.3 and 2.4.4).

- The StorageServer has a version: the LSN of the last record that it has
  applied. It applies a record only when the previous LSN of the record is
  its version. The records of all epochs form one chain, because the
  previous LSN of a recovery transaction is the Recovery Version.
- The StorageServer has one request at each LogServer of one epoch. A
  LogServer answers when it has a new record, or when a Sequencer locks
  it. The StorageServer applies a record when it has received the body
  from one of the k LogServers that have it. Every 5 s, the
  StorageServer sends each request again, because a message can be lost.
- A read at a version returns the last write to the key at or below that
  version. The read waits until the StorageServer has the version.
- The StorageServer keeps the versions of the MVCC window, which is
  5,000,000 versions by default (paper, section 2.4.5). For each key it
  keeps the versions above its oldest version and the last version at or
  below the oldest version. A read below the oldest version returns
  `{error, transaction_too_old}`.
- The oldest version is the lower one of two versions: the version of the
  StorageServer minus the window, and the highest Known Committed Version
  that a LogServer has sent with its records. No recovery discards a
  record at or below a Known Committed Version, so a rollback needs no
  version that the StorageServer has removed.
- The StorageServer reads the log systems of all epochs from the
  coordinated state, at its start and when a LogServer answers that it is
  stopped.

The StorageServer applies records that are not yet stored on every
LogServer. A recovery can discard such a record. When the StorageServer
learns that an epoch has ended, it removes every write above the end
version of the epoch. This is the rollback of the paper. A read at a read
version from a Proxy is not affected, because a read version is the LSN of
a record that no recovery discards.

### LogServer identifiers

The coordinated state identifies a LogServer with `{Epoch, Index}`. A
LogServer registers its identifier with
[deadlift_registry](src/deadlift_registry.erl), a registry of the
processes of one node. The Sequencer and the Proxy find a LogServer
through the registry. They send a request only to a LogServer that runs.

### Departures from the paper

- FoundationDB stores the coordinated state with Active Disk Paxos on the
  Coordinators (paper, section 2.5). deadlift stores the coordinated state
  in the replicated log of a peace cluster. This part is not in the paper.
- The coordinated state of deadlift records an owner for the recovery
  count, so that a lock or a write that is proposed a second time after a
  timeout has no second effect. This part is not in the paper.
- A LogServer of deadlift keeps the records in memory. FoundationDB writes
  the records to disk. This part is not in the paper.
- The recovery copies each record to every new LogServer. FoundationDB
  copies a record to the new LogServers that replicate the tags of the
  record.
- FoundationDB keeps the commit versions near the time of the wall clock.
  deadlift raises the commit version with the elapsed time only.
- A Proxy of deadlift confirms the epoch for each request of a read
  version. FoundationDB confirms once for a batch of requests.
- A Proxy of deadlift stops when a LogServer answers that it is stopped.
  A proxy of FoundationDB does not answer the request, and stops when it
  learns of the recovery.
- The Sequencer of deadlift sends the recovery transaction itself, after
  the write of the coordinated state. The master of FoundationDB sends it
  through a proxy, before the write of the coordinated state.
- The StorageServer keeps the data in memory. A StorageServer of
  FoundationDB keeps the versions of the MVCC window in memory and writes
  older data to disk. This part is not in the paper.
- The rule for the oldest version of the StorageServer is not taken from
  the FoundationDB source.
- One StorageServer has every key. FoundationDB divides the key space
  into shards.
- The StorageServer reads the log systems from the coordinated state. A
  StorageServer of FoundationDB receives them from the ClusterController.
  This part is not in the paper.
- A key is an Erlang term, and the order of the keys is the order of
  Erlang terms. A key of FoundationDB is a byte string.
- A transaction writes keys only. FoundationDB also has an operation that
  clears a key range, and the Resolver of FoundationDB records the range
  as a write range. The Resolver of deadlift accepts write ranges, and no
  other part of deadlift sends one.
- One Resolver handles every key. FoundationDB divides the key space over
  several Resolvers.
- A registry on each node finds the process of a LogServer. In
  FoundationDB the ClusterController has this function. This part is not
  in the paper.
- The time of a batch is fixed, and a batch has no limit in bytes. The
  time of a batch of FoundationDB changes with the load.
- The Proxy chooses the LogServers that receive the body of a record from
  the LSN. FoundationDB chooses them from the tags of the record.

## Verification

| Level | What it checks | Command | Time |
|---|---|---|---|
| Unit tests | The coordinated state machine, alone and on a peace cluster of three servers on one node. One LogServer: the order of the LSNs, the KCV, repeated records and the lock. The recovery on a peace cluster: the first epoch, a second epoch with records to copy, 1 and 2 LogServers down, and two recoveries at the same time. The Proxy: commits in sequence, 50 commits at the same time, a recovery after commits, a LogServer down and the Sequencer down | `make test` | 3 seconds |
| Simulation | The LogServer, the recovery, the Sequencer, the Resolver, the Proxy and the StorageServer under the scheduler and the simulated network of eta, with kills, partitions, and lost and delayed messages | `make dst` | about 4 minutes |
| Model checking | The logging protocol, the recovery, the read versions, the StorageServer and the Resolver, with 2 epochs | `make tlc` | about 5 minutes |
| Model checking | The logging protocol, the recovery and the StorageServer, with 3 epochs | `make tlc-long` | about 4 hours |

The models have 10 planted bugs. TLC finds a counterexample for each. See
[formal/README.md](formal/README.md).

### Simulation

`make dst` runs the system under the scheduler and the simulated network
of eta. The harness is [dst/deadlift_dst.erl](dst/deadlift_dst.erl).

The simulated system has one process for the coordinated state
([deadlift_cstate_local](src/deadlift_cstate_local.erl)) in the place of a
peace cluster, 3 LogServers for each epoch and a replication degree of 2.
One StorageServer runs at a time. The coordinated state, the
StorageServer, each Sequencer, each Proxy, each Resolver and each
LogServer run on a simulated node of their own. The network loses 0.1% of the messages
between nodes and delays 20% of them.

The workload has these operations:

- start a Sequencer and, after its recovery, a Proxy;
- run a transaction: get a read version from a Proxy of any epoch, read
  the read keys from the StorageServer at the read version, and commit
  through a Proxy of any epoch, with read keys and write keys from 4
  keys. One client in three waits 20 ms to 100 ms before the commit, so
  that a recovery can end the epoch of the read version;
- kill a LogServer of the current epoch, at most 1 for each epoch;
- kill a Sequencer, also during its recovery;
- kill a Proxy;
- kill the Resolver of an epoch;
- kill the StorageServer, and start a new StorageServer without data,
  which reads every record from the LogServers of every epoch;
- cut the connection between two nodes, and remove every cut.

The harness checks these invariants after every step:

| Invariant | Statement |
|---|---|
| `durable` | An acknowledged LSN of an ended epoch is at or below the Recovery Version that ended the epoch. `Durable` of the model |
| `unique_versions` | No LSN is acknowledged in two epochs or to two clients. `UniqueVersions` of the model |
| `no_loss` | No recovery ends with a missing body. `NoLoss` of the model |
| `body_available` | The body of every acknowledged record is on a LogServer of its epoch that runs |
| `copied` | Every LogServer of a new epoch has the body of every acknowledged record above the previous epoch's end version |
| `no_conflict` | No acknowledged transaction read a key that another acknowledged transaction wrote between the read version and the commit version of the first one |
| `read_version_order` | A read version is at or above the commit version of every transaction that was acknowledged before the client asked for it, in any epoch |
| `read_value` | A read of a key at a read version returns the write of the acknowledged transaction with the highest commit version at or below the read version, or a later write at or below the read version whose client received no answer |
| `storage_rollback` | When the StorageServer has a version of an epoch, it has no write between the end version of an earlier epoch and the first version of the epoch after that one |
| `kcv_below_dv` | In each epoch, the KCV of every LogServer is at or below the DV of every LogServer |
| `chain` | In each LogServer, the previous LSN of each record is the LSN of the record before it |

Results of `make dst` on 2026-11-22:

| Test | Runs | Result |
|---|---|---|
| The same seed gives the same trace, the trace replays, and the audit of eta reports nothing | 20 seeds, 3 runs each | Pass |
| No kills, no partitions, no lost or delayed messages | 200 seeds in 4 tests, 150 operations each | No violation. 9,562 acknowledged transactions, of which 2,227 with a read range and 2,166 in a batch with another acknowledged transaction, 9,258 rejected for a conflict, 1,884 too old, 22,120 transactions with reads from the StorageServer, 13 reads below the MVCC window of the StorageServer, 2,276 recoveries |
| Kills, partitions, lost and delayed messages | 500 seeds in 5 tests, 150 operations each | No violation. 2,407 acknowledged transactions, of which 433 with a read range and 398 in a batch with another acknowledged transaction, 3,130 rejected for a conflict, 3,095 too old, 22,732 transactions with reads from the StorageServer, of which 6,859 from a StorageServer that started after a kill, 488 reads below the MVCC window of the StorageServer, 386 rollbacks of the StorageServer, 6,030 recoveries, 3,262 kills, of which 353 of the StorageServer, 565 partitions |
| Planted defect: the Proxy answers after 2 of 3 LogServers | Seeds from 1 | Found at seed 1, `kcv_below_dv`. In 40 seeds the defect also violates `durable`, the counterexample of `RecoveryAckReplicasOnly` |
| Planted defect: a recovery copies no record | Seeds from 1 | Found at seed 1, `no_loss` |
| Planted defect: the coordinated state accepts any write | Seeds from 1 | Found at seed 1, `read_version_order` |
| Planted defect: the Resolver admits every transaction | Seeds from 1 | Found at seed 1, `no_conflict` |
| Planted defect: the Resolver does not check the read ranges | Seeds from 1 | Found at seed 2, `no_conflict` |
| Planted defect: the Resolver does not check a transaction against the earlier transactions of its batch | Seeds from 1 | Found at seed 3, `no_conflict` between the transactions with the indexes 0 and 1 of one batch |
| Planted defect: the first commit version of an epoch is the Recovery Version plus 1 | Seeds from 1 | Found at seed 1, `no_conflict` between a transaction of epoch 16 and a write of epoch 15 |
| Planted defect: a Proxy does not report a committed LSN to the Sequencer | Seeds from 1 | Found at seed 1, `read_version_order` |
| Planted defect: a Proxy gives a read version without the confirmation that its epoch is live | Seeds from 1 | Found at seed 18, `read_version_order`: a Proxy of epoch 13 gave a read version below a commit of epoch 16 |
| Planted defect: the StorageServer keeps the writes above the end version of an epoch | Seeds from 1 | Found at seed 3, `storage_rollback` |
| Planted defect: the StorageServer answers a read below its oldest version | Seeds from 1 | Found at seed 6, `read_value`: a read returned no value for a key that an acknowledged transaction wrote below the read version |

`make dst` runs each test in an Erlang node of its own, with several
nodes at the same time ([dst/run.sh](dst/run.sh)). The runs without faults
are 4 tests of 50 seeds, and the runs with faults are 5 tests of 100
seeds. `dst/run.sh` prints the sums of the statistics of these tests.

| Command | Nodes at the same time | Time |
|---|---|---|
| `make dst-serial` | 1 | 10 min 38 s |
| `make dst` | 8 | 3 min 32 s and 3 min 47 s in 2 runs |
| `dst/run.sh -j 5` | 5 | 4 min 6 s |

The machine has 4 performance cores and 6 efficiency cores. The
commands run the same seeds, and the sums of the statistics are equal.
With 8 nodes, the tests use 2.2 times the processor time of the serial
command, because one test takes longer while other nodes run.

A run has about 5,000 steps and takes about 0.65 s in a node alone. About
30% of that time, the node uses no processor. The invariant checks of the
harness take less than 1% of the time.

## Commands

```
make compile    # compile
make test       # unit tests
make dst        # deterministic simulation with eta, in several nodes
make dst-serial # the same tests in one node
make tlc        # the quick TLC configs
make tlc-long   # the 2 TLC configs with three epochs, about 4 hours
```

## Requirements

- Erlang/OTP 28 and rebar3.
- peace. `rebar.config` declares it as a git dependency. For local
  development, link a checkout at `_checkouts/peace`.
- eta, for `make dst` only. The `dst` profile of `rebar.config` declares
  it. For local development, link a checkout at `_checkouts/eta`.
- Java 11 or later for the TLC targets. The TLC jar is in `formal/vendor`.

## Source layout

| Path | Contents |
|---|---|
| [src/deadlift_cstate.erl](src/deadlift_cstate.erl) | The coordinated state: a `peace_machine` and its client |
| [src/deadlift_log.erl](src/deadlift_log.erl) | The LogServer |
| [src/deadlift_recovery.erl](src/deadlift_recovery.erl) | The recovery |
| [src/deadlift_sequencer.erl](src/deadlift_sequencer.erl) | The Sequencer |
| [src/deadlift_resolver.erl](src/deadlift_resolver.erl) | The Resolver |
| [src/deadlift_proxy.erl](src/deadlift_proxy.erl) | The Proxy |
| [src/deadlift_storage.erl](src/deadlift_storage.erl) | The StorageServer |
| [src/deadlift_registry.erl](src/deadlift_registry.erl) | The registry of the processes of one node |
| [src/deadlift_cstate_local.erl](src/deadlift_cstate_local.erl) | The coordinated state in one process, for tests and for the simulation |
| [include/deadlift_eta.hrl](include/deadlift_eta.hrl) | The header that puts a module under the parse transform of eta in the `dst` profile |
| [dst/deadlift_dst.erl](dst/deadlift_dst.erl) | The eta harness: the workload and the invariants |
| [dst/deadlift_dst_client.erl](dst/deadlift_dst_client.erl) | The processes that perform the operations of the simulation |
| [dst/deadlift_dst_tests.erl](dst/deadlift_dst_tests.erl) | The simulation suite |
| [dst/run.sh](dst/run.sh) | Runs each test of the simulation suite in a node of its own |
| [src/deadlift_app.erl](src/deadlift_app.erl), [src/deadlift_sup.erl](src/deadlift_sup.erl) | The application and its supervisor, which starts the registry |
| [test/deadlift_cstate_tests.erl](test/deadlift_cstate_tests.erl) | Tests for `deadlift_cstate` |
| [test/deadlift_log_tests.erl](test/deadlift_log_tests.erl) | Tests for `deadlift_log` |
| [test/deadlift_recovery_tests.erl](test/deadlift_recovery_tests.erl) | Tests for `deadlift_recovery` and `deadlift_sequencer` |
| [test/deadlift_proxy_tests.erl](test/deadlift_proxy_tests.erl) | Tests for `deadlift_proxy` |
| [test/deadlift_resolver_tests.erl](test/deadlift_resolver_tests.erl) | Tests for `deadlift_resolver` |
| [test/deadlift_storage_tests.erl](test/deadlift_storage_tests.erl) | Tests for `deadlift_storage` |
| [formal/Recovery.tla](formal/Recovery.tla) | TLA+ model of the logging protocol, the recovery, read versions and the StorageServer |
| [formal/Resolver.tla](formal/Resolver.tla) | TLA+ model of the Resolver |
| [formal/MCRecovery.tla](formal/MCRecovery.tla), [formal/MCResolver.tla](formal/MCResolver.tla) | Constants and invariants for model checking |
| [formal/README.md](formal/README.md) | The models, the configs and the results |
| [ROADMAP.md](ROADMAP.md) | The planned work |

## Limitations

- A commit waits for the time of a batch, 1 ms by default, also when no
  other transaction arrives. The time does not change with the load.
- A batch has no limit in bytes.
- deadlift has no client library. A caller gets the read version, reads
  and commits with separate calls, and repeats a transaction itself.
- The StorageServer keeps the data in memory, and loses the data when it
  stops. A new StorageServer reads every record again
  from the LogServers, which is possible only while the LogServers of
  every epoch run.
- A read at a version that is not a read version from a Proxy can return
  a write that a recovery discards.
- After a LogServer is stopped, the StorageServer reads the coordinated
  state every 500 ms until the coordinated state has a later epoch.
- A read of a key range examines every key of the StorageServer.
- deadlift has no operation that clears a key range.
- A rise of the oldest version of the StorageServer examines every key.
  The oldest version rises in steps of at least one tenth of the MVCC
  window.
- The MVCC window of the StorageServer and the MVCC window of the Resolver
  are two options. No part of deadlift checks that they are equal.
- The simulation has one StorageServer at a time. A new StorageServer
  starts without data after a kill. The simulation kills at most 1
  LogServer of each epoch, so a LogServer with each body runs.
- A request for a read version waits until the timeout of the client
  when a message to a LogServer or its answer is lost. The Proxy does not
  send the message again.
- A Resolver that stops is not started again. The commits of its epoch
  stop until the next recovery.
- A Proxy that stops between the assignment of an LSN and the push leaves
  an LSN that no LogServer receives. Every later record of the epoch then
  waits in the LogServers until a recovery.
- A commit to a LogServer that is alive and does not answer waits until
  the timeout of the client. The Proxy does not stop in this case.
- A LogServer that has stopped must not start again with the same
  identifier, because it has lost its records.
- No process starts a Sequencer when a Sequencer or a LogServer fails.
  deadlift has no ClusterController.
- Every LogServer runs on the node of the Sequencer that recruits it. The
  registry finds processes of the local node only.
- The recovery waits up to `lock_timeout_ms` (default 1,000 ms) for an old
  LogServer that does not answer.
- The tests of the recovery stop LogServers between steps. No test stops a
  process during a step.
- A LogServer keeps the records in memory. A LogServer that stops loses
  the records.
- A LogServer does not discard records. The memory of a LogServer grows
  with every record.
- The number of records that wait in a LogServer has no limit.
- The model of the recovery and the model of the Resolver are separate.
  No model has both.
- The models have no keys and values in the StorageServer, no tags, no
  clock and no election of a ClusterController. The model of the Resolver
  has a key range as a set of keys, without the bounds.
- The Recovery configs with 3 epochs take about 4 hours. They are not run
  for every change. See "When to run TLC" in
  [formal/README.md](formal/README.md).
- The models are checked with 3 LogServers, a replication degree of 2, at
  most 3 epochs and 7 versions, and for the Resolver 2 keys and 3
  transactions.
- deadlift does not send a message again. In the simulation, a lost
  message to a LogServer stops the commits of the epoch until the next
  recovery.
- The simulation has one process for the coordinated state, and no peace
  cluster. No test runs deadlift on a peace cluster with faults.
- The repository has no licence file.
