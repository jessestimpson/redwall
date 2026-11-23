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
- a client with the function names of erlfdb, with tests;
- a ClusterController that is elected through the coordinated state,
  and a worker on each node, with tests;
- a deterministic simulation of these parts with
  [eta](https://github.com/jessestimpson/eta), with kills, partitions,
  and lost and delayed messages.

A caller runs a transaction with the client: the client gets a read
version from a Proxy, reads keys from the StorageServer at the read
version, commits the read keys and the writes through a Proxy, and repeats
the transaction after a conflict. The client has the basic functions of
erlfdb only. Every part keeps its data in memory. See
[ROADMAP.md](ROADMAP.md).

## Design

In FoundationDB, consensus is not on the commit path. The Coordinators store
a small record, the coordinated state. Every other part of the transaction
system is created again in each epoch (paper, sections 2.3.4 and 2.3.5).
deadlift keeps this design.

| Role in the paper | Function | In deadlift |
|---|---|---|
| Coordinators | Store the coordinated state | A peace cluster that runs [deadlift_cstate](src/deadlift_cstate.erl) |
| ClusterController | Monitors the roles and recruits the Sequencer | [deadlift_cluster_controller](src/deadlift_cluster_controller.erl) and [deadlift_worker](src/deadlift_worker.erl). Not modelled |
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

#### The store of the StorageServer

FoundationDB has an ordered keyspace. Every structure of deadlift that
holds keys keeps the keys in their order.

- The data of the StorageServer is an ETS table of type `ordered_set`
  with one row for each key. The row has the versions of the key, with
  the highest version first.
- A second ETS table of type `ordered_set` has one row for each version
  above the oldest version. A rollback examines only the versions that it
  removes. A rise of the oldest version examines only the versions that
  leave the MVCC window.
- The table of the data is readable by other processes.
  `deadlift_storage:info/1` returns it. No part of deadlift reads it
  directly yet.

A benchmark on 2026-11-22 compared four stores with 1,000,000 keys of 12
bytes and values of 100 bytes, on an Apple M4 with Erlang/OTP 28.3. Each
number is the median of 3 runs. The benchmark is not part of this
repository.

| Measurement | Unit | `gb_trees` | ETS, one row for each key | ETS, one row for each version | NIF with `std::map` |
|---|---|---|---|---|---|
| Read of one key | ns | 1,161 | 1,016 | 1,195 | 1,118 |
| Read of a range of 1,000 keys | µs | 28 | 242 | 481 | 240 |
| New version of one key | ns | 2,414 | 1,726 | 1,414 | 884 |
| Longest single put of 200,000 | µs | 23,658 | 661 | 603 | 676 |
| Clear of a range of 100 keys | µs | 162 | 78 | 92 | 16 |
| Reads by 8 processes at the same time | million per second | Not possible | 5.13 | 2.99 | 5.04 |

The owner of the project chose the ETS table with one row for each key.

### The client

The module [deadlift](src/deadlift.erl) is the client. The functions have
the names, the arguments and the results of
[erlfdb](https://github.com/foundationdb-beam/erlfdb) 1.2.3, so that
deadlift can replace erlfdb in an application later.

```erlang
Db = deadlift:open(#{proxy => Proxy, storage => Storage}),
deadlift:transactional(Db, fun(Tx) ->
    Balance = deadlift:wait(deadlift:get(Tx, account)),
    deadlift:set(Tx, account, Balance + 1)
end).
```

| Group | Functions |
|---|---|
| Database and transaction | `open/1`, `create_transaction/1`, `transactional/2`, `snapshot/1` |
| Reads | `get/2`, `get_range/3`, `wait/1`, `wait/2` |
| Writes | `set/3`, `clear/2`, `clear_range/3` |
| Commit and errors | `commit/1`, `reset/1`, `cancel/1`, `on_error/2` |
| Versions | `get_read_version/1`, `set_read_version/2`, `get_committed_version/1`, `get_versionstamp/1` |
| Conflict ranges and status | `add_read_conflict_key/2`, `add_read_conflict_range/3`, `is_read_only/1` |

- A transaction of erlfdb is a mutable object of the C library of
  FoundationDB. A transaction of deadlift is an ETS table of type
  `ordered_set`, so that several processes can use one transaction, as
  with erlfdb. A benchmark measured 213 ns to create and delete such a
  table, and 1,915 ns for the state of a transaction with 2 reads and 2
  writes.
- A read returns a write of the same transaction (paper, section 2.4.1).
- Another read goes to a StorageServer. A database names one
  StorageServer or several, and each of them has every key:
  1. The read puts the StorageServers in an order. A hash of the read
     version selects the first one.
  2. The read tries each StorageServer on the node of the client. It
     reads the ETS table of the StorageServer in the process of the
     client, without a message. The StorageServer publishes its version
     and its oldest version for this purpose.
  3. If no StorageServer on the node has applied the read version, the
     read sends a message to each StorageServer in the order, on any
     node. The StorageServer process waits for the version. The read has
     5 seconds in total, in equal parts for the StorageServers. When a
     StorageServer is not alive or does not answer in its part of the
     time, the read continues with the next one.
- `get/2` returns a future, as in erlfdb. The future of deadlift is the
  result itself, and `wait/1` returns its argument.
- An error is `error({erlfdb_error, Code})` with the error code of
  FoundationDB 7.3: 1007 `transaction_too_old`, 1009 `future_version`,
  1020 `not_committed`, 1021 `commit_unknown_result`, 1025
  `transaction_cancelled` and 1037 `process_behind`.
- `on_error/2` follows `Transaction::onError` of
  `fdbclient/NativeAPI.actor.cpp`. After 1020, 1021 and 1037 it waits for a
  random part of a backoff that starts at 10 ms, doubles with each error
  and is at most 1 s. After 1007 and 1009 it waits for 10 ms.
  `transactional/2` then runs the function again.

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
- The StorageServer keeps the keys in their order, which is the order of
  Erlang terms. A read or a clear of a key range examines only the keys
  of the range. See "The store of the StorageServer" below.
- A transaction has mutations: `{set, Key, Value}`, `{clear, Key}` and
  `{clear_range, Begin, End}`. The StorageServer applies the mutations of
  a transaction in their order. The clear of a key range is a write range
  for the Resolver, so a transaction that read a key of the range has a
  conflict.
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

#### Several StorageServers

Several StorageServers can run at the same time, on one Erlang node or on
several. This is the replication of the whole key space.

- Each StorageServer has every key. Each one reads every record from the
  LogServers, and has its own version, its own oldest version, its own
  ETS tables and its own rollbacks.
- The StorageServers send no message to each other. A LogServer keeps one
  request for each StorageServer.
- A read version is one version for the whole database. At a read
  version, each StorageServer returns the same result, or waits for the
  version, or answers that the read version is too old.
- The client chooses the StorageServer of a read. See "The client".
- A StorageServer on another Erlang node finds the LogServers through the
  registry. See "LogServer identifiers".

FoundationDB divides the key space into shards. Each shard has a team of
StorageServers, and each mutation has the tags of the team of its key
(paper, section 2.4.3). deadlift has one shard with the whole key space.
The team is the list of StorageServers that the caller of
`deadlift:open/1` names.

### LogServer identifiers

The coordinated state identifies a LogServer with `{Epoch, Index}`. A
LogServer registers its identifier with
[deadlift_registry](src/deadlift_registry.erl). The Sequencer, the Proxy
and the StorageServer find a LogServer through the registry. They send a
request only to a LogServer that runs.

Each Erlang node has one registry process. A lookup reads an ETS table of
the local node and sends no message. The registries exchange the names:

- The registry of a node sends each name that a process registers to the
  registries of the connected nodes, and sends the names of its node to a
  node that connects.
- When the connection to a node ends, a registry keeps the names of the
  processes of that node, because the processes can be alive.
- The registry of a node receives a name after the registration on the
  other node has returned. Three parts of deadlift handle this time:
  1. A Sequencer adds the name of each LogServer and of the Resolver that
     it recruits to the registry of its own node.
  2. A Proxy waits at its start, for at most 1 second, until the registry
     has the names of the Resolver and of the LogServers of its epoch.
  3. A recovery waits, for at most the time of the lock, until the
     registry has the names of enough LogServers of the previous epoch.
- A StorageServer sends its requests again at an interval, so it needs
  no such wait.

The simulation found the need for the three parts: with one registry on
each simulated node and without them, no transaction was acknowledged.

### The ClusterController

A node that can run roles has one worker
([deadlift_worker](src/deadlift_worker.erl)). A node can also run one
candidate for the ClusterController
([deadlift_cluster_controller](src/deadlift_cluster_controller.erl)).

Election:

1. The coordinated state has the term and the process of the
   ClusterController.
2. A candidate reads the coordinated state every `poll_ms` (default
   1,000 ms) and monitors the ClusterController of the term.
3. If the term has no ClusterController, or the monitor of the
   ClusterController has ended, the candidate proposes itself with the
   term that it read.
4. The coordinated state accepts one proposal for a term. The candidate
   of that proposal is the leader of the next term.
5. The leader reads the coordinated state every `poll_ms`. If the
   coordinated state has another term, the leader is a candidate again.

Two processes can be the leader at the same time, for at most `poll_ms`
plus the time of a read. Each of them can recruit a Sequencer. The
recovery count of the coordinated state then lets one Sequencer write, as
for any two recoveries at the same time. FoundationDB has the same
property (`design/recovery-internals.md`).

Recruitment:

1. Each worker reads the ClusterController from the coordinated state
   and registers with it. A worker has the class `log`, `stateless` or
   `unset`.
2. The leader recruits when it has no epoch and at least `min_workers`
   workers (default 1).
3. The leader chooses a worker for each of the `logs` LogServers, and
   prefers the class `log`. It chooses a worker for the Sequencer, for
   the Resolver and for the Proxy, and prefers the class `stateless`.
   Each attempt starts at a different worker.
4. The leader starts the Sequencer on its worker. The Sequencer performs
   the recovery, and starts the LogServers and the Resolver on their
   workers.
5. After the recovery, the leader starts the Proxy.
6. The leader sends the epoch, the Sequencer and the Proxy to every
   worker. `deadlift_worker:db_info/0` returns them on each node with a
   worker.

The leader monitors the Sequencer, the LogServers, the Resolver and the
Proxy. When one of them stops, or the connection to its node ends, or the
recovery fails, the leader recruits again after `retry_ms` (default
500 ms). The leader also recruits again when the recovery count of the
coordinated state is above its epoch, because another Sequencer has then
started a recovery.

A worker sends its registration every `poll_ms`, and the leader answers
with the epoch, the Sequencer and the Proxy. A worker that missed a
message receives them within `poll_ms`. A worker, a candidate and the
leader read the coordinated state in a separate process, so each of them
answers its requests while a read waits.

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
- Each StorageServer has every key. FoundationDB divides the key space
  into shards, and each shard has a team of StorageServers.
- The client selects the first StorageServer of a read with a hash of the
  read version. The client of FoundationDB selects a StorageServer from a
  model of the latency of each one, and sends a second request to
  another one when the first is slow.
- The StorageServer reads the log systems from the coordinated state. A
  StorageServer of FoundationDB receives them from the ClusterController.
  This part is not in the paper.
- A key is an Erlang term, and the order of the keys is the order of
  Erlang terms. A key of FoundationDB is a byte string.
- A transaction has three mutations: the set of a key, the clear of a
  key and the clear of a key range. FoundationDB has more, for example
  atomic operations and versionstamps.
- One Resolver handles every key. FoundationDB divides the key space over
  several Resolvers.
- A registry on each node finds the process of a LogServer. In
  FoundationDB the ClusterController has this function. This part is not
  in the paper.
- The coordinated state stores the ClusterController, and a candidate
  detects the failure of the ClusterController with an Erlang monitor.
  FoundationDB elects the ClusterController with a register on each
  coordinator, which is not a consensus operation, and the
  ClusterController sends a heartbeat to the coordinators
  (`fdbserver/LeaderElection.actor.cpp`). This part is not in the paper.
- The ClusterController starts the Proxy after the recovery. In the
  paper, the Sequencer recruits the Proxies.
- The Sequencer performs the recovery, as in the paper. In FoundationDB
  7.3 the recovery runs in the process of the ClusterController.
- The time of a batch is fixed, and a batch has no limit in bytes. The
  time of a batch of FoundationDB changes with the load.
- The Proxy chooses the LogServers that receive the body of a record from
  the LSN. FoundationDB chooses them from the tags of the record.

## Verification

| Level | What it checks | Command | Time |
|---|---|---|---|
| Unit tests | The coordinated state machine, alone and on a peace cluster of three servers on one node. One LogServer: the order of the LSNs, the KCV, repeated records and the lock. The recovery on a peace cluster: the first epoch, a second epoch with records to copy, 1 and 2 LogServers down, and two recoveries at the same time. The Proxy: commits in sequence, 50 commits at the same time, a recovery after commits, a LogServer down and the Sequencer down | `make test` | 3 seconds |
| Simulation | The LogServer, the recovery, the Sequencer, the Resolver, the Proxy and the StorageServer under the scheduler and the simulated network of eta, with kills, partitions, and lost and delayed messages | `make dst` | about 3 minutes |
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
A run has 1, 2 or 3 StorageServers. The number depends on the seed. Each
node with a Sequencer, a LogServer, a Resolver, a Proxy or a
StorageServer has its own registry process, and the registries exchange
the names over the simulated network. A Sequencer starts each LogServer,
the Resolver and the Proxy on their nodes through `erpc`. The harness
connects each new node to the other nodes, as an Erlang cluster does by
default. The
coordinated state, each StorageServer, each Sequencer, each Proxy, each Resolver and each
LogServer run on a simulated node of their own. The network loses 0.1% of the messages
between nodes and delays 20% of them.

The workload has these operations:

- start a Sequencer and, after its recovery, a Proxy;
- run a transaction: get a read version from a Proxy of any epoch, read
  the read keys from the StorageServers at the read version, and commit
  through a Proxy of any epoch, with read keys and write keys from 4
  keys. One client in three waits 20 ms to 100 ms before the commit, so
  that a recovery can end the epoch of the read version;
- kill a LogServer of the current epoch, at most 1 for each epoch;
- kill a Sequencer, also during its recovery;
- kill a Proxy;
- kill the Resolver of an epoch;
- kill one StorageServer, and start a new StorageServer without data,
  which reads every record from the LogServers of every epoch. The
  clients read from the other StorageServers in that time. 1 client in
  2 runs on the node of a StorageServer and reads the ETS table of that
  StorageServer. The other clients read by message;
- cut the connection between two nodes, and remove every cut.

In the runs with ClusterControllers, the harness starts no Sequencer:

- A run has 8 worker nodes: 4 of the class `stateless`, of which 3 have
  a candidate for the ClusterController, and 4 of the class `log`.
- The ClusterController recruits the Sequencer of each epoch.
- The first operation of the list above asks the ClusterController for a
  recovery, and starts a new worker node for each killed worker node.
- A kill of a LogServer, a Sequencer, a Proxy or a Resolver kills the
  worker node of the role, with every role on the node and with the
  candidate of the node.
- The harness kills a worker node only if no epoch then has 2 killed
  LogServers.
- After the last operation, the harness removes every cut, replaces
  every killed worker node, and stops the loss of messages. The run
  continues until the checks at the end of a run hold, for at most
  20,000 steps.

These runs have one more invariant after every step, `one_leader`: no
two ClusterController processes are the leader of one term. The harness
checks at the end of each of these runs:

| Check | Statement |
|---|---|
| `leader` | The ClusterController of the coordinated state runs and is the leader |
| `stale_leader` | No other process is a leader |
| `epoch` | The leader has the current epoch of the coordinated state, and the Sequencer and the Proxy of the epoch run |
| `worker_db_info` | Each worker that runs has the information of the leader about the database |

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
| `storage_rollback` | When a StorageServer has a version of an epoch, it has no write between the end version of an earlier epoch and the first version of the epoch after that one |
| `replicas_agree` | Two StorageServers have the same writes for each key, for the versions that both have applied and that are above the oldest version of both |
| `kcv_below_dv` | In each epoch, the KCV of every LogServer is at or below the DV of every LogServer |
| `chain` | In each LogServer, the previous LSN of each record is the LSN of the record before it |

Results of `make dst` on 2026-11-22:

| Test | Runs | Result |
|---|---|---|
| The same seed gives the same trace, the trace replays, and the audit of eta reports nothing | 20 seeds, 3 runs each | Pass |
| No kills, no partitions, no lost or delayed messages | 200 seeds in 4 tests, 80 operations each, 399 StorageServers at the start of the runs | No violation. 5,540 acknowledged transactions, of which 1,474 with a read range, 679 with a clear of keys and 784 in a batch with another acknowledged transaction, 3,227 rejected for a conflict, 455 too old, 9,436 transactions with reads, of which 3,783 with direct reads on the node of a StorageServer and 1,888 by message only, 0 reads below the MVCC window of the StorageServer, 1,127 recoveries, 344 failed recoveries |
| Kills, partitions, lost and delayed messages | 500 seeds in 5 tests, 80 operations each, 999 StorageServers at the start of the runs | No violation. 1,067 acknowledged transactions, of which 227 with a read range, 134 with a clear of keys and 205 in a batch with another acknowledged transaction, 1,623 rejected for a conflict, 742 too old, 6,882 transactions with reads, of which 1,067 after the start of a new StorageServer, 217 while one of several StorageServers was not alive, 2,741 with direct reads on the node of a StorageServer and 1,362 by message only, 42 reads below the MVCC window of the StorageServer, 245 rollbacks of StorageServers, 2,300 recoveries, 2,838 failed recoveries, 1,560 kills, of which 203 of a StorageServer, 272 partitions |
| ClusterControllers, no kills, no partitions, no lost or delayed messages | 50 seeds in 1 test, 80 operations each | No violation, and each run ends with a leader and an epoch. 1,462 acknowledged transactions, 270 recoveries, 2 failed recoveries, 50 elections |
| ClusterControllers, kills of worker nodes, partitions, lost and delayed messages | 200 seeds in 2 tests, 80 operations each | No violation, and each run ends with a leader and an epoch. 483 acknowledged transactions, 997 recoveries, 133 failed recoveries, 462 kills of a worker node, of which 232 with a ClusterController process, 286 elections, 210 recoveries after the failure of a role, 98 rollbacks of StorageServers, 122 partitions |
| ClusterControllers, kills of worker nodes, cuts between nodes with a ClusterController process only, lost and delayed messages | 100 seeds in 1 test, 80 operations each | No violation, and each run ends with a leader and an epoch. 30 runs with two leaders at the same time, 219 elections, 2 epochs that the leader found superseded, 262 acknowledged transactions, 528 recoveries, 229 kills of a worker node, 46 cuts |
| Planted defect: the coordinated state accepts the proposal of a candidate for the ClusterController with any term | Seeds from 1, runs with cuts between ClusterController nodes | Found at seed 2, `one_leader`: two processes are the leader of term 1 |
| Planted defect: a ClusterController does not recruit after the failure of a role | Seeds from 1, runs with cuts between ClusterController nodes | Found at seed 2, `epoch`: the run ends with an epoch whose Sequencer or Proxy does not run |
| Planted defect: a ClusterController stays the leader when the coordinated state has another term | Seeds from 1, runs with cuts between ClusterController nodes | Found at seed 7, `stale_leader`: the run ends with the leaders of term 1 and term 2 |
| Planted defect: the Proxy answers after 2 of 3 LogServers | Seeds from 1 | Found at seed 15, `kcv_below_dv`. In 120 seeds the defect also violates `durable`, the counterexample of `RecoveryAckReplicasOnly` |
| Planted defect: a recovery copies no record | Seeds from 1 | Found at seed 1, `copied` |
| Planted defect: the coordinated state accepts any write | Seeds from 1 | Found at seed 1, `durable` |
| Planted defect: the Resolver admits every transaction | Seeds from 1 | Found at seed 1, `no_conflict` |
| Planted defect: the Resolver does not check the read ranges | Seeds from 1 | Found at seed 2, `no_conflict` |
| Planted defect: the Resolver does not check a transaction against the earlier transactions of its batch | Seeds from 1 | Found at seed 2, `no_conflict` between two transactions of one batch |
| Planted defect: the Resolver does not record the write ranges | Seeds from 1 | Found at seed 3, `no_conflict`: a transaction read a key that another transaction cleared in a range |
| Planted defect: the first commit version of an epoch is the Recovery Version plus 1 | Seeds from 1 | Found at seed 2, `no_conflict` between a transaction of epoch 6 and a write of epoch 1 |
| Planted defect: a Proxy does not report a committed LSN to the Sequencer | Seeds from 1 | Found at seed 1, `read_version_order` |
| Planted defect: a Proxy gives a read version without the confirmation that its epoch is live | Seeds from 1 | Found at seed 39, `read_version_order`: a Proxy of epoch 3 gave a read version below a commit of epoch 4 |
| Planted defect: the StorageServer keeps the writes above the end version of an epoch | Seeds from 1 | Found at seed 9, `storage_rollback` |
| Planted defect: the StorageServer answers a read below its oldest version | Seeds from 1 | Found at seed 130, `read_value`: a read returned no value for a key that an acknowledged transaction wrote below the read version |
| Planted defect: the StorageServer does not apply a clear | Seeds from 1 | Found at seed 3, `read_value`: a read returned a value for a key that a later acknowledged transaction cleared |
| Planted defect: the second StorageServer of a run does not apply a clear | Seeds from 1, runs without faults | Found at seed 2, `replicas_agree`: the first StorageServer has a clear of a key at a version, and the second one has none |
| Planted defect: the StorageServer answers a read at a version that it has not applied | Seeds from 1 | Found at seed 2, `read_value` |
| Planted defect: a registry sends no name to the registry of another node | Seeds 1 to 10, runs without faults | No violation, as expected. 0 acknowledged transactions and 10 recoveries, 1 in each run. The runs without the defect have 5,540 acknowledged transactions in 200 seeds |

`make dst` runs each test in an Erlang node of its own, with several
nodes at the same time ([dst/run.sh](dst/run.sh)). The runs without faults
are 4 tests of 50 seeds, and the runs with faults are 5 tests of 100
seeds. `dst/run.sh` prints the sums of the statistics of these tests.

| Command | Nodes at the same time | Time |
|---|---|---|
| `make dst-serial` | 1 | 10 min 38 s |
| `make dst` | 8 | 3 min 32 s and 3 min 47 s in 2 runs with one StorageServer and one registry. 7 min 7 s in 1 run with 1 to 3 StorageServers and a registry on each simulated node. 3 min 4 s in 1 run after the change of a run from 150 operations to 80. 3 min 3 s in 1 run with the 8 tests with ClusterControllers, 33 tests in total |
| `dst/run.sh -j 5` | 5 | 4 min 6 s |

The machine has 4 performance cores and 6 efficiency cores. The
commands run the same seeds, and the sums of the statistics are equal.
With 8 nodes, the tests use 2.2 times the processor time of the serial
command, because one test takes longer while other nodes run.

A run has about 5,000 steps and takes about 0.65 s in a node alone. About
30% of that time, the node uses no processor. The invariant checks of the
harness take less than 1% of the time.

## Benchmark

`make bench` runs [bench/deadlift_bench.erl](bench/deadlift_bench.erl). The
benchmark is not a test. It has no limits and does not check the results
of the transactions.

The cluster of the benchmark:

- 3 Erlang nodes with a worker of the class `log`, for the LogServers;
- 3 Erlang nodes with a worker of the class `stateless`, for the
  Sequencer, the Resolver and the Proxy. 2 of them have a candidate for
  the ClusterController;
- 1 Erlang node with a StorageServer;
- the node of the benchmark, with the peace cluster of 3 servers and the
  clients.

Each of the 7 nodes has 2 scheduler threads. Every role has its default
options. A client is one process that runs one operation after another.
A value has 100 bytes.

| Operation | Description |
|---|---|
| `write` | A transaction that sets one key, without a read |
| `read_write` | A transaction that reads one key by message and sets one key, with 1,000,000 keys |
| `conflict` | `read_write` with 100 keys |
| `read_version` | One read version from the Proxy |
| `read_by_message` | The read of one key at a read version that the client has, from the node of the benchmark |
| `read_direct` | The same read by a client on the node of the StorageServer, from the ETS table |

Results of one run on 2026-11-23, with Erlang/OTP 28 and 3 seconds for
each row. The machine has 4 performance cores and 6 efficiency cores.

| Operation | Clients | Operations per second | p50 (us) | p99 (us) | p99.9 (us) | Attempts per operation |
|---|---|---|---|---|---|---|
| `write` | 1 | 492 | 2002 | 2862 | 4334 | 1.00 |
| `write` | 4 | 1389 | 2909 | 4869 | 12250 | 1.00 |
| `write` | 16 | 4333 | 3781 | 5237 | 5744 | 1.00 |
| `write` | 64 | 11136 | 5606 | 8343 | 9924 | 1.00 |
| `write` | 256 | 10193 | 24948 | 29403 | 30591 | 1.00 |
| `read_write` | 1 | 185 | 5138 | 8848 | 10523 | 1.00 |
| `read_write` | 4 | 1112 | 3542 | 5388 | 20198 | 1.00 |
| `read_write` | 16 | 3507 | 4669 | 5662 | 9350 | 1.00 |
| `read_write` | 64 | 8380 | 7422 | 13273 | 16351 | 1.00 |
| `read_write` | 256 | 7122 | 36335 | 50349 | 53173 | 1.00 |
| `conflict` | 64 | 2375 | 6043 | 221131 | 444345 | 1.33 |
| `read_version` | 1 | 12894 | 78 | 101 | 123 | 1.00 |
| `read_version` | 4 | 30537 | 129 | 178 | 359 | 1.00 |
| `read_version` | 16 | 41107 | 382 | 595 | 784 | 1.00 |
| `read_version` | 64 | 49116 | 1289 | 1936 | 2340 | 1.00 |
| `read_version` | 256 | 50408 | 5050 | 6623 | 8233 | 1.00 |
| `read_by_message` | 1 | 38547 | 26 | 35 | 81 | 1.00 |
| `read_by_message` | 16 | 173370 | 88 | 158 | 212 | 1.00 |
| `read_by_message` | 64 | 179280 | 360 | 562 | 645 | 1.00 |
| `read_direct` | 1 | 617647 | 2 | 2 | 16 | 1.00 |
| `read_direct` | 2 | 1168685 | 2 | 3 | 17 | 1.00 |
| `read_direct` | 16 | 1076327 | 2 | 788 | 1093 | 1.00 |

The benchmark then kills one process while 8 clients write, and measures
the longest time without an acknowledged commit. The first column of
times is for the default options of the ClusterController (`retry_ms`
500). The second column is for `retry_ms` 35, which is
`ATTEMPT_RECRUITMENT_DELAY` of FoundationDB.

| Killed process | Time without a commit, `retry_ms` 500 (ms) | Time without a commit, `retry_ms` 35 (ms) |
|---|---|---|
| A LogServer | 639 | 50 |
| The Resolver | 560 | 56 |
| The Proxy | 576 | 53 |
| The Sequencer | 540 | 50 |
| The ClusterController | 15 | 12 |

Observations:

- A `write` of one client takes 2 ms. The Proxy collects the commits of
  1 ms into a batch (`batch_ms`).
- The commits reach about 11,000 per second with 64 clients, and 256
  clients add none.
- A `read_write` of one client takes 5 ms. The read waits until the
  StorageServer has applied the record of the read version, and the
  StorageServer asks the LogServers at an interval.
- A read from the ETS table takes 2 us, and a read by message takes
  26 us.
- After the kill of a role, the time without a commit is `retry_ms` plus
  about 50 ms.
- After the kill of the ClusterController, the old epoch accepts commits
  until the Sequencer of the new ClusterController stops its LogServers.

Limitations of the benchmark:

- All nodes share the processors of one computer, and a message between
  two nodes has no network delay.
- Every LogServer and the StorageServer keep their data in memory.
- The results are from one run. In an earlier run of 1 second for each
  row, `write` with 64 clients had 21,154 operations per second.
- The clients run on the node of the benchmark, so the rows with many
  clients measure that node too.
- A kill stops one process. The node of the process continues.

## Commands

```
make compile    # compile
make test       # unit tests
make dst        # deterministic simulation with eta, in several nodes
make dst-serial # the same tests in one node
make bench      # the benchmark of a cluster of 8 Erlang nodes
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
| [src/deadlift.erl](src/deadlift.erl) | The client, with the function names of erlfdb |
| [src/deadlift_cluster_controller.erl](src/deadlift_cluster_controller.erl) | The ClusterController: the election, the workers and the recruitment |
| [src/deadlift_worker.erl](src/deadlift_worker.erl) | The worker of one node, which starts roles for the ClusterController |
| [src/deadlift_registry.erl](src/deadlift_registry.erl) | The registry of the processes of one node |
| [src/deadlift_cstate_local.erl](src/deadlift_cstate_local.erl) | The coordinated state in one process, for tests and for the simulation |
| [include/deadlift_eta.hrl](include/deadlift_eta.hrl) | The header that puts a module under the parse transform of eta in the `dst` profile |
| [dst/deadlift_dst.erl](dst/deadlift_dst.erl) | The eta harness: the workload and the invariants |
| [dst/deadlift_dst_client.erl](dst/deadlift_dst_client.erl) | The processes that perform the operations of the simulation |
| [dst/deadlift_dst_tests.erl](dst/deadlift_dst_tests.erl) | The simulation suite |
| [bench/deadlift_bench.erl](bench/deadlift_bench.erl), [bench/run.sh](bench/run.sh) | The benchmark of a cluster on one computer |
| [dst/run.sh](dst/run.sh) | Runs each test of the simulation suite in a node of its own |
| [src/deadlift_app.erl](src/deadlift_app.erl), [src/deadlift_sup.erl](src/deadlift_sup.erl) | The application and its supervisor, which starts the registry |
| [test/deadlift_cstate_tests.erl](test/deadlift_cstate_tests.erl) | Tests for `deadlift_cstate` |
| [test/deadlift_cluster_controller_tests.erl](test/deadlift_cluster_controller_tests.erl) | Tests for `deadlift_cluster_controller` and `deadlift_worker` |
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
- The client has the basic functions of erlfdb only. It has no key
  selectors, no options of `get_range`, no atomic operations, no
  versionstamped keys and values, no watches, no transaction options, no
  tenants and no `get_last_error/0`. It has no tuple layer, no subspaces
  and no directory layer.
- A database of the client is a Proxy and a list of StorageServers that
  the caller names. deadlift has no process that tells a client which
  Proxy and which StorageServers run.
- A read by message waits for its part of the 5 seconds at a
  StorageServer that is behind or cut off, before it continues with the
  next StorageServer. The client has no record of the StorageServers that
  did not answer an earlier read.
- A registry receives names only from the nodes to which its node is
  connected, and it does not connect nodes. The nodes of an Erlang
  cluster connect to each other by default. In the simulation, the
  harness connects each new node to the other nodes.
- A process cannot tell a LogServer whose name has not arrived from a
  LogServer that does not run.
- The registry keeps the name of a process on a node that has stopped.
- Two unit tests, in
  [test/deadlift_nodes_tests.erl](test/deadlift_nodes_tests.erl), use two
  Erlang nodes on one computer. One of them has a worker and a candidate
  for the ClusterController on each node. Each test starts Erlang
  distribution and
  needs `epmd`. No test cuts the connection between two Erlang nodes.
  The simulation cuts the connection between simulated nodes.
- Two processes on two nodes can register one name at the same time.
  The client raises the error 1037 when the Proxy does not answer. The
  client of FoundationDB waits for a new Proxy.
- A transaction of `create_transaction/1` keeps its ETS table until its
  process ends. erlfdb frees a transaction when no process has a
  reference to it.
- Two processes that use one transaction at the same time get no error.
- The simulation does not use `transactional/2` and `on_error/2`. Unit
  tests cover them.
- The direct read was not measured in deadlift. The store benchmark
  measured about 1,000 ns for a read of one key from such a table.
- The StorageServer keeps the data in memory, and loses the data when it
  stops. A new StorageServer reads every record again
  from the LogServers, which is possible only while the LogServers of
  every epoch run.
- A read at a version that is not a read version from a Proxy can return
  a write that a recovery discards.
- After a LogServer is stopped, the StorageServer reads the coordinated
  state every 500 ms until the coordinated state has a later epoch.
- A read of a key range returns every key of the range. A read has no
  limit of rows.
- A read copies each value from the ETS table to the StorageServer, and
  from the StorageServer to the client.
- In the simulation, a read that returns no value is accepted when a
  transaction without a known result cleared the key at any version.
- The MVCC window of the StorageServer and the MVCC window of the Resolver
  are two options. No part of deadlift checks that they are equal.
- In the simulation, a new StorageServer starts without data after a
  kill. The simulation kills at most 1
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
- A candidate for the ClusterController proposes itself when the
  connection to the node of the ClusterController ends. If two candidates
  cannot connect to each other and both can connect to the coordinators,
  each election replaces the ClusterController, and each new
  ClusterController starts a recovery.
- A leader that abandons an attempt stops the Sequencer of the attempt
  when it learns the Sequencer. If that Sequencer has raised the recovery
  count, the recovery of the next attempt can fail, and the leader
  recruits again. In 50 simulation runs without faults, 2 of 270
  recoveries failed in this way.
- A new ClusterController starts a recovery, also when the roles of the
  current epoch run. FoundationDB has the same property.
- The ClusterController does not stop the roles of an ended epoch. A
  Sequencer and a Proxy of an ended epoch continue to run.
- The ClusterController does not start StorageServers.
- The roles do not read the information of the ClusterController about
  the database. They find each other through the registry.
- The client does not find the Proxy from the coordinators. A caller
  reads the Proxy with `deadlift_worker:db_info/0` on a node with a
  worker.
- A worker and a candidate read the coordinated state every `poll_ms`. A
  process of FoundationDB waits for a change with one request.
- The application does not start the worker or the candidate. A caller
  starts them with `deadlift_worker:start/1` and
  `deadlift_cluster_controller:start/1`.
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
