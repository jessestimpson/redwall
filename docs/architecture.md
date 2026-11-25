# Architecture

In FoundationDB, consensus is not on the commit path. The Coordinators store
a small record, the coordinated state. Every other part of the transaction
system is created again in each epoch (paper, sections 2.3.4 and 2.3.5).
redwall keeps this design.

| Role in the paper | Function | In redwall |
|---|---|---|
| Coordinators | Store the coordinated state | A peace cluster that runs [redwall_cstate](../src/redwall_cstate.erl) |
| ClusterController | Monitors the roles and recruits the Sequencer | [redwall_cluster_controller](../src/redwall_cluster_controller.erl) and [redwall_worker](../src/redwall_worker.erl). Not modelled |
| Sequencer | Assigns read versions and commit versions, and performs the recovery | A role of the process of an epoch, [redwall_epoch](../src/redwall_epoch.erl), with the rules in [redwall_sequencer](../src/redwall_sequencer.erl) and [redwall_recovery](../src/redwall_recovery.erl). The recovery, the recovery transaction and the versions are modelled in [formal/Recovery.tla](../formal/Recovery.tla) |
| Proxy | Gives read versions to clients and commits transactions | A role of the process of an epoch, [redwall_epoch](../src/redwall_epoch.erl). The commit to the LogServers and the read version are modelled |
| Resolver | Checks a transaction for conflicts | A role of the process of an epoch, with the functions of [redwall_resolver](../src/redwall_resolver.erl). Modelled in [formal/Resolver.tla](../formal/Resolver.tla) |
| LogServer | Stores the log records of an epoch | [redwall_log](../src/redwall_log.erl). Modelled |
| StorageServer | Stores data and answers reads at a version | [redwall_storage](../src/redwall_storage.erl). Modelled, without keys and values |

## One process for an epoch

The Sequencer, the Resolver and the Proxy of an epoch are one process in
redwall ([redwall_epoch](../src/redwall_epoch.erl)). The documents of this directory
use the names of the three roles for the parts of its work.

- FoundationDB has one process for each role. A process of FoundationDB
  runs on one thread, and separate processes are its way to use several
  processors.
- The three roles have the same lifetime. They start with an epoch, and
  the failure of each of them starts a recovery. One process for the
  three adds no failure case.
- The process assigns the commit version of a batch and checks the batch
  for conflicts in one step. The checks are in the order of the versions
  without a protocol for the order.
- The process raises the read version when the LogServers have stored a
  record, before it answers the client. No report to another process is
  needed.
- A commit has one round trip between nodes, to the LogServers.
- One process is one place for the conflict check. FoundationDB can add
  Proxies and Resolvers on more machines. redwall cannot, in this
  design.

This part is not in the paper.

## The coordinated state

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

## Departures from the paper

- FoundationDB stores the coordinated state with Active Disk Paxos on the
  Coordinators (paper, section 2.5). redwall stores the coordinated state
  in the replicated log of a peace cluster. This part is not in the paper.
- The coordinated state of redwall records an owner for the recovery
  count, so that a lock or a write that is proposed a second time after a
  timeout has no second effect. This part is not in the paper.
- A LogServer of redwall keeps the records in memory, with every
  storage engine. FoundationDB writes the records to disk. This part is
  not in the paper.
- The recovery copies each record to every new LogServer. FoundationDB
  copies a record to the new LogServers that replicate the tags of the
  record.
- FoundationDB keeps the commit versions near the time of the wall clock.
  redwall raises the commit version with the elapsed time only.
- A Proxy of redwall confirms the epoch for each request of a read
  version. FoundationDB confirms once for a batch of requests.
- A Proxy of redwall stops when a LogServer answers that it is stopped.
  A proxy of FoundationDB does not answer the request, and stops when it
  learns of the recovery.
- The Sequencer of redwall sends the recovery transaction itself, after
  the write of the coordinated state. The master of FoundationDB sends it
  through a proxy, before the write of the coordinated state.
- With the storage engine `ets`, the StorageServer keeps the data in
  memory. A StorageServer of FoundationDB keeps the versions of the MVCC
  window in memory and writes older data to its storage engine on a
  disk. redwall has the interface of a storage engine
  ([redwall_storage_engine](../src/redwall_storage_engine.erl)) and no
  engine on a disk. This part is not in the paper.
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
- The coordinated state stores the ClusterController, and a candidate
  detects the failure of the ClusterController with an Erlang monitor.
  FoundationDB elects the ClusterController with a register on each
  coordinator, which is not a consensus operation, and the
  ClusterController sends a heartbeat to the coordinators
  (`fdbserver/LeaderElection.actor.cpp`). This part is not in the paper.
- The Sequencer, the Resolver and the Proxy of an epoch are one process.
  See "One process for an epoch".
- The Sequencer performs the recovery, as in the paper. In FoundationDB
  7.3 the recovery runs in the process of the ClusterController.
- The time of a batch is fixed, and a batch has no limit in bytes. The
  time of a batch of FoundationDB changes with the load.
- The Proxy chooses the LogServers that receive the body of a record from
  the LSN. FoundationDB chooses them from the tags of the record.
