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

- a TLA+ model of the logging protocol and of the recovery of the
  transaction system, checked with TLC;
- the coordinated state, as a state machine for peace, with tests;
- the LogServer, with tests;
- the recovery and a Sequencer that assigns commit versions, with tests;
- a Proxy that commits a record without a conflict check, with tests;
- a deterministic simulation of these parts with
  [eta](https://github.com/jessestimpson/eta), with kills of LogServers,
  Sequencers and Proxies.

deadlift has no Resolver and no StorageServer yet. A client can commit a
record to the LogServers. A client cannot read, and deadlift cannot run a
transaction. See [ROADMAP.md](ROADMAP.md).

## Design

In FoundationDB, consensus is not on the commit path. The Coordinators store
a small record, the coordinated state. Every other part of the transaction
system is created again in each epoch (paper, sections 2.3.4 and 2.3.5).
deadlift keeps this design.

| Role in the paper | Function | In deadlift |
|---|---|---|
| Coordinators | Store the coordinated state | A peace cluster that runs [deadlift_cstate](src/deadlift_cstate.erl) |
| ClusterController | Recruits the Sequencer | Not implemented |
| Sequencer | Assigns read versions and commit versions, and performs the recovery | [deadlift_sequencer](src/deadlift_sequencer.erl) and [deadlift_recovery](src/deadlift_recovery.erl), without read versions. Modelled in [formal/Recovery.tla](formal/Recovery.tla) |
| Proxy | Gives read versions to clients and commits transactions | [deadlift_proxy](src/deadlift_proxy.erl), without read versions and without a conflict check. Modelled |
| Resolver | Checks a transaction for conflicts | Not implemented |
| LogServer | Stores the log records of an epoch | [deadlift_log](src/deadlift_log.erl). Modelled |
| StorageServer | Stores data and answers reads at a version | Not implemented |

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

The Sequencer then assigns the LSNs above the RV. If a step fails, the
Sequencer stops. A recovery that fails after step 1 leaves a higher
recovery count. A recovery that fails after step 2 leaves stopped
LogServers. The next recovery handles both.

### The Proxy

A Proxy commits a record in three steps (paper, sections 2.4.1 and 2.4.3):

1. The Proxy asks the Sequencer for an LSN and the previous LSN.
2. The Proxy sends the record to every LogServer of the epoch. The k
   LogServers that replicate the record receive the body.
3. When every LogServer of the epoch has answered, the Proxy answers the
   client and raises its KCV to the LSN.

The Proxy waits for every LogServer, and not only for the k LogServers
with the body. The config `RecoveryAckReplicasOnly` of the model shows
that a Proxy that waits for k LogServers lets a recovery discard an
acknowledged record.

If a LogServer answers with an error or is not alive, the Proxy answers
`{error, commit_unknown_result}` to every client that waits, and stops.
The Proxy of an ended epoch stops in this way at its next commit.

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
- The Sequencer raises the LSN by 1 for each commit version. FoundationDB
  raises the version with the time.
- A registry on each node finds the process of a LogServer. In
  FoundationDB the ClusterController has this function. This part is not
  in the paper.
- The Proxy has no Resolver, and sends one record for each commit.
  FoundationDB collects transactions into a batch with one commit version.
- The Proxy chooses the LogServers that receive the body of a record from
  the LSN. FoundationDB chooses them from the tags of the record.

## Verification

| Level | What it checks | Command | Time |
|---|---|---|---|
| Unit tests | The coordinated state machine, alone and on a peace cluster of three servers on one node. One LogServer: the order of the LSNs, the KCV, repeated records and the lock. The recovery on a peace cluster: the first epoch, a second epoch with records to copy, 1 and 2 LogServers down, and two recoveries at the same time. The Proxy: commits in sequence, 50 commits at the same time, a recovery after commits, a LogServer down and the Sequencer down | `make test` | 3 seconds |
| Simulation | The LogServer, the recovery, the Sequencer and the Proxy under the scheduler and the simulated network of eta, with kills, partitions, and lost and delayed messages | `make dst` | about 3.5 minutes |
| Model checking | The logging protocol and the recovery, with 2 epochs | `make tlc` | about 1 minute |
| Model checking | The same model with 3 epochs | `make tlc-long` | about 5 minutes |

The model has three planted bugs. TLC finds a counterexample for each. See
[formal/README.md](formal/README.md).

### Simulation

`make dst` runs the system under the scheduler and the simulated network
of eta. The harness is [dst/deadlift_dst.erl](dst/deadlift_dst.erl).

The simulated system has one process for the coordinated state
([deadlift_cstate_local](src/deadlift_cstate_local.erl)) in the place of a
peace cluster, 3 LogServers for each epoch and a replication degree of 2.
The coordinated state, each Sequencer, each Proxy and each LogServer run
on a simulated node of their own. The network loses 0.1% of the messages
between nodes and delays 20% of them.

The workload has these operations:

- start a Sequencer and, after its recovery, a Proxy;
- commit a record through a Proxy of any epoch;
- kill a LogServer of the current epoch, at most 1 for each epoch;
- kill a Sequencer, also during its recovery;
- kill a Proxy;
- cut the connection between two nodes, and remove every cut.

The harness checks these invariants after every step:

| Invariant | Statement |
|---|---|
| `durable` | An acknowledged LSN of an ended epoch is at or below the Recovery Version that ended the epoch. `Durable` of the model |
| `unique_versions` | No LSN is acknowledged in two epochs or to two clients. `UniqueVersions` of the model |
| `no_loss` | No recovery ends with a missing body. `NoLoss` of the model |
| `body_available` | The body of every acknowledged record is on a LogServer of its epoch that runs |
| `copied` | Every LogServer of a new epoch has the body of every acknowledged record above the previous epoch's end version |
| `kcv_below_dv` | In each epoch, the KCV of every LogServer is at or below the DV of every LogServer |
| `chain` | In each LogServer, the previous LSN of each record is the LSN of the record before it |

Results of `make dst` on 2026-11-22:

| Test | Runs | Result |
|---|---|---|
| The same seed gives the same trace, the trace replays, and the audit of eta reports nothing | 20 seeds, 3 runs each | Pass |
| No kills, no partitions, no lost or delayed messages | 200 seeds, 150 operations each | No violation. 23,735 acknowledged commits, 2,404 recoveries |
| Kills, partitions, lost and delayed messages | 500 seeds, 150 operations each | No violation. 28,238 acknowledged commits, 7,422 recoveries, 2,708 kills, 703 partitions |
| Planted defect: the Proxy answers after 2 of 3 LogServers | Seeds from 1 | Found at seed 1, `durable`. This is the counterexample of `RecoveryAckReplicasOnly` |
| Planted defect: a recovery copies no record | Seeds from 1 | Found at seed 1, `copied` |
| Planted defect: the coordinated state accepts any write | Seeds from 1 | Found at seed 2, `unique_versions`. This is the counterexample of `RecoveryNoCas` |

The suite takes about 3.5 minutes.

The simulation needs the branch `send-request-and-dead-monitors` of eta.
The branch adds `gen_server:send_request/4` to the simulated network, and
corrects `is_process_alive/1` on a suspended process.

## Commands

```
make compile    # compile
make test       # unit tests
make dst        # deterministic simulation with eta
make tlc        # the quick TLC configs
make tlc-long   # the TLC config with three epochs
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
| [src/deadlift_proxy.erl](src/deadlift_proxy.erl) | The Proxy |
| [src/deadlift_registry.erl](src/deadlift_registry.erl) | The registry of the processes of one node |
| [src/deadlift_cstate_local.erl](src/deadlift_cstate_local.erl) | The coordinated state in one process, for tests and for the simulation |
| [include/deadlift_eta.hrl](include/deadlift_eta.hrl) | The header that puts a module under the parse transform of eta in the `dst` profile |
| [dst/deadlift_dst.erl](dst/deadlift_dst.erl) | The eta harness: the workload and the invariants |
| [dst/deadlift_dst_client.erl](dst/deadlift_dst_client.erl) | The processes that perform the operations of the simulation |
| [dst/deadlift_dst_tests.erl](dst/deadlift_dst_tests.erl) | The simulation suite |
| [src/deadlift_app.erl](src/deadlift_app.erl), [src/deadlift_sup.erl](src/deadlift_sup.erl) | The application and its supervisor, which starts the registry |
| [test/deadlift_cstate_tests.erl](test/deadlift_cstate_tests.erl) | Tests for `deadlift_cstate` |
| [test/deadlift_log_tests.erl](test/deadlift_log_tests.erl) | Tests for `deadlift_log` |
| [test/deadlift_recovery_tests.erl](test/deadlift_recovery_tests.erl) | Tests for `deadlift_recovery` and `deadlift_sequencer` |
| [test/deadlift_proxy_tests.erl](test/deadlift_proxy_tests.erl) | Tests for `deadlift_proxy` |
| [formal/Recovery.tla](formal/Recovery.tla) | TLA+ model of the logging protocol and the recovery |
| [formal/MCRecovery.tla](formal/MCRecovery.tla) | Constants and invariants for model checking |
| [formal/README.md](formal/README.md) | The model, the configs and the results |
| [ROADMAP.md](ROADMAP.md) | The planned work |

## Limitations

- deadlift cannot run a transaction, and a client cannot read. A client
  can commit a record to the LogServers.
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
- The model has no Resolver, no StorageServer, no tags, no recovery
  transaction and no election of a ClusterController.
- The model is checked with 3 LogServers, a replication degree of 2, and at
  most 3 epochs and 3 versions.
- deadlift does not send a message again. In the simulation, a lost
  message to a LogServer stops the commits of the epoch until the next
  recovery.
- The simulation has one process for the coordinated state, and no peace
  cluster. No test runs deadlift on a peace cluster with faults.
- The repository has no licence file.
