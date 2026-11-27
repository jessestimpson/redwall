# Verified properties of redwall

This file lists the properties of redwall that a mechanical check supports.
It follows the [verified_md](https://github.com/jessestimpson/verified_md) convention.

| Field | Value |
|---|---|
| Project | redwall, a transactional key-value store in Erlang |
| Detailed sources | [docs/verification.md](docs/verification.md), [formal/README.md](formal/README.md), [docs/limitations.md](docs/limitations.md) |

## How to read this file

1. A reader can treat each statement under "Verified properties" as true
   for the version of redwall in the same commit as this file, within the
   bound that the entry states and under the assumptions in "Assumptions".
2. An entry is not verified beyond its bound. The entry can be true or
   false there. An entry with the method `TLC` for 2 epochs does not state
   anything about 4 epochs.
3. A property that is not in this file is not verified. The property can
   be true or false. "Not verified" lists the properties that a reader is
   most likely to assume.
4. Each entry has an identifier, such as `R1`. The identifier does not
   change when the file is edited.
5. "Reproduction" gives the date of the last recorded run of each check
   that GitHub Actions does not run on each push. No later run supports
   the entries of that check.
6. An entry that relies on peace is verified only within the bounds of the
   entries of peace that "Dependencies" names.

## Methods

| Method | Check | What a pass shows | What a pass does not show |
|---|---|---|---|
| `TLC` | TLC visits every reachable state of a TLA+ model. For the temporal property `Fires`, TLC checks every behaviour under the fairness conditions of the model. | The property holds in the model, for the constants of the config. | Anything about other constants, or about the code. `TRACE` connects the code to `Recovery.tla` for the checked runs. |
| `SIM` | The processes of redwall run under [eta](https://github.com/jessestimpson/eta) with a seeded scheduler, a virtual clock and a simulated network. The harness kills nodes, cuts connections, and loses and delays messages. The harness checks invariants after every step, and further statements at the end of a run. | The invariant held at every step of every run. | Anything about a schedule that no seed produced. |
| `UNIT` | EUnit tests in [test/](test/). | The named case behaves as stated. | Anything about other cases. |
| `PORC` | The clients of a simulated run record what they observe: the values that a transaction read, the keys that it set and cleared, and whether its commit returned. [Porcupine](https://github.com/anishathalye/porcupine), a checker that shares no code with redwall, searches for an order of the transactions that explains every read and that agrees with the order of the transactions in time. See [dst/porcupine/main.go](dst/porcupine/main.go). | The client history of each checked run is strictly serializable. | Anything about other runs. A read of a record that a recovery discards later is accepted, unless a later read shows that the value is gone. |
| `TRACE` | Trace validation. The harness records the state of the coordinated state, of each LogServer, of the process of each epoch and of each StorageServer after every step of a simulated run. [dst/redwall_dst_trace.erl](dst/redwall_dst_trace.erl) derives the actions of [formal/Recovery.tla](formal/Recovery.tla) from the changes, without a statement of the code about its action. TLC checks that the actions, in their order, are a behaviour of the model, and that the model has the state of the code after each change. | Each checked run is a behaviour of the model for its 15 actions, and the invariants `Durable`, `NoLoss`, `UniqueVersions`, `KcvSound`, `ReadVersionOrder`, `StorageSafe` and `StorageComplete` hold in it. | Anything about other runs. The values of the LSNs, the limit on the versions in flight, the LogServer that answers a confirmation, and the keys and values of a StorageServer are not validated. |

`TLC` is exhaustive within its bounds. `SIM`, `PORC`, `TRACE` and `UNIT`
are tests: each one checks a finite set of runs.

The three models are [formal/Recovery.tla](formal/Recovery.tla),
[formal/Resolver.tla](formal/Resolver.tla) and
[formal/Watch.tla](formal/Watch.tla). The models are separate. No model
contains two of the three subjects.

## Assumptions

- **A1. The coordinated state is one register with atomic operations.**
  Each `lock`, `write` and `elect` value takes effect at most once in one
  order, an acknowledged value is not lost, and a read returns the result
  of every acknowledged value. peace provides the register. See
  "Dependencies".
- **A2. The assumptions of peace.** redwall inherits the assumptions `A1`
  to `A4`, `A6` and `A8` of the `VERIFIED.md` of peace. redwall does not
  use read leases or sessions, so `A5` and `A7` of peace do not apply.
- **A3. No Byzantine faults.** A process runs the code of redwall. A
  message is lost or delayed, and is not altered.
- **A4. The LogServers of an epoch are not all lost.** Every entry about
  acknowledged transactions holds while at most 1 of the 3 LogServers of
  each epoch has stopped. When the LogServers of an epoch are lost, the
  recovery resets the database.
- **A5. At least one node keeps running.** The LogServers keep their data
  in memory. A StorageServer with the storage engine `ets` keeps its data
  in memory, and every check uses the engine `ets`. After the stop of
  every node, the cluster starts without data.
- **A6. A read uses a read version from a Proxy.** A read at another
  version can return a write that a recovery discards.
- **A7. The two MVCC windows are equal.** The window of the StorageServer
  and the window of the Resolver are two options. No part of redwall
  checks that they are equal.
- **A8. The other dependencies behave as "Dependencies" states.**

## Dependencies

Dependencies with a `VERIFIED.md`:

| Dependency | Version | Entries relied on | Used for | Treatment |
|---|---|---|---|---|
| [peace](https://github.com/jessestimpson/peace/blob/main/VERIFIED.md) | The commit that `rebar.lock` records. | `C1`, `C2`, `C3`, `K1`, `K2`, `K4`, `K5`, `D1`, `D2`, `L1`, `L2`, `L3` | The coordinated state, which is assumption `A1`. The coordinators are a peace cluster with majority quorums, the write-ahead log backend, no read leases, no sessions and no membership change. A value is proposed with `peace:propose/3` and read with `peace:consistent_query/3`. | `TLC`: assumed. `Recovery.tla` has one register with the atomic operations `LockCState` and `WriteCState`. `SIM`: replaced by one `redwall_cstate_local` process in 43 tests, and included in the 9 tests with the name `peace_`. `UNIT`: included in the tests of the coordinated state, the coordinator and the recovery, and replaced in others. |

Comparison of the use with the bounds of the entries relied on:

- redwall uses majority quorums. The entries of peace cover majority
  quorums with 3 and 5 servers. The example configuration of redwall has
  3 coordinators.
- redwall proposes a value again after a timeout, without a session. The
  entry `S1` of peace is therefore not relied on. Each value of the
  coordinated state is a compare-and-set, and a value that is applied a
  second time for the same owner gives the same reply and does not change
  the state. No check of redwall plants a second application of a value.
- redwall has no function that replaces a coordinator or changes the
  coordinators. The entries of peace about membership change and member
  identity are not relied on.
- No check supports the statement that the entries relied on provide
  assumption `A1`. The 9 `SIM` tests with a peace cluster test redwall and
  peace together. See `P1` to `P3`.

Dependencies without a `VERIFIED.md`. No check in redwall verifies the
behaviour assumed.

| Dependency | Version | Used for | Behaviour assumed |
|---|---|---|---|
| Erlang/OTP: the emulator, `kernel` and `stdlib`, with ETS | 28.3 in GitHub Actions | Processes, distribution, ETS tables and timers | A message between two processes is not altered. An ETS table returns the objects that were written to it. |
| [eta](https://github.com/jessestimpson/eta) | The commit that the `dst` profile of `rebar.config` fixes. `rebar.lock` does not hold the dependencies of a profile. | `SIM` | A run is determined by its seed. The simulated network, clock and disk produce only faults that a real system can produce. |
| TLC | The jar in `formal/vendor` | `TLC` | TLC visits every reachable state of a model and reports every violation, except for a state that a fingerprint collision hides. |
| EUnit | The version in Erlang/OTP | `UNIT`, and the runner of `SIM` | A test that fails is reported as failed. |
| [Porcupine](https://github.com/anishathalye/porcupine), with Go | 1.3.1, in `dst/porcupine/go.mod`. Go 1.19 in the last recorded run. | `PORC` | Porcupine reports a history as legal only if the model of [dst/porcupine/main.go](dst/porcupine/main.go) has an order of the operations that explains the history. |

## Verified properties

### Logging protocol and recovery: the model

The model is `Recovery.tla`. Every config has 3 LogServers, a replication
degree of 2 and a `Gap` of 2.

| Id | Property | Method | Bound |
|---|---|---|---|
| `R1` | `Durable`. When an epoch has ended, every LSN that the Proxy of the epoch acknowledged is at or below the Recovery Version. | `TLC` | 2 epochs and 7 versions. 3 epochs and 6 versions. |
| `R2` | `NoLoss`. Every record that a recovery must copy has its body on an old LogServer that answered. | `TLC` | The same. |
| `R3` | `UniqueVersions`. An LSN is acknowledged in one epoch only. | `TLC` | The same. Assumes `A1`. The config `RecoveryNoCas` shows that the property fails without a compare-and-set on the register. |
| `R4` | `KcvSound`. The Known Committed Version of a LogServer is an acknowledged LSN, or the previous epoch's end version of its epoch. | `TLC` | The same. |
| `R5` | `BelowNextEpoch`. No LogServer persists a record of a client at or above the first LSN of the next epoch. | `TLC` | The same. |
| `R6` | `ReadVersionOrder`. A read version is at or above every LSN that was acknowledged, in any epoch, before the client asked. | `TLC` | 2 epochs, 5 versions and 1 request. Not checked with 3 epochs. |
| `R7` | `StorageSafe`. When the StorageServer has the version of a read version, it has no record of a client at or below that version that a recovery discarded. | `TLC` | 2 epochs and 6 versions. 3 epochs and 6 versions. 1 StorageServer. |
| `R8` | `StorageComplete`. At that time the StorageServer has every acknowledged record at or below that version. | `TLC` | The same. |

### Conflict checks: the model

The model is `Resolver.tla`. Every config has 2 keys, 3 transactions, an
MVCC window of 3 versions and a `MaxStep` of 2.

| Id | Property | Method | Bound |
|---|---|---|---|
| `T1` | `NoConflict`. No committed transaction read a key that another committed transaction wrote between the read version and the commit version of the first one, or at that commit version and earlier in the batch. | `TLC` | 2 epochs, 11 versions and `Gap` 3. 3 epochs, 15 versions and `Gap` 3. 2 epochs, 10 versions and `Gap` 2. |
| `T2` | `BlindWritesCommit`. A transaction without read keys is never rejected. | `TLC` | The same. |
| `T3` | The ordered set of bounds of `redwall_resolver` gives the same decisions as one version for each key. | `UNIT` | 3,000 random transactions over 6 keys. |

### Watches: the model

The model is `Watch.tla`. The config has 1 key, 1 watch, 1 StorageServer,
2 values, 6 LSNs and 2 recoveries.

| Id | Property | Method | Bound |
|---|---|---|---|
| `W1` | `ReadySound`. When the future of the watch is ready, an acknowledged record above the version of the watch has written another value than the value of the watch. | `TLC` | The config above. |
| `W2` | `Fires`, a liveness property. If the value of the last acknowledged record differs from the value of the watch from some time on, the future becomes ready. | `TLC` | The config above, under the fairness conditions of the model. The property does not depend on the time limit of a request. |

### Logging protocol and recovery: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `K1` | `durable`. An acknowledged LSN of an ended epoch is at or below the Recovery Version that ended the epoch. | `SIM` | Every run. See "Simulation runs". |
| `K2` | `unique_versions`. No LSN is acknowledged in two epochs or to two clients. | `SIM` | Every run. |
| `K3` | `no_loss`. No recovery ends with a missing body. | `SIM` | Every run. |
| `K4` | `body_available`. The body of every acknowledged record is on a LogServer of its epoch that runs, or the record is at or below the popped version of such a LogServer. | `SIM` | Every run. |
| `K5` | `copied`. Every LogServer of a new epoch has the body of every acknowledged record above the previous epoch's end version and above the popped version of the LogServer. | `SIM` | Every run. |
| `K6` | `kcv_below_dv`. In each epoch, the Known Committed Version of every LogServer is at or below the Durable Version of every LogServer. | `SIM` | Every run. |
| `K7` | `chain`. In each LogServer, the previous LSN of each record is the LSN of the record before it. | `SIM` | Every run. |
| `K8` | `popped_stored`. A StorageServer that runs has a version at or above the popped version of every LogServer that runs. | `SIM` | Every run. LogServers discard records in the 190 runs with the pop. |
| `K9` | The recovery handles the first epoch, a second epoch with records to copy, 1 and 2 LogServers down, and two recoveries at the same time. | `UNIT` | On a peace cluster of 3 servers on one node. |

### The code against the model of the recovery

| Id | Property | Method | Bound |
|---|---|---|---|
| `V1` | A simulated run is a behaviour of `Recovery.tla` for these actions: `LockCState`, `LockLog`, `Recruit`, `WriteCState`, `RecoveryTxn`, `Accept`, `Assign`, `Persist` and `Ack`. After each change, the model has the recovery count, the current epoch and the end versions of the coordinated state, the Durable Version and the Known Committed Version of the LogServer, and the last LSN, the read version and the Known Committed Version of the process of the epoch. Each new LogServer has, with a body, the records that the model copies from the previous epoch: the records above the previous epoch's end version, up to the Recovery Version. | `TRACE` | 220 runs in 4 tests of [dst/redwall_dst_trace_tests.erl](dst/redwall_dst_trace_tests.erl), with 85,639 actions of the model: 40 seeds without faults, 100 with faults, 40 with ClusterControllers and faults, and 40 with a peace cluster and faults. Each action of `V1`, `V2` and `V3` occurs at least 230 times in each test. The trace spec has `LimitInFlight = FALSE`. An LSN of the code has a number of the model that keeps the order of the LSNs. |
| `V2` | In the same runs, the confirmations of the process of an epoch are a behaviour of `Recovery.tla` for the actions `ReadStart`, `ReadConfirm`, `ReadSeq` and `ReadDone`, with `Confirms = 2`. One confirmation is one request of the model. The model has the read version that the process takes for the confirmation and the read version of its answer. `ReadVersionOrder` holds: the read version is at or above every LSN that the trace acknowledged, in any epoch, before the start of the confirmation. | `TRACE` | The runs of `V1`: 8,416 confirmations, of which 7,436 have an answer. A `ReadConfirm` action is placed at the start of the confirmation, for a LogServer that no `LockLog` action had stopped. The process counts the answers and does not keep the names of the LogServers. |
| `V3` | In the same runs, each StorageServer that the trace follows is a behaviour of the StorageServer of `Recovery.tla` for the actions `SSApply` and `SSFollow`. After each change, the model has the epoch and the version of the StorageServer, and the StorageServer of the model has applied, and has not removed, the record of each write of the store between the version before and the version after. `StorageSafe` and `StorageComplete` hold for each StorageServer. | `TRACE` | The runs of `V1`: 521 StorageServers from their start, 9,692 `SSApply` actions and 2,839 `SSFollow` actions. The trace does not follow a StorageServer after a copy from a peer: 50 StorageServers. The keys and values of a write are not compared. |

### Transactions: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `X1` | `no_conflict`. No acknowledged transaction read a key that another acknowledged transaction wrote between the read version and the commit version of the first one. | `SIM` | Every run. Transactions have read keys, read ranges, sets and clears from 4 keys. |
| `X2` | `read_version_order`. A read version is at or above the commit version of every transaction that was acknowledged before the client asked for it, in any epoch. | `SIM` | Every run. |
| `X3` | `read_value`. A read of a key at a read version returns the write of the acknowledged transaction with the highest commit version at or below the read version, or a later write at or below the read version whose client received no answer. | `SIM` | Every run. |
| `X4` | `own_writes`. Before the commit, the reads of a transaction return its sets and its clears. After a failed commit, `on_error/2` leaves a transaction without a read version and without a mutation. | `SIM` | Every run. |
| `X5` | `X1` to `X4`, `G1`, `G2`, `C1` and the options `reverse` and `limit` of `C2` hold with keys of 4 types: a number, an atom, a tuple and a binary. They hold when two clients use two forms of one key that are equal and not identical: `1` and `1.0`, and `{k, 3}` and `{k, 3.0}`. | `SIM` | 210 runs with mixed keys: 40 without faults, 70 with faults, 40 with ClusterControllers and faults, 30 with the pop and kills of StorageServers, and 30 with a peace cluster and faults. These runs have no mapped range read, no `redwall:get_key/2` and no watch. No key is a list or a map. |
| `X6` | The client history of a run is strictly serializable: the transactions have an order in which each transaction reads the values of the transactions before it, and a transaction that ended before another one started is before it. The check uses only what the clients observed, and not the commit versions. | `PORC` | 360 runs in 8 tests of [dst/redwall_dst_porcupine_tests.erl](dst/redwall_dst_porcupine_tests.erl), with 12,838 operations: 40 seeds without faults, 100 with faults, 40 with ClusterControllers and faults, 30 with the pop and kills of StorageServers, 40 with mixed keys and faults, 40 with a peace cluster and faults, 40 with watches and faults, and 30 with mixed keys and the pop. The history has the transactions on the 4 keys of the workload. A transaction without a known result is in the order only if a client read its value or if it cleared a key. |

### StorageServers: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `G1` | `storage_rollback`. When a StorageServer has a version of an epoch, it has no write between the end version of an earlier epoch and the first version of the epoch after that one. | `SIM` | Every run. |
| `G2` | `replicas_agree`. Two StorageServers have the same writes for each key, for the versions that both have applied and that are above the oldest version of both. | `SIM` | Every run with 2 or 3 StorageServers. |
| `G3` | `X1` to `X4` hold when a StorageServer is killed and a new StorageServer without data copies from a peer or reads the log. | `SIM` | The runs with faults and the runs with the pop. |

### Reads of the client: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `C1` | `select_agrees`. `redwall:select/4` returns the result of the match spec on the result of `get_range/3` at the same read version. | `SIM` | Every run. |
| `C2` | `range_options_agree`. `redwall:get_range/4` with the options `reverse` and `limit` returns the rows of `get_range/3` at the same read version in the reverse order, the first rows, and the last rows. `redwall:get_key/2` returns the first key at or above the start of the range, and the last key below its end. | `SIM` | Every run. |
| `C3` | `mapped_range_agrees`. `redwall:get_mapped_range/4` returns the rows of `get_range/3` at the same read version, each with the result of its second read. | `SIM` | Every run. A mapper with a second read of a key, and a mapper with a second read of a prefix. |
| `C4` | `X3` and `C1` to `C3` hold for a client that reads the ETS tables of a StorageServer on its node, and for a client that reads by message. | `SIM` | 1 client in 2 reads the ETS tables. |

### Watches: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `H1` | `watch_change`. The read of a client after a ready future returns another value than the value of the watch. | `SIM` | 210 runs with watches: 60 without faults and 150 with faults. 2 keys. No client clears a key of a watch, and the value of a key never changes back. |
| `H2` | `watch_error`. The future of a watch whose commit failed has the error of the commit, and the future of a watch whose commit succeeded has no error. | `SIM` | The same. |
| `H3` | `watch_fires`. At the end of a run, the future of every watch is ready if an acknowledged transaction set the key of the watch at a commit version above the version of the watch. | `SIM` | The 60 runs without faults and without a time limit of a watch. |
| `H4` | `watch_change` with clears of the keys of the watches. When the future of a watch is ready, the read after it returns another value than the value of the watch, or the key has another value than the value of the watch at a version between the version of the watch and the read version of the read. The value at a version is the value after the last change at or below the version that no recovery discarded. | `SIM` | 220 runs in which 1 watch client in 3 clears the key: 50 without faults, 70 with faults, and the 100 runs of `H6`. The value of a key changed back in each of the 4 tests. `H2` holds in these runs. |
| `H5` | `watch_fires` with clears. At the end of a run, each StorageServer that runs has applied the changes of the watch clients, and the future of each watch is ready if the value of the key in a StorageServer that runs differs from the value of the watch. | `SIM` | The 50 runs with clears, without faults and without a time limit of a watch. In these 50 runs no watch stayed not ready after a change and a change back. |
| `H6` | After the faults stop, a watch becomes ready. The harness starts 4 watch clients and 2 clients that set the keys after the last operation. Each of the 6 clients commits (`late_commit`), and `H5` holds for the 4 late watches. | `SIM` | 100 runs with ClusterControllers and faults: 50 without the pop and 50 with the pop, for at most 20,000 steps after the last operation. A StorageServer of these runs has no time limit of a watch. The late changes are sets. |

### ClusterController: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `E1` | `one_leader`. No two ClusterController processes are the leader of one term. | `SIM` | The 350 runs with ClusterControllers, and the 200 runs of `P3`. |
| `E2` | After the faults stop, a run ends with one leader, which is the ClusterController of the coordinated state (`leader`, `stale_leader`). | `SIM` | The runs with ClusterControllers, for at most 20,000 steps after the last operation. |
| `E3` | At that time the leader has the current epoch of the coordinated state, and the Sequencer and the Proxy of the epoch run (`epoch`). Each worker that runs has the information of the leader about the database (`worker_db_info`). | `SIM` | The same. |

### redwall on a peace cluster: the code

| Id | Property | Method | Bound |
|---|---|---|---|
| `P1` | The invariants of `K1` to `K8`, `X1` to `X4`, `G1`, `G2` and `C1` to `C3` hold when a peace cluster of 3 servers stores the coordinated state. | `SIM` | 450 runs: 50 without faults, 200 with faults, and 200 with ClusterControllers. The harness kills the node of a coordinator, and in 1 kill operation of 4 the node of every coordinator. A killed node loses the data that was not synced. |
| `P2` | `coordinators_agree`. No two servers have two states of the coordinated state for one index of the log of peace. | `SIM` | The same. The invariant has not failed in a run, also not with a planted defect. |
| `P3` | `E1` to `E3` hold on a peace cluster. | `SIM` | The 200 runs with ClusterControllers of `P1`. |

### Simulation harness

| Id | Property | Method | Bound |
|---|---|---|---|
| `N1` | The same seed gives the same trace, the trace replays, and the audit of eta reports nothing. | `SIM` | 94 seeds in 7 tests, 3 runs each: 20 seeds of the basic runs, 18 with ClusterControllers, 12 with a peace cluster, 12 with the pop, 12 with watches, 12 with mixed keys and 8 with clears of the keys of the watches and with late watches. |

### Other unit tests

| Id | Property | Method | Bound |
|---|---|---|---|
| `U1` | The application starts, and `redwall:open/0` returns a database, on two Erlang nodes. | `UNIT` | Two tests in `test/redwall_nodes_tests.erl`. No test cuts the connection between the two nodes. |
| `U2` | One LogServer keeps the order of the LSNs, the Known Committed Version, repeated records and the lock. | `UNIT` | |
| `U3` | The Proxy handles commits in sequence, 50 commits at the same time, a recovery after commits, a LogServer down and the Sequencer down. | `UNIT` | |
| `U4` | `redwall_status:status/1` reports the epoch, the ClusterController, the storage engine, the counts of coordinators, LogServers, workers and StorageServers, and these problems: no coordinated state, a coordinator down, no epoch, no ClusterController, too few workers, a StorageServer down and a LogServer down. With 2 nodes, the fault tolerance in nodes follows from the replication degree, the StorageServers and `min_workers`. | `UNIT` | Two tests in `test/redwall_status_tests.erl`: one Erlang node, and two Erlang nodes on one computer. No test has the problems `epoch_not_recruited`, `storage_copying` or `storage_behind`, and no test has more than 2 nodes. The test `engine` of `test/redwall_storage_tests.erl` has the problem `storage_engines_differ`. |
| `U5` | The second application of a value of the coordinated state gives the same reply. It does not change the state for the values `lock`, `write`, `elect`, `storage` and `forget_storage`. For the value `reset`, it raises the generation again and changes nothing else. | `UNIT` | `values_are_idempotent_test`, `elect_is_compare_and_set_test` and `second_application_test` in `test/redwall_cstate_tests.erl`, on the function `redwall_cstate:apply/3`. |

## Simulation runs

The entries with the method `SIM` and the bound "every run" refer to the
39 tests of [dst/redwall_dst_tests.erl](dst/redwall_dst_tests.erl) without
a planted defect.

| Runs | Seeds |
|---|---|
| No faults | 200 |
| Kills, partitions, lost and delayed messages | 500 |
| ClusterControllers: no faults, faults, and cuts between ClusterController nodes | 50, 200 and 100 |
| The pop: no faults, faults, and kills of StorageServers only | 30, 100 and 60 |
| Watches: no faults, and faults | 60 and 150 |
| Watches with clears: no faults, and faults | 50 and 70 |
| Late watches with ClusterControllers and faults: without the pop, and with the pop | 50 and 50 |
| Mixed keys: no faults, and faults | 40 and 70 |
| Mixed keys: ClusterControllers with faults, the pop with kills of StorageServers, and a peace cluster with faults | 40, 30 and 30 |
| A peace cluster: no faults, faults, and ClusterControllers | 50, 200 and 200 |
| Determinism | 94 |

Every run has:

- 80 operations
- 3 LogServers for each epoch, with a replication degree of 2
- 1, 2 or 3 StorageServers
- a network that loses 0.1% of the messages between nodes and delays 20%
  of them, in the runs with faults
- at most 1 killed LogServer for each epoch

## Evidence that the checks can fail

| Method | Changes that the method must reject | Count |
|---|---|---|
| `TLC` | Configs with a planted bug. Each config produces a counterexample. | 13: 7 of Recovery, 3 of Resolver, 3 of Watch |
| `TLC` | Reachability configs. Each config reaches a state that the properties depend on. | 16 |
| `SIM` | Planted defects in the code or in the harness. Each one is found by an invariant or by a check at the end of a run. `watch_no_trigger` and `watch_no_verify` are also found in the runs with clears, and `watch_no_trigger` in the runs of `H6`. | 27 |
| `SIM` | Defects that were planted in the source on 2026-11-23 and are not in the repository | 6 |
| `PORC` | Planted defects of the simulation whose history Porcupine rejects: `no_check`, `no_range_check`, `no_batch_check`, `no_write_range`, `no_equal_keys`, `no_confirm`, `no_report`, `no_clear` and `no_copy_rows`. For each one, Porcupine accepts the history of the same seed without the defect. Porcupine rejects `no_copy_rows` at seed 10, where no invariant of the simulation is violated. | 9 |
| `TRACE` | Planted defects of the simulation whose trace TLC rejects: `ack_replicas`, `no_cas`, `no_report`, `no_push_order`, `no_copy`, `no_confirm` and `no_rollback`. For each one, TLC accepts the trace of the same seed without the defect. TLC rejects `ack_replicas` and `no_confirm` at seed 1, where no invariant of the simulation is violated. | 7 |

Entries without a planted defect that is specific to them:

- `P2`. No planted defect violates `coordinators_agree`.
- `K2`, `K3`, `K4` and `K8`. No planted defect is recorded as found by
  `unique_versions`, `no_loss`, `body_available` or `popped_stored`.
- `C2`, `C3`, `X4` and `H2`. The planted defects of `mapped_range_agrees`
  and `own_writes` are the 6 defects that are not in the repository. No
  planted defect is recorded for `range_options_agree` or `watch_error`.
- Every `UNIT` entry.

## Not verified

- **Strict serializability outside the bounds of `X6`.** The
  transactions of the watches are in no history. Porcupine rejected no
  history with the planted defect `no_rollback` in 300 seeds, and none
  with `no_copy` or `ack_replicas` in 120 seeds. The invariants of the
  simulation find these three defects. No check examines a history of
  real nodes.
- **The code against the models, outside the bounds of `V1`, `V2` and
  `V3`.** Trace validation covers `Recovery.tla`. It does not cover
  `Resolver.tla` or `Watch.tla`. Within `Recovery.tla`:
  - It does not compare the content of the body of a record, on the
    LogServers of its epoch or on the new LogServers after a recovery.
    It compares which LogServers have a body.
  - It does not validate the values of the LSNs or the limit on the
    versions in flight.
  - A `LockLog` action is placed at the write of the Sequencer, and not
    at the step in which the LogServer was stopped.
  - It does not validate that the LogServer that answers a confirmation
    is not stopped. The trace does not have the name of that LogServer.
  - It does not have the time at which a client sent a request for a
    read version. One confirmation is one request of the model.
  - It does not follow a StorageServer after a copy from a peer or after
    a reset of the database, and it has no run with the pop.
  - It does not compare the keys and values of a StorageServer.
- **The composition of the models.** Resolver assumes that a recovery
  keeps every record. Recovery treats every transaction as admitted. Watch
  assumes `R7` and `R8`.
- **Values in the model of the recovery.** `R7` and `R8` are statements
  about sets of records, and not about values that a client reads.
- **Liveness, other than `W2`.** `E2`, `E3` and `H3` are tests of the end
  of a run.
- **The election of a ClusterController, in a model.** `SIM` covers it.
- **The pop, in a model.** `SIM` covers it.
- **More than 3 epochs, more than 3 LogServers or a replication degree
  other than 2**, in a model or in the simulation.
- **The loss of 2 LogServers of one epoch, and the reset of the
  database.** The simulation kills at most 1 LogServer of each epoch.
- **The stop of every node.**
- **A storage engine other than `ets`.** A cluster is configured with a
  storage engine, a module with the behaviour `redwall_storage_engine`.
  redwall has one engine, `ets`, and every entry is a statement about a
  cluster with that engine. One unit test, `engine` in
  `test/redwall_storage_tests.erl`, runs a StorageServer with an engine
  of the test. No simulation test and no model has another engine, and
  no check covers an engine that keeps data after a stop.
- **The replacement of a coordinator whose data is lost, and a change of
  the coordinators.** redwall has no function for them.
- **A value of the coordinated state that is applied twice, in the
  simulation or on a peace cluster.** `U5` covers the function
  `redwall_cstate:apply/3`. No simulation run and no test with a peace
  cluster proposes a value twice on purpose.
- **`redwall:transactional/2` in the simulation.** Unit tests cover it.
- **The errors of a mapper in the simulation.** Unit tests cover them.
- **A watch that becomes ready while faults occur.** `H3` and `H5` have
  no faults, and `H6` starts its watches after the faults stop. In the
  runs with faults, the simulation checks only a future that is ready.
- **A late change that is a clear, and late watches with a time limit of
  a watch in the StorageServer.** See `H6`.
- **A record that `H4` treats as kept and that a recovery discarded.**
  The harness can accept such a record for an ended epoch with an end
  version of 0, when the recovery lost its write of the coordinated
  state. No run was constructed for this case.
- **Mixed keys with watches, and keys that are lists or maps.** See
  `X5`.
- **The original form of the Resolver defect with equal keys.** The
  defect of commit `7b1ee91` stopped the process of an epoch. The planted
  defect `no_equal_keys` misses a conflict and does not stop the process.
  With the original form, 30 runs with mixed keys violated no invariant,
  and the number of acknowledged transactions fell from about 1,130 to
  290.
- **`batch_depth` above 1, with a planted defect of its own.** The runs
  use it. The planted defect `no_push_order` covers the order of two
  records of one Proxy that are on the way at the same time. The defect
  is not specific to `batch_depth` above 1: with `batch_depth` of 1 for
  every Proxy, the simulation finds it at the same seed, because a Proxy
  also closes a batch at the end of the time of the batch.
- **A process that stops during a step of the recovery**, in the unit
  tests. The simulation kills the process of an epoch during its recovery.
- **Real nodes under faults.** redwall has no Jepsen test. Two unit tests
  use two Erlang nodes on one computer without faults.
- **The functions of erlfdb that redwall does not have.** See
  [docs/limitations.md](docs/limitations.md).
- **Performance.** The README holds measurements, not verified bounds.
- **etx.** etx depends on redwall and is a separate project.

## Limitations

- The bounds of the models are small: 3 LogServers, at most 3 epochs and
  7 versions in Recovery; 2 keys, 3 transactions and at most 3 epochs in
  Resolver; 1 key, 1 watch and 6 LSNs in Watch.
- `Recovery.tla` has no clock. An LSN is the previous LSN plus 1.
- `Ack` in `Recovery.tla` joins the report to the Sequencer and the answer
  to the client in one step.
- `Resolver.tla` has a key range as a set of keys. `T3` compares the
  ordered set of bounds of the code with one version for each key.
- In the simulation, a read that returns no value is accepted when a
  transaction without a known result cleared the key at any version.
- 59 of the 69 simulation tests replace the peace cluster with one
  process.
- In the runs with a peace cluster, a recovery waits until the first read
  of the cluster has succeeded. This wait is in the harness.

## Reproduction

| Method | Command | Time | Runs in GitHub Actions | Last recorded run |
|---|---|---|---|---|
| `UNIT` | `make test` | 40 seconds | Yes, on each push | |
| `SIM` | `make dst` | about 6 minutes | Yes, on each push | |
| `TRACE` | `make trace`. The command needs Java. | 6 minutes | No | 2026-11-26: 11 tests passed in 5 min 57 s. |
| `PORC` | `make porcupine`. The command needs Go. | 3 to 7 minutes | No | 2026-11-26: 17 tests passed in 6 min 26 s, at the same time as `make dst`. 14 of them passed in 2 min 36 s alone on the same day. 17 tests passed in 3 min 35 s after the change of the published state of `redwall_epoch` and `redwall_storage`. |
| `TLC`, quick configs | `make tlc` | about 6 minutes | Yes, on each change in `formal/` | 2026-11-22 for Recovery and Resolver, 2026-11-24 for Watch |
| `TLC`, Recovery with 3 epochs | `make tlc-long` | about 4 hours | On a manual start | Commit `1b56582` of 2026-11-22. `Recovery.tla` has changed only in comments since that commit. |

The 3-epoch bounds of `R1` to `R5`, `R7` and `R8` rest on the last row.
[formal/README.md](formal/README.md), "When to run TLC", gives the rule for
the next run.

## Maintenance

1. A change that adds, removes or weakens a check updates this file in the
   same commit.
2. A change that makes an entry false removes the entry or narrows its
   bound in the same commit.
3. An entry is added only when a check that can fail supports it.
4. A run of a check that GitHub Actions does not run on each push updates
   the "Last recorded run" column in "Reproduction".
5. A change of the version of peace, or of the options with which redwall
   uses peace, compares the "Dependencies" section with the `VERIFIED.md`
   of peace again.
