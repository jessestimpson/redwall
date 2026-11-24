# Verification

| Level | What it checks | Command | Time |
|---|---|---|---|
| Unit tests | The coordinated state machine, alone and on a peace cluster of three servers on one node. One LogServer: the order of the LSNs, the KCV, repeated records and the lock. The recovery on a peace cluster: the first epoch, a second epoch with records to copy, 1 and 2 LogServers down, and two recoveries at the same time. The Proxy: commits in sequence, 50 commits at the same time, a recovery after commits, a LogServer down and the Sequencer down | `make test` | 3 seconds |
| Simulation | The LogServer, the recovery, the Sequencer, the Resolver, the Proxy and the StorageServer under the scheduler and the simulated network of eta, with kills, partitions, and lost and delayed messages | `make dst` | about 3 minutes |
| Model checking | The logging protocol, the recovery, the read versions, the StorageServer and the Resolver, with 2 epochs | `make tlc` | about 5 minutes |
| Model checking | The logging protocol, the recovery and the StorageServer, with 3 epochs | `make tlc-long` | about 4 hours |

The models have 10 planted bugs. TLC finds a counterexample for each. See
[formal/README.md](../formal/README.md).

## Simulation

`make dst` runs the system under the scheduler and the simulated network
of eta. The harness is [dst/redwall_dst.erl](../dst/redwall_dst.erl).

The simulated system has one process for the coordinated state
([redwall_cstate_local](../src/redwall_cstate_local.erl)), or a peace
cluster of 3 servers in the tests with the name `peace_`. It has 3 LogServers for each epoch and a replication degree of 2.
A run has 1, 2 or 3 StorageServers. The number depends on the seed. Each
node with the process of an epoch, a LogServer or a StorageServer has
the process of each LogServer from the coordinated state. The process of
an epoch starts each LogServer on its node through `erpc`. Two simulated
nodes connect with the first message between them. The coordinated
state, each StorageServer, each process of an epoch and each LogServer
run on a
simulated node of their own. The network loses 0.1% of the messages
between nodes and delays 20% of them.

The workload has these operations:

- start a Sequencer and, after its recovery, a Proxy;
- run a transaction: get a read version from a Proxy of any epoch, read
  the read keys from the StorageServers at the read version, and commit
  through a Proxy of any epoch, with read keys and write keys from 4
  keys. One client in three waits 20 ms to 100 ms before the commit, so
  that a recovery can end the epoch of the read version;
- kill a LogServer of the current epoch, at most 1 for each epoch;
- kill the process of an epoch, also during its recovery. The workload
  has three operations for this kill, from the time when the Sequencer,
  the Proxy and the Resolver were three processes;
- kill one StorageServer, and start a new StorageServer without data,
  which reads every record from the LogServers of every epoch. The
  clients read from the other StorageServers in that time. 1 client in
  2 runs on the node of a StorageServer and reads the ETS table of that
  StorageServer. The other clients read by message;
- cut the connection between two nodes, and remove every cut.

In the runs with ClusterControllers, the harness starts no process of an
epoch:

- A run has 8 worker nodes: 4 of the class `stateless`, of which 3 have
  a candidate for the ClusterController, and 4 of the class `log`.
- The ClusterController recruits the process of each epoch.
- The first operation of the list above asks the ClusterController for a
  recovery, and starts a new worker node for each killed worker node.
- A kill of a LogServer or of the process of an epoch kills the worker
  node of the role, with every role on the node and with the
  candidate of the node.
- The harness kills a worker node only if no epoch then has 2 killed
  LogServers.
- After the last operation, the harness removes every cut, replaces
  every killed worker node, and stops the loss of messages. The run
  continues until the checks at the end of a run hold, for at most
  20,000 steps.

In the runs with the pop (the tests with the name `pop_`), the
LogServers discard records:

- The runs have ClusterControllers. The harness records the
  StorageServers of the places 1 and 2 in the list of the coordinated
  state. A run with 1 StorageServer has no pop.
- The StorageServer of the place 3 is not in the list, so the LogServers
  can discard a record that it has not stored.
- A StorageServer of the place 3 that starts after a kill finds no peer
  at its first question, and reads the log from the start. A LogServer
  answers that it has discarded the first records, and the StorageServer
  then copies from a peer.
- The harness kills a StorageServer only if another StorageServer runs
  with a version above 0 that is at or above the version of the first
  one. The discarded records are lost when every StorageServer with
  them stops.
- The planted defect `pop_no_limit`: the pop version is the highest
  version of a StorageServer, and a LogServer discards the records
  without the limit of its Known Committed Version.

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
| `body_available` | The body of every acknowledged record is on a LogServer of its epoch that runs, or the record is at or below the popped version of such a LogServer |
| `copied` | Every LogServer of a new epoch has the body of every acknowledged record above the previous epoch's end version and above the popped version of the LogServer |
| `popped_stored` | A StorageServer that runs has a version at or above the popped version of every LogServer that runs |
| `no_conflict` | No acknowledged transaction read a key that another acknowledged transaction wrote between the read version and the commit version of the first one |
| `read_version_order` | A read version is at or above the commit version of every transaction that was acknowledged before the client asked for it, in any epoch |
| `read_value` | A read of a key at a read version returns the write of the acknowledged transaction with the highest commit version at or below the read version, or a later write at or below the read version whose client received no answer |
| `storage_rollback` | When a StorageServer has a version of an epoch, it has no write between the end version of an earlier epoch and the first version of the epoch after that one |
| `replicas_agree` | Two StorageServers have the same writes for each key, for the versions that both have applied and that are above the oldest version of both |
| `select_agrees` | A `redwall:select/4` of a client returns the result of the match spec on the result of `get_range/3` at the same read version |
| `mapped_range_agrees` | A `redwall:get_mapped_range/4` of a client returns the rows of `get_range/3` at the same read version, each with the result of its second read. The client uses a mapper with a second read of a key, and a mapper with a second read of a prefix |
| `own_writes` | Before the commit, the reads of a transaction return its sets and its clears, and `get_mapped_range/4` raises 2039. After a failed commit, `on_error/2` leaves a transaction without a read version and without a mutation |
| `coordinators_agree` | In a run with a peace cluster, no two servers have two states of the coordinated state for one index of the log of peace |
| `kcv_below_dv` | In each epoch, the KCV of every LogServer is at or below the DV of every LogServer |
| `chain` | In each LogServer, the previous LSN of each record is the LSN of the record before it |

Results of `make dst` on 2026-11-22:

| Test | Runs | Result |
|---|---|---|
| The same seed gives the same trace, the trace replays, and the audit of eta reports nothing | 20 seeds, 3 runs each | Pass |
| The same for the runs with ClusterControllers: without faults, with faults, and with cuts between ClusterController nodes | 18 seeds, 3 runs each | Pass |
| The same for the runs with a peace cluster: with faults, and with ClusterControllers and faults | 12 seeds, 3 runs each | Pass |
| No kills, no partitions, no lost or delayed messages | 200 seeds in 4 tests, 80 operations each, 399 StorageServers at the start of the runs | No violation. 8,883 acknowledged transactions, of which 2,289 with a read range, 1,100 with a clear of keys and 546 in a batch with another acknowledged transaction, 3,446 rejected for a conflict, 391 too old, 12,683 transactions with reads, of which 4,995 with direct reads on the node of a StorageServer and 2,517 by message only, 7,610 selects, 12 reads below the MVCC window of the StorageServer, 1,388 recoveries, 89 failed recoveries |
| Kills, partitions, lost and delayed messages | 500 seeds in 5 tests, 80 operations each, 999 StorageServers at the start of the runs | No violation. 3,934 acknowledged transactions, of which 839 with a read range, 497 with a clear of keys and 371 in a batch with another acknowledged transaction, 3,756 rejected for a conflict, 1,470 too old, 13,757 transactions with reads, of which 2,475 after the start of a new StorageServer, 237 while one of several StorageServers was not alive, 5,646 with direct reads on the node of a StorageServer and 2,781 by message only, 8,326 selects, 164 reads below the MVCC window of the StorageServer, 1,145 rollbacks of StorageServers, 3,666 recoveries, 1,397 failed recoveries, 1,641 kills, of which 210 of a StorageServer, 285 partitions |
| ClusterControllers, no kills, no partitions, no lost or delayed messages | 50 seeds in 1 test, 80 operations each | No violation, and each run ends with a leader and an epoch. 1,708 acknowledged transactions, 328 recoveries, 2 failed recoveries, 50 elections |
| ClusterControllers, kills of worker nodes, partitions, lost and delayed messages | 200 seeds in 2 tests, 80 operations each | No violation, and each run ends with a leader and an epoch. 1,267 acknowledged transactions, 1,256 recoveries, 165 failed recoveries, 541 kills of a worker node, of which 306 with a ClusterController process, 308 elections, 244 recoveries after the failure of a role, 368 rollbacks of StorageServers, 96 partitions |
| ClusterControllers, kills of worker nodes, cuts between nodes with a ClusterController process only, lost and delayed messages | 100 seeds in 1 test, 80 operations each | No violation, and each run ends with a leader and an epoch. 16 runs with two leaders at the same time, 172 elections, 2 epochs that the leader found superseded, 619 acknowledged transactions, 641 recoveries, 264 kills of a worker node, 30 cuts |
| Planted defect: the coordinated state accepts the proposal of a candidate for the ClusterController with any term | Seeds from 1, runs with cuts between ClusterController nodes | Found at seed 1, `one_leader`: two processes are the leader of term 1 |
| Planted defect: a ClusterController does not recruit after the failure of a role | Seeds from 1, runs with cuts between ClusterController nodes | Found at seed 2, `epoch`: the run ends with an epoch whose Sequencer or Proxy does not run |
| Planted defect: a ClusterController stays the leader when the coordinated state has another term | Seeds from 1, runs with cuts between ClusterController nodes | Found at seed 1, `stale_leader` |
| Planted defect: the Proxy answers after 2 of 3 LogServers | Seeds from 1 | Found at seed 9, `kcv_below_dv`. In 120 seeds the defect also violates `durable`, the counterexample of `RecoveryAckReplicasOnly` |
| Planted defect: a recovery copies no record | Seeds from 1 | Found at seed 1, `copied` |
| Planted defect: the coordinated state accepts any write | Seeds from 1 | Found at seed 3, `kcv_below_dv` |
| Planted defect: the Resolver admits every transaction | Seeds from 1 | Found at seed 1, `no_conflict` |
| Planted defect: the Resolver does not check the read ranges | Seeds from 1 | Found at seed 1, `no_conflict` |
| Planted defect: the Resolver does not check a transaction against the earlier transactions of its batch | Seeds from 1 | Found at seed 1, `no_conflict` between two transactions of one batch |
| Planted defect: the Resolver does not record the write ranges | Seeds from 1 | Found at seed 1, `no_conflict`: a transaction read a key that another transaction cleared in a range |
| Planted defect: the first commit version of an epoch is the Recovery Version plus 1 | Seeds from 1 | Found at seed 2, `no_conflict` |
| Planted defect: the process of an epoch does not raise its read version after a commit | Seeds from 1 | Found at seed 1, `read_version_order` |
| Planted defect: a Proxy gives a read version without the confirmation that its epoch is live | Seeds from 1 | Found at seed 1, `read_version_order` |
| Planted defect: the StorageServer keeps the writes above the end version of an epoch | Seeds from 1 | Found at seed 8, `storage_rollback` |
| Planted defect: the StorageServer answers a read below its oldest version | Seeds from 1 | Found at seed 3, `select_agrees`: a select and a read of one range differ, because the StorageServer removed versions between the two |
| Planted defect: the StorageServer does not apply a clear | Seeds from 1 | Found at seed 3, `read_value`: a read returned a value for a key that a later acknowledged transaction cleared |
| Planted defect: the second StorageServer of a run does not apply a clear | Seeds from 1, runs without faults | Found at seed 2, `replicas_agree`: the first StorageServer has a clear of a key at a version, and the second one has none |
| Planted defect: the StorageServer answers a read at a version that it has not applied | Seeds from 1 | Found at seed 1, `read_value` |

Results of the 5 tests with the pop in a run of `make dst` on
2026-11-24. The 46 tests of that run pass in 219 s. The test
`pop_determinism_test_` was added after that run and passes in 18 s:

| Test | Runs | Result |
|---|---|---|
| Pop, no kills, no partitions, no lost or delayed messages | 30 seeds in 1 test, 80 operations each | No violation. 795 acknowledged transactions. 267 LogServers that ran at the end of a run had discarded records |
| Pop, kills of worker nodes and of StorageServers, partitions, lost and delayed messages | 100 seeds in 1 test | No violation, and each run ends with a leader and an epoch. 383 acknowledged transactions, 257 kills of a worker node, 23 kills of a StorageServer, 341 LogServers with discarded records |
| Pop, kills of StorageServers only | 60 seeds in 1 test | No violation. 1,577 acknowledged transactions, 150 kills of a StorageServer, 594 LogServers with discarded records, 139 copies from another StorageServer. A StorageServer received the answer `popped` above its version 25 times |
| The same seed gives the same trace, the trace replays, and the audit of eta reports nothing, for the runs with the pop | 12 seeds, 3 runs each | Pass |
| Planted defect `pop_no_limit` | Seeds from 1 | Found at seed 2, `epoch`: a recovery finds no LogServer with a record that it must copy, and the run ends without an epoch. The same seed passes without the defect |

A run of `make dst` on 2026-11-23, with the invariants
`mapped_range_agrees` and `own_writes`: the 32 tests pass in 130 s
without a violation. The test `controller_determinism_test_` was added
after that run and passes in 22 s. The checks of the client in that run:

| Check | No faults, 4 tests | Faults, 5 tests | ClusterControllers, 4 tests |
|---|---|---|---|
| Selects compared with `get_range/3`, each also with a limit | 5,884 | 6,356 | 4,672 |
| Mapped range reads compared with `get_range/3`, each also with a limit | 7,627 | 7,528 | 5,600 |
| Read ranges of a transaction that were read with `get_mapped_range/4` only | 881 | 831 | 615 |
| Transactions whose reads of their own writes were checked before the commit | 12,582 | 11,530 | 8,716 |
| Calls of `on_error/2` after a failed commit | 6,102 | 11,054 | 6,937 |

The process of an epoch has `batch_depth` 1 in 1 epoch of 2, and 3 in
the others.

On 2026-11-23, six defects were planted in the source one at a time, to
check that the invariants of the client fail. The defects are not part
of the repository. Each one was found at seed 1 of the runs with faults:

| Planted defect | Invariant that failed |
|---|---|
| The second read of a mapped range read uses another version than the read version | `mapped_range_agrees` |
| `get_mapped_range/5` adds no read conflict range | `no_conflict` |
| The StorageServer returns one row too many for a limit of a mapped range read | `mapped_range_agrees` |
| `get_mapped_range/5` does not raise 2039 after a write of the transaction | `own_writes` |
| `select/5` returns a stored row of a key range that the transaction cleared | `own_writes` |
| `select/5` sends the limit to the StorageServer when the transaction has written a key of the range | `own_writes` |

### Runs with a peace cluster

In the runs above, the coordinated state is one `redwall_cstate_local`
process. The tests with the name `peace_` run a peace cluster of 3
servers in its place, under the scheduler and the network of eta:

- peace is compiled with the parse transform of eta in the `dst`
  profile (`overrides` in [rebar.config](../rebar.config)).
- Each server is on a simulated node of its own. The cluster has
  majority quorums.
- Each server is a coordinator with the configuration of
  [redwall_coordinator](../src/redwall_coordinator.erl): it keeps its data
  in the write-ahead log of peace, on the simulated disk of eta.
- The harness kills the node of a coordinator, and in 1 kill operation
  of 4 the node of every coordinator. A kill is also a crash of the
  disk of the node: the node loses the data that was not synced. The
  next `recover` operation starts each killed coordinator again, with
  its data.
- The invariant `coordinators_agree` holds when no two servers have two
  states of the coordinated state for one index of the log of peace.
- A partition can cut a server off, and can leave the cluster without a
  majority until the next heal.
- [dst/redwall_dst_machine.erl](../dst/redwall_dst_machine.erl) is the
  machine of the servers. It records the decided state for the
  invariants, and plants the defects `no_cas` and `no_elect_cas`.
- A step injects an operation with the probability 0.004 in the place
  of 0.015, because the heartbeats of peace add steps for each unit of
  the virtual clock.

Results of `make dst` on 2026-11-23. The 42 tests pass in 260 s:

| Test | Runs | Result |
|---|---|---|
| peace cluster, no faults | 50 seeds in 1 test, 80 operations each | No violation. 1,991 acknowledged transactions, 346 recoveries, 16 failed recoveries |
| peace cluster, kills, partitions, lost and delayed messages | 200 seeds in 2 tests | No violation. 866 acknowledged transactions, 1,261 recoveries, 735 failed recoveries, 180 kills of a coordinator, of which 30 times every coordinator, 160 starts of a killed coordinator |
| peace cluster, ClusterControllers, kills of worker nodes, partitions | 100 seeds in 1 test | No violation, and each run ends with a leader and an epoch. 270 acknowledged transactions, 375 recoveries, 134 elections, 96 kills of a coordinator, of which 20 times every coordinator |
| peace cluster, ClusterControllers, cuts between ClusterController nodes | 100 seeds in 1 test | No violation, and each run ends with a leader and an epoch. 274 acknowledged transactions, 176 elections, 94 kills of a coordinator, of which 20 times every coordinator |
| Planted defect `no_cas` in the machine of the peace cluster | Seeds from 1 | Found at seed 4, `replicas_agree`. The same seed passes without the defect |
| Planted defect `no_elect_cas` in the machine of the peace cluster | Seeds from 1 | Found at seed 1, `one_leader` |
| Planted defect: a coordinator never syncs its files | Seeds from 1, runs with ClusterControllers | Found at seed 20: the run ends without an epoch. The same seed passes without the defect. In 60 seeds, 10 runs ended without an epoch or without a leader. The coordinators synced 2 times, against 44,646 times without the defect |

The failed recoveries of the runs with a peace cluster were examined on
2026-11-23, in 100 seeds with faults, before the coordinators had a
disk:

| Reason of a failed recovery | Count | Cause |
|---|---|---|
| `count` | 252 | Another recovery raised the recovery count first. The harness starts recoveries at times that the seed selects, so recoveries overlap |
| `recovery_transaction` | 60 | The first commit of the new epoch found a LogServer that a later recovery had stopped, or a killed node |
| `locked`, `noproc`, `server_down`, `write_unknown` | 22 | A LogServer was stopped or killed, or a call to the peace cluster ended without an answer |
| `no_leader` | 0 | The peace cluster had a leader for every recovery |

- The same 100 seeds without the peace cluster and with the same rate
  of operations have 137 failures with the reason `count`. A call to
  the peace cluster takes several messages, each of which the network
  can delay, so two recoveries overlap more often. This cause was
  concluded from the counts and was not measured.
- Before this examination, the first recovery of a run failed in 99
  runs of 100 with `no_leader` or `noproc`, because the first operation
  starts the servers of the cluster and the first recovery at the same
  time. A recovery now waits until the first read of the cluster has
  succeeded. This wait is in the harness. In a deployment, the
  ClusterController recruits again after a failed recovery.

Limitations of the simulation:

- The invariant `coordinators_agree` has not failed in a run, also not
  with the planted defect of a coordinator that never syncs. peace does
  not let a server that lost its data act as the member that it was, so
  that defect costs the availability of the coordinated state, and
  `check_final/2` finds it in the runs with ClusterControllers.
- The simulation does not replace a coordinator whose data is lost, and
  does not change the members of the peace cluster.
- A run with a peace cluster and faults acknowledges 4.3 transactions
  on average, against 5.0 without the cluster.

- The application start (`redwall_sup`) and `redwall:open/0` are not
  in the simulation. Unit tests on two nodes cover them.
- `redwall:transactional/2` is not in the simulation. A client calls
  `on_error/2` after a failed commit, and does not run the transaction
  again.
- The errors of a mapper are not in the simulation. Unit tests cover
  them.
- No planted defect is specific to `batch_depth` above 1.
- Temporary processes of a worker and of a ClusterController can run
  after the end of a run, until the tables of eta are deleted. They
  write an error report. Two runs of the suite with the same source
  differed by 1 in two counters of the statistics. The traces of the
  runs are the same: the determinism tests compare them.

`make dst` runs each test in an Erlang node of its own, with several
nodes at the same time ([dst/run.sh](../dst/run.sh)). The runs without faults
are 4 tests of 50 seeds, and the runs with faults are 5 tests of 100
seeds. `dst/run.sh` prints the sums of the statistics of these tests.

| Command | Nodes at the same time | Time |
|---|---|---|
| `make dst-serial` | 1 | 10 min 38 s |
| `make dst` | 8 | 3 min 32 s and 3 min 47 s in 2 runs with one StorageServer and one registry. 7 min 7 s in 1 run with 1 to 3 StorageServers and a registry on each simulated node. 3 min 4 s in 1 run after the change of a run from 150 operations to 80. 3 min 3 s in 1 run with the 8 tests with ClusterControllers, 33 tests in total. 2 min 23 s in 1 run with one process for each epoch. 2 min 1 s in 1 run of 32 tests without the registry. 3 min 3 s in 1 run of 41 tests, with the 8 tests with a peace cluster. 4 min 20 s in 1 run of 42 tests, with the coordinators on the simulated disk |
| `dst/run.sh -j 5` | 5 | 4 min 6 s |

The machine has 4 performance cores and 6 efficiency cores. The
commands run the same seeds, and the sums of the statistics are equal.
With 8 nodes, the tests use 2.2 times the processor time of the serial
command, because one test takes longer while other nodes run.

A run has about 5,000 steps and takes about 0.65 s in a node alone. About
30% of that time, the node uses no processor. The invariant checks of the
harness take less than 1% of the time.
