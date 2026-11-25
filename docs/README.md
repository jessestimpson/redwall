# Design documents of redwall

These documents describe the design of redwall. They use the terms of
this paper:

> Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
> Value Store", SIGMOD 2021.

The documents call it "the paper". The second source is
`design/recovery-internals.md` in the FoundationDB repository. The
documents state where redwall departs from the two sources.

| Document | Contents |
|---|---|
| [architecture.md](architecture.md) | The roles of the paper and their modules, the process of an epoch, the coordinated state, and the departures from the paper |
| [log-server.md](log-server.md) | The LogServer, the processes of the LogServers, and the pop |
| [recovery.md](recovery.md) | The recovery, the versions of an epoch, and the reset after the loss of LogServers |
| [transactions.md](transactions.md) | The Proxy, the batches, the read versions and the Resolver |
| [storage-server.md](storage-server.md) | The StorageServer, its store, the copy from another StorageServer, select and the mapped range read |
| [client.md](client.md) | The client, the kinds of key and the idempotency ids |
| [cluster.md](cluster.md) | The ClusterController, the start of a node, the coordinators and the status |
| [verification.md](verification.md) | The unit tests, the simulation with its invariants and results, and the planted defects |
| [limitations.md](limitations.md) | The complete list of limitations |
| [../formal/README.md](../formal/README.md) | The TLA+ models, the configs and the results |
| [../ROADMAP.md](../ROADMAP.md) | The planned work |

## Source layout

| Path | Contents |
|---|---|
| [src/redwall_cstate.erl](../src/redwall_cstate.erl) | The coordinated state: a `peace_machine` and its client |
| [src/redwall_log.erl](../src/redwall_log.erl) | The LogServer |
| [src/redwall_recovery.erl](../src/redwall_recovery.erl) | The recovery |
| [src/redwall_sequencer.erl](../src/redwall_sequencer.erl) | The rules of the Sequencer for the versions, as functions |
| [src/redwall_resolver.erl](../src/redwall_resolver.erl) | The conflict check of the Resolver, as functions on two ETS tables |
| [src/redwall_epoch.erl](../src/redwall_epoch.erl) | The process of an epoch: the Sequencer, the Resolver and the Proxy |
| [src/redwall_storage.erl](../src/redwall_storage.erl) | The StorageServer |
| [src/redwall_storage_engine.erl](../src/redwall_storage_engine.erl) | The behaviour of a storage engine, and the names of the engines |
| [src/redwall_storage_ets.erl](../src/redwall_storage_ets.erl) | The storage engine `ets`: the data in ETS tables in memory |
| [src/redwall.erl](../src/redwall.erl) | The client, with the function names of erlfdb |
| [src/redwall_status.erl](../src/redwall_status.erl) | The health of a cluster and its fault tolerance in Erlang nodes |
| [src/redwall_key.erl](../src/redwall_key.erl) | The kinds of key, and the key range of a prefix |
| [src/redwall_idempotency.erl](../src/redwall_idempotency.erl) | The keys of the idempotency ids |
| [src/redwall_mapper.erl](../src/redwall_mapper.erl) | The mapper of a mapped range read |
| [src/redwall_tuple.erl](../src/redwall_tuple.erl), [src/redwall_float.erl](../src/redwall_float.erl) | The tuple layer: copies of `erlfdb_tuple` and `erlfdb_float` of erlfdb 1.2.3 |
| [src/redwall_cluster_controller.erl](../src/redwall_cluster_controller.erl) | The ClusterController: the election, the workers and the recruitment |
| [src/redwall_worker.erl](../src/redwall_worker.erl) | The worker of one node, which starts roles for the ClusterController |
| [src/redwall_coordinator.erl](../src/redwall_coordinator.erl) | A coordinator: a server of the peace cluster with the coordinated state on a disk |
| [src/redwall_cstate_local.erl](../src/redwall_cstate_local.erl) | The coordinated state in one process, for tests and for the simulation |
| [include/redwall_eta.hrl](../include/redwall_eta.hrl) | The header that puts a module under the parse transform of eta in the `dst` profile |
| [dst/redwall_dst.erl](../dst/redwall_dst.erl) | The eta harness: the workload and the invariants |
| [dst/redwall_dst_client.erl](../dst/redwall_dst_client.erl) | The processes that perform the operations of the simulation |
| [dst/redwall_dst_tests.erl](../dst/redwall_dst_tests.erl) | The simulation suite |
| [dst/redwall_dst_machine.erl](../dst/redwall_dst_machine.erl) | The machine of the peace cluster of a simulation run |
| [dst/redwall_dst_nosync_io.erl](../dst/redwall_dst_nosync_io.erl) | A planted defect: file operations of a coordinator that never sync |
| [test/redwall_coordinator_tests.erl](../test/redwall_coordinator_tests.erl) | Tests for `redwall_coordinator`: the options, and the coordinated state after the stop of one coordinator and of every coordinator |
| [bench/redwall_bench.erl](../bench/redwall_bench.erl), [bench/run.sh](../bench/run.sh) | The benchmark of a cluster on one computer |
| [dst/run.sh](../dst/run.sh) | Runs each test of the simulation suite in a node of its own |
| [src/redwall_app.erl](../src/redwall_app.erl), [src/redwall_sup.erl](../src/redwall_sup.erl) | The application and its supervisor, which has no child |
| [test/redwall_cstate_tests.erl](../test/redwall_cstate_tests.erl) | Tests for `redwall_cstate` |
| [test/redwall_cluster_controller_tests.erl](../test/redwall_cluster_controller_tests.erl) | Tests for `redwall_cluster_controller` and `redwall_worker` |
| [test/redwall_log_tests.erl](../test/redwall_log_tests.erl) | Tests for `redwall_log` |
| [test/redwall_recovery_tests.erl](../test/redwall_recovery_tests.erl) | Tests for `redwall_recovery` and `redwall_sequencer` |
| [test/redwall_epoch_tests.erl](../test/redwall_epoch_tests.erl) | Tests for `redwall_epoch` |
| [test/redwall_resolver_tests.erl](../test/redwall_resolver_tests.erl) | Tests for `redwall_resolver` |
| [test/redwall_storage_tests.erl](../test/redwall_storage_tests.erl) | Tests for `redwall_storage` |
| [test/redwall_mapper_tests.erl](../test/redwall_mapper_tests.erl) | Tests for `redwall_mapper`, with the cases of the unit test of FoundationDB |
| [formal/Recovery.tla](../formal/Recovery.tla) | TLA+ model of the logging protocol, the recovery, read versions and the StorageServer |
| [formal/Resolver.tla](../formal/Resolver.tla) | TLA+ model of the Resolver |
| [formal/MCRecovery.tla](../formal/MCRecovery.tla), [formal/MCResolver.tla](../formal/MCResolver.tla) | Constants and invariants for model checking |
| [formal/README.md](../formal/README.md) | The models, the configs and the results |
| [ROADMAP.md](../ROADMAP.md) | The planned work |
