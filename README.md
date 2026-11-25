# redwall

[![CI](https://github.com/jessestimpson/redwall/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jessestimpson/redwall/actions/workflows/ci.yml)
[![Model check](https://github.com/jessestimpson/redwall/actions/workflows/model.yml/badge.svg?branch=main)](https://github.com/jessestimpson/redwall/actions/workflows/model.yml)

redwall is a transactional key-value store in Erlang. It implements the
transaction system of FoundationDB, and follows this paper:

> Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
> Value Store", SIGMOD 2021.

- A transaction has strict serializability. The client reads at a read
  version, and a commit fails if another transaction has written a key
  that the transaction read.
- The client has the function names of
  [erlfdb](https://github.com/foundationdb-beam/erlfdb), the Erlang
  client of FoundationDB.
- A [peace](https://github.com/jessestimpson/peace) cluster has the role
  of the Coordinators.
- A cluster is configured with a storage engine. redwall has one
  engine, `ets`, which keeps the data in memory. See "Limitations".

[etx](https://github.com/jessestimpson/etx) is an application on redwall
with the interface of `ets`.

## Verified properties

[VERIFIED.md](VERIFIED.md) lists the properties of redwall that a
mechanical check supports. Each entry states the property, the method that
checked it and the bound of the check. The file also lists the
assumptions, the entries of peace that redwall relies on, and the
properties that are not verified.

A reader, human or LLM, who has a question about the behaviour of redwall
should read VERIFIED.md first. A statement in that file is true for
redwall within the bound that the entry states.

## Status

redwall is experimental. It is not ready for data that must not be lost.

| Part | Status |
|---|---|
| Transactions: read versions, commits, conflict checks | Done |
| Recovery after the failure of a role | Done |
| Election of a ClusterController, and recruitment of the roles on the nodes | Done |
| Replication of the log on several nodes | Done, in memory |
| Replication of the data on several nodes | Done. Each StorageServer has every key |
| Selection of a storage engine for a cluster | Done: the interface of an engine, and the setting `storage_engine`. redwall has one engine, `ets`, with the data in memory |
| Data on disk | Not done. No storage engine writes to disk, and the LogServers do not. Only the Coordinators write to disk |
| Shards of the key space | Not done |
| The whole API of erlfdb | In progress. See "Limitations" |

[ROADMAP.md](ROADMAP.md) has the planned work.

## Example

`sys.config` for the node `a@host` of a cluster of three nodes. Each
node runs every role. The other two nodes have the ids `{c2, 'b@host'}`
and `{c3, 'c@host'}`:

```erlang
[
    {redwall, [
        {coordinator, #{id => {c1, 'a@host'}, dir => "/var/lib/redwall/coordinator"}},
        {worker, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}], candidate => #{min_workers => 2}}},
        {storage, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}]}}
    ]}
].
```

1. Start the application `redwall` on each node.
2. Call `redwall_coordinator:form([{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}])`
   once, on one node.

A transaction on any of these nodes:

```erlang
Db = redwall:open(),
redwall:transactional(Db, fun(Tx) ->
    Balance = redwall:wait(redwall:get(Tx, account)),
    redwall:set(Tx, account, Balance + 1)
end).
```

`transactional/2` runs the function again after a conflict.
[docs/cluster.md](docs/cluster.md) has every option of the start, and
[docs/client.md](docs/client.md) has the functions of the client.

## Features

- **Transactions.** Reads of keys and of key ranges, sets, clears and
  clears of key ranges, with optimistic concurrency control and an MVCC
  window of 5 seconds.
- **Ordered keys.** A key is an Erlang term, and the keys are in the
  order of Erlang terms. The tuple layer of erlfdb is included.
- **Reads without a message.** With the storage engine `ets`, a client
  on the node of a StorageServer reads the ETS tables of the
  StorageServer in its own process.
- **Range reads.** `redwall:get_range/4` has the options `limit` and
  `reverse` of erlfdb, and `redwall:get_key/2` resolves a key selector.
  The StorageServer reads only the keys of the result.
- **Watches.** `redwall:watch/2` returns a future that becomes ready
  when the value of a key changes, as a watch of FoundationDB does.
- **Select.** `redwall:select/4` evaluates an ETS match spec on a key
  range in the StorageServer. FoundationDB has no such read.
- **Mapped range read.** `redwall:get_mapped_range/4` reads the rows of
  an index and the record of each row in one request, as
  `getMappedRange` of FoundationDB does.
- **Idempotency ids.** A transaction whose commit has no answer is not
  applied a second time.
- **Fault tolerance.** With the defaults, each log record is on 2 of 3
  LogServers on different nodes. The cluster starts a new epoch after
  the failure of a role. A new StorageServer copies the data from
  another StorageServer.
- **Storage engine.** A cluster is configured with a storage engine,
  as a cluster of FoundationDB is. The engine `ets` keeps the data in
  ETS tables in memory. A module with the behaviour
  `redwall_storage_engine` is another engine.
- **Status.** `redwall_status:status/1` returns the health of a cluster,
  the number of nodes that can fail, and the nodes that are safe to
  stop.

## Architecture

In FoundationDB, consensus is not on the commit path. The Coordinators
store a small record, the coordinated state. Every other part of the
transaction system is created again in each epoch. redwall keeps this
design.

| Role in the paper | Function | Module |
|---|---|---|
| Coordinators | Store the coordinated state | [redwall_cstate](src/redwall_cstate.erl) on a peace cluster, [redwall_coordinator](src/redwall_coordinator.erl) |
| ClusterController | Monitors the roles and recruits the Sequencer | [redwall_cluster_controller](src/redwall_cluster_controller.erl), [redwall_worker](src/redwall_worker.erl) |
| Sequencer | Assigns versions and performs the recovery | [redwall_epoch](src/redwall_epoch.erl), [redwall_sequencer](src/redwall_sequencer.erl), [redwall_recovery](src/redwall_recovery.erl) |
| Proxy | Gives read versions and commits transactions | [redwall_epoch](src/redwall_epoch.erl) |
| Resolver | Checks a transaction for conflicts | [redwall_epoch](src/redwall_epoch.erl), [redwall_resolver](src/redwall_resolver.erl) |
| LogServer | Stores the log records of an epoch | [redwall_log](src/redwall_log.erl) |
| StorageServer | Stores data and answers reads at a version | [redwall_storage](src/redwall_storage.erl), with a storage engine: [redwall_storage_engine](src/redwall_storage_engine.erl), [redwall_storage_ets](src/redwall_storage_ets.erl) |

The largest departure from the paper: the Sequencer, the Proxy and the
Resolver of an epoch are one Erlang process. A commit then has one round
trip between nodes, to the LogServers.

## Documentation

The design is in [docs](docs/README.md):

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | The roles, the process of an epoch, the coordinated state, and the departures from the paper |
| [docs/log-server.md](docs/log-server.md) | The LogServer and the pop |
| [docs/recovery.md](docs/recovery.md) | The recovery, the versions, and the reset after the loss of LogServers |
| [docs/transactions.md](docs/transactions.md) | The Proxy, the batches, the read versions and the Resolver |
| [docs/storage-server.md](docs/storage-server.md) | The StorageServer, the storage engine, select, the mapped range read and the watches |
| [docs/client.md](docs/client.md) | The client, the kinds of key, the watches and the idempotency ids |
| [docs/cluster.md](docs/cluster.md) | The ClusterController, the start of a node, the coordinators and the status |
| [docs/verification.md](docs/verification.md) | The simulation, its invariants and its results |
| [docs/limitations.md](docs/limitations.md) | The complete list of limitations |
| [formal/README.md](formal/README.md) | The TLA+ models |

Each module also has its design in its module documentation.

## Verification

| Level | What it checks | Command | Time |
|---|---|---|---|
| Unit tests | Each module, and clusters on one node and on two Erlang nodes. 130 tests | `make test` | 30 seconds |
| Simulation | The whole system under the scheduler and the simulated network of [eta](https://github.com/jessestimpson/eta), with kills, partitions, and lost and delayed messages. 52 tests with about 1,900 seeds and 24 planted defects | `make dst` | about 4 minutes |
| Model checking | Three TLA+ models with TLC: the logging protocol, the recovery, the read versions and the StorageServer; the Resolver; and the watch. 13 planted bugs | `make tlc` | about 6 minutes |
| Model checking | The recovery with 3 epochs | `make tlc-long` | about 4 hours |

The simulation checks 21 invariants after every step, for example: no
acknowledged transaction is lost by a recovery, no two acknowledged
transactions conflict, and a read returns the last acknowledged write at
or below its read version. See
[docs/verification.md](docs/verification.md).

The verification of redwall depends on the verification of
[peace](https://github.com/jessestimpson/peace):

- redwall stores the coordinated state in a peace cluster. The recovery
  and the election of the ClusterController are correct only if peace
  decides one value for each operation on the coordinated state.
- The models of redwall treat the coordinated state as one register with
  atomic operations. Most simulation runs of redwall use one process in
  the place of the peace cluster.
- peace has its own verification. [VERIFIED.md](VERIFIED.md),
  "Dependencies", names the entries of the `VERIFIED.md` of peace that
  redwall relies on.

The dependencies form a sequence: etx depends on redwall, and redwall
depends on peace. A result of a higher level holds only if the
verification of each level below it holds.

GitHub Actions runs the unit tests and the simulation on each push to
`main` and on each pull request, and the quick TLC configs when a file
in `formal/` changes.

## Performance

`make bench` runs a cluster of 7 Erlang nodes on one computer
([bench/redwall_bench.erl](bench/redwall_bench.erl)). Results of one run
on 2026-11-23 on a computer with 10 cores. The cluster has the storage engine `ets`, so every node keeps its data
in memory, and a message between two nodes has no network delay.

| Operation | Clients | Operations per second | p50 |
|---|---|---|---|
| Transaction with one write | 1 | 10,980 | 91 us |
| Transaction with one write | 64 | 103,292 | 607 us |
| Transaction with one read and one write | 64 | 38,056 | 1,661 us |
| Read of one key by message | 1 | 39,599 | 25 us |
| Read of one key on the node of the StorageServer | 1 | 618,957 | 2 us |

## Limitations

The most important limitations are:

- redwall has one storage engine, `ets`. The following holds for a
  cluster with this engine:
  - The LogServers and the StorageServers keep their data in memory.
    After the stop of every node, the cluster starts without data.
  - When the LogServers of an epoch are lost, the recovery resets the
    database, and the cluster continues without data. The log and the
    status report a reset.
- A storage engine that keeps the data over the stop of every node does
  not exist. It also needs LogServers on disk, or a recovery from the
  StorageServers.
- Each StorageServer has every key. The key space has no shards.
- One process performs the conflict checks of an epoch. redwall cannot
  add Proxies or Resolvers.
- The client has a part of the functions of erlfdb. It has no atomic
  operations, no versionstamps, no transaction options and no tenants.
- redwall does not send a message again. A lost message to a LogServer
  stops the commits of the epoch until the next recovery.
- The coordinators of a cluster cannot change.

[docs/limitations.md](docs/limitations.md) has the complete list.

## Commands

| Command | Meaning |
|---|---|
| `make compile` | Compile |
| `make test` | Run the unit tests. Two tests need `epmd` |
| `make dst` | Run the simulation suite, each test in an Erlang node of its own |
| `make dst-serial` | Run the simulation suite in one node |
| `make bench` | Run the benchmark |
| `make tlc` | Run the quick TLC configs |
| `make tlc-long` | Run the 2 TLC configs with three epochs |

## Requirements

- Erlang/OTP 28 and rebar3.
- peace. `rebar.config` declares it as a git dependency, and `rebar.lock`
  records its commit. `rebar3 upgrade peace` moves the lock to the newest
  commit of the branch `main`. For local development, link a checkout at
  `_checkouts/peace`.
- eta, for `make dst` only. The `dst` profile of `rebar.config` declares
  it and fixes its commit, because `rebar.lock` does not hold the
  dependencies of a profile. For local development, link a checkout at
  `_checkouts/eta`.
- A checkout in `_checkouts` replaces the recorded commit.
- Java 11 or later for the TLC targets. The TLC jar is in `formal/vendor`.

## Source layout

| Path | Contents |
|---|---|
| `src/` | The application. [docs/README.md](docs/README.md) lists each module |
| `test/` | The unit tests |
| `dst/` | The simulation: the eta harness, its clients and the suite |
| `formal/` | The TLA+ models and their configs |
| `bench/` | The benchmark |
| `docs/` | The design documents |

## License

Apache License 2.0. See [LICENSE.md](LICENSE.md).
