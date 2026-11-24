# Roadmap

redwall follows the design of FoundationDB as the paper describes it. See
[README.md](README.md) for the sources.

## Current status

State on 2026-11-24.

| Part | Status |
|---|---|
| TLA+ models of the logging protocol, the recovery and the Resolver | Done, checked with TLC |
| Coordinated state on peace | Done. A coordinator keeps its data on a disk (`redwall_coordinator`) |
| LogServer | Done in memory. A LogServer keeps every record |
| Process of an epoch: Sequencer, Resolver and Proxy | Done, as one process |
| StorageServer | Done in memory, in ETS tables, with a copy from another StorageServer |
| ClusterController and workers | Done: election, recruitment, one LogServer for each worker |
| Client with the function names of erlfdb | The basic functions, `select`, `get_mapped_range`, the tuple layer, idempotency ids, list keys and binary keys |
| Status | Done: the health of a cluster and its fault tolerance in Erlang nodes (`redwall_status`) |
| Simulation with eta | 42 tests: kills, partitions, lost and delayed messages, a peace cluster with coordinators on the simulated disk |
| Benchmark | Done for one computer with 7 nodes |
| etx | A separate application with the interface of `ets` on redwall, with its own unit tests and simulation |

Each new part goes into the simulation in the step that adds the part.

## Milestones

### Milestone 1: Recovery

The correctness of the design is in the recovery, so the recovery is built
before the commit path is complete.

- [x] Model the logging protocol and the recovery in TLA+ (paper, sections
      2.4.3 and 2.4.4).
- [x] Implement the coordinated state as a state machine for peace.
- [x] Implement the LogServer: it stores records in the order of the LSNs,
      keeps the Durable Version and the Known Committed Version, and stops
      when a Sequencer of a higher epoch locks it.
- [x] Implement the recovery in the Sequencer: lock the coordinated state,
      stop the old LogServers, compute the Recovery Version and the
      previous epoch's end version, copy the records between them, and
      write the coordinated state.
- [x] Implement a Proxy that commits a record without a conflict check.
- [x] Identify a LogServer in the coordinated state with a stable
      identifier, and find its process through a registry.
- [x] Run these parts under eta, with kills at every step of the recovery.
      Check the invariants of the model in the simulation: `Durable`,
      `NoLoss` and `UniqueVersions`.
- [x] Run the simulation with the network of eta: lost and delayed
      messages, and nodes that are cut off.
- [x] Run the coordinated state on a peace cluster in the simulation.
- [x] Require a disk for a coordinator (`redwall_coordinator`), give the
      coordinators a disk in the simulation, and start a killed
      coordinator again.
- [ ] Replace a coordinator whose data is lost, and change the
      coordinators of a cluster.
- [ ] Form the peace cluster of the coordinators without a call of the
      operator, for example from a list of coordinators in the
      application environment. etx does this for its clusters. redwall
      has `redwall_coordinator:form/1` for the operator.

Done when: the simulation kills Sequencers, Proxies and LogServers during
commits and recoveries, and no run violates an invariant.

### Milestone 2: Transactions

- [x] Implement the Resolver with Algorithm 1 of the paper and an MVCC
      window.
- [x] Give a new epoch a first version that makes every transaction of the
      earlier epoch too old for the new Resolvers. The rule is from the
      FoundationDB source: the Recovery Version plus the versions of 100
      seconds.
- [x] Implement read versions in the Sequencer and the Proxy.
- [x] Raise the commit version with the time, as FoundationDB does.
- [x] Confirm that the epoch is live before a Proxy gives a read version.
- [x] Check key ranges in the Resolver, and read a key range from the
      StorageServer.
- [x] Add an operation that clears a key range: a mutation in the log
      record, a write range for the Resolver, and the removal of the keys
      in the StorageServer.
- [x] Collect the transactions of a short time into one batch with one
      commit version in the Proxy.
- [x] Close a batch at once when no batch is in progress, and share one
      confirmation among the requests for a read version.
- [ ] Change the time of a batch with the load, as FoundationDB does, and
      limit the bytes of a batch.
- [x] Find the cause of the p99.9 of a commit in the benchmark: garbage
      collections of the LogServer and of the Resolver, and the removal
      of old entries in the Resolver. The LogServer keeps its records in
      an ETS table now.
- [x] Keep the history of the Resolver in two ETS tables, chosen from a
      benchmark of three structures in the README.
- [x] Keep the newest version of a key in the row of the key, and the
      older versions in a second table of the StorageServer, chosen from
      a benchmark of three layouts in the README.
- [x] Add the client profile `ets` for the StorageServer with ETS
      tables, with a first backoff of 1 ms and a highest backoff of
      100 ms. The profile `default` has the values of FoundationDB.
- [ ] Add a client profile for each further store of the StorageServer,
      and choose its values from a benchmark.
- [ ] Decide the defaults of `retry_ms` (500 ms today, 35 ms in
      FoundationDB) and of the placement of the Sequencer, the Resolver
      and the Proxy (`spread` today) from the benchmark.
- [x] Compare keys with `==` in the Resolver and the StorageServer, as
      their ETS tables do. An integer and a float with the same value are
      one key. Before, a write of such a key stopped the process of the
      epoch.
- [x] Kill the Resolver in the simulation.
- [x] Implement a StorageServer that pulls records from the LogServers,
      answers reads at a version, and discards the records above the
      Recovery Version after a recovery.
- [x] Implement the recovery transaction (paper, section 2.4.4), without
      content.
- [x] Limit the versions in flight in the Sequencer, so that a discarded
      record has an LSN below every version of the next epoch.
- [x] Kill the StorageServer in the simulation and start a new one.
- [x] Run several StorageServers that each have every key, on one Erlang
      node or on several. The client reads from a StorageServer that
      answers.
- [x] Find a LogServer on another Erlang node through the registry.
- [x] Run a registry on each simulated node, and start each LogServer on
      its own simulated node, so that the simulation covers the exchange
      of names between registries.
- [x] Record the process of each LogServer in the coordinated state, as
      FoundationDB records the interface of each LogServer.
- [x] Reduce the time of the simulation suite: run each test in a node of
      its own, with several nodes at the same time.
- [ ] Find where a simulation run waits without the use of a processor,
      which is about 30% of the time of a run. The wait is not in the
      harness of redwall. A change of eta needs the agreement of the
      owner of eta.
- [x] Keep only the versions of the MVCC window in the StorageServer.
- [x] Write a client: get a read version, read, commit, retry on conflict.
      The client has the function names of erlfdb.
- [ ] Check strict serializability of client histories with Elle.

Done when: a Jepsen test with a transactional workload passes under kills
and partitions.

### The API of erlfdb

The goal is that redwall replaces erlfdb in an application. The client
has the basic functions. The remaining parts, in the planned order:

- [x] `select/4`: evaluate an ETS match spec on a key range in the
      StorageServer with ETS tables. erlfdb has no such function.
- [x] `get_mapped_range/4,5` with the mapper of FoundationDB, and the
      tuple layer (`redwall_tuple`). The argument `{term, Term}` is
      reserved for a mapper that is an Erlang term.
- [ ] A mapper that is an Erlang term, for `get_mapped_range`. The
      design is open. FoundationDB has no such mapper.
- [ ] `get_mapped_range` with key selectors, `reverse`, a database as
      the first argument, and the fold and iterator forms of erlfdb.
- [ ] `select` for every key with a prefix, with `ets:select/2` and a
      bound prefix of the key, together with `get_range_startswith`.
- [ ] A count with a match spec, and a continuation for `select` with a
      limit.
- [ ] `get_range` with `limit` and `reverse`, key selectors and
      `get_key`. This step makes subspaces and the
      directory layer of erlfdb usable.
- [x] Keys with the prefix property of the tuple layer: list keys and
      binary keys, the option `keys` of `open/1`, `redwall_key:range/1`,
      `get_range_startswith/2` and `clear_range_startswith/2`. Tuple
      keys of the form `{'$redwall', _, _}` are reserved.
- [ ] Decide whether `keys` has another default than `any`, and whether
      a database rejects the tuple keys of an application.
- [ ] An encoding of a term that keeps the order of the Erlang terms,
      for a storage engine with keys in the order of their bytes.
- [ ] The fold functions.
- [x] Idempotency ids: a transaction whose commit has no answer is not
      applied a second time (`automatic_idempotency`).
- [ ] An idempotency id that the application supplies, and the functions
      of FoundationDB for the result and the expiry of an id.
- [ ] Transaction options, for example `timeout` and `retry_limit`, and
      the limits of a transaction for time and size.
- [ ] `add_write_conflict_key`, `add_write_conflict_range` and
      `get_conflicting_keys`.
- [ ] Atomic operations, versionstamped keys and versionstamped values.
- [ ] Watches.
- [ ] Tenants, and the functions for locality and statistics.
- [ ] `get_last_error/0`. erlfdb keeps the last error in the process
      dictionary, and an error that causes a retry is hidden by a later
      timeout error of the transaction. The design is open.
- [x] Shorten the start of a node from about 3 s to about 40 ms: a
      process that waits for the ClusterController, for the coordinated
      state or for an epoch reads again after 10 ms.
- [x] `open/0`: find the Proxy and the StorageServer on a node that runs
      the application with a worker and a StorageServer.
- [ ] Find the Proxy and the StorageServers from the coordinators in
      `open`, on a node without a worker.
- [ ] Client options for the read version, not planned soon:
  - [ ] A cache of the read version in the client, as `USE_GRV_CACHE` of
        FoundationDB. A read can then miss a commit that was
        acknowledged in the time of the cache.
  - [ ] A read version without the confirmation of the LogServers, as
        `CAUSAL_READ_RISKY` of FoundationDB.
- [ ] Run the test suite of erlfdb against redwall.

### Milestone 3: Scale and operations

- [x] Elect a ClusterController through the coordinated state. The
      ClusterController starts a Sequencer when a Sequencer, a LogServer,
      the Resolver or the Proxy fails.
- [x] Recruit LogServers on other nodes, through a worker on each node.
- [x] Make the Sequencer, the Resolver and the Proxy of an epoch one
      process. The process recruits its LogServers itself, so the wait of
      the Proxy for the names of its epoch is removed.
- [x] Record the process of each LogServer in the coordinated state, and
      remove the registry and its waits.
- [x] Let the worker start the candidate for the ClusterController of
      its node, as two linked processes. One process for both was
      considered and not done: the two state machines are simpler apart.
- [x] Name the module of the process of an epoch `redwall_epoch`. Its
      earlier name was `redwall_proxy`.
- [ ] Decide whether a candidate replaces the ClusterController only
      after a majority of the coordinators has no heartbeat, as in
      FoundationDB. Today one candidate without a connection to the
      ClusterController replaces it.
- [ ] Wait for the stop of the Sequencer of an abandoned attempt before
      the next attempt, so that the recovery of the next attempt does not
      fail on the recovery count.
- [ ] Send a message again after a lost message, or treat a lost message
      as a failed connection. Today a lost message costs the time limit
      of a call, and the simulation stops the loss of messages before its
      checks at the end of a run.
- [ ] Stop the Sequencer, the Resolver and the Proxy of an ended epoch.
- [x] Start the worker, with its candidate, and the StorageServer of a
      node in the application, from the environment.
- [x] Start the coordinator of a node in the application, from the
      environment.
- [x] Place each LogServer of an epoch on a worker of its own, when the
      ClusterController has several workers.
- [x] Report the health of a cluster and its fault tolerance in Erlang
      nodes (`redwall_status:status/1`).
- [x] Keep a list of the StorageServers of a cluster in the coordinated
      state. A StorageServer registers with the worker of its node, and
      the ClusterController records it. `redwall:open/0`, the peers of a
      copy and the status use the list.
- [ ] Remove a StorageServer from the list without a call of the
      operator, when its node does not start again.
- [ ] Start StorageServers through the ClusterController.
- [ ] Examine partitions in the status, and return the state of one
      moment.
- [ ] Decide the time after which a commit, a read or the determination
      of a commit result gives up. Today a client waits without a limit
      while the cluster has no epoch.
- [x] Start a cluster again after the stop of every node: with the
      option `reset_after_log_loss`, a recovery that finds `replication`
      LogServers of the previous epoch lost resets the database. The
      cluster starts without data.
- [ ] Keep the data over the stop of every node. This needs LogServers
      or StorageServers on disk.
- [ ] Model the election and the recruitment in TLA+, or state why the
      recovery count makes a model unnecessary.
- [ ] Shard the key space over StorageServers, with tags on log records:
  - [ ] A shard map from a key range to a team of StorageServers, fixed
        at the start of the cluster.
  - [ ] Tags on the mutations of a log record, and a tail request of a
        StorageServer for its tag.
  - [ ] A cache of the shard map in the client, and a read of a key range
        over several shards.
  - [ ] The copy of records in a recovery for each tag, in the code and in
        the model.
  - [ ] The shard map in system keys, changed by transactions, and the
        move of a shard between StorageServers.
- [ ] Decide how redwall scales the conflict check and the entry point
      for clients. The Sequencer, the Resolver and the Proxy of an epoch
      are one process, so the items "more than one Proxy" and "more than
      one Resolver" of FoundationDB do not apply in their form.
- [ ] Write the records of a LogServer to disk, and start a LogServer
      again from the disk.
- [x] Copy the data of a StorageServer from another StorageServer, so
      that a new StorageServer does not need every record of the
      LogServers of every epoch.
- [ ] Copy a part of the keys, and limit the time for which a peer keeps
      old versions for a copy.
- [ ] Discard the records of a LogServer that every StorageServer has
      stored (the pop of FoundationDB).
- [ ] Remove the LogServers of an ended epoch when no StorageServer needs
      them, and remove the epoch from the coordinated state.
- [ ] Add a DataDistributor and a Ratekeeper.

### Use case: a table in memory for the whole cluster

The goal is a storage backend in memory with low latency and high
throughput, which an application uses as it uses an ETS table, on every
node of the cluster. See "Use cases" in [README.md](README.md).

- [x] Keep the keys of the StorageServer in their order, in an ETS table
      of type `ordered_set`.
- [x] Read from the ETS table of the StorageServer in the process of the
      client, without a message to the StorageServer.
- [x] Keep the data of the StorageServer in memory, in ETS tables.
- [x] Write an interface that follows the functions of `ets`: the
      application etx, in its own repository. Its unit tests compare it
      with an ETS table of the type `ordered_set`.
- [x] State what a restart of every node loses: the data of every table.
      The coordinators write to disk, and the LogServers and the
      StorageServers do not.
- [ ] Decide whether this configuration gets LogServers on disk, or a
      rule for the start after the stop of every node.
- [ ] Measure the latency and the throughput of reads and of commits
      through etx, and compare them with an ETS table on one node. One
      measurement exists: `update_counter/3` of etx takes about 135 us
      with three nodes on one computer.
- [ ] Read in the reverse order, and count the keys of a range, so that
      `last/1`, `prev/2` and `info(T, size)` of etx do not read every
      key.

## Verification work

- [x] Extend the model with the read version, the recovery transaction
      and the first version of an epoch.
- [x] Extend the model with the StorageServer and the rollback above the
      Recovery Version.
- [x] Model the Resolver, in the separate model `formal/Resolver.tla`.
- [ ] Add keys and values to the model of the StorageServer, so that the
      model has the value that a client reads.
- [ ] Model the copy of records to a subset of the new LogServers. The
      model copies each record to every new LogServer.
- [ ] Reduce the time of the Recovery configs with 3 epochs, which is
      about 4 hours. Until then, run them as `formal/README.md` states
      under "When to run TLC".
- [ ] Validate traces of the implementation against the model, as peace
      does.
- [ ] Use keys of several types in the simulation, also keys that are
      equal and not identical. The keys of the simulation are packed
      tuples, and the simulation did not find the defect of such keys in
      the Resolver.
- [ ] Add unit tests for `redwall_status` in redwall. Its tests are in
      etx.
- [ ] Add a planted defect for `batch_depth` above 1, and one for the
      copy of a StorageServer in the simulation of redwall.
- [ ] Stop the temporary processes of a worker and of a
      ClusterController at the end of a simulation run. They run until
      the tables of eta are deleted, and two runs of the suite differed
      by 1 in two counters of the statistics.
