# Roadmap

deadlift follows the design of FoundationDB as the paper describes it. See
[README.md](README.md) for the sources.

## Current status

| Part | Status |
|---|---|
| TLA+ model of the logging protocol and the recovery | Done, checked with TLC |
| Coordinated state on peace | Done, with unit tests |
| LogServer | Done in memory, with unit tests |
| Sequencer and recovery | Done, with unit tests |
| Proxy | Done, with unit tests |
| Resolver | Done for keys and key ranges, with unit tests |
| StorageServer | Done in memory, with unit tests |
| Simulation with eta | Done: kills, partitions, and lost and delayed messages |

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
- [ ] Run the coordinated state on a peace cluster in the simulation.
- [ ] Add each new part to the simulation in the step that adds the
      part.

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
- [ ] Decide the defaults of `retry_ms` (500 ms today, 35 ms in
      FoundationDB) and of the placement of the Sequencer, the Resolver
      and the Proxy (`spread` today) from the benchmark.
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
- [ ] Record the node of each LogServer in the coordinated state, as
      FoundationDB records the interface of each LogServer, so that a
      process can tell a LogServer whose name has not arrived from a
      LogServer that does not run.
- [x] Reduce the time of the simulation suite: run each test in a node of
      its own, with several nodes at the same time.
- [ ] Find where a simulation run waits without the use of a processor,
      which is about 30% of the time of a run. The wait is not in the
      harness of deadlift. A change of eta needs the agreement of the
      owner of eta.
- [x] Keep only the versions of the MVCC window in the StorageServer.
- [x] Write a client: get a read version, read, commit, retry on conflict.
      The client has the function names of erlfdb.
- [ ] Check strict serializability of client histories with Elle.

Done when: a Jepsen test with a transactional workload passes under kills
and partitions.

### The API of erlfdb

The goal is that deadlift replaces erlfdb in an application. The client
has the basic functions. The remaining parts, in the planned order:

- [ ] `get_range` with `limit` and `reverse`, key selectors and
      `get_key`. This step makes the tuple layer, subspaces and the
      directory layer of erlfdb usable.
- [ ] The fold functions, `get_range_startswith` and
      `clear_range_startswith`.
- [ ] Transaction options, for example `timeout` and `retry_limit`, and
      the limits of a transaction for time and size.
- [ ] `add_write_conflict_key`, `add_write_conflict_range` and
      `get_conflicting_keys`.
- [ ] Atomic operations, versionstamped keys and versionstamped values.
- [ ] Watches.
- [ ] Tenants, mapped ranges, and the functions for locality and
      statistics.
- [ ] `get_last_error/0`. erlfdb keeps the last error in the process
      dictionary, and an error that causes a retry is hidden by a later
      timeout error of the transaction. The design is open.
- [ ] Find the Proxy and the StorageServer from the coordinators in
      `open`.
- [ ] Client options for the read version, not planned soon:
  - [ ] A cache of the read version in the client, as `USE_GRV_CACHE` of
        FoundationDB. A read can then miss a commit that was
        acknowledged in the time of the cache.
  - [ ] A read version without the confirmation of the LogServers, as
        `CAUSAL_READ_RISKY` of FoundationDB.
- [ ] Run the test suite of erlfdb against deadlift.

### Milestone 3: Scale and operations

- [x] Elect a ClusterController through the coordinated state. The
      ClusterController starts a Sequencer when a Sequencer, a LogServer,
      the Resolver or the Proxy fails.
- [x] Recruit LogServers on other nodes, through a worker on each node.
- [ ] Send the processes of the LogServers and of the Resolver in the
      information of the ClusterController about the database, and read
      them in the Proxy, the Sequencer and the StorageServer. This step
      replaces the registry and its three waits.
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
- [ ] Start the worker and the candidate in the application, and start
      StorageServers through the ClusterController.
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
- [ ] Run more than one Proxy and more than one Resolver.
- [ ] Write the records of a LogServer to disk, and start a LogServer
      again from the disk.
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
- [ ] Write a storage engine for the StorageServer that keeps the data in
      memory.
- [ ] Decide which parts of the transaction system write to disk in this
      configuration, and state what a restart of every node loses.
- [ ] Write an interface that follows the functions of `ets`.
- [ ] Measure the latency and the throughput of reads and of commits, and
      compare them with an ETS table on one node.

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
