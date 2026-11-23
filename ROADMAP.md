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
- [ ] Add an operation that clears a key range: a mutation in the log
      record, a write range for the Resolver, and the removal of the keys
      in the StorageServer.
- [x] Collect the transactions of a short time into one batch with one
      commit version in the Proxy.
- [ ] Change the time of a batch with the load, as FoundationDB does, and
      limit the bytes of a batch.
- [x] Kill the Resolver in the simulation.
- [x] Implement a StorageServer that pulls records from the LogServers,
      answers reads at a version, and discards the records above the
      Recovery Version after a recovery.
- [x] Implement the recovery transaction (paper, section 2.4.4), without
      content.
- [x] Limit the versions in flight in the Sequencer, so that a discarded
      record has an LSN below every version of the next epoch.
- [x] Kill the StorageServer in the simulation and start a new one.
- [ ] Reduce the time of the simulation suite. It takes about 10 minutes
      since the StorageServer is part of a run.
- [ ] Keep only the versions of the MVCC window in the StorageServer.
- [ ] Write a client: get a read version, read, commit, retry on conflict.
- [ ] Check strict serializability of client histories with Elle.

Done when: a Jepsen test with a transactional workload passes under kills
and partitions.

### Milestone 3: Scale and operations

- [ ] Elect a ClusterController through the peace cluster. The
      ClusterController starts a Sequencer when a Sequencer or a LogServer
      fails.
- [ ] Recruit LogServers on other nodes.
- [ ] Shard the key space over StorageServers, with tags on log records.
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
