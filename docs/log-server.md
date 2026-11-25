# The LogServer

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
- A LogServer discards the records that every StorageServer of the
  cluster has stored. See "Pop".

The first four rules are the actions `Persist` and `LockLog` of the
model.

## LogServer processes

The entry of a LogServer in a log system of the coordinated state is
`{{Epoch, Index}, Pid}`: an identifier, and the process that the recovery
started.

- The process of an epoch has the processes of its own LogServers,
  because its recovery started them.
- A recovery reads the coordinated state at its start, and so has the
  processes of the LogServers of the previous epoch.
- A StorageServer reads the log systems of all epochs from the
  coordinated state, and so has the process of each LogServer.

No part of redwall looks up a LogServer by a name, and no part waits
for a name. FoundationDB stores the interface of each LogServer in the
coordinated state in the same way.

Until 2026-11-23, a registry process on each Erlang node had the names
of the LogServers, and the registries exchanged the names in messages. A
recovery and a Proxy waited, with a limit of time, for names that had
not arrived. The registry was removed when the Sequencer, the Resolver
and the Proxy became one process.

## Pop

The LogServers discard the records that every StorageServer of the
cluster has stored. Without the pop, a LogServer keeps every record, and
its memory grows with each commit. FoundationDB has the same step with
the name pop.

1. The ClusterController reads the coordinated state every `poll_ms`
   (default 1,000 ms). After each read, it starts a process for one pop,
   if the process of the pop before has ended.
2. The process asks each StorageServer of the list of the coordinated
   state for its stored version (`redwall_storage:stored/2`). The stored
   version is the lowest one of the version of the StorageServer, of its
   Known Committed Version, and of the version of each copy that another
   StorageServer reads from it. No recovery discards a record at or
   below that version.
3. The pop version is the lowest stored version. The process sends it to
   every LogServer of every log system of the coordinated state
   (`redwall_log:pop/2`).
4. A LogServer discards the records at or below the pop version, up to
   its Known Committed Version. The highest discarded LSN is its popped
   version.

The ClusterController sends no pop in these cases:

| Case | Reason |
|---|---|
| The list has fewer StorageServers than `pop_storages` (default: `replication`, which is 2 by default) | A discarded record is on the StorageServers only. With the default, it is on as many StorageServers as a record has LogServers with its body |
| A StorageServer of the list does not answer | The StorageServer can need the records when it answers again. A StorageServer of a node that does not start again stays in the list until `redwall_cstate:forget_storage/3` removes the node |
| A StorageServer of the list copies from another StorageServer | The StorageServer has no stored version before the end of the copy |
| A StorageServer answers with another generation of the coordinated state than the ClusterController has read | The versions start again after a reset of the database |

Rules that keep the other parts correct:

- **Recovery.** A recovery copies the records above the previous epoch's
  end version, which is the maximum of the Known Committed Versions of
  the LogServers that answer the lock. The popped version of a LogServer
  is at or below its Known Committed Version, so a LogServer has every
  record that a recovery copies from it.
- **StorageServer.** A LogServer answers `{popped, Popped}` to a request
  of a StorageServer below its popped version. A StorageServer applies a
  record only when the previous LSN of the record is its version, so it
  never applies a record after a discarded one. If `Popped` is above its
  version, it copies the data from a peer with a copy above its version.
- **Repeated push.** A LogServer answers a push of a record at or below
  its popped version as a push of a stored record.

Departure from FoundationDB: each StorageServer of FoundationDB sends a
pop for its tag to each LogServer. redwall has no tags, and every
StorageServer reads every record, so one process collects the stored
versions. This part is not in the paper.

`redwall_log:info/1` has the popped version and the number of records
of a LogServer in the keys `popped` and `records`.

Limitations:

- After a pop, the discarded records are on the StorageServers only.
  With the storage engine `ets`, a StorageServer keeps its data in
  memory. If every StorageServer with these records stops while
  LogServers run, the data is then lost. A new
  StorageServer then applies no record, and the database is not reset.
  Before the pop, a new StorageServer read every record from the
  LogServers in this case.
- A cluster with one StorageServer has no pop with the default of
  `pop_storages`. The memory of its LogServers grows with each record.
- A StorageServer without peers that needs a discarded record does not
  continue.
- A LogServer of an ended epoch keeps the records above its Known
  Committed Version, and the coordinated state keeps the log system of
  each ended epoch.
- `formal/Recovery.tla` has no pop.
- `redwall_status:status/1` does not report the popped versions.
