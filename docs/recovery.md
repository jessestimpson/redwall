# The recovery

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

If the coordinated state refuses the write of step 6, the recovery stops
the new LogServers. If the write has no answer, the recovery fails and
does not stop them, because the coordinated state can have the new log
system, and the next recovery then needs its LogServers. Until
2026-11-23 the recovery stopped the new LogServers in both cases. A
simulation run with a lost answer then ended with a coordinated state
whose LogServers were stopped, and no later recovery succeeded.

The Sequencer then commits the recovery transaction (paper, section
2.4.4). The recovery transaction is the first record of the epoch. Its
LSN is the first commit version of the epoch, and its previous LSN is the
RV. The process of the epoch sends it to every LogServer of the epoch,
and gives its version to the conflict check of the Resolver. The read version at the
start of an epoch is therefore the LSN of a record that exists, and a
StorageServer reaches it. The recovery transaction of redwall has no
content.

The Sequencer then assigns the LSNs above the recovery transaction. If a
step fails, the Sequencer stops. A recovery that fails after step 1 leaves a higher
recovery count. A recovery that fails after step 2 leaves stopped
LogServers. The next recovery handles both.

## Versions

The paper does not state the rules below. They are the rules of
FoundationDB 7.3, from `fdbserver/ClusterRecovery.actor.cpp`,
`fdbserver/masterserver.actor.cpp`, `fdbserver/Resolver.actor.cpp`,
`fdbserver/SkipList.cpp` and `fdbclient/ServerKnobs.cpp`.

| Rule | Value in redwall and in FoundationDB |
|---|---|
| Versions for each second | 1,000,000 |
| A commit version | The last commit version plus the versions of the time since then; at least 1 more, at most 5,000,000 more |
| The first commit version of an epoch | The Recovery Version plus 100,000,000; 1 if the Recovery Version is 0 |
| The read version | The highest LSN that every LogServer of the epoch has stored; at the start of an epoch, the LSN of the recovery transaction |
| The MVCC window of the Resolver | 5,000,000 versions |

The gap at the start of an epoch is larger than the MVCC window. The
Resolver of a new epoch has no history. Because of the gap, it treats a
transaction with a read version of the previous epoch as too old.

The Sequencer of redwall assigns a commit version only if the version is
less than 100,000,000 above the read version. Otherwise the Sequencer
answers `{error, versions_in_flight}` and the Proxy stops. With this
limit, no LogServer stores a record of a client at or above the first
commit version of the next epoch. A record of a client that a recovery
discards therefore has an LSN below every version of the next epoch. A
discarded recovery transaction can have the first LSN of the next epoch;
it has no content. The StorageServer depends on this
rule. FoundationDB uses the constant `MAX_VERSIONS_IN_FLIGHT` for the gap.
The limit in the Sequencer of redwall is not taken from the FoundationDB
source.

## Reset after the loss of LogServers

A LogServer keeps its records in memory. When `replication` LogServers of
an epoch do not run, a record of that epoch can be on no LogServer, and
no recovery can end the epoch. This is the state of a cluster after the
stop of every node, and after the stop of a second node before the
cluster has recovered from the stop of the first.

The recovery then resets the database
([redwall_recovery](../src/redwall_recovery.erl), "Reset"). The reset
depends on the kind of LogServer of the previous epoch. The log system of
the coordinated state has the kind in the key `durability`, from
`redwall_log:durability/0`.

| `durability` | Records of a LogServer after its process stops | Recovery after the loss of `replication` LogServers |
|---|---|---|
| `memory` | Lost | Resets the database |
| `disk` (no such LogServer exists yet) | Kept | Waits for the LogServers |

The steps of a reset are:

1. The recovery asks the node of each LogServer of the previous epoch
   whether the process runs. A LogServer counts as lost only if its node
   answers that it does not run. A node that does not answer can be cut
   off.
2. If at least `replication` LogServers are lost, the recovery removes
   every log system from the coordinated state and raises its
   generation.
3. The recovery continues as the first recovery of a database, and the
   versions start again.
4. A StorageServer that reads the higher generation removes its data.

A reset is reported in three places:

- The recovery writes a warning to the log with `logger`, and a
  StorageServer writes a warning when it removes its data.
- The ClusterController sends the event `{reset, Epoch}` to its `notify`
  function, after the event `recovered`.
- `redwall_status:status/1` has the number of resets of the database in
  the key `generation`.

The option `reset_after_log_loss => false` of the process of an epoch
turns the reset off. The recovery then fails with `too_few_replies`, and
the cluster has no epoch.

- The data of the database is lost by a reset.
- The default is `false`: the recovery fails, and the cluster has no
  epoch. FoundationDB has no reset, because its LogServers write to disk.
- The Resolver rejects a transaction with a read version at or above its
  commit version, so a transaction of the time before a reset does not
  commit after it.
