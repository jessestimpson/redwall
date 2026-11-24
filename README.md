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

- two TLA+ models, checked with TLC: one of the logging protocol, the
  recovery, read versions and the StorageServer, and one of the
  Resolver;
- the coordinated state, as a state machine for peace, with tests;
- the LogServer, with tests;
- the recovery, and a Sequencer that assigns read versions and commit
  versions and commits the recovery transaction, with tests;
- the Resolver, with tests;
- a Proxy that gives read versions and commits transactions, with tests;
- a StorageServer that reads the records from the LogServers and answers
  reads at a version, with tests;
- a client with the function names of erlfdb, with tests;
- a ClusterController that is elected through the coordinated state,
  and a worker on each node, with tests;
- a deterministic simulation of these parts with
  [eta](https://github.com/jessestimpson/eta), with kills, partitions,
  and lost and delayed messages.

A caller runs a transaction with the client: the client gets a read
version from a Proxy, reads keys from the StorageServer at the read
version, commits the read keys and the writes through a Proxy, and repeats
the transaction after a conflict. The client has the basic functions of
erlfdb only. Every part keeps its data in memory. See
[ROADMAP.md](ROADMAP.md).

## Design

In FoundationDB, consensus is not on the commit path. The Coordinators store
a small record, the coordinated state. Every other part of the transaction
system is created again in each epoch (paper, sections 2.3.4 and 2.3.5).
deadlift keeps this design.

| Role in the paper | Function | In deadlift |
|---|---|---|
| Coordinators | Store the coordinated state | A peace cluster that runs [deadlift_cstate](src/deadlift_cstate.erl) |
| ClusterController | Monitors the roles and recruits the Sequencer | [deadlift_cluster_controller](src/deadlift_cluster_controller.erl) and [deadlift_worker](src/deadlift_worker.erl). Not modelled |
| Sequencer | Assigns read versions and commit versions, and performs the recovery | A role of the process of an epoch, [deadlift_epoch](src/deadlift_epoch.erl), with the rules in [deadlift_sequencer](src/deadlift_sequencer.erl) and [deadlift_recovery](src/deadlift_recovery.erl). The recovery, the recovery transaction and the versions are modelled in [formal/Recovery.tla](formal/Recovery.tla) |
| Proxy | Gives read versions to clients and commits transactions | A role of the process of an epoch, [deadlift_epoch](src/deadlift_epoch.erl). The commit to the LogServers and the read version are modelled |
| Resolver | Checks a transaction for conflicts | A role of the process of an epoch, with the functions of [deadlift_resolver](src/deadlift_resolver.erl). Modelled in [formal/Resolver.tla](formal/Resolver.tla) |
| LogServer | Stores the log records of an epoch | [deadlift_log](src/deadlift_log.erl). Modelled |
| StorageServer | Stores data and answers reads at a version | [deadlift_storage](src/deadlift_storage.erl). Modelled, without keys and values |

### One process for an epoch

The Sequencer, the Resolver and the Proxy of an epoch are one process in
deadlift ([deadlift_epoch](src/deadlift_epoch.erl)). The sections below
use the names of the three roles for the parts of its work.

- FoundationDB has one process for each role. A process of FoundationDB
  runs on one thread, and separate processes are its way to use several
  processors.
- The three roles have the same lifetime. They start with an epoch, and
  the failure of each of them starts a recovery. One process for the
  three adds no failure case.
- The process assigns the commit version of a batch and checks the batch
  for conflicts in one step. The checks are in the order of the versions
  without a protocol for the order.
- The process raises the read version when the LogServers have stored a
  record, before it answers the client. No report to another process is
  needed.
- A commit has one round trip between nodes, to the LogServers.
- One process is one place for the conflict check. FoundationDB can add
  Proxies and Resolvers on more machines. deadlift cannot, in this
  design.

This part is not in the paper.

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
StorageServer reaches it. The recovery transaction of deadlift has no
content.

The Sequencer then assigns the LSNs above the recovery transaction. If a
step fails, the Sequencer stops. A recovery that fails after step 1 leaves a higher
recovery count. A recovery that fails after step 2 leaves stopped
LogServers. The next recovery handles both.

### Versions

The paper does not state the rules below. They are the rules of
FoundationDB 7.3, from `fdbserver/ClusterRecovery.actor.cpp`,
`fdbserver/masterserver.actor.cpp`, `fdbserver/Resolver.actor.cpp`,
`fdbserver/SkipList.cpp` and `fdbclient/ServerKnobs.cpp`.

| Rule | Value in deadlift and in FoundationDB |
|---|---|
| Versions for each second | 1,000,000 |
| A commit version | The last commit version plus the versions of the time since then; at least 1 more, at most 5,000,000 more |
| The first commit version of an epoch | The Recovery Version plus 100,000,000; 1 if the Recovery Version is 0 |
| The read version | The highest LSN that every LogServer of the epoch has stored; at the start of an epoch, the LSN of the recovery transaction |
| The MVCC window of the Resolver | 5,000,000 versions |

The gap at the start of an epoch is larger than the MVCC window. The
Resolver of a new epoch has no history. Because of the gap, it treats a
transaction with a read version of the previous epoch as too old.

The Sequencer of deadlift assigns a commit version only if the version is
less than 100,000,000 above the read version. Otherwise the Sequencer
answers `{error, versions_in_flight}` and the Proxy stops. With this
limit, no LogServer stores a record of a client at or above the first
commit version of the next epoch. A record of a client that a recovery
discards therefore has an LSN below every version of the next epoch. A
discarded recovery transaction can have the first LSN of the next epoch;
it has no content. The StorageServer depends on this
rule. FoundationDB uses the constant `MAX_VERSIONS_IN_FLIGHT` for the gap.
The limit in the Sequencer of deadlift is not taken from the FoundationDB
source.

### The Resolver

The Resolver follows Algorithm 1 of the paper (section 2.4.2). It keeps,
for each key range, the commit version of the last transaction that wrote
the range.

- A transaction has read keys and read ranges. The read range
  `{Begin, End}` has every key `K` with `Begin =< K` and `K < End`, in the
  order of Erlang terms.
- A transaction has a conflict if a key that it read, alone or in a read
  range, has a commit version above the read version of the transaction.
- A read range has the keys without a value. A transaction that reads a
  range gets a conflict when another transaction writes a new key in the
  range.
- A transaction with read keys or read ranges and a read version below
  the MVCC window is too old.
- A request has the transactions of one batch of the Proxy. The Resolver
  checks them in the order of the batch. A transaction has a conflict
  with an earlier transaction of the batch that the Resolver admitted.
- `deadlift_storage:read_range/4` reads the keys of a range at a read
  version.
- The Resolver handles the commit versions in their order. A request that
  arrives before the request with the previous version waits.
- The commit versions of the key ranges are in an ETS table of the kind
  `ordered_set`, with the bound of a range as the key. A second ETS table
  of the kind `ordered_set` has the bounds in the order of their
  versions. The Resolver removes the versions below the MVCC window for
  each transaction, and reads only the entries that it removes. See "The
  history of the Resolver" under "Benchmark".

#### The store of the StorageServer

FoundationDB has an ordered keyspace. Every structure of deadlift that
holds keys keeps the keys in their order.

- The data of the StorageServer is an ETS table of type `ordered_set`
  with one row for each key. The row has the newest version of the key.
- A second ETS table of type `ordered_set` has one row for each older
  version of a key, with the key and the version as its key. A read at a
  version below the newest version of a key reads one row of this
  table.
- A second ETS table of type `ordered_set` has one row for each version
  above the oldest version. A rollback examines only the versions that it
  removes. A rise of the oldest version examines only the versions that
  leave the MVCC window.
- The table of the data is readable by other processes.
  `deadlift_storage:info/1` returns it. No part of deadlift reads it
  directly yet.

A benchmark on 2026-11-22 compared four stores with 1,000,000 keys of 12
bytes and values of 100 bytes, on an Apple M4 with Erlang/OTP 28.3. Each
number is the median of 3 runs. The benchmark is not part of this
repository.

| Measurement | Unit | `gb_trees` | ETS, one row for each key | ETS, one row for each version | NIF with `std::map` |
|---|---|---|---|---|---|
| Read of one key | ns | 1,161 | 1,016 | 1,195 | 1,118 |
| Read of a range of 1,000 keys | µs | 28 | 242 | 481 | 240 |
| New version of one key | ns | 2,414 | 1,726 | 1,414 | 884 |
| Longest single put of 200,000 | µs | 23,658 | 661 | 603 | 676 |
| Clear of a range of 100 keys | µs | 162 | 78 | 92 | 16 |
| Reads by 8 processes at the same time | million per second | Not possible | 5.13 | 2.99 | 5.04 |

The owner of the project chose the ETS table with one row for each key.
In that benchmark each key had about one version. On 2026-11-23 a
benchmark with many versions of each key showed that a row with every
version of a key costs a copy of every version for each read and each
write. The owner then chose the row with the newest version and the
second table for the older versions. See "The versions of a hot key"
under "Benchmark".

### The client

The module [deadlift](src/deadlift.erl) is the client. The functions have
the names, the arguments and the results of
[erlfdb](https://github.com/foundationdb-beam/erlfdb) 1.2.3, so that
deadlift can replace erlfdb in an application later.

```erlang
Db = deadlift:open(#{proxy => Proxy, storage => Storage}),
deadlift:transactional(Db, fun(Tx) ->
    Balance = deadlift:wait(deadlift:get(Tx, account)),
    deadlift:set(Tx, account, Balance + 1)
end).
```

| Group | Functions |
|---|---|
| Database and transaction | `open/1`, `create_transaction/1`, `transactional/2`, `snapshot/1` |
| Reads | `get/2`, `get_range/3`, `wait/1`, `wait/2` |
| Writes | `set/3`, `clear/2`, `clear_range/3` |
| Commit and errors | `commit/1`, `reset/1`, `cancel/1`, `on_error/2` |
| Versions | `get_read_version/1`, `set_read_version/2`, `get_committed_version/1`, `get_versionstamp/1` |
| Conflict ranges and status | `add_read_conflict_key/2`, `add_read_conflict_range/3`, `is_read_only/1` |

- A transaction of erlfdb is a mutable object of the C library of
  FoundationDB. A transaction of deadlift is an ETS table of type
  `ordered_set`, so that several processes can use one transaction, as
  with erlfdb. A benchmark measured 213 ns to create and delete such a
  table, and 1,915 ns for the state of a transaction with 2 reads and 2
  writes.
- A read returns a write of the same transaction (paper, section 2.4.1).
- Another read goes to a StorageServer. A database names one
  StorageServer or several, and each of them has every key:
  1. The read puts the StorageServers in an order. A hash of the read
     version selects the first one.
  2. The read tries each StorageServer on the node of the client. It
     reads the ETS table of the StorageServer in the process of the
     client, without a message. The StorageServer publishes its version
     and its oldest version for this purpose.
  3. If no StorageServer on the node has applied the read version, the
     read sends a message to each StorageServer in the order, on any
     node. The StorageServer process waits for the version. The read has
     5 seconds in total, in equal parts for the StorageServers. When a
     StorageServer is not alive or does not answer in its part of the
     time, the read continues with the next one.
- `get/2` returns a future, as in erlfdb. The future of deadlift is the
  result itself, and `wait/1` returns its argument.
- An error is `error({erlfdb_error, Code})` with the error code of
  FoundationDB 7.3: 1007 `transaction_too_old`, 1009 `future_version`,
  1020 `not_committed`, 1021 `commit_unknown_result`, 1025
  `transaction_cancelled` and 1037 `process_behind`.
- `on_error/2` follows `Transaction::onError` of
  `fdbclient/NativeAPI.actor.cpp`. After 1020, 1021 and 1037 it waits for a
  random part of a backoff that starts at 10 ms, doubles with each error
  and is at most 1 s. After 1007 and 1009 it waits for 10 ms. The options
  `retry_delay_ms` and `max_retry_delay_ms` of `deadlift:open/1` set the
  first backoff and the highest backoff. FoundationDB has the
  transaction option `max_retry_delay` for the highest backoff, and no
  option for the first one.
- A profile is a set of client options for one kind of StorageServer.
  `deadlift:open/1` takes the option `profile`, and an option of
  `open/1` replaces the option of the profile. A StorageServer with
  another store gets a profile of its own.

  | Profile | For | `retry_delay_ms` | `max_retry_delay_ms` | `direct` |
  |---|---|---|---|---|
  | `default` | The values of FoundationDB | 10 | 1,000 | `true` |
  | `ets` | The StorageServer of deadlift, with the data in ETS tables in memory | 1 | 100 | `true` |
  `transactional/2` then runs the function again.

### Idempotency ids

A client that receives no answer to a commit does not know whether the
transaction is committed (the error 1021). `transactional/2` runs the
function again, so a committed transaction can be applied a second time.
A transaction with an idempotency id is applied once. The design follows
`design/idempotency_ids.md` of FoundationDB 7.3.62.

```erlang
Db = deadlift:open(#{automatic_idempotency => true}),
deadlift:transactional(Db, fun(Tx) ->
    N = deadlift:get(Tx, counter),
    deadlift:set(Tx, counter, N + 1)
end).
```

1. The commit of the transaction writes a record with the id and the
   commit version, through the mutation `{set_versionstamp, Key}`, and has
   a lock key of the id as a read conflict key.
2. When the commit has no answer, the client commits a transaction that
   clears the lock key. After this commit, the Resolver rejects the first
   transaction, and a commit before it is visible to a later read.
3. The client reads the record of the id. With the record, `commit/1`
   returns `ok` with the commit version of the record. Without it,
   `commit/1` raises 1007, and `transactional/2` runs the function again.
4. After a commit with an answer, the client tells the Proxy that the
   record is not needed, and the Proxy clears it with its next batch.

- `deadlift:set_option(Tx, automatic_idempotency)` gives an id to one
  transaction.
- The Proxy removes the records that are older than
  `idempotency_min_age_ms` (default 1 hour) and records that time. A
  client whose id is older cannot determine the result, and `commit/1`
  raises 1022, `commit_unknown_result_fatal`.
- `commit/1` does not return while the result is not determined, also
  while no Proxy runs.
- A transaction with an id and without a read asks the Proxy for a read
  version at its commit.
- A key of the form `{'$deadlift', _, _}` is reserved. The records of the
  ids have such keys ([deadlift_idempotency](src/deadlift_idempotency.erl)).
- Differences from FoundationDB: the key of a record has the id, so the
  client reads one key and not the keys of every possible commit version.
  The time of an id is the time of the client. An application cannot
  supply an id.

A unit test puts a process in the place of the Proxy that stops without
an answer after 2 commits in 7, and before 1 commit in 7. Without an id,
100 increments of a counter give a value above 110. With an id, 200
increments return 1 to 200 and give 200. In the benchmark of etx with
three nodes on one computer, the p50 of `update_counter/3` is 135 us to
139 us with ids, against 130 us to 135 us without.

### Select

`deadlift:select/4` evaluates an ETS match spec on the keys of a range,
and returns the results in the order of the keys:

```erlang
%% The numbers of the users with the status `active'.
Spec = [{{{user, '$1'}, #{status => active}}, [], ['$1']}],
deadlift:transactional(Db, fun(Tx) ->
    deadlift:select(Tx, {user, 0}, {user, 1000000}, Spec, [{limit, 100}])
end).
```

- The match spec is for rows `{Key, Value}`. The result is the result of
  `ets:match_spec_run/2` on the result of `get_range/3`.
- The StorageServer evaluates the match spec. A value that does not
  match does not reach the client, and on the node of the StorageServer
  it is not copied for the caller.
- The transaction gets the whole range as a read conflict range, as
  `get_range/3` does. A write of another transaction to a key of the
  range that does not match is a conflict too, because the write can
  make the key match. A select of a snapshot gives no conflict.
- The function sees the sets and the clears of its transaction.
- The options are a list. `{limit, N}` returns the first `N` results.
- The StorageServer walks the rows of the range. The cost of a select
  grows with the number of keys of the range, and not with the number of
  results. A select is not an index.
- Only the StorageServer with ETS tables supports the function. Its
  handle has the feature `select`. On a StorageServer without the
  feature, a select on the node of the StorageServer raises
  `{unsupported, select}`.
- FoundationDB and erlfdb have no such function. This part is not in the
  paper.

A benchmark on 2026-11-23 compared three ways to evaluate a match spec
on a range of an ETS table of the kind `ordered_set` with 1,000,000 rows
of the StorageServer, of which 1 in 100 matches. The benchmark is not
part of this repository. The owner of the project chose the walk.

| Keys | Keys of the range | `ets:select/2` with guards for the range (us) | `ets:select/2` with a bound prefix of the key (us) | Walk with `ets:next_lookup/2` (us) |
|---|---|---|---|---|
| Integers | 100 | 19,796 | Not possible | 6 |
| Integers | 10,000 | 20,044 | Not possible | 567 |
| Integers | 250,000 | 20,003 | Not possible | 13,389 |
| Tuples, 2 prefixes of 500,000 keys | 100 | 20,671 | 10,755 | 8 |
| Tuples, 2 prefixes of 500,000 keys | 10,000 | 20,945 | 10,787 | 803 |
| Tuples, 2 prefixes of 500,000 keys | 250,000 | 20,995 | 10,880 | 21,052 |

A select with guards reads every row of the table, and a select with a
bound prefix reads every row of the prefix. The walk reads the rows of
the range.

### Mapped range

`deadlift:get_mapped_range/4,5` is `erlfdb:get_mapped_range/4,5`, which
is `getMappedRange` of FoundationDB 7.3. The function reads a key range,
and the StorageServer performs a second read for each row of the range.
An application reads the rows of an index and the record of each row in
one request:

```erlang
%% The key of an index row is {<<"index">>, City, Id}, and the key of a
%% record is {<<"user">>, Id}. Both are packed with deadlift_tuple.
Mapper = {<<"user">>, <<"{K[2]}">>, <<"{...}">>},
{Begin, End} = deadlift_tuple:range({<<"index">>, <<"Oslo">>}),
deadlift:transactional(Db, fun(Tx) ->
    deadlift:get_mapped_range(Tx, Begin, End, Mapper, [{limit, 100}])
end).
%% [{{IndexKey, IndexValue}, {SecondBegin, SecondEnd}, [{UserKey, User}]}, ...]
```

The mapper describes the key of the second read. deadlift has two kinds
of mapper:

| Mapper | Argument of `get_mapped_range` | Status |
|---|---|---|
| The mapper of FoundationDB: a tuple of the tuple layer | A tuple, which the client packs as erlfdb does, or a binary with the packed tuple | Supported |
| A mapper that is an Erlang term | `{term, Term}` | Reserved. The call raises `{unsupported, term_mapper}`. deadlift is free to define this mapper later, because FoundationDB has none |

The elements of the mapper of FoundationDB:

| Element of the mapper | Element of the mapped key |
|---|---|
| `{K[i]}` | Element `i` of the key of the row, from 0. The key must be a packed tuple |
| `{V[i]}` | Element `i` of the value of the row, from 0. The value must be a packed tuple |
| `{...}`, as the last element | None. The second read is a read of every key that starts with the mapped key. Without it, the second read is a read of the mapped key |
| A string with `{{` or `}}` | The string with `{{` replaced by `{` and `}}` replaced by `}` |
| Another element | The element itself |

- The result has one element for each row of the range, in the order of
  the keys: the row, the key range of the second read, and the rows that
  the second read found. A row whose second read finds nothing has an
  empty list. The type is `mapped_kv()` of erlfdb.
- The second reads use the read version of the transaction.
- The transaction gets a read conflict range for the range and one for
  each second read.
- The function does not return the writes of its transaction. It raises
  2039 if the transaction has written or cleared a key of the range or
  of a second read. It raises 2108 for a snapshot. FoundationDB has both
  rules.
- The errors of the mapper have the codes of FoundationDB: 2030
  (`mapper_bad_index`), 2032 (`mapper_bad_range_decriptor`), 2041
  (`key_not_tuple`), 2042 (`value_not_tuple`) and 2043
  (`mapper_not_tuple`).
- The options are a list. `{limit, N}` returns the first `N` rows of the
  range. A caller continues after the key of the last row.
- [src/deadlift_tuple.erl](src/deadlift_tuple.erl) is the tuple layer:
  a copy of `erlfdb_tuple` of erlfdb 1.2.3 (Apache License 2.0).
- The handle of the StorageServer has the feature `mapped_range`.

Differences from FoundationDB 7.3.62 and erlfdb 1.2.3:

- A mapper without `{...}` returns every row with its key, its value and
  the result of the second read. The StorageServer of FoundationDB
  returns the key and the value of the first and of the last row only,
  and the bindings for C and for Erlang decode only the result of a
  mapper with `{...}`.
- A second read of a prefix has no limit. FoundationDB returns the first
  2,000 rows of it.
- The read has no limit of bytes. FoundationDB ends a reply at 80,000
  bytes.
- A mapper with `{...}` as the only element raises 2032. The
  StorageServer of FoundationDB fails an assertion.
- The function takes keys and a transaction. erlfdb also takes key
  selectors and a database, and has the options `reverse`, `wait`,
  `target_bytes` and `streaming_mode`.

### The Proxy

The process of an epoch commits a batch of transactions in five steps
(paper, sections 2.4.1 and 2.4.3):

1. When the process closes a batch, it assigns an LSN, with the last LSN
   as the previous LSN. This is the work of the Sequencer.
2. The process checks the read version, the read keys, the read ranges
   and the write keys of each transaction of the batch. This is the work
   of the Resolver. If the check rejects a transaction, the process
   answers the client of that transaction with an error.
3. The process sends one record to every LogServer of the epoch. The
   record has the writes of the admitted transactions, in the order of
   the batch. The k LogServers that replicate the record receive the
   body.
4. When every LogServer of the epoch has answered, the process raises its
   KCV and its read version to the LSN.
5. The process then answers the clients of the admitted transactions.

The Proxy also sends the record when the Resolver admitted no transaction
of the batch. A LogServer stores a record only after the record with the
previous LSN, so every LSN must reach the LogServers.

#### Batches

The Proxy collects the transactions of a short time into one batch
(paper, section 2.4.1). A batch has one commit version and is one log
record.

| Rule | In deadlift | In the FoundationDB source |
|---|---|---|
| A batch closes when no earlier batch is in progress | Yes, with `batch_idle` (default `true`) and `batch_depth` (default 1). A transaction at an idle Proxy does not wait, and the next batch has the transactions that arrive while a batch is in progress | No. This part is not in the paper |
| A batch closes after a time | `batch_ms`, default 1 ms, fixed. With the rule above, this time is an upper limit | A time that changes with the load (`commitBatcher` in `fdbserver/CommitProxyServer.actor.cpp`) |
| A batch closes at a number of transactions | `batch_max`, default 1,000 | `COMMIT_TRANSACTION_BATCH_COUNT_MAX` |
| A batch closes at a number of bytes | No limit | A limit that depends on the number of CommitProxies |
| One commit version for each batch | Yes | Yes (`preresolutionProcessing`) |
| The Resolver checks a transaction against the earlier admitted transactions of its batch | Yes | Yes (`checkIntraBatchConflicts` in `fdbserver/SkipList.cpp`) |
| The writes of a rejected transaction give no conflict | Yes | Yes |

- The index of a transaction is its position in the batch, from 0.
  `deadlift_epoch:commit_with_index/3` returns the LSN and the index. The
  pair of the LSN and the index gives the order of all committed
  transactions.
- When two transactions of a batch write one key, the StorageServer keeps
  the value of the transaction with the higher index.
- The rules in the table are taken from the named functions of
  FoundationDB 7.3. The default values of the knobs of FoundationDB are
  not taken from the source.

Because of step 4, the Sequencer knows that an LSN is committed before any
client does. A client that starts after an answer gets a read version at
or above that LSN.

The Proxy waits for every LogServer, and not only for the k LogServers
with the body. The config `RecoveryAckReplicasOnly` of the model shows
that a Proxy that waits for k LogServers lets a recovery discard an
acknowledged record.

If the Resolver, a LogServer or the Sequencer answers with an error or is
not alive, the Proxy answers `{error, commit_unknown_result}` to every
client that waits, and stops. The Proxy of an ended epoch stops in this
way at its next commit.

### Read versions

A Proxy gives a read version in three steps. The rule is from
`getLiveCommittedVersion` in `fdbserver/GrvProxyServer.actor.cpp` and
`confirmEpochLive` in `fdbserver/TagPartitionedLogSystem.actor.cpp` of
FoundationDB 7.3. The paper does not state it.

1. The process of the epoch takes its read version.
2. At the same time, it asks every LogServer of the epoch that runs
   whether the epoch is live. A LogServer confirms until a Sequencer of a
   later epoch locks it.
3. When the Sequencer and k LogServers have answered, the Proxy answers
   the client. k is the replication degree.

The requests of several clients share these messages. The requests that
arrive while the Proxy waits for the answers of one confirmation wait,
and then share the next confirmation. A request does not use a
confirmation that started before the request arrived. The option
`read_version_share => false` gives each request its own messages.

A recovery needs answers to its lock from more than m - k of the m
LogServers of the epoch, so at least one LogServer that confirmed is among
them. That LogServer answered the lock after the client asked. A commit of
a later epoch is therefore acknowledged after the client asked. The read
version is at or above the commit version of every transaction that was
acknowledged before the client asked, in any epoch.

The Proxy stops in two cases:

- A LogServer answers that it is stopped. The epoch has ended.
- More than m - k LogServers of the epoch do not run. The Proxy cannot
  confirm the epoch, and the epoch cannot commit.

### The StorageServer

A StorageServer reads the records from the LogServers and applies their
writes to a store with versions (paper, sections 2.4.3 and 2.4.4).

- The StorageServer has a version: the LSN of the last record that it has
  applied. It applies a record only when the previous LSN of the record is
  its version. The records of all epochs form one chain, because the
  previous LSN of a recovery transaction is the Recovery Version.
- The StorageServer has one request at each LogServer of one epoch. A
  LogServer answers when it has a new record, or when a Sequencer locks
  it. The StorageServer applies a record when it has received the body
  from one of the k LogServers that have it. Every 5 s, the
  StorageServer sends each request again, because a message can be lost.
- The StorageServer keeps the keys in their order, which is the order of
  Erlang terms. A read or a clear of a key range examines only the keys
  of the range. See "The store of the StorageServer" below.
- A transaction has mutations: `{set, Key, Value}`, `{clear, Key}` and
  `{clear_range, Begin, End}`. The StorageServer applies the mutations of
  a transaction in their order. The clear of a key range is a write range
  for the Resolver, so a transaction that read a key of the range has a
  conflict.
- A read at a version returns the last write to the key at or below that
  version. The read waits until the StorageServer has the version.
- The StorageServer keeps the versions of the MVCC window, which is
  5,000,000 versions by default (paper, section 2.4.5). For each key it
  keeps the versions above its oldest version and the last version at or
  below the oldest version. A read below the oldest version returns
  `{error, transaction_too_old}`.
- The oldest version is the lower one of two versions: the version of the
  StorageServer minus the window, and the highest Known Committed Version
  that a LogServer has sent with its records. No recovery discards a
  record at or below a Known Committed Version, so a rollback needs no
  version that the StorageServer has removed.
- The StorageServer reads the log systems of all epochs from the
  coordinated state, at its start and when a LogServer answers that it is
  stopped.

The StorageServer applies records that are not yet stored on every
LogServer. A recovery can discard such a record. When the StorageServer
learns that an epoch has ended, it removes every write above the end
version of the epoch. This is the rollback of the paper. A read at a read
version from a Proxy is not affected, because a read version is the LSN of
a record that no recovery discards.

#### Several StorageServers

Several StorageServers can run at the same time, on one Erlang node or on
several. This is the replication of the whole key space.

- Each StorageServer has every key. Each one reads every record from the
  LogServers, and has its own version, its own oldest version, its own
  ETS tables and its own rollbacks.
- The StorageServers send no message to each other. A LogServer keeps one
  request for each StorageServer.
- A read version is one version for the whole database. At a read
  version, each StorageServer returns the same result, or waits for the
  version, or answers that the read version is too old.
- The client chooses the StorageServer of a read. See "The client".
- A StorageServer on another Erlang node reads the process of each
  LogServer from the coordinated state. See "LogServer processes".

FoundationDB divides the key space into shards. Each shard has a team of
StorageServers, and each mutation has the tags of the team of its key
(paper, section 2.4.3). deadlift has one shard with the whole key space.
The team is the list of StorageServers that the caller of
`deadlift:open/1` names.

### LogServer processes

The entry of a LogServer in a log system of the coordinated state is
`{{Epoch, Index}, Pid}`: an identifier, and the process that the recovery
started.

- The process of an epoch has the processes of its own LogServers,
  because its recovery started them.
- A recovery reads the coordinated state at its start, and so has the
  processes of the LogServers of the previous epoch.
- A StorageServer reads the log systems of all epochs from the
  coordinated state, and so has the process of each LogServer.

No part of deadlift looks up a LogServer by a name, and no part waits
for a name. FoundationDB stores the interface of each LogServer in the
coordinated state in the same way.

Until 2026-11-23, a registry process on each Erlang node had the names
of the LogServers, and the registries exchanged the names in messages. A
recovery and a Proxy waited, with a limit of time, for names that had
not arrived. The registry was removed when the Sequencer, the Resolver
and the Proxy became one process.

### The ClusterController

A node that can run roles has one worker
([deadlift_worker](src/deadlift_worker.erl)). A node can also run one
candidate for the ClusterController
([deadlift_cluster_controller](src/deadlift_cluster_controller.erl)).

Election:

1. The coordinated state has the term and the process of the
   ClusterController.
2. A candidate reads the coordinated state every `poll_ms` (default
   1,000 ms) and monitors the ClusterController of the term.
3. If the term has no ClusterController, or the monitor of the
   ClusterController has ended, the candidate proposes itself with the
   term that it read.
4. The coordinated state accepts one proposal for a term. The candidate
   of that proposal is the leader of the next term.
5. The leader reads the coordinated state every `poll_ms`. If the
   coordinated state has another term, the leader is a candidate again.

Two processes can be the leader at the same time, for at most `poll_ms`
plus the time of a read. Each of them can recruit a Sequencer. The
recovery count of the coordinated state then lets one Sequencer write, as
for any two recoveries at the same time. FoundationDB has the same
property (`design/recovery-internals.md`).

Recruitment:

1. Each worker reads the ClusterController from the coordinated state
   and registers with it. A worker has the class `log`, `stateless` or
   `unset`.
2. The leader recruits when it has no epoch and at least `min_workers`
   workers (default 1). A cluster of several nodes sets `min_workers` to
   2 or more. With the default, the leader recruits when the first
   worker has registered, and every LogServer of the first epoch is on
   that worker.
3. With several workers, each LogServer of an epoch is on a worker of
   its own: an epoch has `logs` LogServers, or one for each worker if
   the leader has fewer workers, and `replication` is at most the number
   of LogServers. Before 2026-11-23 the leader used a worker for two
   LogServers when it had fewer workers than `logs`, and the stop of one
   node could then end every LogServer with a record. With one worker,
   every LogServer is on that worker. The leader prefers the class `log`
   for a LogServer. It chooses a worker for the process of the
   epoch, and prefers the class `stateless`. Each attempt starts at a
   different worker.
4. The leader starts the process of the epoch on its worker. The process
   performs the recovery, and starts the LogServers on their workers.
5. The leader sends the epoch and the process of the epoch to every
   worker. `deadlift_worker:db_info/0` returns them on each node with a
   worker.

The leader monitors the process of the epoch and the LogServers. When
one of them stops, or the connection to its node ends, or the
recovery fails, the leader recruits again after `retry_ms` (default
500 ms). The leader also recruits again when the recovery count of the
coordinated state is above its epoch, because another Sequencer has then
started a recovery.

A worker sends its registration every `poll_ms`, and the leader answers
with the epoch, the Sequencer and the Proxy. A worker that missed a
message receives them within `poll_ms`. A worker, a candidate and the
leader read the coordinated state in a separate process, so each of them
answers its requests while a read waits.

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
- FoundationDB keeps the commit versions near the time of the wall clock.
  deadlift raises the commit version with the elapsed time only.
- A Proxy of deadlift confirms the epoch for each request of a read
  version. FoundationDB confirms once for a batch of requests.
- A Proxy of deadlift stops when a LogServer answers that it is stopped.
  A proxy of FoundationDB does not answer the request, and stops when it
  learns of the recovery.
- The Sequencer of deadlift sends the recovery transaction itself, after
  the write of the coordinated state. The master of FoundationDB sends it
  through a proxy, before the write of the coordinated state.
- The StorageServer keeps the data in memory. A StorageServer of
  FoundationDB keeps the versions of the MVCC window in memory and writes
  older data to disk. This part is not in the paper.
- The rule for the oldest version of the StorageServer is not taken from
  the FoundationDB source.
- Each StorageServer has every key. FoundationDB divides the key space
  into shards, and each shard has a team of StorageServers.
- The client selects the first StorageServer of a read with a hash of the
  read version. The client of FoundationDB selects a StorageServer from a
  model of the latency of each one, and sends a second request to
  another one when the first is slow.
- The StorageServer reads the log systems from the coordinated state. A
  StorageServer of FoundationDB receives them from the ClusterController.
  This part is not in the paper.
- A key is an Erlang term, and the order of the keys is the order of
  Erlang terms. A key of FoundationDB is a byte string.
- A transaction has three mutations: the set of a key, the clear of a
  key and the clear of a key range. FoundationDB has more, for example
  atomic operations and versionstamps.
- One Resolver handles every key. FoundationDB divides the key space over
  several Resolvers.
- The coordinated state stores the ClusterController, and a candidate
  detects the failure of the ClusterController with an Erlang monitor.
  FoundationDB elects the ClusterController with a register on each
  coordinator, which is not a consensus operation, and the
  ClusterController sends a heartbeat to the coordinators
  (`fdbserver/LeaderElection.actor.cpp`). This part is not in the paper.
- The Sequencer, the Resolver and the Proxy of an epoch are one process.
  See "One process for an epoch".
- The Sequencer performs the recovery, as in the paper. In FoundationDB
  7.3 the recovery runs in the process of the ClusterController.
- The time of a batch is fixed, and a batch has no limit in bytes. The
  time of a batch of FoundationDB changes with the load.
- The Proxy chooses the LogServers that receive the body of a record from
  the LSN. FoundationDB chooses them from the tags of the record.

## Start

A node runs deadlift from the environment of the application. The
application starts a worker and a StorageServer if the environment has
their options.

| Key of the environment | Value | Process |
|---|---|---|
| `worker` | The options of `deadlift_worker:start_link/1`: `coordinators`, `class`, `candidate` for a candidate for the ClusterController on the node, and `name` for a node that runs the workers of several clusters | The worker of the node. With `candidate`, the worker starts the candidate, and the two processes stop together |
| `storage` | The options of `deadlift_storage:start_link/1`: `coordinators` | The StorageServer of the node |
| `coordinator` | The options of `deadlift_coordinator:start_link/1`: `id` and `dir`, both required | The coordinator of the node: one server of the peace cluster with the coordinated state. The supervisor starts it before the worker and the StorageServer |

Example, in `sys.config`, for a node that runs every role:

```erlang
[
    {deadlift, [
        {worker, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}], candidate => #{}}},
        {storage, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}]}}
    ]}
].
```

`coordinators` are the servers of a peace cluster that runs
`deadlift_cstate:machine/0`.

### Coordinators

A coordinator keeps the coordinated state on a disk, as a coordinator of
FoundationDB does. [deadlift_coordinator](src/deadlift_coordinator.erl)
starts a server of peace with the write-ahead log of peace
(`peace_storage_wal`), and the option `dir` is required:

```erlang
{deadlift, [
    {coordinator, #{id => {c1, 'a@host'}, dir => "/var/lib/deadlift/coordinator"}},
    {worker, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}], candidate => #{}}}
]}
```

1. Start the application on each node with a coordinator.
2. Call `deadlift_coordinator:form([{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}])`
   once. peace stores the configuration of the cluster on the disk of
   each coordinator.

- A coordinator that starts again reads its data from `dir`, and is the
  same member of the cluster as before. The second step is not repeated.
- Without `dir`, the coordinator does not start, and the error is
  `{missing_option, dir}`. The options `storage` and `machine` are not
  allowed.
- A peace cluster that a caller starts with `peace:start_cluster/3` can
  still be named in `coordinators`. The tests of deadlift do this with
  the storage in memory.

### Start time

The times of the steps of a start of one node, with all roles on the
node, measured on 2026-11-23 in 3 to 5 runs:

| Step | Before | After |
|---|---|---|
| The peace cluster has a leader | 1,290 ms to 1,538 ms | 0 ms to 1 ms with `peace:trigger_election/1`, and 1,010 ms to 1,284 ms without it |
| The worker knows the ClusterController and is registered | 1,002 ms to 1,004 ms | 11 ms to 13 ms |
| The first epoch | 0 ms to 6 ms | 0 ms to 6 ms |
| The first commit | 0 ms to 2 ms | 0 ms to 1 ms |
| The first read | 493 ms to 500 ms | 0 ms to 25 ms |
| The whole start | 2,793 ms to 3,061 ms | 35 ms to 46 ms with `peace:trigger_election/1` |

- A worker without an answer of a ClusterController, a candidate without
  an answer of the coordinated state, and a StorageServer without a log
  system read the coordinated state after 10 ms, and double the time
  with each read up to their poll interval. Before, each of them waited
  for its poll interval, which is 1,000 ms for the worker and 500 ms for
  the StorageServer.
- A peace cluster has no leader until an election timeout has passed.
  `peace:trigger_election/1` starts an election at once. A caller that
  starts a peace cluster for deadlift calls it after
  `peace:start_cluster/3`.
- The same early reads shorten the time after a recovery in which a
  read of a client waits: a StorageServer follows the epoch after a
  recovery within some 10 ms. Before, it followed at its poll interval.

On such a node, `deadlift:open/0` returns a database:

```erlang
Db = deadlift:open(),
ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, key, value) end).
```

- The Proxy of the database is the process of the current epoch, from
  the information that the worker of the node has.
- The StorageServer of the database is the StorageServer of the node. A
  read uses its ETS tables in the process of the caller.
- `deadlift:open/1` takes the same options as before, and each option
  replaces a default. For example, `deadlift:open(#{profile => ets})`
  uses the profile for the ETS StorageServer.

## Verification

| Level | What it checks | Command | Time |
|---|---|---|---|
| Unit tests | The coordinated state machine, alone and on a peace cluster of three servers on one node. One LogServer: the order of the LSNs, the KCV, repeated records and the lock. The recovery on a peace cluster: the first epoch, a second epoch with records to copy, 1 and 2 LogServers down, and two recoveries at the same time. The Proxy: commits in sequence, 50 commits at the same time, a recovery after commits, a LogServer down and the Sequencer down | `make test` | 3 seconds |
| Simulation | The LogServer, the recovery, the Sequencer, the Resolver, the Proxy and the StorageServer under the scheduler and the simulated network of eta, with kills, partitions, and lost and delayed messages | `make dst` | about 3 minutes |
| Model checking | The logging protocol, the recovery, the read versions, the StorageServer and the Resolver, with 2 epochs | `make tlc` | about 5 minutes |
| Model checking | The logging protocol, the recovery and the StorageServer, with 3 epochs | `make tlc-long` | about 4 hours |

The models have 10 planted bugs. TLC finds a counterexample for each. See
[formal/README.md](formal/README.md).

### Simulation

`make dst` runs the system under the scheduler and the simulated network
of eta. The harness is [dst/deadlift_dst.erl](dst/deadlift_dst.erl).

The simulated system has one process for the coordinated state
([deadlift_cstate_local](src/deadlift_cstate_local.erl)), or a peace
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
| `body_available` | The body of every acknowledged record is on a LogServer of its epoch that runs |
| `copied` | Every LogServer of a new epoch has the body of every acknowledged record above the previous epoch's end version |
| `no_conflict` | No acknowledged transaction read a key that another acknowledged transaction wrote between the read version and the commit version of the first one |
| `read_version_order` | A read version is at or above the commit version of every transaction that was acknowledged before the client asked for it, in any epoch |
| `read_value` | A read of a key at a read version returns the write of the acknowledged transaction with the highest commit version at or below the read version, or a later write at or below the read version whose client received no answer |
| `storage_rollback` | When a StorageServer has a version of an epoch, it has no write between the end version of an earlier epoch and the first version of the epoch after that one |
| `replicas_agree` | Two StorageServers have the same writes for each key, for the versions that both have applied and that are above the oldest version of both |
| `select_agrees` | A `deadlift:select/4` of a client returns the result of the match spec on the result of `get_range/3` at the same read version |
| `mapped_range_agrees` | A `deadlift:get_mapped_range/4` of a client returns the rows of `get_range/3` at the same read version, each with the result of its second read. The client uses a mapper with a second read of a key, and a mapper with a second read of a prefix |
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

#### Runs with a peace cluster

In the runs above, the coordinated state is one `deadlift_cstate_local`
process. The tests with the name `peace_` run a peace cluster of 3
servers in its place, under the scheduler and the network of eta:

- peace is compiled with the parse transform of eta in the `dst`
  profile (`overrides` in [rebar.config](rebar.config)).
- Each server is on a simulated node of its own. The cluster has
  majority quorums.
- Each server is a coordinator with the configuration of
  [deadlift_coordinator](src/deadlift_coordinator.erl): it keeps its data
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
- [dst/deadlift_dst_machine.erl](dst/deadlift_dst_machine.erl) is the
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

- The application start (`deadlift_sup`) and `deadlift:open/0` are not
  in the simulation. Unit tests on two nodes cover them.
- `deadlift:transactional/2` is not in the simulation. A client calls
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
nodes at the same time ([dst/run.sh](dst/run.sh)). The runs without faults
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

## Benchmark

`make bench` runs [bench/deadlift_bench.erl](bench/deadlift_bench.erl). The
benchmark is not a test. It has no limits and does not check the results
of the transactions.

The cluster of the benchmark:

- 3 Erlang nodes with a worker of the class `log`, for the LogServers;
- 3 Erlang nodes with a worker of the class `stateless`. One of them has
  the process of the epoch. 2 of them have a candidate for the
  ClusterController;
- 1 Erlang node with a StorageServer;
- the node of the benchmark, with the peace cluster of 3 servers and the
  clients.

Each of the 7 nodes has 2 scheduler threads. Every role has its default
options. A client is one process that runs one operation after another.
A value has 100 bytes.

| Operation | Description |
|---|---|
| `write` | A transaction that sets one key, without a read |
| `read_write` | A transaction that reads one key by message and sets one key, with 1,000,000 keys |
| `conflict` | `read_write` with 100 keys. Each key then has several hundred versions in the MVCC window |
| `read_version` | One read version from the Proxy |
| `read_after_write` | A client commits a write and reads the key at the commit version. The row has the time of the read, which ends when the StorageServer has applied the record |
| `select_by_message`, `select_direct` | `deadlift:select/4` on a range of 1,000 of 100,000 rows with values of 1,000 bytes, of which 10 match, from the node of the benchmark and on the node of the StorageServer |
| `filter_by_message`, `filter_direct` | The same result with `get_range/3` and the match spec in the client |
| `mapped_by_message`, `mapped_direct` | `deadlift:get_mapped_range/4` on 100 index rows of 10,000, each with a second read of one record with a value of 1,000 bytes, from the node of the benchmark and on the node of the StorageServer |
| `join_by_message`, `join_direct` | The same result with `get_range/3` for the index rows and one `get_range/3` for each record |
| `read_by_message` | The read of one key at a read version that the client has, from the node of the benchmark |
| `read_direct` | The same read by a client on the node of the StorageServer, from the ETS table |

Results of one run on 2026-11-23, with Erlang/OTP 28 and 3 seconds for
each row. In three runs of an earlier version on the same day, the
operations per second and the p50 of each row differed by at most 10%.
The p99.9 differed by at most 23% in the rows with a p99.9 of 200 us or
more. The machine has 4 performance cores and 6 efficiency cores.

| Operation | Clients | Operations per second | p50 (us) | p99 (us) | p99.9 (us) | Attempts per operation |
|---|---|---|---|---|---|---|
| `write` | 1 | 10980 | 91 | 115 | 152 | 1.00 |
| `write` | 4 | 25980 | 150 | 231 | 301 | 1.00 |
| `write` | 16 | 57208 | 276 | 407 | 484 | 1.00 |
| `write` | 64 | 103292 | 607 | 910 | 1053 | 1.00 |
| `write` | 256 | 119996 | 2084 | 3293 | 3899 | 1.00 |
| `read_write` | 1 | 4072 | 232 | 443 | 615 | 1.00 |
| `read_write` | 4 | 10020 | 365 | 836 | 1084 | 1.00 |
| `read_write` | 16 | 22785 | 700 | 955 | 1078 | 1.00 |
| `read_write` | 64 | 38056 | 1661 | 2375 | 2615 | 1.00 |
| `read_write` | 256 | 45404 | 5596 | 7396 | 8096 | 1.00 |
| `conflict` | 64 | 24076 | 1116 | 24496 | 58496 | 1.20 |
| `read_version` | 1 | 15045 | 67 | 91 | 104 | 1.00 |
| `read_version` | 4 | 42210 | 93 | 143 | 171 | 1.00 |
| `read_version` | 16 | 105178 | 149 | 232 | 298 | 1.00 |
| `read_version` | 64 | 156377 | 411 | 646 | 889 | 1.00 |
| `read_version` | 256 | 159358 | 1573 | 2167 | 2442 | 1.00 |
| `read_after_write` | 1 | 8091 | 30 | 45 | 66 | 1.00 |
| `read_after_write` | 16 | 37800 | 121 | 254 | 310 | 1.00 |
| `read_by_message` | 1 | 39599 | 25 | 32 | 42 | 1.00 |
| `read_by_message` | 16 | 169207 | 92 | 153 | 192 | 1.00 |
| `read_by_message` | 64 | 177140 | 361 | 572 | 665 | 1.00 |
| `read_direct` | 1 | 618957 | 2 | 2 | 3 | 1.00 |
| `read_direct` | 2 | 1104973 | 2 | 3 | 15 | 1.00 |
| `read_direct` | 16 | 1031356 | 2 | 744 | 1006 | 1.00 |
| `select_by_message` | 1 | 3446 | 289 | 375 | 404 | 1.00 |
| `select_by_message` | 16 | 6855 | 2309 | 3157 | 4796 | 1.00 |
| `filter_by_message` | 1 | 831 | 1197 | 1329 | 1398 | 1.00 |
| `filter_by_message` | 16 | 1429 | 11096 | 12096 | 12496 | 1.00 |
| `select_direct` | 1 | 5538 | 170 | 272 | 301 | 1.00 |
| `select_direct` | 2 | 7654 | 255 | 366 | 402 | 1.00 |
| `filter_direct` | 1 | 1467 | 677 | 834 | 870 | 1.00 |
| `filter_direct` | 2 | 2603 | 762 | 982 | 1049 | 1.00 |
| `mapped_by_message` | 1 | 2496 | 400 | 438 | 467 | 1.00 |
| `mapped_by_message` | 16 | 6554 | 2443 | 2658 | 3053 | 1.00 |
| `join_by_message` | 1 | 348 | 2899 | 3056 | 3355 | 1.00 |
| `join_by_message` | 16 | 1395 | 11396 | 12496 | 13196 | 1.00 |
| `mapped_direct` | 1 | 3717 | 269 | 293 | 313 | 1.00 |
| `mapped_direct` | 2 | 6870 | 291 | 331 | 356 | 1.00 |
| `join_direct` | 1 | 2612 | 384 | 421 | 457 | 1.00 |
| `join_direct` | 2 | 4480 | 446 | 507 | 598 | 1.00 |

The rows `mapped` and `join` are from a later run on 2026-11-23. With
one client on the node of the benchmark, a mapped range read takes 400
us, and the same reads by the client take 2,899 us, because the client
sends 101 requests. On the node of the StorageServer the times are 269
us and 384 us.

The steps of a `write` batch in the Proxy, as the mean time of a batch:

| Clients | Transactions per batch | Wait for the close (us) | Conflict check (us) | LogServers (us) |
|---|---|---|---|---|
| 1 | 1.0 | 0 | 3 | 43 |
| 4 | 1.8 | 23 | 6 | 61 |
| 16 | 5.7 | 44 | 19 | 77 |
| 64 | 21.1 | 64 | 74 | 128 |
| 256 | 66.5 | 145 | 294 | 260 |

The longest single time of each step:

| Clients | Wait for the close (us) | Conflict check (us) | LogServers (us) |
|---|---|---|---|
| 1 | 9 | 27 | 181 |
| 4 | 124 | 108 | 165 |
| 16 | 168 | 89 | 227 |
| 64 | 240 | 220 | 347 |
| 256 | 691 | 1270 | 992 |

The confirmations of the Proxy for `read_version`:

| Clients | Requests per confirmation | Time of a confirmation (us) |
|---|---|---|
| 1 | 1.0 | 31 |
| 4 | 1.7 | 36 |
| 16 | 5.3 | 46 |
| 64 | 10.9 | 66 |
| 256 | 12.6 | 72 |

Garbage collections and uninterrupted runs of a process of 2 ms or more, since the start of the cluster:

| Kind | Process | Count | Longest (ms) | Sum (ms) |
|---|---|---|---|---|
| long_gc | `deadlift_log` | 8 | 4 | 27 |
| long_schedule | `deadlift_storage` | 5 | 14 | 38 |
| long_gc | `deadlift_storage` | 4 | 13 | 40 |
| long_schedule | `deadlift_log` | 2 | 3 | 6 |

The default options of the ClusterController (`retry_ms` 500, `poll_ms` 1000):

| Killed process | Time without an acknowledged commit (ms) | Epoch before | Epoch after |
|---|---|---|---|
| log | 604 | 1 | 2 |
| proxy | 562 | 2 | 3 |
| cluster_controller | 12 | 3 | 4 |

`retry_ms` 35:

| Killed process | Time without an acknowledged commit (ms) | Epoch before | Epoch after |
|---|---|---|---|
| log | 58 | 1 | 2 |
| proxy | 74 | 2 | 3 |
| cluster_controller | 10 | 3 | 4 |

In the two tables of kills, 8 clients write, and the time is the longest
time without an acknowledged commit after the kill of one process.
`retry_ms` 35 is `ATTEMPT_RECRUITMENT_DELAY` of FoundationDB.

### Changes that the benchmark led to

The first run of the benchmark, on the same day, had these results. Six
changes followed. Before the last change, the Sequencer, the Resolver and
the Proxy were three processes, on three nodes or on one node.

| Measurement | First run | Three processes on three nodes | Three processes on one node | One process |
|---|---|---|---|---|
| `write`, 1 client, p50 | 2,002 us | 170 us | 91 us | 90 us |
| `write`, 1 client, p99.9 | 4,334 us | 251 us | 308 us | 166 us |
| `write`, 64 clients, p99.9 | 9,924 us | 1,254 us | 1,017 us | 1,069 us |
| `write`, 256 clients, p99.9 | 30,591 us | 4,596 us | 3,454 us | 3,720 us |
| `write`, 64 clients, operations per second | 11,136 | 83,920 | 108,514 | 101,930 |
| `write`, 256 clients, operations per second | 10,193 | 106,725 | 119,415 | 118,271 |
| `read_write`, 1 client, p50 | 5,138 us | 329 us | 237 us | 238 us |
| `read_version`, 1 client, p50 | 78 us | 76 us | 69 us | 68 us |
| `read_version`, 256 clients, operations per second | 50,408 | 146,064 | 156,508 | 157,230 |

| Change | Reason |
|---|---|
| The Proxy closes a batch at once when no batch is in progress, and collects the next batch while one is in progress | A commit at an idle Proxy waited 1 ms to 2 ms for the batch timer |
| The history of the Resolver is two ETS tables. The second table has the entries in the order of their versions, so the Resolver reads only the entries that it removes | The Resolver examined every entry of its history for each batch, to remove the old entries. With 16 clients, a batch waited 2.5 ms for the Resolver, and with 256 clients 24 ms. The history was also on the heap of the Resolver process, which had garbage collections of up to 29 ms |
| The requests for a read version that arrive during a confirmation share the next confirmation | Each request had its own 4 messages from the Proxy |
| The Sequencer, the Resolver and the Proxy of an epoch are one process | A commit had 4 round trips between the three processes, and the option that placed them on one node showed that the round trips between nodes were half of the time of a write. The purpose of the change is a simpler design: see "One process for an epoch" |
| The LogServer keeps its records in an ETS table | The records were on the heap of the LogServer process. The LogServers had 163 garbage collections of 2 ms or more in one run, the longest 90 ms, and the longest wait of a batch for the LogServers was 38 ms. After the change it is 0.4 ms |

`bench/deadlift_bench.erl` also has `depth/1`, which measures `write`
for several values of `batch_depth`, the number of batches in progress
below which the Proxy closes a batch at once. In one run of 2 seconds for
each row, 256 clients committed 99,997, 86,107, 65,014 and 67,672
transactions per second with the values 1, 2, 4 and 8. The run was
before the change to one process, with the three roles on one node. The
default is 1.

Observations:

- A `write` of one client takes 90 us: 3 us for the conflict check,
  44 us for the LogServers, and the round trip of the client to the
  process of the epoch.
- The p99.9 of `write` is 0.2 ms to 3.7 ms, and one process of the
  cluster had one garbage collection of 2 ms or more. Before the changes
  of the LogServer and of the Resolver, the p99.9 was up to 85 ms.
- The p99.9 of `conflict` is 59 ms. A client waits before it runs a
  transaction again after a conflict. See "The versions of a hot key"
  below.
- An earlier version of the benchmark kept each latency in a list, and
  the garbage collections of its own processes were in the results.
- After the kill of a LogServer or of the process of the epoch, the time
  without a commit is `retry_ms` plus 50 ms to 330 ms in three runs.
- After the kill of the ClusterController, the old epoch accepts commits
  until the process of the new epoch stops its LogServers. The time
  without a commit was 5 ms, 10 ms and 616 ms in three runs.

### The history of the Resolver

A benchmark on 2026-11-23 compared three structures for the history of
the Resolver. The benchmark is not part of this repository. Each run has
batches of 32 transactions with one read and one write of 1,000,000
keys, 12 seconds of versions, and an MVCC window of 5 seconds. The three
structures gave the same result for every transaction.

| Structure | Writes per second | Mean of a batch (us) | p99.9 (us) | Longest batch (us) | Garbage collections of 2 ms or more | Longest collection (ms) |
|---|---|---|---|---|---|---|
| A `gb_trees` tree, rebuilt at an interval | 100,000 | 168 | 6,491 | 88,578 | 131 | 31 |
| A `gb_trees` tree and a `gb_sets` set of the versions, with a removal for each batch | 100,000 | 287 | 5,855 | 51,044 | 170 | 50 |
| Two ETS tables of the kind `ordered_set`, with a removal for each batch | 100,000 | 117 | 174 | 316 | 0 | 0 |
| A `gb_trees` tree, rebuilt at an interval | 20,000 | 137 | 12,890 | 15,020 | 21 | 7 |
| A `gb_trees` tree and a `gb_sets` set of the versions, with a removal for each batch | 20,000 | 212 | 3,814 | 10,324 | 8 | 9 |
| Two ETS tables of the kind `ordered_set`, with a removal for each batch | 20,000 | 94 | 141 | 165 | 0 | 0 |

The Resolver of deadlift has the third structure. Before this benchmark
it had the first one.

### The versions of a hot key

`conflict` has 100 keys. `deadlift_bench:conflicts/1` measures it for
several values of the client option `retry_delay_ms`, with the number of
each error code and the steps of a batch in the Proxy. One run on
2026-11-23:

| `retry_delay_ms` | Clients | Operations per second | p50 (us) | p99.9 (us) | Attempts per operation |
|---|---|---|---|---|---|
| 10 (default) | 16 | 13,661 | 696 | 23,838 | 1.07 |
| 10 (default) | 64 | 15,344 | 1,702 | 96,177 | 1.25 |
| 10 (default) | 256 | 9,487 | 9,087 | 1,135,943 | 1.83 |
| 0 | 16 | 14,114 | 1,041 | 3,457 | 1.11 |
| 0 | 64 | 13,842 | 3,444 | 27,709 | 1.46 |
| 0 | 256 | 9,295 | 20,866 | 152,838 | 2.53 |

- Every failed attempt had the error 1020 (not committed).
- The wait of the client after a conflict sets the p99.9. Without the
  wait, the p99.9 is lower and the p50 is higher, because more attempts
  fail.
- The operations per second do not rise with the clients and do not
  depend on the wait. A batch of the Proxy has 3 to 15 transactions and
  takes 0.3 ms, so the Proxy, the Resolver and the LogServers are not the
  limit.
- The limit was the StorageServer. It kept every version of a key in one
  ETS row. Each of the 100 keys has several hundred versions in the MVCC
  window of 5 seconds, and each read and each write copied the row.

A benchmark on 2026-11-23 compared three layouts for 100 keys with values
of 100 bytes. The benchmark is not part of this repository.

| Versions of each key | Layout | New version (ns) | Read at the newest version (ns) | Read at a version in the middle (ns) | Read by 8 processes (million per second) | Range of 100 keys at the newest version (us) |
|---|---|---|---|---|---|---|
| 1 | One row for each key | 334 | 180 | 126 | 9.14 | 12 |
| 1 | One row for each version | 209 | 165 | 158 | 8.51 | 23 |
| 1 | The newest version in the row of the key, the older versions in a second table | 286 | 123 | 112 | 8.73 | 10 |
| 16 | One row for each key | 1,244 | 809 | 795 | 0.93 | 73 |
| 16 | One row for each version | 305 | 204 | 182 | 7.77 | 34 |
| 16 | The newest version in the row of the key, the older versions in a second table | 434 | 132 | 246 | 8.33 | 8 |
| 256 | One row for each key | 16,208 | 9,997 | 10,260 | 0.06 | 847 |
| 256 | One row for each version | 385 | 226 | 240 | 8.49 | 40 |
| 256 | The newest version in the row of the key, the older versions in a second table | 419 | 123 | 286 | 9.00 | 7 |
| 1,024 | One row for each key | 72,936 | 40,765 | 46,746 | 0.01 | 2,924 |
| 1,024 | One row for each version | 330 | 281 | 272 | 10.62 | 28 |
| 1,024 | The newest version in the row of the key, the older versions in a second table | 491 | 102 | 345 | 9.73 | 8 |

The StorageServer of deadlift has the third layout since this benchmark.
`conflicts/1` after the change, in one run:

| `retry_delay_ms` | Clients | Operations per second before | Operations per second after | p99.9 before (us) | p99.9 after (us) |
|---|---|---|---|---|---|
| 10 (default) | 16 | 13,661 | 13,856 | 23,838 | 24,096 |
| 10 (default) | 64 | 15,344 | 22,167 | 96,177 | 59,596 |
| 10 (default) | 256 | 9,487 | 24,166 | 1,135,943 | 452,496 |
| 0 | 16 | 14,114 | 17,300 | 3,457 | 2,708 |
| 0 | 64 | 13,842 | 25,580 | 27,709 | 9,796 |
| 0 | 256 | 9,295 | 18,491 | 152,838 | 76,696 |

After the change, the attempts that fail and the wait of the client
after a conflict set the results: with 256 clients and no wait, a
transaction has 2.3 attempts.

The two profiles of the client, in one run of `conflicts/1`:

| Profile | Clients | Operations per second | p50 (us) | p99 (us) | p99.9 (us) | Attempts per operation |
|---|---|---|---|---|---|---|
| `default` | 16 | 13,942 | 676 | 10,896 | 25,996 | 1.07 |
| `default` | 64 | 22,870 | 1,183 | 24,796 | 57,696 | 1.21 |
| `default` | 256 | 24,346 | 2,973 | 104,096 | 441,096 | 1.56 |
| `ets` | 16 | 17,316 | 767 | 3,450 | 6,096 | 1.10 |
| `ets` | 64 | 26,001 | 1,705 | 10,896 | 24,696 | 1.35 |
| `ets` | 256 | 21,001 | 7,396 | 76,796 | 266,096 | 2.04 |

With 256 clients on 100 keys, the profile `ets` has fewer operations per
second than `default`, because more attempts fail. The other rows of the
benchmark use the profile `default`.

Limitations of the benchmark:

- All nodes share the processors of one computer, and a message between
  two nodes has no network delay.
- Every LogServer and the StorageServer keep their data in memory.
- The runs are on one computer. Three runs differ by at most 10% in the
  operations per second and in the p50.
- The clients run on the node of the benchmark, so the rows with many
  clients measure that node too.
- A kill stops one process. The node of the process continues.

## Commands

```
make compile    # compile
make test       # unit tests
make dst        # deterministic simulation with eta, in several nodes
make dst-serial # the same tests in one node
make bench      # the benchmark of a cluster of 8 Erlang nodes
make tlc        # the quick TLC configs
make tlc-long   # the 2 TLC configs with three epochs, about 4 hours
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
| [src/deadlift_sequencer.erl](src/deadlift_sequencer.erl) | The rules of the Sequencer for the versions, as functions |
| [src/deadlift_resolver.erl](src/deadlift_resolver.erl) | The conflict check of the Resolver, as functions on two ETS tables |
| [src/deadlift_epoch.erl](src/deadlift_epoch.erl) | The process of an epoch: the Sequencer, the Resolver and the Proxy |
| [src/deadlift_storage.erl](src/deadlift_storage.erl) | The StorageServer |
| [src/deadlift.erl](src/deadlift.erl) | The client, with the function names of erlfdb |
| [src/deadlift_idempotency.erl](src/deadlift_idempotency.erl) | The keys of the idempotency ids |
| [src/deadlift_mapper.erl](src/deadlift_mapper.erl) | The mapper of a mapped range read |
| [src/deadlift_tuple.erl](src/deadlift_tuple.erl), [src/deadlift_float.erl](src/deadlift_float.erl) | The tuple layer: copies of `erlfdb_tuple` and `erlfdb_float` of erlfdb 1.2.3 |
| [src/deadlift_cluster_controller.erl](src/deadlift_cluster_controller.erl) | The ClusterController: the election, the workers and the recruitment |
| [src/deadlift_worker.erl](src/deadlift_worker.erl) | The worker of one node, which starts roles for the ClusterController |
| [src/deadlift_coordinator.erl](src/deadlift_coordinator.erl) | A coordinator: a server of the peace cluster with the coordinated state on a disk |
| [src/deadlift_cstate_local.erl](src/deadlift_cstate_local.erl) | The coordinated state in one process, for tests and for the simulation |
| [include/deadlift_eta.hrl](include/deadlift_eta.hrl) | The header that puts a module under the parse transform of eta in the `dst` profile |
| [dst/deadlift_dst.erl](dst/deadlift_dst.erl) | The eta harness: the workload and the invariants |
| [dst/deadlift_dst_client.erl](dst/deadlift_dst_client.erl) | The processes that perform the operations of the simulation |
| [dst/deadlift_dst_tests.erl](dst/deadlift_dst_tests.erl) | The simulation suite |
| [dst/deadlift_dst_machine.erl](dst/deadlift_dst_machine.erl) | The machine of the peace cluster of a simulation run |
| [dst/deadlift_dst_nosync_io.erl](dst/deadlift_dst_nosync_io.erl) | A planted defect: file operations of a coordinator that never sync |
| [test/deadlift_coordinator_tests.erl](test/deadlift_coordinator_tests.erl) | Tests for `deadlift_coordinator`: the options, and the coordinated state after the stop of one coordinator and of every coordinator |
| [bench/deadlift_bench.erl](bench/deadlift_bench.erl), [bench/run.sh](bench/run.sh) | The benchmark of a cluster on one computer |
| [dst/run.sh](dst/run.sh) | Runs each test of the simulation suite in a node of its own |
| [src/deadlift_app.erl](src/deadlift_app.erl), [src/deadlift_sup.erl](src/deadlift_sup.erl) | The application and its supervisor, which has no child |
| [test/deadlift_cstate_tests.erl](test/deadlift_cstate_tests.erl) | Tests for `deadlift_cstate` |
| [test/deadlift_cluster_controller_tests.erl](test/deadlift_cluster_controller_tests.erl) | Tests for `deadlift_cluster_controller` and `deadlift_worker` |
| [test/deadlift_log_tests.erl](test/deadlift_log_tests.erl) | Tests for `deadlift_log` |
| [test/deadlift_recovery_tests.erl](test/deadlift_recovery_tests.erl) | Tests for `deadlift_recovery` and `deadlift_sequencer` |
| [test/deadlift_epoch_tests.erl](test/deadlift_epoch_tests.erl) | Tests for `deadlift_epoch` |
| [test/deadlift_resolver_tests.erl](test/deadlift_resolver_tests.erl) | Tests for `deadlift_resolver` |
| [test/deadlift_storage_tests.erl](test/deadlift_storage_tests.erl) | Tests for `deadlift_storage` |
| [test/deadlift_mapper_tests.erl](test/deadlift_mapper_tests.erl) | Tests for `deadlift_mapper`, with the cases of the unit test of FoundationDB |
| [formal/Recovery.tla](formal/Recovery.tla) | TLA+ model of the logging protocol, the recovery, read versions and the StorageServer |
| [formal/Resolver.tla](formal/Resolver.tla) | TLA+ model of the Resolver |
| [formal/MCRecovery.tla](formal/MCRecovery.tla), [formal/MCResolver.tla](formal/MCResolver.tla) | Constants and invariants for model checking |
| [formal/README.md](formal/README.md) | The models, the configs and the results |
| [ROADMAP.md](ROADMAP.md) | The planned work |

## Limitations

- A commit waits for the time of a batch, 1 ms by default, also when no
  other transaction arrives. The time does not change with the load.
- A batch has no limit in bytes.
- The client has the basic functions of erlfdb only. It has no key
  selectors, no options of `get_range`, no atomic operations, no
  versionstamped keys and values, no watches, no transaction options, no
  tenants and no `get_last_error/0`. It has the tuple layer, but no subspaces
  and no directory layer.
- A database of the client is a Proxy and a list of StorageServers that
  the caller names. deadlift has no process that tells a client which
  Proxy and which StorageServers run.
- A read by message waits for its part of the 5 seconds at a
  StorageServer that is behind or cut off, before it continues with the
  next StorageServer. The client has no record of the StorageServers that
  did not answer an earlier read.
- The coordinated state has the process of each LogServer. A LogServer
  that starts again after a stop is another process, and no log system
  names it.
- Two unit tests, in
  [test/deadlift_nodes_tests.erl](test/deadlift_nodes_tests.erl), use two
  Erlang nodes on one computer. One of them has a worker and a candidate
  for the ClusterController on each node. Each test starts Erlang
  distribution and
  needs `epmd`. No test cuts the connection between two Erlang nodes.
  The simulation cuts the connection between simulated nodes.
- Two processes on two nodes can register one name at the same time.
  The client raises the error 1037 when the Proxy does not answer. The
  client of FoundationDB waits for a new Proxy.
- A transaction of `create_transaction/1` keeps its ETS table until its
  process ends. erlfdb frees a transaction when no process has a
  reference to it.
- Two processes that use one transaction at the same time get no error.
- The simulation does not use `transactional/2` and `on_error/2`. Unit
  tests cover them.
- The direct read was not measured in deadlift. The store benchmark
  measured about 1,000 ns for a read of one key from such a table.
- The StorageServer keeps the data in memory, and loses the data when it
  stops. A new StorageServer reads every record again
  from the LogServers, which is possible only while the LogServers of
  every epoch run.
- A read at a version that is not a read version from a Proxy can return
  a write that a recovery discards.
- After a LogServer is stopped, the StorageServer reads the coordinated
  state every 500 ms until the coordinated state has a later epoch.
- A read of a key range returns every key of the range. A read has no
  limit of rows.
- A read copies each value from the ETS table to the StorageServer, and
  from the StorageServer to the client.
- In the simulation, a read that returns no value is accepted when a
  transaction without a known result cleared the key at any version.
- The MVCC window of the StorageServer and the MVCC window of the Resolver
  are two options. No part of deadlift checks that they are equal.
- In the simulation, a new StorageServer starts without data after a
  kill. The simulation kills at most 1
  LogServer of each epoch, so a LogServer with each body runs.
- A request for a read version waits until the timeout of the client
  when a message to a LogServer or its answer is lost. The Proxy does not
  send the message again.
- A Resolver that stops is not started again. The commits of its epoch
  stop until the next recovery.
- A Proxy that stops between the assignment of an LSN and the push leaves
  an LSN that no LogServer receives. Every later record of the epoch then
  waits in the LogServers until a recovery.
- A commit to a LogServer that is alive and does not answer waits until
  the timeout of the client. The Proxy does not stop in this case.
- A LogServer that has stopped must not start again with the same
  identifier, because it has lost its records.
- A candidate for the ClusterController proposes itself when the
  connection to the node of the ClusterController ends. If two candidates
  cannot connect to each other and both can connect to the coordinators,
  each election replaces the ClusterController, and each new
  ClusterController starts a recovery.
- A leader that abandons an attempt stops the Sequencer of the attempt
  when it learns the Sequencer. If that Sequencer has raised the recovery
  count, the recovery of the next attempt can fail, and the leader
  recruits again. In 50 simulation runs without faults, 2 of 270
  recoveries failed in this way.
- A new ClusterController starts a recovery, also when the roles of the
  current epoch run. FoundationDB has the same property.
- The ClusterController does not stop the process of an ended epoch.
  The process continues to run until its next commit or its next request
  for a read version.
- The ClusterController does not start StorageServers.
- The roles do not read the information of the ClusterController about
  the database. They read the process of each LogServer from the
  coordinated state.
- The client does not find the Proxy from the coordinators.
  `deadlift:open/0` works on a node with a worker and a StorageServer of
  the application. On another node, a caller names the Proxy and the
  StorageServers.
- `deadlift:open/0` reads the StorageServer of the node once. A database
  from before a new start of the StorageServer has the process of the
  earlier one.
- The worker and the candidate for the ClusterController of a node are
  two linked processes. They are not one process, because each of them
  is simpler alone: a worker lives as long as its node, and a candidate
  changes between candidate and leader.
- A worker and a candidate read the coordinated state every `poll_ms`. A
  process of FoundationDB waits for a change with one request.
- The application starts the coordinator of a node, and does not form
  the peace cluster. A caller forms it once with
  `deadlift_coordinator:form/1`.
- deadlift has no function that replaces a coordinator whose directory
  is lost, and none that changes the coordinators of a cluster.
- The recovery waits up to `lock_timeout_ms` (default 1,000 ms) for an old
  LogServer that does not answer.
- The tests of the recovery stop LogServers between steps. No test stops a
  process during a step.
- A LogServer keeps the records in memory. A LogServer that stops loses
  the records.
- A LogServer does not discard records. The memory of a LogServer grows
  with every record.
- The number of records that wait in a LogServer has no limit.
- The model of the recovery and the model of the Resolver are separate.
  No model has both.
- The models have no keys and values in the StorageServer, no tags, no
  clock and no election of a ClusterController. The model of the Resolver
  has a key range as a set of keys, without the bounds.
- The Recovery configs with 3 epochs take about 4 hours. They are not run
  for every change. See "When to run TLC" in
  [formal/README.md](formal/README.md).
- The models are checked with 3 LogServers, a replication degree of 2, at
  most 3 epochs and 7 versions, and for the Resolver 2 keys and 3
  transactions.
- deadlift does not send a message again. In the simulation, a lost
  message to a LogServer stops the commits of the epoch until the next
  recovery.
- The repository has no licence file.
