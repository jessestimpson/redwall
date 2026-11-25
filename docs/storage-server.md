# The StorageServer

A StorageServer reads the records from the LogServers and applies their
writes to a store with versions (paper, sections 2.4.3 and 2.4.4).

A StorageServer with the option `peers` that starts without data copies
the data from another StorageServer, and then reads the log from the
version of the copy. FoundationDB copies the data of a shard in the same
way (`fetchKeys` of `fdbserver/storageserver.actor.cpp`).

1. The new StorageServer asks each peer for the start of a copy. A peer
   with data answers with the version of the copy: the lower one of its
   version and its Known Committed Version, so no recovery discards a
   write of the copy.
2. The new StorageServer reads the keys of the peer in pages of 1,000
   keys at that version. The peer keeps its oldest version at or below
   the version of the copy until the copy ends.
3. The new StorageServer has the version of the copy as its version and
   as its oldest version, and reads the log of the epoch of that version
   from there. A read below that version is too old.
4. If every peer that answers has no data, the new StorageServer reads
   the log from the start, as at the start of a new cluster.

- Without the copy, a new StorageServer reads every record from the
  LogServers of every epoch. A LogServer keeps its records in memory, so
  after the stops of several nodes a record of an earlier epoch can be on
  no LogServer. After a pop, the LogServers do not have the discarded
  records either. See [Pop](log-server.md#pop).
- A StorageServer that reads an epoch that has ended and applies no
  record in 10 intervals asks its peers. If a peer has a copy above its
  version, it removes its data and copies.
- The StorageServer reads the coordinated state at every tenth interval
  and after a failed request to a LogServer of its epoch. Before
  2026-11-23 it waited for an ended epoch without an end when no
  LogServer of that epoch could answer that it was stopped.
- In the simulation, a StorageServer that starts after a kill has the
  other StorageServers of the run as peers. In the suite of 2026-11-24,
  158 StorageServers finished a copy, without a violation.
- Limitations: a copy has every key of the peer, and the peer keeps the
  versions above the version of the copy for the time of the copy.

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

## Known Committed Version

The StorageServer keeps a Known Committed Version: a version at or below
which no recovery discards a record. The oldest version of the MVCC
window, the version of a copy for another StorageServer, and the stored
version for the pop are at or below it. It rises in two ways:

- A LogServer sends its Known Committed Version with its records. A
  Proxy sends the Known Committed Version with the next record, so on a
  cluster without commits this version stays below the last record.
- When the StorageServer has the end version of an ended epoch as its
  version, that version is its Known Committed Version. Every record at
  or below the end version of an ended epoch is committed.

The second rule is from 2026-11-24. Before it, a StorageServer on a
cluster without commits offered a copy at a version of an earlier epoch.
A new StorageServer then could not read the records after that version,
because the LogServers of that epoch did not run, and it stayed behind
until the next commits.

## Several StorageServers

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
- The client chooses the StorageServer of a read. See [The client](client.md).
- A StorageServer on another Erlang node reads the process of each
  LogServer from the coordinated state. See [LogServer processes](log-server.md#logserver-processes).

FoundationDB divides the key space into shards. Each shard has a team of
StorageServers, and each mutation has the tags of the team of its key
(paper, section 2.4.3). redwall has one shard with the whole key space.
The team is the list of StorageServers that the caller of
`redwall:open/1` names.

## The store of the StorageServer

FoundationDB has an ordered keyspace. Every structure of redwall that
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
  `redwall_storage:info/1` returns it. No part of redwall reads it
  directly yet.

The owner of the project chose this layout from two benchmarks of
2026-11-22 and 2026-11-23, which are not part of this repository:

- Four stores were compared: `gb_trees`, an ETS table with one row for
  each key, an ETS table with one row for each version, and a NIF with
  `std::map`. An ETS table has a read of one key in about 1 us, lets
  other processes read at the same time, and has no long single write.
- A row with every version of a key costs a copy of every version for
  each read and each write of a key with many versions. The row
  therefore has the newest version only, and the older versions are in
  the second table.

## The StorageServers of a cluster

The coordinated state has the StorageServer of each node of the cluster.

1. A StorageServer with the option `worker` registers with the worker of
   its node. The application sets the option for its StorageServer.
2. The worker reports the StorageServer to the ClusterController with its
   registration.
3. The ClusterController records it in the coordinated state
   (`redwall_cstate:storage/4`), and gives the StorageServers of the
   coordinated state to every worker in the information about the
   database.

- `redwall:open/0` reads from the StorageServers of that list, with the
  StorageServers of the node of the client first. A node without a
  StorageServer reads from the other nodes by message.
- A StorageServer with the option `worker` copies from the other
  StorageServers of that list.
- `redwall_status:status/1` reports on the StorageServers of that list.
- A StorageServer that has stopped stays in the list until a
  StorageServer of its node registers, so the status names it as a
  StorageServer that does not run. `redwall_cstate:forget_storage/3`
  removes a node that does not start again.

## Select

`redwall:select/4` evaluates an ETS match spec on the keys of a range,
and returns the results in the order of the keys:

```erlang
%% The numbers of the users with the status `active'.
Spec = [{{{user, '$1'}, #{status => active}}, [], ['$1']}],
redwall:transactional(Db, fun(Tx) ->
    redwall:select(Tx, {user, 0}, {user, 1000000}, Spec, [{limit, 100}])
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

The StorageServer walks the range with `ets:next_lookup/2`. A benchmark
on 2026-11-23, which is not part of this repository, compared the walk
with `ets:select/2` on a table of 1,000,000 rows. `ets:select/2` with
guards for the range reads every row of the table: about 20 ms for a
range of 100 keys, against 6 us for the walk. The owner of the project
chose the walk.

## Watches

A watch is a request with a key, a value and a version
(`redwall_storage:watch_value/5`). It is the `WatchValueRequest` of
FoundationDB 7.3.62. The client sends it after the commit of a
transaction with a watch. See [Watches](client.md#watches).

1. The StorageServer keeps the request until its version is at or above
   the version of the request.
2. The StorageServer reads the key at its version. If the value differs
   from the value of the request, it answers `{ok, Version}` with its
   version.
3. Otherwise it keeps the request in an ETS table of the type
   `ordered_set` with the key `{Key, Reference}`. When it applies a
   record with a set or a clear of the key, or with a clear of a key
   range that contains the key, it reads the key again.
4. After `watch_timeout_ms` (default 30,000, `WATCH_TIMEOUT` of
   FoundationDB), it answers `{error, timed_out}` and removes the
   request. A request of a client that has stopped therefore leaves the
   StorageServer.
5. With `max_watches` requests (default 100,000), it answers
   `{error, watch_cancelled}` to a new request whose value does not
   differ.

- The version of the StorageServer can be the LSN of a record that a
  recovery discards. An answer `{ok, Version}` is therefore not a proof
  of a committed change, and the client reads the key at a read version.
- After a rollback, the StorageServer reads the key of every request
  again. A discarded record can be the reason that a value did not
  differ. The planted bug `WatchNoRecheck` of the model shows a watch
  that never becomes ready without this read.
- A StorageServer that removes its data, for a copy from a peer or after
  a reset of the database, answers `{error, not_ready}` to every
  request.
- A StorageServer without a watch performs one comparison of an integer
  for each record that it applies.

Differences from FoundationDB:

- The StorageServer keeps one entry for each request. FoundationDB keeps
  one entry for each key, and the requests for the key share it.
- The limit is a number of requests. The limit of FoundationDB is a
  number of bytes.
- redwall has one time limit. FoundationDB uses 20 seconds in the place
  of 30 seconds while a StorageServer has no recent update.

## Mapped range

`redwall:get_mapped_range/4,5` is `erlfdb:get_mapped_range/4,5`, which
is `getMappedRange` of FoundationDB 7.3. The function reads a key range,
and the StorageServer performs a second read for each row of the range.
An application reads the rows of an index and the record of each row in
one request:

```erlang
%% The key of an index row is {<<"index">>, City, Id}, and the key of a
%% record is {<<"user">>, Id}. Both are packed with redwall_tuple.
Mapper = {<<"user">>, <<"{K[2]}">>, <<"{...}">>},
{Begin, End} = redwall_tuple:range({<<"index">>, <<"Oslo">>}),
redwall:transactional(Db, fun(Tx) ->
    redwall:get_mapped_range(Tx, Begin, End, Mapper, [{limit, 100}])
end).
%% [{{IndexKey, IndexValue}, {SecondBegin, SecondEnd}, [{UserKey, User}]}, ...]
```

The mapper describes the key of the second read. redwall has two kinds
of mapper:

| Mapper | Argument of `get_mapped_range` | Status |
|---|---|---|
| The mapper of FoundationDB: a tuple of the tuple layer | A tuple, which the client packs as erlfdb does, or a binary with the packed tuple | Supported |
| A mapper that is an Erlang term | `{term, Term}` | Reserved. The call raises `{unsupported, term_mapper}`. redwall is free to define this mapper later, because FoundationDB has none |

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
- [src/redwall_tuple.erl](../src/redwall_tuple.erl) is the tuple layer:
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
