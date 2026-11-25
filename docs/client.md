# The client

The module [redwall](../src/redwall.erl) is the client. The functions have
the names, the arguments and the results of
[erlfdb](https://github.com/foundationdb-beam/erlfdb) 1.2.3, so that
redwall can replace erlfdb in an application later.

```erlang
Db = redwall:open(#{proxy => Proxy, storage => Storage}),
redwall:transactional(Db, fun(Tx) ->
    Balance = redwall:wait(redwall:get(Tx, account)),
    redwall:set(Tx, account, Balance + 1)
end).
```

| Group | Functions |
|---|---|
| Database and transaction | `open/1`, `create_transaction/1`, `transactional/2`, `snapshot/1` |
| Reads | `get/2`, `get_range/3,4`, `get_key/2`, `wait/1`, `wait/2` |
| Writes | `set/3`, `clear/2`, `clear_range/3` |
| Commit and errors | `commit/1`, `reset/1`, `cancel/1`, `on_error/2` |
| Versions | `get_read_version/1`, `set_read_version/2`, `get_committed_version/1`, `get_versionstamp/1` |
| Conflict ranges and status | `add_read_conflict_key/2`, `add_read_conflict_range/3`, `is_read_only/1` |

- A transaction of erlfdb is a mutable object of the C library of
  FoundationDB. A transaction of redwall is an ETS table of type
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
- `get/2` returns a future, as in erlfdb. The future of redwall is the
  result itself, and `wait/1` returns its argument.
- An error is `error({erlfdb_error, Code})` with the error code of
  FoundationDB 7.3: 1007 `transaction_too_old`, 1009 `future_version`,
  1020 `not_committed`, 1021 `commit_unknown_result`, 1025
  `transaction_cancelled` and 1037 `process_behind`.
- `on_error/2` follows `Transaction::onError` of
  `fdbclient/NativeAPI.actor.cpp`. After 1020, 1021 and 1037 it waits for a
  random part of a backoff that starts at 10 ms, doubles with each error
  and is at most 1 s. After 1007 and 1009 it waits for 10 ms. The options
  `retry_delay_ms` and `max_retry_delay_ms` of `redwall:open/1` set the
  first backoff and the highest backoff. FoundationDB has the
  transaction option `max_retry_delay` for the highest backoff, and no
  option for the first one.
- A profile is a set of client options for one kind of StorageServer.
  `redwall:open/1` takes the option `profile`, and an option of
  `open/1` replaces the option of the profile. A StorageServer with
  another store gets a profile of its own.

  | Profile | For | `retry_delay_ms` | `max_retry_delay_ms` | `direct` |
  |---|---|---|---|---|
  | `default` | The values of FoundationDB | 10 | 1,000 | `true` |
  | `ets` | The StorageServer of redwall, with the data in ETS tables in memory | 1 | 100 | `true` |
  `transactional/2` then runs the function again.

## Keys

A key of redwall is an Erlang term, and the keys are in the order of the
Erlang terms. A key of FoundationDB is a byte string, and its tuple layer
encodes a tuple so that the keys with a common prefix of elements are one
key range. An Erlang tuple does not have this property, because Erlang
compares two tuples by their sizes first:

```erlang
lists:sort([{b}, {a, 1}, {a}, {a, 1, x}, {a, 2}]).
%% [{a},{b},{a,1},{a,2},{a,1,x}]
lists:sort([[b], [a, 1], [a], [a, 1, x], [a, 2]]).
%% [[a],[a,1],[a,1,x],[a,2],[b]]
```

redwall supports two kinds of key with the property
([redwall_key](../src/redwall_key.erl)):

| Kind of key | Example | Key range of the prefix `P` | Encoding |
|---|---|---|---|
| List | `[users, 42, name]` | From `P` up to the improper list with the elements of `P` and the tail `<<>>`. A binary is above every list | None |
| Binary | `redwall_tuple:pack({<<"users">>, 42})` | From `P` up to `strinc(P)`, as in FoundationDB | The tuple layer, as in erlfdb |

```erlang
Db = redwall:open(#{keys => list}),
redwall:transactional(Db, fun(Tx) ->
    ok = redwall:set(Tx, [users, 42, name], <<"Ada">>),
    ok = redwall:set(Tx, [users, 42, city], <<"Oslo">>),
    redwall:get_range_startswith(Tx, [users, 42])
end).
%% [{[users,42,city],<<"Oslo">>},{[users,42,name],<<"Ada">>}]
```

- `get_range_startswith/2` and `clear_range_startswith/2` take a prefix
  that is a list or a binary. `redwall_key:range/1` returns the key range
  of a prefix for the other functions.
- The option `keys` of `open/1` is `list`, `binary` or `any`. With `list`
  or `binary`, a function with a key of another kind raises `badarg`. A
  database with keys of two kinds has key ranges without the keys that
  the application expects.
- The default is `any`. A key is then any term, and the application has
  to know the order of the Erlang terms for its key ranges.

## Range options and key selectors

`redwall:get_range/4` is `erlfdb:get_range/4`, and `redwall:get_key/2`
is `erlfdb:get_key/2`.

```erlang
Db = redwall:open(#{proxy => Proxy, storage => Storage, keys => list}),
redwall:transactional(Db, fun(Tx) ->
    {Begin, End} = redwall_key:range([users]),
    %% The 10 highest keys with the prefix, the highest key first.
    Rows = redwall:get_range(Tx, Begin, End, [{reverse, true}, {limit, 10}]),
    %% The last key with the prefix.
    Last = redwall:get_key(Tx, redwall_key:last_less_than(End)),
    {Rows, Last}
end).
```

| Option of `get_range/4` | Meaning |
|---|---|
| `{limit, N}` | The result has the first `N` keys. 0 is no limit |
| `{reverse, true}` | The result has the highest key first, and a limit keeps the highest keys |
| `{snapshot, true}` | The read adds no read conflict range |

| Key selector | Key |
|---|---|
| `redwall_key:last_less_than(Key)`, `{Key, lt}` | The last key below `Key` |
| `redwall_key:last_less_or_equal(Key)`, `{Key, lteq}` | The last key at or below `Key` |
| `redwall_key:first_greater_than(Key)`, `{Key, gt}` | The first key above `Key` |
| `redwall_key:first_greater_or_equal(Key)`, `{Key, gteq}` | The first key at or above `Key` |
| `{Key, Selector, Offset}` | The key `Offset` keys after the key of `{Key, Selector}` |

- The StorageServer applies the limit and the order
  (`redwall_storage:scan/6`). It reads the keys of the result and the
  key after them, in the order of the keys or in the reverse order.
- A read with a limit returns the sets and the clears of the
  transaction. If the transaction has changed a key of the range, the
  client reads the stored keys in parts until it has `N` keys.
- With a limit that the result reaches, the read conflict range ends at
  the last key of the result, as in FoundationDB. A key selector has a
  read conflict range from its key to the result.
- Only a database with the kind `list` or `binary` has key selectors,
  because the Erlang terms have no lowest and no highest term. With the
  kind `any`, `get_key/2` raises `{unsupported, key_selector}`, and a
  tuple as a bound of `get_range/4` is a key.
- A key selector that names a key before the first key gives `[]` or
  `<<>>`. A key selector that names a key after the last key gives
  `<<>>` for the kind `list` and `<<255>>` for the kind `binary`.
- In a database with the kind `binary`, a key selector does not find a
  key at or above `<<255>>`. In FoundationDB these keys are the system
  keys.
- Differences from erlfdb: the first argument is a transaction or a
  snapshot, and the result is the list or the key. erlfdb also takes a
  database, and can return a future.
- Recommendation: list keys for an application in Erlang, and binary
  keys of the tuple layer for an application that comes from erlfdb.
  `get_mapped_range/5` needs binary keys of the tuple layer.
- A tuple key of the form `{'$redwall', _, _}` is a key of redwall
  itself. A tuple is below every list and every binary, so no key range
  of a database with the kind `list` or `binary` has such a key.
- The elements of a list key of different types are in the order of the
  Erlang terms: numbers, atoms, tuples, maps, lists, binaries. The tuple
  layer has another order of the types. The two agree for two integers
  and for two binaries.
- Two keys that are equal and not identical are one key, as in an ETS
  table of the type `ordered_set`: an integer and a float with the same
  value, also as a part of a key, for example `[1]` and `[1.0]`. A write
  of one replaces the other, a read or a clear of one finds the other,
  and a read of one conflicts with a write of the other. The tuple layer
  has two keys for them. Before 2026-11-24, a write of such a key stopped
  the process of the epoch, because the Resolver compared the stored key
  with `=:=`, and the StorageServer did not apply a clear of such a key.
- A storage engine with keys in the order of their bytes would need an
  encoding that keeps the order of the Erlang terms. redwall has no such
  engine and no such encoding.

## Watches

A watch is a future that becomes ready when the value of a key differs
from the value that a transaction read. The functions are the functions
of erlfdb: `watch/2,3`, `get_and_watch/2`, `set_and_watch/3`,
`clear_and_watch/2` and `has_watches/1`. The design is the design of
FoundationDB 7.3.62 (`fdbclient/ReadYourWrites.actor.cpp`,
`fdbclient/NativeAPI.actor.cpp` and `fdbserver/storageserver.actor.cpp`).

```erlang
Future = redwall:transactional(Db, fun(Tx) -> redwall:watch(Tx, Key) end),
%% Another transaction sets the key to another value.
ok = redwall:wait(Future)
```

In the transaction:

- `watch/2` reads the key as `get/2` does. The value of the watch is the
  value at the read version, or the write of this transaction. The read
  adds a read conflict key.
- A later set or clear of the key in the same transaction with another
  value makes the future ready at once.
- The watch starts when the transaction commits. The version of the
  watch is the commit version, or the read version for a transaction
  without a mutation. `transactional/2` commits a transaction with a
  watch also when the transaction has no mutation.
- If the commit fails, the future has the error of the commit. If the
  transaction is reset or cancelled before its commit, the future has
  the error 1025, `transaction_cancelled`.
- A database has at most `max_watches` watches whose futures are not
  ready (default 10,000). Above the limit, `watch/2` raises 1032,
  `too_many_watches`.

After the commit, one process for each watch follows the watch:

1. The process sends the key, the value and the version to a
   StorageServer. The StorageServer answers when the value of the key at
   its version differs from the value of the watch. See
   [Watches](storage-server.md#watches).
2. The process asks the Proxy for read versions until one is at or above
   the version of the answer. It then reads the key at that read
   version.
3. If the value differs from the value of the watch, the future is
   ready. Otherwise the process sends the watch again, with the read
   version as its version.
4. After the answer `timed_out` or `watch_cancelled`, and when the
   StorageServer does not answer in `watch_call_timeout_ms` (default
   35,000) or is not alive, the process waits, performs the read of
   step 2, and sends the watch again. After a failed request, the next
   request goes to the next StorageServer.

The guarantees are the guarantees of FoundationDB:

- If the value of the key differs from the value of the watch from some
  commit on, the future becomes ready.
- If the value changes and changes back, the future can stay not ready.
- When a future is ready without an error, a read at a read version at
  or above the version of the watch returned another value.

The future is `{redwall_future, Reference, Watch}`. When it becomes
ready or gets an error, a process receives the message
`{Reference, ready}`, as with erlfdb. That process is the caller of
`watch/2`, or the process of the option `to` of `watch/3`.

| Function | Result for the future of a watch |
|---|---|
| `wait/1,2` | `ok`, or the error of the future. `{timeout, Ms}` limits the wait. Only the process that receives the message can wait |
| `is_ready/1` | Whether the future is ready, has an error or is cancelled |
| `get_error/1` | `ok` or `{erlfdb_error, Code}` |
| `get/1` | `ok`, or the error of the future |
| `cancel/1,2` | Ends the watch. `wait/1` then raises 1101, `operation_cancelled` |
| `wait_for_any/1,2` | A future of the list that is ready |
| `wait_for_all/1,2` | The result of each future of the list |

Differences from FoundationDB and erlfdb:

- Step 3 is a read of the key. FoundationDB compares versions: the
  future is ready if the read version is less than 50,000,000 versions
  above the version of the answer, because a recovery adds 100,000,000
  versions. In redwall, a record that a recovery discards can be 1
  version below the first version of the next epoch, so a comparison of
  versions does not show a recovery.
- The process performs the read of step 2 also after a failed request.
  A watch therefore also becomes ready when no StorageServer has
  answered that the value differs.
- Each watch has its own process and its own request. The client of
  FoundationDB keeps one request for each key, and the watches of a
  database on the key share it.
- After a reset of the database (see
  [Reset after the loss of LogServers](recovery.md#reset-after-the-loss-of-logservers)), the future of
  each watch is ready.
- A watch ends when the process that receives its message ends. A
  future of FoundationDB is cancelled when the application drops it.
- A future of a read is the result itself, so `wait_for_any/1` waits
  only for the futures of watches.

## Idempotency ids

A client that receives no answer to a commit does not know whether the
transaction is committed (the error 1021). `transactional/2` runs the
function again, so a committed transaction can be applied a second time.
A transaction with an idempotency id is applied once. The design follows
`design/idempotency_ids.md` of FoundationDB 7.3.62.

```erlang
Db = redwall:open(#{automatic_idempotency => true}),
redwall:transactional(Db, fun(Tx) ->
    N = redwall:get(Tx, counter),
    redwall:set(Tx, counter, N + 1)
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

- `redwall:set_option(Tx, automatic_idempotency)` gives an id to one
  transaction.
- The Proxy removes the records that are older than
  `idempotency_min_age_ms` (default 1 hour) and records that time. A
  client whose id is older cannot determine the result, and `commit/1`
  raises 1022, `commit_unknown_result_fatal`.
- `commit/1` does not return while the result is not determined, also
  while no Proxy runs.
- A transaction with an id and without a read asks the Proxy for a read
  version at its commit.
- A key of the form `{'$redwall', _, _}` is reserved. The records of the
  ids have such keys ([redwall_idempotency](../src/redwall_idempotency.erl)).
  See "Keys".
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
