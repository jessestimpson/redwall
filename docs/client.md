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
