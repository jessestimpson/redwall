# Transactions

## The Proxy

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

### Batches

The Proxy collects the transactions of a short time into one batch
(paper, section 2.4.1). A batch has one commit version and is one log
record.

| Rule | In redwall | In the FoundationDB source |
|---|---|---|
| A batch closes when no earlier batch is in progress | Yes, with `batch_idle` (default `true`) and `batch_depth` (default 1). A transaction at an idle Proxy does not wait, and the next batch has the transactions that arrive while a batch is in progress | No. This part is not in the paper |
| A batch closes after a time | `batch_ms`, default 1 ms, fixed. With the rule above, this time is an upper limit | A time that changes with the load (`commitBatcher` in `fdbserver/CommitProxyServer.actor.cpp`) |
| A batch closes at a number of transactions | `batch_max`, default 1,000 | `COMMIT_TRANSACTION_BATCH_COUNT_MAX` |
| A batch closes at a number of bytes | No limit | A limit that depends on the number of CommitProxies |
| One commit version for each batch | Yes | Yes (`preresolutionProcessing`) |
| The Resolver checks a transaction against the earlier admitted transactions of its batch | Yes | Yes (`checkIntraBatchConflicts` in `fdbserver/SkipList.cpp`) |
| The writes of a rejected transaction give no conflict | Yes | Yes |

- The index of a transaction is its position in the batch, from 0.
  `redwall_epoch:commit_with_index/3` returns the LSN and the index. The
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

## Read versions

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

## The Resolver

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
- `redwall_storage:read_range/4` reads the keys of a range at a read
  version.
- The Resolver handles the commit versions in their order. A request that
  arrives before the request with the previous version waits.
- The commit versions of the key ranges are in an ETS table of the kind
  `ordered_set`, with the bound of a range as the key. A second ETS table
  of the kind `ordered_set` has the bounds in the order of their
  versions. The Resolver removes the versions below the MVCC window for
  each transaction, and reads only the entries that it removes. A
  benchmark on 2026-11-23 compared this structure with two structures
  of `gb_trees`. With the ETS tables, the Resolver had no garbage
  collection of 2 ms or more, and the p99.9 of a batch was 174 us
  against about 6 ms.
