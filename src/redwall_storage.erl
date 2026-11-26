%% @doc The StorageServer.
%%
%% A StorageServer reads the log records from the LogServers, applies the
%% writes of each record to a store with versions, and answers a read at a
%% version. See sections 2.4.3 and 2.4.4 of Zhou et al., "FoundationDB: A
%% Distributed Unbundled Transactional Key Value Store", SIGMOD 2021.
%% `formal/Recovery.tla' models the StorageServer with the actions
%% `SSApply' and `SSFollow'.
%%
%% == Records ==
%%
%% The StorageServer has a version: the LSN of the last record that it has
%% applied. The StorageServer applies a record only when the previous LSN
%% of the record is the version of the StorageServer. The records of all
%% epochs form one chain, because the previous LSN of the first record of
%% an epoch is the Recovery Version.
%%
%% The StorageServer has one request at each LogServer of one epoch that
%% runs (`redwall_log:tail/2'). A LogServer answers when it has a record
%% that the StorageServer has not received from it, or when a Sequencer
%% locks it. After an answer, the StorageServer sends the next request.
%% Each record has its body on k of the LogServers. The StorageServer
%% applies a record when it has received the body.
%%
%% While the StorageServer has no log system, or a LogServer of its epoch
%% is stopped, it reads the coordinated state after 10 ms, and doubles the
%% time with each read up to `poll_ms'. It then follows the first epoch
%% and the epoch after a recovery within some 10 ms.
%%
%% At an interval of `poll_ms', the StorageServer sends a request to each
%% LogServer of the epoch that has received none, because a LogServer can
%% start after the first request. At every tenth interval, the
%% StorageServer sends every request again, because a request or an answer
%% can be lost. At that interval, and when a request to a LogServer of
%% its epoch fails, it also reads the coordinated state: an epoch can end
%% while none of its LogServers can answer that it is stopped.
%%
%% The body of a record of a Proxy is `{batch, Entries}', with one entry
%% for each transaction of the batch, in the order of the batch. The entry
%% of a transaction is a list of mutations (`redwall_epoch:mutation()'):
%% `{set, Key, Value}', `{clear, Key}' and `{clear_range, Begin, End}'.
%% Another entry, and a record with another body, contain no mutation. The
%% StorageServer applies the mutations of an entry in their order. A clear
%% adds a version without a value to each key that has a value. A clear of
%% a range examines the keys of the range. The StorageServer applies the
%% entries in their order. When two transactions of a batch write one key,
%% the StorageServer keeps the value of the later transaction for the
%% version of the batch.
%%
%% == Epochs ==
%%
%% The StorageServer reads the log systems of all epochs from the
%% coordinated state. It reads the coordinated state at its start, and when
%% a LogServer answers that it is stopped. While a LogServer of the epoch
%% is stopped and the coordinated state has no later epoch, the
%% StorageServer reads the coordinated state at the interval of `poll_ms'.
%% The StorageServer reads the records of one epoch at a time, from the
%% LogServers of that epoch. A LogServer of an ended epoch still
%% answers.
%%
%% When the StorageServer learns that the epoch that it reads has ended
%% with the end version `End':
%%
%% <ul>
%% <li>If the version of the StorageServer is above `End', the
%%   StorageServer has applied records that the recovery discarded. The
%%   StorageServer removes every write above `End', and sets its version to
%%   `End'. This is the rollback of the paper (section 2.4.4).</li>
%% <li>If the version is `End', the StorageServer continues with the next
%%   epoch.</li>
%% <li>If the version is below `End', the StorageServer reads the records
%%   up to `End' from the LogServers of the ended epoch.</li>
%% </ul>
%%
%% == Reads ==
%%
%% A read at the version `V' returns the value of the last write to the key
%% at or below `V'. The read waits until the version of the StorageServer
%% is at or above `V'.
%%
%% `V' must be a read version from a Proxy. A read version is the LSN of an
%% acknowledged record, or of the recovery transaction of the epoch. No
%% recovery discards these records, so a rollback does not change the
%% answer to such a read:
%%
%% <ul>
%% <li>A write of the same epoch at or below `V' is at or below the end
%%   version of the epoch.</li>
%% <li>A record of a client that a recovery discards has an LSN below the
%%   first version of the next epoch, because the Sequencer limits the
%%   versions in flight (see {@link redwall_sequencer}). The
%%   StorageServer therefore has no discarded write at or below a version
%%   of a later epoch when it reaches that version. A discarded recovery
%%   transaction can have the first LSN of the next epoch. It contains no
%%   write.</li>
%% </ul>
%%
%% == Storage engine ==
%%
%% The StorageServer keeps the keys and the values in a storage engine
%% ({@link redwall_storage_engine}). The option `engine' selects the
%% engine, and a cluster has one engine.
%%
%% <ul>
%% <li>The engine `ets' is the default and the only engine of redwall
%%   ({@link redwall_storage_ets}). It keeps the data in ETS tables in
%%   memory, and a StorageServer with it loses its data when it
%%   stops.</li>
%% <li>FoundationDB has an ordered keyspace. An engine keeps the keys in
%%   their order, so that a read or a clear of a key range examines only
%%   the keys of the range. The order of the keys is the order of Erlang
%%   terms.</li>
%% <li>`info/1' returns the module of the engine, the view of its store
%%   for another process, and the durability of the engine.</li>
%% <li>A process other than the StorageServer has the view of the store
%%   (`view/1' of the engine) and not the store: a client with a handle,
%%   the process of a page of a copy, and the harness of the simulation.
%%   The view of the engine `ets' has no private table.</li>
%% </ul>
%%
%% == Select ==
%%
%% `select/7' and the query `{select, Begin, End, MatchSpec, Limit}' of
%% `read_direct/3' evaluate an ETS match spec on the keys of a range at a
%% read version. The match spec is for rows `{Key, Value}'. The result is
%% the result of `ets:match_spec_run/2' on the result of a read of the
%% range, with the key of each row: `{Key, Result}'.
%%
%% <ul>
%% <li>The StorageServer reads the rows of the range in the order of the
%%   keys with `ets:next_lookup/2', takes the value of each key at the
%%   read version, and runs the match spec on batches of 100 rows. It
%%   reads no row outside the range, and stops at `Limit' results.</li>
%% <li>A benchmark on 2026-11-23 compared this walk with `ets:select/2'.
%%   A select with guards on the key for the range reads every row of the
%%   table: 20 ms for a table of 1,000,000 rows and a range of 100 keys,
%%   against 6 us for the walk. See "Select" in `docs/storage-server.md'.</li>
%% <li>For a read by message, a temporary process performs the select as
%%   a direct read and answers the client. The StorageServer applies
%%   records in that time.</li>
%% <li>A match spec cannot call a function of a module, so a client cannot
%%   run its own code in the StorageServer.</li>
%% </ul>
%%
%% Only this StorageServer, with its ETS tables, has the select. Its
%% handle has the feature `select'. FoundationDB has no such read. This
%% part is not in the paper.
%%
%% == Scan ==
%%
%% `scan/6' and the query `{scan, From, To, Options}' of `read_direct/3'
%% read the keys between two bounds at a read version, in the order of the
%% keys or in the reverse order, with a limit of rows. The client uses
%% the scan for `get_range/4' with the options `limit' and `reverse' and
%% for key selectors (see {@link redwall}).
%%
%% <ul>
%% <li>`From' is the lower bound and `To' is the upper bound. A bound is
%%   `{incl, Key}' or `{excl, Key}': the key is in the range or is not.</li>
%% <li>With `reverse => true', the scan starts at the upper bound and reads
%%   towards the lower bound (`ets:prev_lookup/2'). The result has the
%%   highest key first.</li>
%% <li>With `limit => N', the scan stops after `N' keys that have a value
%%   at the read version. The scan reads only the rows up to that key,
%%   and the row after it.</li>
%% </ul>
%%
%% The handle has the feature `scan'.
%%
%% == Watches ==
%%
%% A watch is a request that the StorageServer answers when the value of
%% a key differs from a value (`watch_value/5'). The request is the
%% `WatchValueRequest' of FoundationDB 7.3 (`watchValueSendReply' and
%% `watchWaitForValueChange' of `fdbserver/storageserver.actor.cpp'). The
%% client of redwall sends the request after the commit of a transaction
%% with a watch (see "Watches" in {@link redwall}).
%%
%% The request has a key, a value and a version. The value is `{ok, Value}',
%% or `not_found' for a key without a value.
%%
%% <ol>
%% <li>The StorageServer keeps the request until its version is at or
%%   above the version of the request.</li>
%% <li>The StorageServer reads the key at its version. If the value
%%   differs from the value of the request, the StorageServer answers
%%   `{ok, Version}' with its version.</li>
%% <li>Otherwise the StorageServer keeps the request in an ETS table of
%%   type `ordered_set' with the key `{Key, Reference}'. When the
%%   StorageServer applies a record with a set or a clear of the key, or
%%   with a clear of a key range that contains the key, it reads the key
%%   again after the record, as in step 2.</li>
%% <li>After `watch_timeout_ms' (default 30,000, `WATCH_TIMEOUT' of
%%   FoundationDB) the StorageServer answers `{error, timed_out}' and
%%   removes the request. The client sends a new request. A request of a
%%   client that has stopped therefore leaves the StorageServer.</li>
%% <li>With `max_watches' requests (default 100,000), the StorageServer
%%   answers `{error, watch_cancelled}' to a new request whose value does
%%   not differ. The client then asks again at an interval.</li>
%% </ol>
%%
%% The StorageServer compares with the value at its version. That version
%% can be the LSN of a record that a recovery discards later, so an answer
%% `{ok, Version}' is not a proof of a committed change. The client
%% confirms the change with a read at a read version.
%%
%% <ul>
%% <li>After a rollback, the StorageServer reads the key of every request
%%   again. A discarded record can be the reason that a value did not
%%   differ.</li>
%% <li>A StorageServer that removes its data, for a copy from a peer or
%%   after a reset of the database, answers `{error, not_ready}' to every
%%   request.</li>
%% </ul>
%%
%% Departures from FoundationDB:
%%
%% <ul>
%% <li>The StorageServer keeps one entry for each request. FoundationDB
%%   keeps one entry for each key, and the requests for the key share the
%%   entry. A request with another value and a higher version then
%%   answers the earlier requests.</li>
%% <li>The limit is a number of requests. The limit of FoundationDB is a
%%   number of bytes (`MAX_STORAGE_SERVER_WATCH_BYTES').</li>
%% <li>FoundationDB uses a shorter time limit of 20 seconds while a
%%   StorageServer has no recent update (`FAST_WATCH_TIMEOUT'). redwall
%%   has one time limit.</li>
%% </ul>
%%
%% `watch_trigger => false' plants a defect for the simulation: the
%% StorageServer does not read the key of a request when it applies a
%% record.
%%
%% == Copy from another StorageServer ==
%%
%% A StorageServer with the option `peers' that starts without data copies
%% the data from another StorageServer, and then reads the log from the
%% version of the copy. Without the copy, a new StorageServer reads every
%% record from the LogServers of every epoch. A LogServer keeps its
%% records in memory, so after the stops of several nodes a record of an
%% earlier epoch can be on no LogServer. FoundationDB copies the data of a
%% shard in the same way (`fetchKeys' and `AddingShard' of
%% `fdbserver/storageserver.actor.cpp'): it reads the data at one version
%% from other StorageServers, and then applies the later mutations.
%%
%% <ol>
%% <li>The new StorageServer asks each peer for the start of a copy
%%   (`copy_start'). A peer without data answers `empty'. A peer with data
%%   answers with the version of the copy: the lower one of its version
%%   and of its Known Committed Version, so no recovery discards a write
%%   of the copy. The peer keeps its oldest version at or below the
%%   version of the copy while the copy is in progress.</li>
%% <li>If a peer has data, the new StorageServer reads the keys of the
%%   peer in pages of 1,000 keys at the version of the copy
%%   (`copy_page'), and writes them to its tables. A temporary process of
%%   the peer reads each page, so the peer applies records in that
%%   time.</li>
%% <li>The new StorageServer then has the version of the copy as its
%%   version and as its oldest version. It reads the log of the epoch of
%%   that version from that version on. A read below that version is
%%   answered with `transaction_too_old'.</li>
%% <li>If every peer that answers is empty, the new StorageServer reads
%%   the log from the start. This is the case at the start of a new
%%   cluster.</li>
%% <li>If no peer answers, or a peer stops during the copy, the new
%%   StorageServer removes the copied keys and starts again after 200
%%   ms.</li>
%% </ol>
%%
%% A StorageServer that reads the log can also need a copy later. It reads
%% an epoch that has ended, and the LogServers with the records that it
%% needs do not run, or it is cut off from them. At every tenth interval,
%% a StorageServer with peers that reads an ended epoch and has applied no
%% record since the tenth interval before asks its peers. If a peer has a
%% copy above its version, it removes its data and copies. A read of its
%% old data is then answered with `transaction_too_old'.
%%
%% A StorageServer that copies does not answer a read, and answers
%% `not_ready' to the start of a copy. The peer ends a copy without a
%% page for 30 seconds.
%%
%% Limitations: the copy has every key of the peer, and the peer keeps the
%% versions above the version of the copy for the time of the copy.
%%
%% == Pop ==
%%
%% The LogServers discard the records that every StorageServer of the
%% cluster has stored (see "Pop" in {@link redwall_cluster_controller}
%% and in {@link redwall_log}).
%%
%% <ul>
%% <li>The ClusterController asks the StorageServer for its stored version
%%   (`stored/2'). The stored version is the lowest one of the version of
%%   the StorageServer, of its Known Committed Version, and of the version
%%   of each copy that another StorageServer reads from this one. No
%%   recovery discards a record at or below the stored version, and no
%%   rollback removes one. A StorageServer that copies from a peer answers
%%   `not_ready'.</li>
%% <li>The answer has the generation of the coordinated state that the
%%   StorageServer has read. After a reset of the database the versions
%%   start again, so the ClusterController uses only the answers with the
%%   generation that it has read itself.</li>
%% <li>A LogServer answers `{popped, Popped}' to a request below its
%%   popped version. If `Popped' is at or below the version of the
%%   StorageServer, the StorageServer sends the request again from its
%%   version. Otherwise the StorageServer cannot read the records that it
%%   needs from this LogServer. It asks its peers, and copies from a peer
%%   with a copy above its version. A StorageServer without peers does
%%   not continue.</li>
%% </ul>
%%
%% A StorageServer that starts without data and without a peer with data
%% reads the log from the start. After a pop the LogServers do not have
%% every record. See "Limitations" of the pop in
%% {@link redwall_cluster_controller}.
%%
%% == Equal keys ==
%%
%% The tables of the engine `ets' are of the type `ordered_set', which
%% compares two keys with `=='. Two keys that are equal and not identical are one
%% key, as in an ETS table of that type: an integer and a float with the
%% same value, also as a part of a key, for example `[users, 1]' and
%% `[users, 1.0]'. A write of one of them replaces the other, and a read
%% or a clear of one of them finds the other. A read returns the key of
%% the last write. The Resolver compares keys in the same way.
%%
%% == Mapped range ==
%%
%% `mapped_range/7' and the query `{mapped, Begin, End, Mapper, Limit}' of
%% `read_direct/3' are the mapped range read of FoundationDB
%% (`getMappedKeyValuesQ' and `mapKeyValues' of
%% `fdbserver/storageserver.actor.cpp', FoundationDB 7.3.62). The
%% StorageServer reads the rows of the range `{Begin, End}' at the read
%% version, builds a mapped key from each row with the mapper (see
%% {@link redwall_mapper}), and performs a second read at the same read
%% version: a read of the mapped key, or a read of every key that starts
%% with the mapped key.
%%
%% The result has one element for each row of the range, in the order of
%% the keys: `{{Key, Value}, {SecondBegin, SecondEnd}, Rows}'.
%% `{SecondBegin, SecondEnd}' is the key range of the second read, and
%% `Rows' is the list of `{Key, Value}' that the second read found. A row
%% whose second read finds nothing is in the result with an empty list.
%% With a `Limit', the result has the first `Limit' rows of the range.
%%
%% <ul>
%% <li>The StorageServer walks the range with `ets:next_lookup/2', as a
%%   select does, and performs each second read on the same tables. A
%%   StorageServer of redwall has every key, so no second read goes to
%%   another StorageServer. A StorageServer of FoundationDB reads a mapped
%%   key of another shard through a client transaction at the same
%%   version.</li>
%% <li>A second read of a prefix has no limit of rows. FoundationDB
%%   returns the first 2,000 rows and 10,000,000 bytes of it
%%   (`QUICK_GET_KEY_VALUES_LIMIT'), with a flag that the client must
%%   examine.</li>
%% <li>A mapped range read has no limit of bytes. FoundationDB ends a
%%   reply at 80,000 bytes.</li>
%% <li>For a read by message, a temporary process performs the read as a
%%   direct read, as for a select.</li>
%% <li>An error of the mapper ends the read with `{error, {mapper,
%%   Reason}}'.</li>
%% </ul>
%%
%% The handle has the feature `mapped_range'.
%%
%% == Direct reads ==
%%
%% A client can read the store of the storage engine in its own process,
%% without a message to the StorageServer (`handle/1' and
%% `read_direct/3'). The rules below are those of the engine `ets'. The
%% StorageServer publishes its version and its oldest version in an
%% `atomics' array for this purpose:
%%
%% <ul>
%% <li>The StorageServer writes the rows of a record before it publishes
%%   the version of the record. A client that finds the published version
%%   at or above its read version finds every row at or below the read
%%   version.</li>
%% <li>The StorageServer publishes a new oldest version before it removes
%%   the versions below it. A client reads the published oldest version
%%   after it has read its rows. If its read version is below that oldest
%%   version, the read is too old, and the client discards the rows.</li>
%% <li>A read at a read version returns only versions at or below the read
%%   version, so a record that the StorageServer applies during the read
%%   does not change the result.</li>
%% <li>A write of a key adds the row of the older version before it
%%   changes the row of the key. A rollback changes the row of the key
%%   before it removes the row of the older version. A direct read that
%%   uses the table of the older versions reads the row of the key again
%%   afterwards, and starts again if that row has changed.</li>
%% </ul>
%%
%% == Replicas ==
%%
%% Several StorageServers can run at the same time, on one node or on
%% several nodes. Each of them has every key. Each of them reads every
%% record from the LogServers, and has its own version, its own oldest
%% version, its own tables and its own rollbacks. A LogServer keeps one
%% request for each StorageServer. The StorageServers send no message to
%% each other.
%%
%% Two StorageServers apply the same chain of records. At a read version,
%% each of them returns the same result, or waits for the version, or
%% answers that the read version is too old. The client chooses the
%% StorageServer of a read (see {@link redwall}).
%%
%% == MVCC window ==
%%
%% The StorageServer keeps the versions of the MVCC window (paper, section
%% 2.4.5; `mvcc_window', default: 5,000,000 versions, the versions of 5
%% seconds). The StorageServer has an oldest version:
%%
%% <ul>
%% <li>The oldest version rises to the lower one of two versions: the
%%   version of the StorageServer minus the window, and the highest Known
%%   Committed Version that a LogServer has sent with its records. No
%%   recovery discards a record at or below a Known Committed Version, so
%%   a rollback never needs a version that the StorageServer has
%%   removed.</li>
%% <li>For each key, the StorageServer keeps the versions above the oldest
%%   version, and the last version at or below the oldest version.</li>
%% <li>A read at a version below the oldest version returns
%%   `{error, transaction_too_old}'.</li>
%% </ul>
%%
%% == Known Committed Version ==
%%
%% The StorageServer keeps a Known Committed Version: a version at or below
%% which no recovery discards a record. The oldest version, the version of
%% a copy for another StorageServer and the stored version for the pop are
%% at or below it. The StorageServer raises it in two ways:
%%
%% <ul>
%% <li>A LogServer sends its Known Committed Version with its records. A
%%   Proxy sends the Known Committed Version with the next record, so on a
%%   cluster without commits this version stays below the last
%%   record.</li>
%% <li>When the StorageServer has the end version of an ended epoch as its
%%   version, that version is its Known Committed Version. Every record at
%%   or below the end version of an ended epoch is committed. Without this
%%   rule, a StorageServer on a cluster without commits offers a copy at a
%%   version of an earlier epoch, and a new StorageServer cannot read the
%%   records after that version when the LogServers of that epoch do not
%%   run.</li>
%% </ul>
%%
%% The first version of an epoch is more than the window above the last
%% version of the previous epoch (see {@link redwall_sequencer}). A read
%% at a read version of the previous epoch is therefore too old when the
%% StorageServer has applied a committed record of the new epoch.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>With the storage engine `ets', the StorageServer keeps the data
%%   in memory. A StorageServer of FoundationDB keeps the versions of the
%%   MVCC window in memory and writes older data to its storage engine
%%   on a disk. This part is not in the paper.</li>
%% <li>Each StorageServer has every key. FoundationDB divides the key
%%   space into shards. Each shard has a team of StorageServers, and a
%%   StorageServer reads the records with its tag.</li>
%% <li>The StorageServer reads the log systems from the coordinated state.
%%   A StorageServer of FoundationDB receives them from the
%%   ClusterController. This part is not in the paper.</li>
%% <li>The StorageServer does not send a pop to a LogServer. A
%%   StorageServer of FoundationDB pops its tag at each LogServer. In
%%   redwall the ClusterController asks every StorageServer and sends one
%%   pop to the LogServers, because redwall has no tags: every
%%   StorageServer reads every record.</li>
%% </ul>
-module(redwall_storage).

-behaviour(gen_server).

-include("redwall_eta.hrl").
-eta_observe({state, [version, oldest, engine, view, rollbacks, copies, resets, popped, watching, watches_fired, watches_timed_out]}).

-export([start/1, start_link/1, stop/1]).
-export([read/3, read/4, read_with_version/4, info/1, stored/2]).
-export([read_range/4, read_range/5, read_range_with_version/5]).
-export([handle/1, read_direct/3, versions/1]).
-export_type([version/0]).
-export([select/7, select_rows/2, mapped_range/7, scan/6, watch_value/5, watch_request/4]).
-export_type([watched/0]).
-export_type([bound/0, scan_options/0]).
-export_type([mapped_row/0]).
-export_type([handle/0, query/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2]).

-export_type([options/0]).

-type version() :: redwall_cstate:version().

%% `mvcc_window' is a number of versions, or `infinity'.
%%
%% `rollback => false' plants a defect for the simulation: the
%% StorageServer keeps the writes above the end version of an epoch.
%% `check_window => false' plants a second defect: the StorageServer
%% answers a read below the oldest version. `clear => false' plants a third
%% defect: the StorageServer does not apply a clear. `wait => false' plants
%% a fourth defect: the StorageServer answers a read at a version that it
%% has not applied. `copy_rows => false' plants a fifth defect: the
%% StorageServer does not store the rows of a copy from another
%% StorageServer, and has the version of the copy without its data.
-type options() :: #{
    coordinators := redwall_cstate:servers(),
    poll_ms => pos_integer(),
    cstate_timeout_ms => timeout(),
    mvcc_window => pos_integer() | infinity,
    rollback => boolean(),
    check_window => boolean(),
    clear => boolean(),
    wait => boolean(),
    copy_rows => boolean(),
    %% The time after which the StorageServer answers `timed_out' to a
    %% watch, and the highest number of watches. See "Watches" in the
    %% module documentation. `watch_trigger => false' plants a defect.
    watch_timeout_ms => pos_integer() | infinity,
    max_watches => non_neg_integer(),
    watch_trigger => boolean(),
    %% `false' plants a defect for the simulation: the end version of an
    %% ended epoch is not a Known Committed Version. See "Known Committed
    %% Version" in the module documentation.
    committed_at_epoch_end => boolean(),
    %% The storage engine: `ets' (the default), or a module with the
    %% behaviour `redwall_storage_engine'. Every StorageServer of a
    %% cluster has the same engine. See "Storage engine" in the module
    %% documentation.
    engine => redwall_storage_engine:engine(),
    %% A registered name for the StorageServer, so that a StorageServer of
    %% another node can name it as a peer: `{Name, Node}'.
    name => atom(),
    %% The registered name of the worker of this node. The StorageServer
    %% registers with the worker, so that the ClusterController has it in
    %% the list of the StorageServers of the cluster
    %% (`redwall_worker:register_storage/2').
    worker => atom(),
    %% The other StorageServers of the cluster, or a function that returns
    %% them. See "Copy from another StorageServer". With the option
    %% `worker' and without this option, the peers are the other
    %% StorageServers of that list.
    peers => [gen_server:server_ref()] | fun(() -> [gen_server:server_ref()] | undefined)
}.

%% The storage engine, the view of its store and the published versions
%% of a StorageServer. Element 1 of `published' is the version, and element 2 is the oldest
%% version. `wait' is `false' only with the planted defect `wait => false'.
-type handle() :: #{
    engine := module(),
    store := redwall_storage_engine:store(),
    published := atomics:atomics_ref(),
    wait => boolean(),
    features => [select | mapped_range | scan]
}.

%% The value of a watch: the value of the key, or `not_found' for a key
%% without a value.
-type watched() :: {ok, term()} | not_found.

%% A bound of a scan: the key is in the range (`incl') or is not (`excl').
-type bound() :: {incl, term()} | {excl, term()}.
-type scan_options() :: #{limit => pos_integer() | infinity, reverse => boolean()}.

-type query() ::
    {key, term()}
    | {range, term(), term()}
    | {scan, From :: bound(), To :: bound(), scan_options()}
    | {select, Begin :: term(), End :: term(), ets:match_spec(), Limit :: pos_integer() | infinity}
    | {mapped, Begin :: term(), End :: term(), redwall_mapper:mapper(), Limit :: pos_integer() | infinity}.

%% A row of a mapped range read: the row of the range, the key range of
%% the second read, and the rows that the second read found.
-type mapped_row() :: {{term(), term()}, {binary(), binary()}, [{term(), term()}]}.


%% The number of keys of a page of a copy, the time limit for the answer
%% of a peer to the start of a copy, the time before a new start of a
%% copy, and the time after which a peer ends a copy without a page, in
%% milliseconds.
-define(COPY_PAGE, 1000).
-define(COPY_START_TIMEOUT_MS, 1000).
-define(COPY_RETRY_MS, 200).
-define(COPY_PIN_MS, 30000).

%% The first time until an early read of the coordinated state. The time
%% doubles with each such read, up to `poll_ms'.
-define(REFRESH_MS, 10).

-record(state, {
    coordinators :: redwall_cstate:servers(),
    poll_ms :: pos_integer(),
    cstate_timeout_ms :: timeout(),
    rollback :: boolean(),
    window :: pos_integer() | infinity,
    check_window :: boolean(),
    clear :: boolean(),
    wait :: boolean(),
    copy_rows :: boolean(),
    %% The LSN of the last record that the StorageServer has applied.
    version = 0 :: version(),
    %% The oldest version that a read can have.
    oldest = 0 :: version(),
    %% The highest Known Committed Version that a LogServer has sent.
    kcv = 0 :: version(),
    %% The module of the storage engine, and its store. See "Storage
    %% engine" in the module documentation.
    engine :: module(),
    store :: redwall_storage_engine:store(),
    %% The part of the store for a process other than the StorageServer:
    %% a direct read, a page of a copy, and the simulation.
    view :: redwall_storage_engine:store(),
    %% The version and the oldest version for direct reads.
    published :: atomics:atomics_ref(),
    %% The log systems of all epochs, from the last read of the coordinated
    %% state, in the order of the epochs.
    systems = [] :: [map()],
    %% The log system that the StorageServer reads, with `end_version' if
    %% the epoch has ended.
    system :: map() | undefined,
    %% LSN -> {PreviousLSN, Body}: the records above the version that the
    %% StorageServer has received and not applied.
    buffer :: gb_trees:tree(version(), {version(), redwall_log:body()}),
    %% The requests to LogServers without an answer. The label of a
    %% request is `{Epoch, Log, Number}'.
    requests :: gen_server:request_id_collection(),
    %% Log -> the number of the last request to the LogServer.
    sent = #{} :: #{redwall_log:id() => pos_integer()},
    %% Log -> the highest LSN that the LogServer has sent.
    cursors = #{} :: #{redwall_log:id() => version()},
    %% The LogServers of the epoch that have answered that they are
    %% stopped.
    stopped = [] :: [redwall_log:id()],
    %% The reads that wait for a version.
    waiting = [] :: [{gen_server:from(), {key, term()} | {range, term(), term()}, version()}],
    %% The watches. See "Watches" in the module documentation. `watches'
    %% is the ETS table of the watches whose value did not differ at the
    %% last read of the key, with the rows
    %% `{{Key, Reference}, Value, Version, From, Timer}'. `watching' is
    %% the number of its rows. `watch_waiting' has the watches that wait
    %% for a version of the StorageServer:
    %% `{Reference, Key, Value, Version, From, Timer}'.
    watches :: ets:tid(),
    watching = 0 :: non_neg_integer(),
    watch_waiting = [] :: [{reference(), term(), watched(), version(), gen_server:from() | {watch_request, pid(), reference()}, reference() | undefined}],
    watch_timeout_ms :: pos_integer() | infinity,
    max_watches :: non_neg_integer(),
    watch_trigger :: boolean(),
    committed_at_epoch_end :: boolean(),
    %% The number of answers `{ok, Version}' and of answers `timed_out' to
    %% a watch.
    watches_fired = 0 :: non_neg_integer(),
    watches_timed_out = 0 :: non_neg_integer(),
    %% The number of rollbacks.
    rollbacks = 0 :: non_neg_integer(),
    %% The peers, and the copy in progress: `probe' before a peer has
    %% answered, or the peer, the reference and the version of the copy
    %% and the key after which the next page starts.
    peers = [] :: [gen_server:server_ref()] | fun(() -> [gen_server:server_ref()] | undefined),
    worker :: atom(),
    %% `reprobe' is `probe' for a StorageServer that has made no progress
    %% in an ended epoch.
    %% `{probing, Monitor, Probe}' is the state while a temporary process
    %% asks the peers.
    copy ::
        undefined
        | probe
        | reprobe
        | {probing, reference(), probe | reprobe}
        | #{peer := gen_server:server_ref(), ref := reference(), version := version(), next := term()},
    copies = 0 :: non_neg_integer(),
    %% The number of answers `popped' of a LogServer with a popped version
    %% above the version of the StorageServer.
    popped = 0 :: non_neg_integer(),
    %% The generation of the coordinated state at the last read, and the
    %% number of times that the StorageServer has removed its data after
    %% a reset of the database.
    generation :: non_neg_integer() | undefined,
    resets = 0 :: non_neg_integer(),
    %% Whether the timer of `poll' runs, and the version at the last tenth
    %% interval.
    polling = false :: boolean(),
    checked = -1 :: integer(),
    %% The copies that other StorageServers read from this one: the
    %% version and the timer of each.
    pins = #{} :: #{reference() => {version(), reference()}},
    %% The number of intervals of `poll_ms' since the start.
    ticks = 0 :: non_neg_integer(),
    %% The timer of the next early read of the coordinated state, and the
    %% time until it.
    refresh_timer :: reference() | undefined,
    refresh_ms = ?REFRESH_MS :: pos_integer()
}).

-define(TIMEOUT_MS, 5000).
-define(POLL_MS, 500).
-define(MVCC_WINDOW, 5000000).
-define(CSTATE_TIMEOUT_MS, 1000).
%% The StorageServer sends every request again at each tenth interval.
-define(RESEND_TICKS, 10).
%% WATCH_TIMEOUT of `fdbclient/ClientKnobs.cpp', in milliseconds, and the
%% highest number of watches.
-define(WATCH_TIMEOUT_MS, 30000).
-define(MAX_WATCHES, 100000).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts a StorageServer without a link to the caller.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options = #{name := Name}) ->
    gen_server:start({local, Name}, ?MODULE, Options, []);
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options = #{name := Name}) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []);
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Storage) ->
    gen_server:call(Storage, stop).

%% @equiv read(Storage, Key, Version, 5000)
-spec read(gen_server:server_ref(), term(), version()) -> {ok, term()} | not_found | {error, term()}.
read(Storage, Key, Version) ->
    read(Storage, Key, Version, ?TIMEOUT_MS).

%% @doc Reads the value of `Key' at the read version `Version'. The result
%% is `not_found' if no record at or below `Version' wrote the key. The
%% result is `{error, transaction_too_old}' if `Version' is below the MVCC
%% window. The result is `{error, Reason}' if the StorageServer does not
%% reach `Version' in time, or is not alive.
-spec read(gen_server:server_ref(), term(), version(), timeout()) ->
    {ok, term()} | not_found | {error, term()}.
read(Storage, Key, Version, Timeout) ->
    case read_with_version(Storage, Key, Version, Timeout) of
        {ok, Value, _Written} -> {ok, Value};
        Other -> Other
    end.

%% @doc Reads as `read/4' does. The result also has the LSN of the record
%% that wrote the value.
-spec read_with_version(gen_server:server_ref(), term(), version(), timeout()) ->
    {ok, term(), version()} | not_found | {error, term()}.
read_with_version(Storage, Key, Version, Timeout) ->
    try
        gen_server:call(Storage, {read, Key, Version}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%% @equiv read_range(Storage, Begin, End, Version, 5000)
-spec read_range(gen_server:server_ref(), term(), term(), version()) -> {ok, [{term(), term()}]} | {error, term()}.
read_range(Storage, Begin, End, Version) ->
    read_range(Storage, Begin, End, Version, ?TIMEOUT_MS).

%% @doc Reads the keys of the range `{Begin, End}' at the read version
%% `Version'. The range has every key `K' with `Begin =< K' and `K < End'.
%% The result has each key that a record at or below `Version' wrote, with
%% its value, in the order of the keys. The errors are the errors of
%% `read/4'.
-spec read_range(gen_server:server_ref(), term(), term(), version(), timeout()) ->
    {ok, [{term(), term()}]} | {error, term()}.
read_range(Storage, Begin, End, Version, Timeout) ->
    case read_range_with_version(Storage, Begin, End, Version, Timeout) of
        {ok, Found} -> {ok, [{Key, Value} || {Key, Value, _Written} <- Found]};
        Other -> Other
    end.

%% @doc Reads as `read_range/5' does. The result also has, for each key,
%% the LSN of the record that wrote the value.
-spec read_range_with_version(gen_server:server_ref(), term(), term(), version(), timeout()) ->
    {ok, [{term(), term(), version()}]} | {error, term()}.
read_range_with_version(Storage, Begin, End, Version, Timeout) ->
    try
        gen_server:call(Storage, {read_range, Begin, End, Version}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%% @doc Reads the keys between the bounds `From' and `To' at the read
%% version `Version'. See "Scan" in the module documentation. The result
%% has `{Key, Value, Written}' for each key with a value at `Version', in
%% the order of the keys, or in the reverse order with `reverse => true'.
%% With `limit => N', the result has the first `N' of them. The errors are
%% the errors of `read/4'.
-spec scan(gen_server:server_ref(), bound(), bound(), scan_options(), version(), timeout()) ->
    {ok, [{term(), term(), version()}]} | {error, term()}.
scan(Storage, From, To, Options, Version, Timeout) ->
    try
        gen_server:call(Storage, {scan, From, To, Options, Version}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%% @doc Waits until the value of `Key' differs from `Value' at a version
%% of the StorageServer at or above `Version'. See "Watches" in the module
%% documentation. The results are:
%%
%% <ul>
%% <li>`{ok, Changed}': the value differs at the version `Changed' of the
%%   StorageServer. A recovery can discard the record `Changed'.</li>
%% <li>`{error, timed_out}': the value did not differ in the time
%%   `watch_timeout_ms' of the StorageServer.</li>
%% <li>`{error, watch_cancelled}': the StorageServer has `max_watches'
%%   watches.</li>
%% <li>`{error, not_ready}': the StorageServer has removed its data.</li>
%% <li>`{error, Reason}': the StorageServer is not alive, or did not
%%   answer in `Timeout'.</li>
%% </ul>
%%
%% `watch_request/4' sends the same request and does not wait.
-spec watch_value(gen_server:server_ref(), term(), watched(), version(), timeout()) -> {ok, version()} | {error, term()}.
watch_value(Storage, Key, Value, Version, Timeout) ->
    try
        gen_server:call(Storage, {watch, Key, Value, Version}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%% @doc Sends a watch to the StorageServer, and returns the tag of the
%% request. The caller receives the answer of `watch_value/5' in the
%% message `{redwall_watch_answer, Tag, Answer}'. The tag is a monitor of
%% the StorageServer, so the caller receives `{'DOWN', Tag, process, _,
%% Reason}' if the StorageServer is not alive.
-spec watch_request(gen_server:server_ref(), term(), watched(), version()) -> reference().
watch_request(Storage, Key, Value, Version) ->
    Tag = monitor(process, Storage),
    ok = gen_server:cast(Storage, {watch, {watch_request, self(), Tag}, Key, Value, Version}),
    Tag.

%% @doc Evaluates a match spec on the keys of the range `{Begin, End}' at
%% the read version `Version'. See "Select" in the module documentation.
%% The match spec is for rows `{Key, Value}'. The result has `{Key,
%% Result}' for each row that matches, in the order of the keys, where
%% `Result' is the result of the body of the match spec. With a `Limit',
%% the result has the first `Limit' of them. The errors are the errors of
%% `read/4'.
-spec select(gen_server:server_ref(), term(), term(), ets:match_spec(), pos_integer() | infinity, version(), timeout()) ->
    {ok, [{term(), term()}]} | {error, term()}.
select(Storage, Begin, End, MatchSpec, Limit, Version, Timeout) ->
    try
        gen_server:call(Storage, {select, Begin, End, MatchSpec, Limit, Version}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%% @doc Reads the range `{Begin, End}' at the read version `Version', and
%% performs the second read of the mapper for each row. See "Mapped range"
%% in the module documentation. The errors are the errors of `read/4', and
%% `{error, {mapper, Reason}}' with a reason of {@link redwall_mapper}.
-spec mapped_range(gen_server:server_ref(), term(), term(), redwall_mapper:mapper(), pos_integer() | infinity, version(), timeout()) ->
    {ok, [mapped_row()]} | {error, term()}.
mapped_range(Storage, Begin, End, Mapper, Limit, Version, Timeout) ->
    try
        gen_server:call(Storage, {mapped, Begin, End, Mapper, Limit, Version}, Timeout)
    catch
        exit:Reason -> {error, Reason}
    end.

%% @doc Runs the match spec on the rows `{Key, Value}', and returns
%% `{Key, Result}' for each row that matches. The client uses this
%% function for the writes of a transaction that is not committed.
-spec select_rows([{term(), term()}], ets:match_spec()) -> [{term(), term()}].
select_rows(Rows, MatchSpec) ->
    redwall_storage_engine:select_rows(Rows, MatchSpec).

%% @doc Returns the handle for direct reads, or an error if the
%% StorageServer does not answer.
-spec handle(gen_server:server_ref()) -> {ok, handle()} | {error, term()}.
handle(Storage) ->
    try gen_server:call(Storage, handle, ?TIMEOUT_MS) of
        Handle -> {ok, Handle}
    catch
        exit:Reason -> {error, Reason}
    end.

%% @doc Reads at the read version `Version' in the calling process. See
%% "Direct reads" in the module documentation. The results are:
%%
%% <ul>
%% <li>`{ok, Result}': `Result' is the result of `read_with_version/4' for
%%   a key, or of `read_range_with_version/5' for a range.</li>
%% <li>`behind': the StorageServer has not applied the record `Version'.
%%   `read_with_version/4' waits for the record.</li>
%% <li>`too_old': `Version' is below the oldest version.</li>
%% <li>`gone': the table does not exist. The StorageServer has stopped.</li>
%% </ul>
-spec read_direct(handle(), query(), version()) -> {ok, term()} | behind | too_old | gone.
read_direct(Handle = #{engine := Engine, store := Store, published := Published}, Query, Version) ->
    %% Element 3 changes when the StorageServer removes its data after a
    %% reset of the database. A read that started before that is not
    %% answered from the tables.
    Resets = atomics:get(Published, 3),
    case atomics:get(Published, 1) >= Version orelse not maps:get(wait, Handle, true) of
        false ->
            behind;
        true ->
            try Engine:read(Store, Query, Version) of
                Result ->
                    case {Version < atomics:get(Published, 2), atomics:get(Published, 3)} of
                        {_, Other} when Other =/= Resets -> behind;
                        {true, _} -> too_old;
                        {false, _} -> {ok, Result}
                    end
            catch
                error:badarg -> gone
            end
    end.

-spec info(gen_server:server_ref()) ->
    #{
        version := version(),
        oldest := version(),
        epoch := redwall_cstate:epoch() | undefined,
        rollbacks := non_neg_integer(),
        %% The number of copies from another StorageServer that this one
        %% has finished, and whether a copy is in progress.
        copies := non_neg_integer(),
        copying := boolean(),
        %% The number of times that the StorageServer has removed its
        %% data after a reset of the database.
        resets := non_neg_integer(),
        %% The number of answers `popped' of a LogServer with a popped
        %% version above the version of the StorageServer. See "Pop" in
        %% the module documentation.
        popped := non_neg_integer(),
        %% The number of watches that the StorageServer keeps, and the
        %% numbers of answers `{ok, Version}' and `timed_out' to a watch.
        watches := non_neg_integer(),
        watches_fired := non_neg_integer(),
        watches_timed_out := non_neg_integer(),
        %% The module of the storage engine, the view of its store for
        %% another process, and whether the data of the engine is there
        %% after a stop of the StorageServer.
        %% See "Storage engine" in the module documentation.
        engine := module(),
        store := redwall_storage_engine:store(),
        durability := memory | disk
    }.
info(Storage) ->
    gen_server:call(Storage, info).

%% @doc Returns the stored version of the StorageServer, with the
%% generation of the coordinated state that the StorageServer has read and
%% with its version. The LogServers can discard the records at or below
%% the stored version of every StorageServer. See "Pop" in the module
%% documentation. The result is `{error, not_ready}' if the StorageServer
%% copies from a peer or has not read the coordinated state.
-spec stored(gen_server:server_ref(), timeout()) ->
    {ok, #{generation := non_neg_integer(), stored := version(), version := version()}} | {error, not_ready}.
stored(Storage, Timeout) ->
    gen_server:call(Storage, stored, Timeout).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options = #{coordinators := Coordinators}) ->
    Engine = redwall_storage_engine:module(maps:get(engine, Options, ets)),
    Store = Engine:new(Options),
    State = #state{
        coordinators = Coordinators,
        poll_ms = maps:get(poll_ms, Options, ?POLL_MS),
        cstate_timeout_ms = maps:get(cstate_timeout_ms, Options, ?CSTATE_TIMEOUT_MS),
        rollback = maps:get(rollback, Options, true),
        window = maps:get(mvcc_window, Options, ?MVCC_WINDOW),
        check_window = maps:get(check_window, Options, true),
        clear = maps:get(clear, Options, true),
        wait = maps:get(wait, Options, true),
        copy_rows = maps:get(copy_rows, Options, true),
        engine = Engine,
        store = Store,
        view = Engine:view(Store),
        watches = ets:new(redwall_storage_watches, [ordered_set, private]),
        watch_timeout_ms = maps:get(watch_timeout_ms, Options, ?WATCH_TIMEOUT_MS),
        max_watches = maps:get(max_watches, Options, ?MAX_WATCHES),
        watch_trigger = maps:get(watch_trigger, Options, true),
        committed_at_epoch_end = maps:get(committed_at_epoch_end, Options, true),
        buffer = gb_trees:empty(),
        published = atomics:new(3, [{signed, true}]),
        requests = gen_server:reqids_new(),
        worker = maps:get(worker, Options, undefined),
        peers =
            case Options of
                #{peers := Peers} ->
                    Peers;
                #{worker := Worker} ->
                    %% The other StorageServers that the ClusterController
                    %% has given to the worker of this node, or
                    %% `undefined' before its first answer.
                    Self = self(),
                    fun() ->
                        case redwall_worker:storages(Worker) of
                            undefined -> undefined;
                            Storages -> Storages -- [Self]
                        end
                    end;
                _ ->
                    []
            end
    },
    _ = is_atom(State#state.worker) andalso State#state.worker =/= undefined andalso redwall_worker:register_storage(State#state.worker, self()),
    case State#state.peers of
        [] ->
            {ok, start_polling(State)};
        _ ->
            %% The StorageServer reads the log after the copy.
            {ok, State#state{copy = probe}, {continue, copy}}
    end.

%% @private
%% One step of the copy from another StorageServer. See "Copy from
%% another StorageServer" in the module documentation.
handle_continue(copy, State = #state{copy = Probe, peers = Peers0}) when Probe =:= probe; Probe =:= reprobe ->
    Peers =
        case is_function(Peers0, 0) of
            true -> Peers0();
            false -> Peers0
        end,
    %% A temporary process asks the peers, so that this StorageServer
    %% answers the same question of a peer in that time. Two StorageServers
    %% that ask each other with a call at the same time would both wait
    %% for the time limit of the call.
    Mine = State#state.version,
    {_Pid, Monitor} = spawn_monitor(fun() ->
        %% Without a peer, the StorageServer has nothing to copy.
        %% `undefined' is the answer of a worker that does not know the
        %% StorageServers of the cluster yet, and the StorageServer asks
        %% again later.
        exit(
            {probe,
                case Peers of
                    [] -> empty;
                    undefined -> none;
                    _ -> probe(Peers, Mine, none)
                end}
        )
    end),
    {noreply, State#state{copy = {probing, Monitor, Probe}}};
handle_continue({probed, Answer, Probe}, State) ->
    case {Answer, Probe} of
        {{ok, Peer, Ref, Version}, _} ->
            %% The StorageServer removes its own data before the copy. It
            %% publishes the version of the copy as its oldest version
            %% first, so a direct read of the old data is answered with
            %% `too_old'.
            ok = atomics:put(State#state.published, 2, Version),
            ok = atomics:put(State#state.published, 1, 0),
            ok = (State#state.engine):clear(State#state.store),
            State1 = (watch_drop(State))#state{
                copy = #{peer => Peer, ref => Ref, version => Version, next => first},
                version = 0,
                oldest = Version,
                system = undefined,
                buffer = gb_trees:empty(),
                requests = gen_server:reqids_new(),
                sent = #{},
                cursors = #{},
                stopped = []
            },
            {noreply, State1, {continue, copy}};
        {empty, probe} ->
            %% The StorageServer reads the log from the start.
            ok = atomics:put(State#state.published, 2, 0),
            {noreply, start_polling(State#state{copy = undefined, oldest = 0})};
        {none, probe} ->
            _ = erlang:send_after(?COPY_RETRY_MS, self(), copy),
            {noreply, State#state{copy = probe}};
        {_NoPeerIsAhead, reprobe} ->
            {noreply, State#state{copy = undefined}}
    end;
handle_continue(copy, State = #state{copy = Copy = #{peer := Peer, ref := Ref, version := Version, next := Next}}) ->
    try gen_server:call(Peer, {copy_page, Ref, Next, ?COPY_PAGE}, ?TIMEOUT_MS) of
        {ok, Rows, done} ->
            ok = load_copy(Rows, State),
            ok = gen_server:cast(Peer, {copy_done, Ref}),
            ok = atomics:put(State#state.published, 2, Version),
            ok = atomics:put(State#state.published, 1, Version),
            %% A read that has waited for the copy with a read version
            %% below the version of the copy is too old: the copy has no
            %% version below its own.
            {Old, Waiting} = lists:partition(fun({_From, _Query, V}) -> V < Version end, State#state.waiting),
            lists:foreach(fun({From, _Query, _V}) -> gen_server:reply(From, {error, transaction_too_old}) end, Old),
            {noreply,
                answer_waiting(
                    start_polling(State#state{
                        copy = undefined,
                        copies = State#state.copies + 1,
                        version = Version,
                        oldest = Version,
                        kcv = Version,
                        checked = -1,
                        waiting = Waiting
                    })
                )};
        {ok, Rows, {next, Last}} ->
            ok = load_copy(Rows, State),
            {noreply, State#state{copy = Copy#{next := {after_key, Last}}}, {continue, copy}};
        {error, _Reason} ->
            copy_again(State)
    catch
        exit:_ -> copy_again(State)
    end.

%% Stores the rows of one page of a copy. With the planted defect
%% `copy_rows => false', the StorageServer stores no row.
load_copy(Rows, #state{engine = Engine, store = Store, copy_rows = true}) ->
    Engine:load(Store, Rows);
load_copy(_Rows, #state{copy_rows = false}) ->
    ok.

%% Removes the copied keys, and starts the copy again later.
copy_again(State = #state{engine = Engine, store = Store}) ->
    ok = Engine:clear(Store),
    _ = erlang:send_after(?COPY_RETRY_MS, self(), copy),
    {noreply, State#state{copy = probe}}.

%% Starts the timer of `poll', if it does not run.
start_polling(State = #state{polling = false}) ->
    self() ! poll,
    State#state{polling = true};
start_polling(State) ->
    State.

%% Asks the peers for the start of a copy, one after another. The result
%% is the first peer with a copy above the version `Mine', `empty' if a
%% peer answered and none has such a copy, or `none' if no peer answered.
probe([], _Mine, Answer) ->
    Answer;
probe([Peer | Peers], Mine, Answer) ->
    try gen_server:call(Peer, copy_start, ?COPY_START_TIMEOUT_MS) of
        {ok, Ref, Version} when Version > Mine ->
            {ok, Peer, Ref, Version};
        {ok, Ref, _NotAhead} ->
            ok = gen_server:cast(Peer, {copy_done, Ref}),
            probe(Peers, Mine, empty);
        empty ->
            probe(Peers, Mine, empty);
        {error, _NotReady} ->
            probe(Peers, Mine, Answer)
    catch
        exit:_ -> probe(Peers, Mine, Answer)
    end.

%% @private
handle_call({read, Key, Version}, From, State) ->
    read_at({key, Key}, Version, From, State);
handle_call({read_range, Begin, End, Version}, From, State) ->
    read_at({range, Begin, End}, Version, From, State);
handle_call({scan, Lower, Upper, Options, Version}, From, State) ->
    read_at({scan, Lower, Upper, Options}, Version, From, State);
handle_call({select, Begin, End, MatchSpec, Limit, Version}, From, State) ->
    read_at({select, Begin, End, MatchSpec, Limit}, Version, From, State);
handle_call({mapped, Begin, End, Mapper, Limit, Version}, From, State) ->
    read_at({mapped, Begin, End, Mapper, Limit}, Version, From, State);
handle_call({watch, Key, Value, Version}, From, State) ->
    {noreply, watch_add(Key, Value, Version, From, State)};
handle_call(handle, _From, State) ->
    {reply, handle_of(State), State};
handle_call(copy_start, _From, State = #state{copy = Copy}) when is_map(Copy) ->
    %% This StorageServer copies from a peer itself.
    {reply, {error, not_ready}, State};
handle_call(copy_start, _From, State = #state{version = 0}) ->
    %% This StorageServer has no data. It answers `empty' also while it
    %% asks its own peers: at the start of a new cluster every
    %% StorageServer asks the others, and each of them then reads the
    %% log.
    {reply, empty, State};
handle_call(copy_start, _From, State = #state{version = Version, kcv = KCV, pins = Pins}) ->
    case min(Version, KCV) of
        Copy when Copy > 0, Copy >= State#state.oldest ->
            Ref = make_ref(),
            Timer = erlang:send_after(?COPY_PIN_MS, self(), {unpin, Ref}),
            {reply, {ok, Ref, Copy}, State#state{pins = Pins#{Ref => {Copy, Timer}}}};
        _NoCommittedVersion ->
            %% This StorageServer has no version that is known to be
            %% committed. The other StorageServer reads the log from the
            %% start, which has every record in this case.
            {reply, empty, State}
    end;
handle_call({copy_page, Ref, Next, Limit}, From, State = #state{pins = Pins}) ->
    case Pins of
        #{Ref := {Version, Timer}} ->
            _ = erlang:cancel_timer(Timer),
            Timer1 = erlang:send_after(?COPY_PIN_MS, self(), {unpin, Ref}),
            %% A temporary process reads the page, and the StorageServer
            %% applies records in that time.
            #state{engine = Engine, view = View} = State,
            _ = spawn(fun() -> gen_server:reply(From, Engine:page(View, Next, Limit, Version)) end),
            {noreply, State#state{pins = Pins#{Ref := {Version, Timer1}}}};
        _ ->
            {reply, {error, expired}, State}
    end;
handle_call(stored, _From, State = #state{copy = Copy, generation = Generation}) when is_map(Copy); Generation =:= undefined ->
    {reply, {error, not_ready}, State};
handle_call(stored, _From, State = #state{version = Version, kcv = KCV, pins = Pins, generation = Generation}) ->
    %% A StorageServer that copies from this one reads the log from the
    %% version of its copy.
    Stored = lists:min([Version, KCV | [Pinned || {Pinned, _Timer} <- maps:values(Pins)]]),
    {reply, {ok, #{generation => Generation, stored => Stored, version => Version}}, State};
handle_call(info, _From, State = #state{version = Version, system = System, rollbacks = Rollbacks}) ->
    Epoch =
        case System of
            #{epoch := E} -> E;
            undefined -> undefined
        end,
    Info = #{
        version => Version,
        oldest => State#state.oldest,
        epoch => Epoch,
        rollbacks => Rollbacks,
        copies => State#state.copies,
        copying => State#state.copy =/= undefined,
        resets => State#state.resets,
        popped => State#state.popped,
        watches => State#state.watching + length(State#state.watch_waiting),
        watches_fired => State#state.watches_fired,
        watches_timed_out => State#state.watches_timed_out,
        engine => State#state.engine,
        store => State#state.view,
        durability => (State#state.engine):durability()
    },
    {reply, Info, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% @private
handle_cast({copy_done, Ref}, State) ->
    {noreply, advance_oldest(unpin(Ref, State))};
handle_cast({watch, From = {watch_request, _Pid, _Tag}, Key, Value, Version}, State) ->
    {noreply, watch_add(Key, Value, Version, From, State)};
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info(copy, State = #state{copy = probe}) ->
    {noreply, State, {continue, copy}};
handle_info({'DOWN', Monitor, process, _Pid, Reason}, State = #state{copy = {probing, Monitor, Probe}}) ->
    Answer =
        case Reason of
            {probe, Found} -> Found;
            _Failed -> none
        end,
    {noreply, State, {continue, {probed, Answer, Probe}}};
handle_info(poll, State = #state{poll_ms = PollMs, copy = Copy}) when Copy =/= undefined ->
    %% The StorageServer reads no log during a copy.
    _ = erlang:send_after(PollMs, self(), poll),
    {noreply, State};
handle_info(copy, State) ->
    {noreply, State};
handle_info({unpin, Ref}, State) ->
    {noreply, advance_oldest(unpin(Ref, State))};
handle_info({watch_timeout, Key, Ref}, State) ->
    {noreply, watch_timeout(Key, Ref, State)};
handle_info(poll, State = #state{poll_ms = PollMs, system = System, stopped = Stopped}) ->
    %% The StorageServer also reads the coordinated state at every tenth
    %% interval. A LogServer of an ended epoch that does not run, or that
    %% is cut off, does not answer that it is stopped. Without this read,
    %% the StorageServer would wait for that epoch without an end.
    State1 =
        case System =:= undefined orelse Stopped =/= [] orelse (State#state.ticks + 1) rem ?RESEND_TICKS =:= 0 of
            true -> refresh(State);
            false -> State
        end,
    _ = erlang:send_after(PollMs, self(), poll),
    Ticks = State1#state.ticks + 1,
    case Ticks rem ?RESEND_TICKS of
        0 ->
            %% The worker of the node can have started after this
            %% StorageServer.
            _ = State1#state.worker =/= undefined andalso redwall_worker:register_storage(State1#state.worker, self()),
            case stalled(State1) of
                true ->
                    %% The epoch of the StorageServer has ended, and the
                    %% StorageServer has applied no record in 10
                    %% intervals. The records that it needs can be on no
                    %% LogServer. It asks its peers for a copy.
                    {noreply, State1#state{ticks = Ticks, copy = reprobe}, {continue, copy}};
                false ->
                    {noreply, watch((tail_all(State1))#state{ticks = Ticks, checked = State1#state.version})}
            end;
        _ ->
            {noreply, watch((tail_new(State1))#state{ticks = Ticks})}
    end;
handle_info(refresh, State = #state{poll_ms = PollMs, refresh_ms = Ms}) ->
    State1 = refresh(State#state{refresh_timer = undefined}),
    case waits_for_epoch(State1) of
        true -> {noreply, watch(State1#state{refresh_ms = min(PollMs, Ms * 2)})};
        false -> {noreply, State1#state{refresh_ms = ?REFRESH_MS}}
    end;
handle_info(Message, State = #state{requests = Requests}) ->
    case gen_server:check_response(Message, Requests, true) of
        {{reply, {ok, Records, Stopped, KCV}}, Label, Requests1} ->
            State1 = State#state{requests = Requests1, kcv = max(KCV, State#state.kcv)},
            {noreply, watch(advance_oldest(received(Label, Records, Stopped, State1)))};
        {{reply, {popped, Popped}}, Label, Requests1} ->
            popped(Label, Popped, State#state{requests = Requests1});
        {{error, _Reason}, {Epoch, _Log, _Number}, Requests1} when Epoch =:= map_get(epoch, State#state.system) ->
            %% A LogServer of the epoch does not run, or the connection to
            %% its node has ended. The epoch can have ended, so the
            %% StorageServer reads the coordinated state.
            {noreply, watch(refresh(State#state{requests = Requests1}))};
        {_ErrorOrOtherReply, _Label, Requests1} ->
            {noreply, State#state{requests = Requests1}};
        _NoRequestOrNoReply ->
            {noreply, State}
    end.

%% ---------------------------------------------------------------------------
%% Epochs
%% ---------------------------------------------------------------------------

%% Whether the StorageServer has peers, reads an epoch that has ended, and
%% has the version of the last tenth interval.
stalled(#state{peers = Peers, system = System, version = Version, checked = Checked}) ->
    Peers =/= [] andalso is_map(System) andalso is_map_key(end_version, System) andalso Version =:= Checked.

%% Whether the StorageServer has no log system to read: it has none yet,
%% or a LogServer of its epoch is stopped and the next epoch is not known.
waits_for_epoch(#state{system = System, stopped = Stopped}) ->
    System =:= undefined orelse Stopped =/= [].

%% Starts the timer of an early read of the coordinated state, if the
%% StorageServer waits for an epoch. The first epoch and the epoch after a
%% recovery are in the coordinated state some milliseconds after the
%% start of the recovery. With a read only at the interval `poll_ms', a
%% read of a client waits for up to `poll_ms' in that time. The early
%% read is after 10 ms, and the time doubles up to `poll_ms'.
watch(State = #state{refresh_timer = undefined, poll_ms = PollMs, refresh_ms = Ms}) ->
    case waits_for_epoch(State) of
        true -> State#state{refresh_timer = erlang:send_after(min(Ms, PollMs), self(), refresh)};
        false -> State#state{refresh_ms = ?REFRESH_MS}
    end;
watch(State) ->
    State.

%% Reads the log systems from the coordinated state, and updates the log
%% system that the StorageServer reads.
refresh(State0 = #state{coordinators = Coordinators, cstate_timeout_ms = Timeout}) ->
    try redwall_cstate:read(Coordinators, Timeout) of
        {ok, CState = #{current := Current, old := Old}} ->
            State = generation(CState, State0),
            case Current of
                undefined ->
                    State;
                _ ->
                    Systems = lists:sort(fun(#{epoch := A}, #{epoch := B}) -> A =< B end, [Current | Old]),
                    follow(State#state{systems = Systems})
            end;
        _Error ->
            State0
    catch
        exit:_ -> State0
    end.

%% Compares the generation of the coordinated state with the generation
%% of the StorageServer. After a reset of the database the generation is
%% higher, and the StorageServer removes its data: the log systems of its
%% data are not in the coordinated state, and the versions start again.
%% See "Reset" in `redwall_recovery'. A StorageServer that copies from a
%% peer ends the copy first.
generation(#{generation := Generation}, State = #state{generation = undefined}) ->
    State#state{generation = Generation};
generation(#{generation := Generation}, State = #state{generation = Mine, copy = undefined}) when Generation =/= Mine ->
    logger:warning("redwall: the StorageServer ~p removes its data at the version ~b, after the reset ~b of the database.", [
        self(), State#state.version, Generation
    ]),
    %% A direct read with a read version of the time before the reset
    %% finds a version of 0, and a read in progress finds another number
    %% of resets.
    ok = atomics:put(State#state.published, 1, 0),
    ok = atomics:add(State#state.published, 3, 1),
    ok = (State#state.engine):clear(State#state.store),
    ok = atomics:put(State#state.published, 2, 0),
    lists:foreach(fun({From, _Query, _Version}) -> gen_server:reply(From, {error, transaction_too_old}) end, State#state.waiting),
    (watch_drop(State))#state{
        generation = Generation,
        resets = State#state.resets + 1,
        version = 0,
        oldest = 0,
        kcv = 0,
        checked = -1,
        system = undefined,
        systems = [],
        buffer = gb_trees:empty(),
        requests = gen_server:reqids_new(),
        sent = #{},
        cursors = #{},
        stopped = [],
        waiting = []
    };
generation(_CState, State) ->
    State.

%% Updates the log system that the StorageServer reads from the known log
%% systems and the version.
follow(State = #state{systems = []}) ->
    State;
follow(State = #state{system = undefined, systems = Systems, version = Version}) ->
    %% The epoch of the version: the first epoch without an end version,
    %% or with an end version at or above the version. A StorageServer
    %% without data starts with the first epoch. A StorageServer with a
    %% copy starts with the epoch of the version of the copy.
    [First | _] = [System || System <- Systems, not is_map_key(end_version, System) orelse Version =< map_get(end_version, System)],
    follow(start(First, State));
follow(State = #state{system = #{epoch := Epoch}, systems = Systems, version = Version}) ->
    case [System || System = #{epoch := E} <- Systems, E =:= Epoch] of
        [System = #{end_version := End}] when Version > End ->
            follow(rollback(End, State#state{system = System}));
        [#{end_version := End}] when Version =:= End ->
            %% The next epoch. The coordinated state has it, because the
            %% epoch `Epoch' has an end version.
            [Next | _] = [System || System = #{epoch := E} <- Systems, E > Epoch],
            %% No recovery discards a record at or below the end version
            %% of an ended epoch, so the version of the StorageServer is
            %% committed. See "Known Committed Version" in the module
            %% documentation.
            KCV =
                case State#state.committed_at_epoch_end of
                    true -> max(State#state.kcv, End);
                    %% The planted defect `committed_at_epoch_end =>
                    %% false'.
                    false -> State#state.kcv
                end,
            follow(start(Next, State#state{kcv = KCV}));
        [System = #{end_version := End}] ->
            Buffer = gb_trees:from_orddict([R || R = {LSN, _} <- gb_trees:to_list(State#state.buffer), LSN =< End]),
            State#state{system = System, buffer = Buffer};
        _CurrentEpoch ->
            State
    end.

%% Starts to read the log system `System'.
start(System, State) ->
    tail_all(State#state{system = System, buffer = gb_trees:empty(), sent = #{}, cursors = #{}, stopped = []}).

%% Removes every write above `End'.
rollback(End, State = #state{rollback = false}) ->
    %% The planted defect `rollback => false'.
    State#state{version = End, buffer = gb_trees:empty()};
rollback(End, State = #state{engine = Engine, store = Store, rollbacks = Rollbacks}) ->
    ok = atomics:put(State#state.published, 1, End),
    ok = Engine:rollback(Store, End),
    %% A removed write can be the reason that the value of a watch did not
    %% differ, so the StorageServer reads the key of every watch again.
    watch_ready(watch_requeue(State#state{version = End, buffer = gb_trees:empty(), rollbacks = Rollbacks + 1})).


%% ---------------------------------------------------------------------------
%% Records
%% ---------------------------------------------------------------------------

%% Sends a request to every LogServer of the epoch that runs and has not
%% answered that it is stopped.
tail_all(State = #state{system = undefined}) ->
    State;
tail_all(State = #state{system = #{logs := Logs}, stopped = Stopped}) ->
    lists:foldl(fun tail/2, State, Logs -- Stopped).

%% Sends a request to every LogServer of the epoch that runs and has
%% received no request.
tail_new(State = #state{system = undefined}) ->
    State;
tail_new(State = #state{system = #{logs := Logs}, stopped = Stopped, sent = Sent}) ->
    lists:foldl(fun tail/2, State, (Logs -- Stopped) -- maps:keys(Sent)).

tail(Log, State = #state{system = #{epoch := Epoch}, version = Version, requests = Requests, sent = Sent, cursors = Cursors}) ->
    case redwall_log:ref(Log) of
        undefined ->
            State;
        Pid ->
            Number = maps:get(Log, Sent, 0) + 1,
            %% The request is from the version at the least. The LogServer
            %% can have discarded the records below the version.
            After = max(maps:get(Log, Cursors, Version), Version),
            Requests1 = gen_server:send_request(Pid, {tail, After}, {Epoch, Log, Number}, Requests),
            State#state{requests = Requests1, sent = Sent#{Log => Number}}
    end.

%% The answer `{popped, Popped}' of the LogServer `Log' of `Epoch' to the
%% request `Number': the LogServer has discarded the records at or below
%% `Popped'. See "Pop" in the module documentation.
popped({Epoch, Log, Number}, Popped, State = #state{system = #{epoch := Epoch}, version = Version, sent = Sent}) when Popped =< Version ->
    %% The StorageServer has the discarded records. The next request is
    %% from its version, if this answer is the answer to the last
    %% request.
    case Sent of
        #{Log := Number} -> {noreply, tail(Log, State)};
        _ -> {noreply, State}
    end;
popped({Epoch, _Log, _Number}, _Popped, State = #state{system = #{epoch := Epoch}, copy = undefined, peers = Peers, popped = Count}) when Peers =/= [] ->
    %% The LogServer does not have the records that the StorageServer
    %% needs. A peer with a copy above the version has them.
    {noreply, State#state{copy = reprobe, popped = Count + 1}, {continue, copy}};
popped({Epoch, _Log, _Number}, _Popped, State = #state{system = #{epoch := Epoch}, copy = undefined, popped = Count}) ->
    %% The StorageServer has no peer, and does not continue.
    {noreply, State#state{popped = Count + 1}};
popped(_Label, _Popped, State) ->
    {noreply, State}.

%% The answer of the LogServer `Log' of `Epoch' to the request `Number'.
received({Epoch, Log, Number}, Records, Stopped, State = #state{system = System = #{epoch := Epoch}}) ->
    #state{version = Version, buffer = Buffer, cursors = Cursors} = State,
    End = maps:get(end_version, System, infinity),
    Buffer1 = lists:foldl(
        fun
            ({LSN, Prev, Body}, Acc) when LSN > Version, End =:= infinity orelse LSN =< End ->
                case gb_trees:lookup(LSN, Acc) of
                    none -> gb_trees:insert(LSN, {Prev, Body}, Acc);
                    {value, {Prev, empty}} when Body =/= empty -> gb_trees:update(LSN, {Prev, Body}, Acc);
                    {value, _} -> Acc
                end;
            (_Record, Acc) ->
                Acc
        end,
        Buffer,
        Records
    ),
    Cursor = lists:max([maps:get(Log, Cursors, Version) | [LSN || {LSN, _Prev, _Body} <- Records]]),
    State1 = apply_records(State#state{buffer = Buffer1, cursors = Cursors#{Log => Cursor}}),
    State2 =
        case Stopped of
            true -> refresh(State1#state{stopped = lists:usort([Log | State1#state.stopped])});
            false -> follow(State1)
        end,
    %% The next request, if the StorageServer still reads this epoch and
    %% this answer is the answer to the last request.
    case State2 of
        #state{system = #{epoch := Epoch}, sent = #{Log := Number}} when not Stopped -> tail(Log, State2);
        _ -> State2
    end;
received(_OtherEpoch, _Records, _Stopped, State) ->
    State.

%% Applies the records of the buffer in the order of the chain.
apply_records(State = #state{version = Version, buffer = Buffer}) ->
    case gb_trees:is_empty(Buffer) of
        true ->
            answer_waiting(State);
        false ->
            case gb_trees:smallest(Buffer) of
                {LSN, _Record} when LSN =< Version ->
                    apply_records(State#state{buffer = gb_trees:delete(LSN, Buffer)});
                {LSN, {Version, {body, Body}}} ->
                    ok = write(without_clears(Body, State), LSN, State),
                    %% The rows are written before the version is published.
                    ok = atomics:put(State#state.published, 1, LSN),
                    State1 = State#state{version = LSN, buffer = gb_trees:delete(LSN, Buffer)},
                    apply_records(watch_trigger(Body, State1));
                _BodyNotReceivedOrPreviousRecordNotReceived ->
                    answer_waiting(State)
            end
    end.

%% The planted defect `clear => false': the body without the clears.
without_clears({batch, Entries}, #state{clear = false}) ->
    {batch, [
        case is_list(Entry) of
            true -> [Mutation || Mutation <- Entry, element(1, Mutation) =:= set];
            false -> Entry
        end
     || Entry <- Entries
    ]};
without_clears(Body, _State) ->
    Body.

%% Applies the mutations of each transaction of a record, in their order.
%% An entry that is not a list has no transaction.
write({batch, Entries}, LSN, #state{engine = Engine, store = Store}) ->
    lists:foreach(fun(Entry) -> ok = Engine:write(Store, LSN, Entry) end, [Entry || Entry <- Entries, is_list(Entry)]);
write(_NoTransaction, _LSN, _State) ->
    ok.


%% ---------------------------------------------------------------------------
%% MVCC window
%% ---------------------------------------------------------------------------

%% Raises the oldest version, and removes the versions that no read at or
%% above the oldest version returns.
advance_oldest(State = #state{window = infinity}) ->
    State;
advance_oldest(State = #state{version = Version, kcv = KCV, oldest = Oldest, window = Window, pins = Pins}) ->
    %% The oldest version stays at or below the version of each copy that
    %% another StorageServer reads from this one.
    case lists:min([min(Version - Window, KCV) | [Pinned || {Pinned, _Timer} <- maps:values(Pins)]]) of
        New when New > Oldest ->
            %% The oldest version is published before the versions below
            %% it are removed. With the planted defect
            %% `check_window => false' it is not published.
            _ = State#state.check_window andalso atomics:put(State#state.published, 2, New),
            ok = (State#state.engine):advance(State#state.store, New),
            State#state{oldest = New};
        _ ->
            State
    end.


%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

%% Answers a read at `Version', or keeps the read until the StorageServer
%% has the version.
read_at(_Query, Version, _From, State = #state{oldest = Oldest, check_window = true}) when Version < Oldest ->
    {reply, {error, transaction_too_old}, State};
read_at(Query, Version, From, State = #state{version = Mine, wait = Wait}) when Version =< Mine; not Wait ->
    ok = answer(From, Query, Version, State),
    {noreply, State};
read_at(Query, Version, From, State = #state{waiting = Waiting}) ->
    {noreply, State#state{waiting = [{From, Query, Version} | Waiting]}}.

answer_waiting(State = #state{version = Version, waiting = Waiting}) ->
    {Ready, Later} = lists:partition(fun({_From, _Query, V}) -> V =< Version end, Waiting),
    lists:foreach(fun({From, Query, V}) -> answer(From, Query, V, State) end, Ready),
    watch_ready(State#state{waiting = Later}).

%% Answers a read at a version that the StorageServer has applied. A
%% select and a mapped range read can read many rows, so a temporary
%% process performs each as a direct read, and the StorageServer applies
%% records in that time.
answer(From, Query, Version, State) when element(1, Query) =:= select; element(1, Query) =:= mapped ->
    Handle = handle_of(State),
    _ = spawn(fun() ->
        Reply =
            try read_direct(Handle, Query, Version) of
                {ok, Result} -> Result;
                too_old -> {error, transaction_too_old};
                Other -> {error, Other}
            catch
                error:badarg -> {error, badarg}
            end,
        gen_server:reply(From, Reply)
    end),
    ok;
answer(From, Query, Version, State) ->
    gen_server:reply(From, (State#state.engine):read(State#state.store, Query, Version)),
    ok.

%% ---------------------------------------------------------------------------
%% Watches
%% ---------------------------------------------------------------------------

%% A new watch. See "Watches" in the module documentation.
watch_add(Key, Value, Version, From, State = #state{version = Mine, copy = undefined}) when Version =< Mine ->
    case watch_differs(Key, Value, State) of
        true ->
            watch_reply(From, {ok, Mine}),
            State#state{watches_fired = State#state.watches_fired + 1};
        false ->
            case watch_full(State) of
                true ->
                    watch_reply(From, {error, watch_cancelled}),
                    State;
                false ->
                    Ref = make_ref(),
                    watch_keep(Ref, Key, Value, Version, From, watch_timer(Key, Ref, State), State)
            end
    end;
watch_add(Key, Value, Version, From, State = #state{watch_waiting = Waiting}) ->
    %% The StorageServer has not applied the version of the watch, or it
    %% copies from a peer.
    case watch_full(State) of
        true ->
            watch_reply(From, {error, watch_cancelled}),
            State;
        false ->
            Ref = make_ref(),
            State#state{watch_waiting = [{Ref, Key, Value, Version, From, watch_timer(Key, Ref, State)} | Waiting]}
    end.

%% Sends the answer to a watch: to the caller of `watch_value/5', or to
%% the process of `watch_request/4'.
watch_reply({watch_request, Pid, Tag}, Answer) ->
    Pid ! {redwall_watch_answer, Tag, Answer},
    ok;
watch_reply(From, Answer) ->
    gen_server:reply(From, Answer),
    ok.

watch_full(#state{watching = Watching, watch_waiting = Waiting, max_watches = Max}) ->
    Watching + length(Waiting) >= Max.

watch_timer(_Key, _Ref, #state{watch_timeout_ms = infinity}) ->
    undefined;
watch_timer(Key, Ref, #state{watch_timeout_ms = Ms}) ->
    erlang:send_after(Ms, self(), {watch_timeout, Key, Ref}).

%% Whether the value of the key at the version of the StorageServer
%% differs from the value of a watch.
watch_differs(Key, Value, #state{version = Version, engine = Engine, store = Store}) ->
    Now =
        case Engine:read(Store, {key, Key}, Version) of
            {ok, Stored, _Written} -> {ok, Stored};
            not_found -> not_found
        end,
    Now =/= Value.

%% Adds a watch whose value does not differ to the table of the watches.
watch_keep(Ref, Key, Value, Version, From, Timer, State = #state{watches = Watches, watching = Watching}) ->
    true = ets:insert(Watches, {{Key, Ref}, Value, Version, From, Timer}),
    State#state{watching = Watching + 1}.

%% Handles the watches that have waited for a version at or below the
%% version of the StorageServer: an answer, or a row in the table of the
%% watches.
watch_ready(State = #state{watch_waiting = []}) ->
    State;
watch_ready(State = #state{copy = Copy}) when Copy =/= undefined ->
    State;
watch_ready(State = #state{version = Mine, watch_waiting = Waiting}) ->
    {Ready, Later} = lists:partition(fun({_Ref, _Key, _Value, Version, _From, _Timer}) -> Version =< Mine end, Waiting),
    lists:foldl(
        fun({Ref, Key, Value, Version, From, Timer}, Acc) ->
            case watch_differs(Key, Value, Acc) of
                true -> watch_answer(From, Timer, Acc);
                false -> watch_keep(Ref, Key, Value, Version, From, Timer, Acc)
            end
        end,
        State#state{watch_waiting = Later},
        %% The order of the requests.
        lists:reverse(Ready)
    ).

%% Answers a watch whose value differs.
watch_answer(From, Timer, State = #state{version = Mine, watches_fired = Fired}) ->
    _ = Timer =/= undefined andalso erlang:cancel_timer(Timer),
    watch_reply(From, {ok, Mine}),
    State#state{watches_fired = Fired + 1}.

%% Reads the key of each watch that a mutation of the applied record
%% names. With the planted defect `watch_trigger => false' the function
%% does nothing.
watch_trigger({batch, Entries}, State = #state{watching = Watching, watch_trigger = true, watches = Watches}) when Watching > 0 ->
    Named = lists:usort(lists:append([watch_named(Mutation, Watches) || Entry <- Entries, is_list(Entry), Mutation <- Entry])),
    lists:foldl(
        fun(Id = {Key, _Ref}, Acc) ->
            case ets:lookup(Watches, Id) of
                [{Id, Value, _Version, From, Timer}] ->
                    case watch_differs(Key, Value, Acc) of
                        true ->
                            true = ets:delete(Watches, Id),
                            watch_answer(From, Timer, Acc#state{watching = Acc#state.watching - 1});
                        false ->
                            Acc
                    end;
                [] ->
                    Acc
            end
        end,
        State,
        Named
    );
watch_trigger(_Body, State) ->
    State.

%% The ids `{Key, Reference}' of the watches on the key of a mutation, or
%% on the keys of a cleared range. The rows of a key are next to each
%% other in the table, because a number is below a reference in the order
%% of Erlang terms.
watch_named({clear_range, Begin, End}, Watches) ->
    watch_ids(ets:next(Watches, {Begin, 0}), fun(Key) -> Key < End end, Watches);
watch_named(Mutation, Watches) when tuple_size(Mutation) >= 2 ->
    Key = element(2, Mutation),
    watch_ids(ets:next(Watches, {Key, 0}), fun(Other) -> Other == Key end, Watches);
watch_named(_Other, _Watches) ->
    [].

watch_ids(Id = {Key, _Ref}, Within, Watches) ->
    case Within(Key) of
        true -> [Id | watch_ids(ets:next(Watches, Id), Within, Watches)];
        false -> []
    end;
watch_ids('$end_of_table', _Within, _Watches) ->
    [].

%% Moves every watch of the table to the watches that wait for a version.
watch_requeue(State = #state{watching = 0}) ->
    State;
watch_requeue(State = #state{watches = Watches, watch_waiting = Waiting}) ->
    Rows = [{Ref, Key, Value, Version, From, Timer} || {{Key, Ref}, Value, Version, From, Timer} <- ets:tab2list(Watches)],
    true = ets:delete_all_objects(Watches),
    State#state{watching = 0, watch_waiting = Rows ++ Waiting}.

%% Answers `not_ready' to every watch. The StorageServer removes its
%% data.
watch_drop(State0) ->
    State = #state{watch_waiting = Waiting} = watch_requeue(State0),
    lists:foreach(
        fun({_Ref, _Key, _Value, _Version, From, Timer}) ->
            _ = Timer =/= undefined andalso erlang:cancel_timer(Timer),
            watch_reply(From, {error, not_ready})
        end,
        Waiting
    ),
    State#state{watch_waiting = []}.

%% The time limit of a watch has passed.
watch_timeout(Key, Ref, State = #state{watches = Watches, watch_waiting = Waiting, watches_timed_out = Count}) ->
    case ets:take(Watches, {Key, Ref}) of
        [{_Id, _Value, _Version, From, _Timer}] ->
            watch_reply(From, {error, timed_out}),
            State#state{watching = State#state.watching - 1, watches_timed_out = Count + 1};
        [] ->
            case lists:keytake(Ref, 1, Waiting) of
                {value, {Ref, _Key, _Value, _Version, From, _Timer}, Rest} ->
                    watch_reply(From, {error, timed_out}),
                    State#state{watch_waiting = Rest, watches_timed_out = Count + 1};
                false ->
                    State
            end
    end.

%% Ends a copy that another StorageServer reads from this one.
unpin(Ref, State = #state{pins = Pins}) ->
    case maps:take(Ref, Pins) of
        {{_Version, Timer}, Pins1} ->
            _ = erlang:cancel_timer(Timer),
            State#state{pins = Pins1};
        error ->
            State
    end.


%% The handle for direct reads. `features' has the kinds of read that this
%% StorageServer supports in addition to keys and ranges.
handle_of(#state{engine = Engine, view = View, published = Published, wait = Wait}) ->
    #{engine => Engine, store => View, published => Published, wait => Wait, features => Engine:features()}.

%% @doc Returns every version of every key, from the result of `info/1'
%% or of `handle/1': a map from the key to the list of `{Version, Change}'
%% with the highest version first. `Change' is `{set, Value}' or `clear'.
%% The function is for tests and for the simulation. It reads every row.
-spec versions(#{engine := module(), store := redwall_storage_engine:store(), _ => _}) -> #{term() => [{version(), {set, term()} | clear}]}.
versions(#{engine := Engine, store := Store}) ->
    Engine:versions(Store).
