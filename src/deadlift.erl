%% @doc The client of deadlift.
%%
%% The functions have the names, the arguments and the results of erlfdb
%% (https://github.com/foundationdb-beam/erlfdb, version 1.2.3), so that
%% deadlift can replace erlfdb in an application later. This module has
%% the basic functions only. See "Limitations" below.
%%
%% == Example ==
%%
%% ```
%% Db = deadlift:open(#{proxy => Proxy, storage => Storage}),
%% deadlift:transactional(Db, fun(Tx) ->
%%     Balance = deadlift:wait(deadlift:get(Tx, account)),
%%     deadlift:set(Tx, account, Balance + 1)
%% end).
%% '''
%%
%% == Transaction ==
%%
%% A transaction of erlfdb is a mutable object of the C library of
%% FoundationDB. A transaction of deadlift is an ETS table of type
%% `ordered_set'. The table is `public', so several processes can use one
%% transaction, as with erlfdb. The table has these rows:
%%
%% <ul>
%% <li>`{read_version, Version}': the read version, from the first read
%%   on.</li>
%% <li>`{{r, Key}}' and `{{rr, N}, Begin, End}': the read conflict keys and
%%   the read conflict ranges.</li>
%% <li>`{{w, Key}, {set, Value} | clear}': the last write of each key, for
%%   the reads of the same transaction.</li>
%% <li>`{{cr, N}, Begin, End}': the cleared key ranges.</li>
%% <li>`{{m, N}, Mutation}': the mutations in their order.</li>
%% </ul>
%%
%% The table ends with the process that created the transaction.
%% `transactional/2' deletes the table when it returns.
%%
%% == Reads ==
%%
%% <ul>
%% <li>The first read asks the Proxy for a read version. Every read of the
%%   transaction uses this version.</li>
%% <li>A read returns a write of the same transaction (paper, section
%%   2.4.1). Such a read adds no read conflict key.</li>
%% <li>Another read goes to a StorageServer. See "StorageServers"
%%   below.</li>
%% <li>A read of a transaction adds a read conflict key or a read conflict
%%   range. A read of a snapshot (`snapshot/1') adds none.</li>
%% <li>`get/2' returns a future, as in erlfdb. The future of deadlift is
%%   the result itself, and `wait/1' returns its argument. `wait/1' of
%%   erlfdb also returns an argument that is not a future.</li>
%% </ul>
%%
%% == StorageServers ==
%%
%% A database names one StorageServer or several. Each StorageServer has
%% every key (see {@link deadlift_storage}), so each of them can answer
%% each read. A read uses the StorageServers in this way:
%%
%% <ol>
%% <li>The read puts the StorageServers in an order. The first one is
%%   selected by a hash of the read version, so that the transactions use
%%   the StorageServers in equal parts, and the reads of one transaction
%%   start at the same StorageServer.</li>
%% <li>The read tries each StorageServer on the node of the caller, in the
%%   order. It reads the ETS table of the StorageServer in the process of
%%   the caller, without a message
%%   (`deadlift_storage:read_direct/3'). A StorageServer that has applied
%%   the read version gives the result. An ETS table cannot be read from
%%   another node.</li>
%% <li>If no StorageServer gave a result, the read sends a message to each
%%   StorageServer in the order, on any node. The StorageServer process
%%   waits for the read version. The read has 5 seconds in total, in equal
%%   parts for the StorageServers. When a StorageServer is not alive, or
%%   does not answer in its part of the time, the read continues with the
%%   next one.</li>
%% <li>When a StorageServer answers that the read version is too old, the
%%   read raises 1007 and tries no other StorageServer.</li>
%% <li>When no StorageServer answered, the read raises 1009 if a
%%   StorageServer did not answer in time, and 1037 otherwise.</li>
%% </ol>
%%
%% The option `direct => false' of `open/1' turns step 2 off.
%%
%% The client of FoundationDB chooses a StorageServer of the team from a
%% model of the latency of each one, and sends a second request to another
%% one when the first is slow (`fdbrpc/LoadBalance.actor.h'). It tries the
%% next StorageServer after a failed delivery and after `process_behind',
%% and returns `future_version' and `transaction_too_old' to the caller.
%% deadlift has no model of the latency and no second request. A
%% StorageServer of deadlift does not answer `future_version', so the
%% client continues with the next StorageServer after its part of the
%% time.
%%
%% == Errors ==
%%
%% A function raises `error({erlfdb_error, Code})' with the error code of
%% FoundationDB, as erlfdb does. The codes are from
%% `flow/include/flow/error_definitions.h' of FoundationDB 7.3:
%%
%% <ul>
%% <li>1007, `transaction_too_old': the read version is below the MVCC
%%   window of the Resolver or of the StorageServer.</li>
%% <li>1009, `future_version': the StorageServer did not reach the read
%%   version in time.</li>
%% <li>1020, `not_committed': the transaction has a conflict.</li>
%% <li>1021, `commit_unknown_result': the transaction is committed or is
%%   not committed.</li>
%% <li>1025, `transaction_cancelled'.</li>
%% <li>1037, `process_behind': the Proxy does not answer, or no
%%   StorageServer is alive. FoundationDB has this code for a StorageServer that is
%%   behind. The client of FoundationDB waits for a new Proxy instead of
%%   raising an error.</li>
%% </ul>
%%
%% `on_error/2' follows `Transaction::onError' of
%% `fdbclient/NativeAPI.actor.cpp':
%%
%% <ul>
%% <li>For 1020, 1021 and 1037 it resets the transaction and waits for a
%%   random part of the backoff. The backoff starts at 10 ms, doubles with
%%   each error and is at most 1 s. The options `retry_delay_ms' and
%%   `max_retry_delay_ms' of `open/1' change the two values.</li>
%% <li>For 1007 and 1009 it resets the transaction and waits for 10
%%   ms.</li>
%% <li>For another code it raises the error again.</li>
%% </ul>
%%
%% `transactional/2' runs the function again after `on_error/2' returns.
%% After 1021 the first attempt can be committed, so the function must
%% give the same result when it runs twice.
%%
%% == Profiles ==
%%
%% A profile is a set of client options for one kind of StorageServer.
%% `open/1' takes the profile in the option `profile', and `profile/1'
%% returns its options. An option of `open/1' replaces the option of the
%% profile.
%%
%% <ul>
%% <li>`default': the values of FoundationDB. The first backoff after a
%%   conflict is 10 ms and the highest backoff is 1,000 ms.</li>
%% <li>`ets': for the StorageServer of deadlift, which keeps the data in
%%   ETS tables in memory. A commit takes about 0.2 ms with this
%%   StorageServer, so the first backoff is 1 ms and the highest backoff
%%   is 100 ms. The values are from the benchmark of 2026-11-23 in
%%   README.md.</li>
%% </ul>
%%
%% A StorageServer with another store can need other values, and gets a
%% profile of its own. A profile is not taken from FoundationDB. This part
%% is not in the paper.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The module has no key selectors, no options of `get_range', no
%%   atomic operations, no versionstamped keys and values, no watches, no
%%   transaction options, no tenants and no `get_last_error/0'.</li>
%% <li>A database is a Proxy and a list of StorageServers that the caller
%%   names. deadlift has no process that tells a client which Proxy and
%%   which StorageServers run.</li>
%% <li>A key and a value are Erlang terms. A key and a value of erlfdb are
%%   binaries.</li>
%% <li>A transaction that `create_transaction/1' returns keeps its table
%%   until its process ends. erlfdb frees a transaction when no process
%%   has a reference to it.</li>
%% <li>The transaction has no limit of time or of size on the client. The
%%   MVCC window gives the limit of 5 seconds for a read.</li>
%% <li>Two processes that use one transaction at the same time get no
%%   error. FoundationDB raises `used_during_commit' in some of these
%%   cases.</li>
%% </ul>
-module(deadlift).

-include("deadlift_eta.hrl").

-export([open/0, open/1, profile/1, create_transaction/1, transactional/2, snapshot/1]).
-export([get/2, get_range/3, select/4, select/5, set/3, clear/2, clear_range/3]).
-export([commit/1, reset/1, cancel/1, on_error/2, wait/1, wait/2]).
-export([get_read_version/1, set_read_version/2, get_committed_version/1, get_versionstamp/1]).
-export([add_read_conflict_key/2, add_read_conflict_range/3, is_read_only/1]).

-export_type([database/0, transaction/0, snapshot/0, key/0, value/0, profile/0]).

-type key() :: term().
-type value() :: term().
-type version() :: deadlift_cstate:version().

%% A process, or a function that returns the process that runs now.
-type server() :: gen_server:server_ref() | fun(() -> gen_server:server_ref() | undefined).

%% One StorageServer, a list of StorageServers, or a function that returns
%% the StorageServers that run now.
-type storage() ::
    gen_server:server_ref()
    | [gen_server:server_ref()]
    | fun(() -> gen_server:server_ref() | [gen_server:server_ref()] | undefined).

-type servers() :: #{
    proxy => server(),
    storage => storage(),
    direct => boolean(),
    retry_delay_ms => number(),
    max_retry_delay_ms => number(),
    profile => profile()
}.

-type profile() :: default | ets.

-type database() :: {deadlift_database, servers()}.
-type transaction() :: {deadlift_transaction, ets:tid()}.
-type snapshot() :: {deadlift_snapshot, transaction()}.

-define(IS_DB, {deadlift_database, _}).
-define(IS_TX, {deadlift_transaction, _}).
-define(IS_SS, {deadlift_snapshot, _}).

-define(TIMEOUT_MS, 5000).
%% DEFAULT_BACKOFF, DEFAULT_MAX_BACKOFF and FUTURE_VERSION_RETRY_DELAY of
%% `fdbclient/ClientKnobs.cpp', in milliseconds.
-define(BACKOFF_MS, 10).
-define(MAX_BACKOFF_MS, 1000).
-define(FUTURE_VERSION_RETRY_DELAY_MS, 10).

-define(TRANSACTION_TOO_OLD, 1007).
-define(FUTURE_VERSION, 1009).
-define(NOT_COMMITTED, 1020).
-define(COMMIT_UNKNOWN_RESULT, 1021).
-define(TRANSACTION_CANCELLED, 1025).
-define(PROCESS_BEHIND, 1037).

%% ---------------------------------------------------------------------------
%% Database and transaction
%% ---------------------------------------------------------------------------

%% @doc Returns a database. Without the options `proxy' and `storage', the
%% database uses the worker and the StorageServer that the deadlift
%% application started on this node (see README.md, "Start"): the Proxy is
%% the process of the current epoch, and the StorageServer is the
%% StorageServer of this node. The call raises `no_storage_server' if the
%% application has no StorageServer.
%%
%% With the options, the function returns a database for a Proxy and for
%% one StorageServer or several. The Proxy is a process, or a function that returns the process
%% that runs now. `storage' is a process, a list of processes, or a
%% function that returns one of the two. With `direct => false', no read
%% uses the ETS table of a StorageServer.
%%
%% `retry_delay_ms' is the first backoff of `on_error/2' after a conflict,
%% in milliseconds. The default is 10, which is `DEFAULT_BACKOFF' of
%% FoundationDB. `max_retry_delay_ms' is the highest backoff. The default
%% is 1,000. FoundationDB has the transaction option `max_retry_delay' for
%% it. FoundationDB has no option for the first backoff.
%%
%% `profile' is `default' or `ets'. See "Profiles" in the module
%% documentation. The default is `default'. Each option above replaces the
%% option of the profile.
-spec open(servers()) -> database().
open(Servers) when not is_map_key(proxy, Servers); not is_map_key(storage, Servers) ->
    open(maps:merge(local(), Servers));
open(Servers = #{proxy := _, storage := _}) ->
    Options = maps:with([proxy, storage, direct, retry_delay_ms, max_retry_delay_ms], Servers),
    {deadlift_database, maps:merge(profile(maps:get(profile, Servers, default)), Options)}.

%% @equiv open(#{})
-spec open() -> database().
open() ->
    open(#{}).

%% The Proxy and the StorageServer of a node that runs the deadlift
%% application with a worker and a StorageServer. The Proxy is the process
%% of the current epoch, from the information of the ClusterController
%% that the worker of this node has. The StorageServer is the child of the
%% supervisor at the time of this call.
local() ->
    Storage =
        try [Pid || {deadlift_storage, Pid, _Type, _Modules} <- supervisor:which_children(deadlift_sup), is_pid(Pid)] of
            [Pid] -> Pid;
            [] -> error(no_storage_server)
        catch
            exit:_ -> error(no_storage_server)
        end,
    Proxy = fun() ->
        case deadlift_worker:db_info() of
            #{proxies := [Process | _]} -> Process;
            _ -> undefined
        end
    end,
    #{proxy => Proxy, storage => Storage}.

%% @doc Returns the client options of a profile.
-spec profile(profile()) -> #{direct := boolean(), retry_delay_ms := number(), max_retry_delay_ms := number()}.
profile(default) ->
    #{direct => true, retry_delay_ms => ?BACKOFF_MS, max_retry_delay_ms => ?MAX_BACKOFF_MS};
profile(ets) ->
    #{direct => true, retry_delay_ms => 1, max_retry_delay_ms => 100}.

-spec create_transaction(database()) -> transaction().
create_transaction({deadlift_database, Servers}) ->
    Table = ets:new(deadlift_transaction, [ordered_set, public]),
    true = ets:insert(Table, [{db, Servers}, {state, open}, {seq, 0}, {backoff, maps:get(retry_delay_ms, Servers, ?BACKOFF_MS)}]),
    {deadlift_transaction, Table}.

%% @doc Runs `Fun' in a transaction, and commits the transaction. After an
%% error that `on_error/2' accepts, `transactional/2' runs `Fun' again. With
%% a transaction or a snapshot as the first argument, the function runs
%% `Fun' and does not commit.
-spec transactional(database() | transaction() | snapshot(), fun((transaction() | snapshot()) -> Result)) -> Result.
transactional(?IS_DB = Db, Fun) when is_function(Fun, 1) ->
    Tx = {deadlift_transaction, Table} = create_transaction(Db),
    try
        attempt(Tx, Fun)
    after
        ets:delete(Table)
    end;
transactional(?IS_TX = Tx, Fun) when is_function(Fun, 1) ->
    Fun(Tx);
transactional(?IS_SS = Snapshot, Fun) when is_function(Fun, 1) ->
    Fun(Snapshot).

attempt(Tx, Fun) ->
    try
        Result = Fun(Tx),
        case is_read_only(Tx) of
            true -> ok;
            false -> ok = wait(commit(Tx))
        end,
        {ok, Result}
    of
        {ok, Value} -> Value
    catch
        error:{erlfdb_error, Code} ->
            ok = wait(on_error(Tx, Code)),
            attempt(Tx, Fun)
    end.

%% @doc Returns an object for reads without read conflict keys.
-spec snapshot(transaction() | snapshot()) -> snapshot().
snapshot(?IS_TX = Tx) -> {deadlift_snapshot, Tx};
snapshot(?IS_SS = Snapshot) -> Snapshot.

%% @doc Returns the result of a future. A future of deadlift is the result
%% itself.
-spec wait(Result) -> Result.
wait(Result) -> Result.

-spec wait(Result, [term()]) -> Result.
wait(Result, _Options) -> Result.

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

%% @doc Reads the value of `Key'. The result is the value or `not_found'.
-spec get(transaction() | snapshot(), key()) -> value() | not_found.
get(TxOrSnapshot, Key) ->
    {Table, Conflict} = table(TxOrSnapshot),
    case ets:lookup(Table, {w, Key}) of
        [{_, {set, Value}}] ->
            Value;
        [{_, clear}] ->
            not_found;
        [] ->
            case cleared(Table, Key) of
                true ->
                    not_found;
                false ->
                    Result =
                        case read(Table, {key, Key}) of
                            {ok, Value, _Written} -> Value;
                            not_found -> not_found
                        end,
                    _ = Conflict andalso ets:insert(Table, {{r, Key}}),
                    Result
            end
    end.

%% @doc Reads the keys `K' with `Begin =< K' and `K < End'. The result is
%% the list of `{Key, Value}' in the order of the keys.
-spec get_range(transaction() | snapshot(), key(), key()) -> [{key(), value()}].
get_range(TxOrSnapshot, Begin, End) ->
    {Table, Conflict} = table(TxOrSnapshot),
    {ok, Rows} = read(Table, {range, Begin, End}),
    %% The stored keys that the transaction has not written and not
    %% cleared, and the keys that the transaction has set.
    Stored = [
        {Key, Value}
     || {Key, Value, _Written} <- Rows, not ets:member(Table, {w, Key}), not cleared(Table, Key)
    ],
    Guards = [{'>=', '$1', {const, Begin}}, {'<', '$1', {const, End}}],
    Written = ets:select(Table, [{{{w, '$1'}, {set, '$2'}}, Guards, [{{'$1', '$2'}}]}]),
    _ = Conflict andalso ets:insert(Table, {{rr, next(Table)}, Begin, End}),
    lists:ukeymerge(1, Written, Stored).

%% @equiv select(TxOrSnapshot, Begin, End, MatchSpec, [])
-spec select(transaction() | snapshot(), key(), key(), ets:match_spec()) -> [term()].
select(TxOrSnapshot, Begin, End, MatchSpec) ->
    select(TxOrSnapshot, Begin, End, MatchSpec, []).

%% @doc Evaluates an ETS match spec on the keys of the range
%% `{Begin, End}', and returns the results in the order of the keys.
%%
%% The match spec is for rows `{Key, Value}'. The result is the result of
%% `ets:match_spec_run/2' on the result of `get_range/3'. The
%% StorageServer evaluates the match spec, so a value that does not match
%% does not reach the client. See "Select" in {@link deadlift_storage}.
%%
%% <ul>
%% <li>The transaction gets the whole range as a read conflict range, as
%%   `get_range/3' does. A write of another transaction to a key of the
%%   range that does not match is a conflict too, because the write can
%%   make the key match.</li>
%% <li>The function sees the sets and the clears of this transaction.</li>
%% <li>`Options' is a list. `{limit, N}' returns the first `N' results. To
%%   continue, a caller starts the next call at the last key, and
%%   excludes that key with a guard of the match spec.</li>
%% <li>A match spec that ETS does not accept raises `badarg'.</li>
%% </ul>
%%
%% FoundationDB and erlfdb have no such function. Only a StorageServer
%% whose handle has the feature `select' supports it. This part is not in
%% the paper.
-spec select(transaction() | snapshot(), key(), key(), ets:match_spec(), [{limit, pos_integer()}]) -> [term()].
select(TxOrSnapshot, Begin, End, MatchSpec, Options) when is_list(Options) ->
    {Table, Conflict} = table(TxOrSnapshot),
    Limit = proplists:get_value(limit, Options, infinity),
    %% The writes of this transaction in the range.
    Guards = [{'>=', '$1', {const, Begin}}, {'<', '$1', {const, End}}],
    Written = ets:select(Table, [{{{w, '$1'}, {set, '$2'}}, Guards, [{{'$1', '$2'}}]}]),
    Mine = deadlift_storage:select_rows(Written, MatchSpec),
    Own =
        ets:select_count(Table, [{{{w, '$1'}, '_'}, Guards, [true]}]) > 0 orelse
            ets:select_count(Table, [{{{cr, '_'}, '$1', '$2'}, [{'<', '$1', {const, End}}, {'<', {const, Begin}, '$2'}], [true]}]) > 0,
    %% The StorageServer applies the limit only if the transaction has
    %% changed no key of the range. Otherwise a stored result can be a key
    %% that the transaction cleared, and the limit is applied after the
    %% merge.
    Pushed =
        case Own of
            true -> infinity;
            false -> Limit
        end,
    {ok, Rows} = read(Table, {select, Begin, End, MatchSpec, Pushed}),
    Stored = [Row || Row = {Key, _Result} <- Rows, not ets:member(Table, {w, Key}), not cleared(Table, Key)],
    _ = Conflict andalso ets:insert(Table, {{rr, next(Table)}, Begin, End}),
    Results = [Result || {_Key, Result} <- lists:ukeymerge(1, Mine, Stored)],
    case Limit of
        infinity -> Results;
        _ -> lists:sublist(Results, Limit)
    end.

%% The key is in a key range that the transaction cleared, and the
%% transaction has not written the key after that.
cleared(Table, Key) ->
    Guards = [{'=<', '$1', {const, Key}}, {'<', {const, Key}, '$2'}],
    ets:select_count(Table, [{{{cr, '_'}, '$1', '$2'}, Guards, [true]}]) > 0.

%% Reads from a StorageServer at the read version of the transaction. See
%% "StorageServers" in the module documentation.
read(Table, Query) ->
    Version = read_version(Table),
    Storages = storages(Table, Version),
    case read_direct(Table, Storages, Query, Version) of
        {ok, Result} -> Result;
        too_old -> error({erlfdb_error, ?TRANSACTION_TOO_OLD});
        none -> read_by_message(Storages, Query, Version, ?TIMEOUT_MS div length(Storages), ?PROCESS_BEHIND)
    end.

%% The StorageServers of the database, in the order of this read version.
storages(Table, Version) ->
    [{db, #{storage := Storage}}] = ets:lookup(Table, db),
    Named =
        case is_function(Storage, 0) of
            true -> Storage();
            false -> Storage
        end,
    case Named of
        undefined ->
            error({erlfdb_error, ?PROCESS_BEHIND});
        [] ->
            error({erlfdb_error, ?PROCESS_BEHIND});
        [_ | _] ->
            {Before, From} = lists:split(erlang:phash2(Version, length(Named)), Named),
            From ++ Before;
        One ->
            [One]
    end.

%% Reads the table of each StorageServer on this node in this process,
%% until one of them has applied the read version. The transaction keeps
%% the handle of a StorageServer from the first read on.
read_direct(Table, Storages, Query, Version) ->
    [{db, Servers}] = ets:lookup(Table, db),
    case maps:get(direct, Servers, true) of
        true -> read_direct_each(Table, [Storage || Storage <- Storages, local(Storage)], Query, Version);
        false -> none
    end.

read_direct_each(_Table, [], _Query, _Version) ->
    none;
read_direct_each(Table, [Storage | Storages], Query, Version) ->
    Answer =
        case handle(Table, Storage) of
            none -> gone;
            Handle -> read_handle(Handle, Query, Version)
        end,
    case Answer of
        {ok, Result} -> {ok, Result};
        too_old -> too_old;
        _BehindOrGone -> read_direct_each(Table, Storages, Query, Version)
    end.

%% A direct read. A select needs a StorageServer with the feature
%% `select'.
read_handle(Handle, Query, Version) when element(1, Query) =:= select ->
    case lists:member(select, maps:get(features, Handle, [])) of
        true -> deadlift_storage:read_direct(Handle, Query, Version);
        false -> error({unsupported, select})
    end;
read_handle(Handle, Query, Version) ->
    deadlift_storage:read_direct(Handle, Query, Version).

handle(Table, Storage) ->
    case ets:lookup(Table, {storage_handle, Storage}) of
        [{_, Kept}] ->
            Kept;
        [] ->
            case deadlift_storage:handle(Storage) of
                {ok, New} ->
                    true = ets:insert(Table, {{storage_handle, Storage}, New}),
                    New;
                {error, _Reason} ->
                    none
            end
    end.

%% The StorageServer runs on the node of the caller.
local(Storage) when is_pid(Storage) -> node(Storage) =:= node();
local(Storage) when is_atom(Storage) -> true;
local({Name, Node}) when is_atom(Name), is_atom(Node) -> Node =:= node();
local(_Other) -> false.

%% Sends the read to each StorageServer in the order, until one of them
%% answers. `Code' is the error for the case that none answers.
read_by_message([], _Query, _Version, _Timeout, Code) ->
    error({erlfdb_error, Code});
read_by_message([Storage | Storages], Query, Version, Timeout, Code) ->
    Answer =
        case Query of
            {key, Key} -> deadlift_storage:read_with_version(Storage, Key, Version, Timeout);
            {range, Begin, End} -> deadlift_storage:read_range_with_version(Storage, Begin, End, Version, Timeout);
            {select, Begin, End, MatchSpec, Limit} -> deadlift_storage:select(Storage, Begin, End, MatchSpec, Limit, Version, Timeout)
        end,
    case Answer of
        {error, transaction_too_old} -> error({erlfdb_error, ?TRANSACTION_TOO_OLD});
        {error, badarg} -> error(badarg);
        {error, {timeout, _}} -> read_by_message(Storages, Query, Version, Timeout, ?FUTURE_VERSION);
        {error, _Reason} -> read_by_message(Storages, Query, Version, Timeout, Code);
        Result -> Result
    end.

%% ---------------------------------------------------------------------------
%% Writes
%% ---------------------------------------------------------------------------

-spec set(transaction(), key(), value()) -> ok.
set(?IS_TX = Tx, Key, Value) ->
    {Table, _} = table(Tx),
    true = ets:insert(Table, [{{w, Key}, {set, Value}}, {{m, next(Table)}, {set, Key, Value}}]),
    ok.

-spec clear(transaction(), key()) -> ok.
clear(?IS_TX = Tx, Key) ->
    {Table, _} = table(Tx),
    true = ets:insert(Table, [{{w, Key}, clear}, {{m, next(Table)}, {clear, Key}}]),
    ok.

%% @doc Clears the keys `K' with `Begin =< K' and `K < End'.
-spec clear_range(transaction(), key(), key()) -> ok.
clear_range(?IS_TX = Tx, Begin, End) ->
    {Table, _} = table(Tx),
    %% The earlier writes of the keys of the range are replaced by the
    %% cleared range.
    Guards = [{'>=', '$1', {const, Begin}}, {'<', '$1', {const, End}}],
    _ = ets:select_delete(Table, [{{{w, '$1'}, '_'}, Guards, [true]}]),
    N = next(Table),
    true = ets:insert(Table, [{{cr, N}, Begin, End}, {{m, N}, {clear_range, Begin, End}}]),
    ok.

%% ---------------------------------------------------------------------------
%% Commit
%% ---------------------------------------------------------------------------

%% @doc Commits the transaction. A transaction without a mutation sends no
%% message, and its committed version is -1, as in FoundationDB.
-spec commit(transaction()) -> ok.
commit(?IS_TX = Tx) ->
    {Table, _} = table(Tx),
    case ets:select(Table, [{{{m, '_'}, '$1'}, [], ['$1']}]) of
        [] ->
            true = ets:insert(Table, [{state, committed}, {committed, -1, 0}]),
            ok;
        Mutations ->
            Transaction = #{
                read_version =>
                    case ets:lookup(Table, read_version) of
                        [{_, Version}] -> Version;
                        [] -> 0
                    end,
                reads => ets:select(Table, [{{{r, '$1'}}, [], ['$1']}]),
                read_ranges => ets:select(Table, [{{{rr, '_'}, '$1', '$2'}, [], [{{'$1', '$2'}}]}]),
                mutations => Mutations
            },
            case deadlift_epoch:commit_with_index(server(Table, proxy), Transaction, ?TIMEOUT_MS) of
                {ok, LSN, Index} ->
                    true = ets:insert(Table, [{state, committed}, {committed, LSN, Index}]),
                    ok;
                {error, not_committed} ->
                    error({erlfdb_error, ?NOT_COMMITTED});
                {error, transaction_too_old} ->
                    error({erlfdb_error, ?TRANSACTION_TOO_OLD});
                {error, commit_unknown_result} ->
                    error({erlfdb_error, ?COMMIT_UNKNOWN_RESULT})
            end
    end.

%% @doc Removes the reads and the writes of the transaction.
-spec reset(transaction()) -> ok.
reset({deadlift_transaction, Table}) ->
    [{db, Servers}] = ets:lookup(Table, db),
    [{backoff, Backoff}] = ets:lookup(Table, backoff),
    true = ets:delete_all_objects(Table),
    true = ets:insert(Table, [{db, Servers}, {state, open}, {seq, 0}, {backoff, Backoff}]),
    ok.

%% @doc Cancels the transaction. A later operation of the transaction
%% raises the error 1025.
-spec cancel(transaction()) -> ok.
cancel({deadlift_transaction, Table}) ->
    true = ets:insert(Table, {state, cancelled}),
    ok.

%% @doc Handles the error of an operation of the transaction. See "Errors"
%% in the module documentation.
-spec on_error(transaction(), integer() | {erlfdb_error, integer()}) -> ok.
on_error(Tx, {erlfdb_error, Code}) ->
    on_error(Tx, Code);
on_error({deadlift_transaction, Table} = Tx, Code) when
    Code =:= ?NOT_COMMITTED; Code =:= ?COMMIT_UNKNOWN_RESULT; Code =:= ?PROCESS_BEHIND
->
    [{backoff, Backoff}] = ets:lookup(Table, backoff),
    [{db, Servers}] = ets:lookup(Table, db),
    true = ets:insert(Table, {backoff, min(maps:get(max_retry_delay_ms, Servers, ?MAX_BACKOFF_MS), Backoff * 2)}),
    ok = reset(Tx),
    timer:sleep(round(Backoff * rand:uniform()));
on_error(Tx, Code) when Code =:= ?TRANSACTION_TOO_OLD; Code =:= ?FUTURE_VERSION ->
    ok = reset(Tx),
    timer:sleep(?FUTURE_VERSION_RETRY_DELAY_MS);
on_error(_Tx, Code) ->
    error({erlfdb_error, Code}).

%% ---------------------------------------------------------------------------
%% Versions and conflict ranges
%% ---------------------------------------------------------------------------

-spec get_read_version(transaction() | snapshot()) -> version().
get_read_version(TxOrSnapshot) ->
    {Table, _} = table(TxOrSnapshot),
    read_version(Table).

-spec set_read_version(transaction(), version()) -> ok.
set_read_version(?IS_TX = Tx, Version) when is_integer(Version) ->
    {Table, _} = table(Tx),
    true = ets:insert(Table, {read_version, Version}),
    ok.

%% @doc Returns the commit version of a committed transaction, or -1 for a
%% transaction without a mutation.
-spec get_committed_version(transaction()) -> version() | -1.
get_committed_version({deadlift_transaction, Table}) ->
    [{committed, LSN, _Index}] = ets:lookup(Table, committed),
    LSN.

%% @doc Returns the versionstamp of a committed transaction with a
%% mutation: the commit version in 8 bytes and the index of the
%% transaction in its batch in 2 bytes.
-spec get_versionstamp(transaction()) -> binary().
get_versionstamp({deadlift_transaction, Table}) ->
    [{committed, LSN, Index}] = ets:lookup(Table, committed),
    true = LSN >= 0,
    <<LSN:64, Index:16>>.

-spec add_read_conflict_key(transaction(), key()) -> ok.
add_read_conflict_key(?IS_TX = Tx, Key) ->
    {Table, _} = table(Tx),
    true = ets:insert(Table, {{r, Key}}),
    ok.

-spec add_read_conflict_range(transaction(), key(), key()) -> ok.
add_read_conflict_range(?IS_TX = Tx, Begin, End) ->
    {Table, _} = table(Tx),
    true = ets:insert(Table, {{rr, next(Table)}, Begin, End}),
    ok.

%% @doc The transaction has no mutation.
-spec is_read_only(transaction()) -> boolean().
is_read_only({deadlift_transaction, Table}) ->
    ets:select_count(Table, [{{{m, '_'}, '_'}, [], [true]}]) =:= 0.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% The table of the transaction, and whether a read adds a conflict key.
table({deadlift_transaction, Table}) ->
    ok = check_open(Table),
    {Table, true};
table({deadlift_snapshot, {deadlift_transaction, Table}}) ->
    ok = check_open(Table),
    {Table, false}.

check_open(Table) ->
    case ets:lookup(Table, state) of
        [{state, cancelled}] -> error({erlfdb_error, ?TRANSACTION_CANCELLED});
        [{state, _OpenOrCommitted}] -> ok
    end.

%% The next number for the order of the mutations and of the ranges.
next(Table) ->
    ets:update_counter(Table, seq, 1).

%% The read version of the transaction. The first call asks the Proxy.
read_version(Table) ->
    case ets:lookup(Table, read_version) of
        [{_, Version}] ->
            Version;
        [] ->
            case deadlift_epoch:read_version(server(Table, proxy), ?TIMEOUT_MS) of
                {ok, Version} ->
                    true = ets:insert(Table, {read_version, Version}),
                    Version;
                {error, _Reason} ->
                    error({erlfdb_error, ?PROCESS_BEHIND})
            end
    end.

%% The Proxy of the database of the transaction.
server(Table, proxy) ->
    [{db, #{proxy := Server}}] = ets:lookup(Table, db),
    case is_function(Server, 0) of
        true ->
            case Server() of
                undefined -> error({erlfdb_error, ?PROCESS_BEHIND});
                Process -> Process
            end;
        false ->
            Server
    end.
