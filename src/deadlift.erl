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
%% <li>Another read goes to the ETS table of the StorageServer in the
%%   process of the caller (`deadlift_storage:read_direct/3'). If the
%%   StorageServer has not applied the read version, the read goes to the
%%   StorageServer process, which waits for the version.</li>
%% <li>A read of a transaction adds a read conflict key or a read conflict
%%   range. A read of a snapshot (`snapshot/1') adds none.</li>
%% <li>`get/2' returns a future, as in erlfdb. The future of deadlift is
%%   the result itself, and `wait/1' returns its argument. `wait/1' of
%%   erlfdb also returns an argument that is not a future.</li>
%% </ul>
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
%% <li>1037, `process_behind': the Proxy or the StorageServer does not
%%   answer. FoundationDB has this code for a StorageServer that is
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
%%   each error and is at most 1 s.</li>
%% <li>For 1007 and 1009 it resets the transaction and waits for 10
%%   ms.</li>
%% <li>For another code it raises the error again.</li>
%% </ul>
%%
%% `transactional/2' runs the function again after `on_error/2' returns.
%% After 1021 the first attempt can be committed, so the function must
%% give the same result when it runs twice.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The module has no key selectors, no options of `get_range', no
%%   atomic operations, no versionstamped keys and values, no watches, no
%%   transaction options, no tenants and no `get_last_error/0'.</li>
%% <li>A database is a Proxy and a StorageServer that the caller names.
%%   deadlift has no process that tells a client which Proxy runs.</li>
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

-export([open/1, create_transaction/1, transactional/2, snapshot/1]).
-export([get/2, get_range/3, set/3, clear/2, clear_range/3]).
-export([commit/1, reset/1, cancel/1, on_error/2, wait/1, wait/2]).
-export([get_read_version/1, set_read_version/2, get_committed_version/1, get_versionstamp/1]).
-export([add_read_conflict_key/2, add_read_conflict_range/3, is_read_only/1]).

-export_type([database/0, transaction/0, snapshot/0, key/0, value/0]).

-type key() :: term().
-type value() :: term().
-type version() :: deadlift_cstate:version().

%% A process, or a function that returns the process that runs now.
-type server() :: gen_server:server_ref() | fun(() -> gen_server:server_ref() | undefined).

-type database() :: {deadlift_database, #{proxy := server(), storage := server()}}.
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

%% @doc Returns a database for a Proxy and a StorageServer. Each one is a
%% process, or a function that returns the process that runs now.
-spec open(#{proxy := server(), storage := server()}) -> database().
open(Servers = #{proxy := _, storage := _}) ->
    {deadlift_database, maps:with([proxy, storage], Servers)}.

-spec create_transaction(database()) -> transaction().
create_transaction({deadlift_database, Servers}) ->
    Table = ets:new(deadlift_transaction, [ordered_set, public]),
    true = ets:insert(Table, [{db, Servers}, {state, open}, {seq, 0}, {backoff, ?BACKOFF_MS}]),
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

%% The key is in a key range that the transaction cleared, and the
%% transaction has not written the key after that.
cleared(Table, Key) ->
    Guards = [{'=<', '$1', {const, Key}}, {'<', {const, Key}, '$2'}],
    ets:select_count(Table, [{{{cr, '_'}, '$1', '$2'}, Guards, [true]}]) > 0.

%% Reads from the StorageServer at the read version of the transaction.
read(Table, Query) ->
    Version = read_version(Table),
    case direct(Table, Query, Version) of
        {ok, Result} ->
            Result;
        too_old ->
            error({erlfdb_error, ?TRANSACTION_TOO_OLD});
        _BehindOrGone ->
            %% The StorageServer process waits for the version.
            Storage = server(Table, storage),
            Answer =
                case Query of
                    {key, Key} -> deadlift_storage:read_with_version(Storage, Key, Version, ?TIMEOUT_MS);
                    {range, Begin, End} -> deadlift_storage:read_range_with_version(Storage, Begin, End, Version, ?TIMEOUT_MS)
                end,
            case Answer of
                {error, transaction_too_old} -> error({erlfdb_error, ?TRANSACTION_TOO_OLD});
                {error, {timeout, _}} -> error({erlfdb_error, ?FUTURE_VERSION});
                {error, _Reason} -> error({erlfdb_error, ?PROCESS_BEHIND});
                Result -> Result
            end
    end.

%% Reads the table of the StorageServer in this process. The transaction
%% keeps the handle of the StorageServer from its first read on.
direct(Table, Query, Version) ->
    Handle =
        case ets:lookup(Table, storage_handle) of
            [{_, Kept}] ->
                Kept;
            [] ->
                case deadlift_storage:handle(server(Table, storage)) of
                    {ok, New} ->
                        true = ets:insert(Table, {storage_handle, New}),
                        New;
                    {error, _Reason} ->
                        none
                end
        end,
    case Handle of
        none -> gone;
        _ -> deadlift_storage:read_direct(Handle, Query, Version)
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
            case deadlift_proxy:commit_with_index(server(Table, proxy), Transaction, ?TIMEOUT_MS) of
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
    true = ets:insert(Table, {backoff, min(?MAX_BACKOFF_MS, Backoff * 2)}),
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
            case deadlift_proxy:read_version(server(Table, proxy), ?TIMEOUT_MS) of
                {ok, Version} ->
                    true = ets:insert(Table, {read_version, Version}),
                    Version;
                {error, _Reason} ->
                    error({erlfdb_error, ?PROCESS_BEHIND})
            end
    end.

%% The Proxy or the StorageServer of the database of the transaction.
server(Table, Role) ->
    [{db, #{Role := Server}}] = ets:lookup(Table, db),
    case is_function(Server, 0) of
        true ->
            case Server() of
                undefined -> error({erlfdb_error, ?PROCESS_BEHIND});
                Process -> Process
            end;
        false ->
            Server
    end.
