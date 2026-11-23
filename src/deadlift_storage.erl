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
%% runs (`deadlift_log:tail/2'). A LogServer answers when it has a record
%% that the StorageServer has not received from it, or when a Sequencer
%% locks it. After an answer, the StorageServer sends the next request.
%% Each record has its body on k of the LogServers. The StorageServer
%% applies a record when it has received the body.
%%
%% At an interval of `poll_ms', the StorageServer sends a request to each
%% LogServer of the epoch that has received none, because a LogServer can
%% start after the first request. At every tenth interval, the
%% StorageServer sends every request again, because a request or an answer
%% can be lost.
%%
%% The body of a record of a Proxy is `{batch, Entries}', with one entry
%% for each transaction of the batch, in the order of the batch. The entry
%% of a transaction is a list of mutations (`deadlift_epoch:mutation()'):
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
%%   versions in flight (see {@link deadlift_sequencer}). The
%%   StorageServer therefore has no discarded write at or below a version
%%   of a later epoch when it reaches that version. A discarded recovery
%%   transaction can have the first LSN of the next epoch. It contains no
%%   write.</li>
%% </ul>
%%
%% == Store ==
%%
%% FoundationDB has an ordered keyspace. The StorageServer keeps the keys
%% in their order, so that a read or a clear of a key range examines only
%% the keys of the range. The order of the keys is the order of Erlang
%% terms.
%%
%% <ul>
%% <li>The data is an ETS table of type `ordered_set' with one row for each
%%   key: `{Key, Version, Change}', the newest version of the key.
%%   `Change' is `{set, Value}', or `clear' for a version without a
%%   value. The table is `protected', so another process can read it.
%%   `info/1' returns the table.</li>
%% <li>A second ETS table of type `ordered_set' has one row
%%   `{{Key, -Version}, Change}' for each older version of a key. The
%%   rows of a key are in the order from the highest version to the
%%   lowest one, so the first row at or after `{Key, -V}' is the last
%%   version of the key at or below `V'. A read or a write of a key
%%   therefore does not depend on the number of versions of the key.</li>
%% <li>A second ETS table of type `ordered_set' has one row
%%   `{{Version, Key}}' for each version above the oldest version. With
%%   this table, a rollback examines only the versions that it removes,
%%   and a rise of the oldest version examines only the versions that
%%   leave the MVCC window.</li>
%% </ul>
%%
%% == Direct reads ==
%%
%% A client can read the table of the data in its own process, without a
%% message to the StorageServer (`handle/1' and `read_direct/3'). The
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
%% Two benchmarks compared layouts of the store. The benchmarks are not
%% part of this repository. See "The store of the StorageServer" and "The
%% versions of a hot key" in README.md.
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
%% StorageServer of a read (see {@link deadlift}).
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
%% The first version of an epoch is more than the window above the last
%% version of the previous epoch (see {@link deadlift_sequencer}). A read
%% at a read version of the previous epoch is therefore too old when the
%% StorageServer has applied a committed record of the new epoch.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The StorageServer keeps the data in memory, and loses the data
%%   below the MVCC window. A StorageServer of FoundationDB keeps the
%%   versions of the MVCC window in memory and writes older data to disk.
%%   This part is not in the paper.</li>
%% <li>Each StorageServer has every key. FoundationDB divides the key
%%   space into shards. Each shard has a team of StorageServers, and a
%%   StorageServer reads the records with its tag.</li>
%% <li>The StorageServer reads the log systems from the coordinated state.
%%   A StorageServer of FoundationDB receives them from the
%%   ClusterController. This part is not in the paper.</li>
%% <li>The StorageServer does not tell a LogServer which records it has
%%   stored (the pop of FoundationDB).</li>
%% </ul>
-module(deadlift_storage).

-behaviour(gen_server).

-include("deadlift_eta.hrl").
-eta_observe({state, [version, oldest, data, history, rollbacks]}).

-export([start/1, start_link/1, stop/1]).
-export([read/3, read/4, read_with_version/4, info/1]).
-export([read_range/4, read_range/5, read_range_with_version/5]).
-export([handle/1, read_direct/3, versions/1]).
-export_type([handle/0, query/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([options/0]).

-type version() :: deadlift_cstate:version().

%% `mvcc_window' is a number of versions, or `infinity'.
%%
%% `rollback => false' plants a defect for the simulation: the
%% StorageServer keeps the writes above the end version of an epoch.
%% `check_window => false' plants a second defect: the StorageServer
%% answers a read below the oldest version. `clear => false' plants a third
%% defect: the StorageServer does not apply a clear. `wait => false' plants
%% a fourth defect: the StorageServer answers a read at a version that it
%% has not applied.
-type options() :: #{
    coordinators := deadlift_cstate:servers(),
    poll_ms => pos_integer(),
    cstate_timeout_ms => timeout(),
    mvcc_window => pos_integer() | infinity,
    rollback => boolean(),
    check_window => boolean(),
    clear => boolean(),
    wait => boolean()
}.

%% The table of the data and the published versions of a StorageServer.
%% Element 1 of `published' is the version, and element 2 is the oldest
%% version. `wait' is `false' only with the planted defect `wait => false'.
-type handle() :: #{data := ets:tid(), history := ets:tid(), published := atomics:atomics_ref(), wait => boolean()}.

-type query() :: {key, term()} | {range, term(), term()}.

-record(state, {
    coordinators :: deadlift_cstate:servers(),
    poll_ms :: pos_integer(),
    cstate_timeout_ms :: timeout(),
    rollback :: boolean(),
    window :: pos_integer() | infinity,
    check_window :: boolean(),
    clear :: boolean(),
    wait :: boolean(),
    %% The LSN of the last record that the StorageServer has applied.
    version = 0 :: version(),
    %% The oldest version that a read can have.
    oldest = 0 :: version(),
    %% The highest Known Committed Version that a LogServer has sent.
    kcv = 0 :: version(),
    %% The ETS table of the data, with the rows `{Key, Versions}'. See
    %% "Store" above.
    data :: ets:tid(),
    history :: ets:tid(),
    %% The ETS table with a row `{{Version, Key}}' for each version above
    %% the oldest version.
    changes :: ets:tid(),
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
    buffer :: gb_trees:tree(version(), {version(), deadlift_log:body()}),
    %% The requests to LogServers without an answer. The label of a
    %% request is `{Epoch, Log, Number}'.
    requests :: gen_server:request_id_collection(),
    %% Log -> the number of the last request to the LogServer.
    sent = #{} :: #{deadlift_log:id() => pos_integer()},
    %% Log -> the highest LSN that the LogServer has sent.
    cursors = #{} :: #{deadlift_log:id() => version()},
    %% The LogServers of the epoch that have answered that they are
    %% stopped.
    stopped = [] :: [deadlift_log:id()],
    %% The reads that wait for a version.
    waiting = [] :: [{gen_server:from(), {key, term()} | {range, term(), term()}, version()}],
    %% The number of rollbacks.
    rollbacks = 0 :: non_neg_integer(),
    %% The number of intervals of `poll_ms' since the start.
    ticks = 0 :: non_neg_integer()
}).

-define(TIMEOUT_MS, 5000).
-define(POLL_MS, 500).
-define(MVCC_WINDOW, 5000000).
-define(CSTATE_TIMEOUT_MS, 1000).
%% The StorageServer sends every request again at each tenth interval.
-define(RESEND_TICKS, 10).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts a StorageServer without a link to the caller.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
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
read_direct(Handle = #{data := Data0, history := History, published := Published}, Query, Version) ->
    Data = {Data0, History},
    case atomics:get(Published, 1) >= Version orelse not maps:get(wait, Handle, true) of
        false ->
            behind;
        true ->
            try lookup(Query, Version, Data) of
                Result ->
                    case Version < atomics:get(Published, 2) of
                        true -> too_old;
                        false -> {ok, Result}
                    end
            catch
                error:badarg -> gone
            end
    end.

-spec info(gen_server:server_ref()) ->
    #{
        version := version(),
        oldest := version(),
        epoch := deadlift_cstate:epoch() | undefined,
        rollbacks := non_neg_integer(),
        %% The ETS table of the data. See "Store" in the module
        %% documentation.
        data := ets:tid(),
        history := ets:tid()
    }.
info(Storage) ->
    gen_server:call(Storage, info).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options = #{coordinators := Coordinators}) ->
    State = #state{
        coordinators = Coordinators,
        poll_ms = maps:get(poll_ms, Options, ?POLL_MS),
        cstate_timeout_ms = maps:get(cstate_timeout_ms, Options, ?CSTATE_TIMEOUT_MS),
        rollback = maps:get(rollback, Options, true),
        window = maps:get(mvcc_window, Options, ?MVCC_WINDOW),
        check_window = maps:get(check_window, Options, true),
        clear = maps:get(clear, Options, true),
        wait = maps:get(wait, Options, true),
        data = ets:new(deadlift_storage_data, [ordered_set, protected, {read_concurrency, true}]),
        history = ets:new(deadlift_storage_history, [ordered_set, protected, {read_concurrency, true}]),
        changes = ets:new(deadlift_storage_changes, [ordered_set, private]),
        buffer = gb_trees:empty(),
        published = atomics:new(2, [{signed, true}]),
        requests = gen_server:reqids_new()
    },
    self() ! poll,
    {ok, State}.

%% @private
handle_call({read, Key, Version}, From, State) ->
    read_at({key, Key}, Version, From, State);
handle_call({read_range, Begin, End, Version}, From, State) ->
    read_at({range, Begin, End}, Version, From, State);
handle_call(handle, _From, State = #state{data = Data, history = History, published = Published}) ->
    {reply, #{data => Data, history => History, published => Published, wait => State#state.wait}, State};
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
        data => State#state.data,
        history => State#state.history
    },
    {reply, Info, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info(poll, State = #state{poll_ms = PollMs, system = System, stopped = Stopped}) ->
    State1 =
        case System =:= undefined orelse Stopped =/= [] of
            true -> refresh(State);
            false -> State
        end,
    _ = erlang:send_after(PollMs, self(), poll),
    Ticks = State1#state.ticks + 1,
    State2 =
        case Ticks rem ?RESEND_TICKS of
            0 -> tail_all(State1);
            _ -> tail_new(State1)
        end,
    {noreply, State2#state{ticks = Ticks}};
handle_info(Message, State = #state{requests = Requests}) ->
    case gen_server:check_response(Message, Requests, true) of
        {{reply, {ok, Records, Stopped, KCV}}, Label, Requests1} ->
            State1 = State#state{requests = Requests1, kcv = max(KCV, State#state.kcv)},
            {noreply, advance_oldest(received(Label, Records, Stopped, State1))};
        {_ErrorOrOtherReply, _Label, Requests1} ->
            {noreply, State#state{requests = Requests1}};
        _NoRequestOrNoReply ->
            {noreply, State}
    end.

%% ---------------------------------------------------------------------------
%% Epochs
%% ---------------------------------------------------------------------------

%% Reads the log systems from the coordinated state, and updates the log
%% system that the StorageServer reads.
refresh(State = #state{coordinators = Coordinators, cstate_timeout_ms = Timeout}) ->
    try deadlift_cstate:read(Coordinators, Timeout) of
        {ok, #{current := Current, old := Old}} when Current =/= undefined ->
            Systems = lists:sort(fun(#{epoch := A}, #{epoch := B}) -> A =< B end, [Current | Old]),
            follow(State#state{systems = Systems});
        _NoLogSystemOrError ->
            State
    catch
        exit:_ -> State
    end.

%% Updates the log system that the StorageServer reads from the known log
%% systems and the version.
follow(State = #state{systems = []}) ->
    State;
follow(State = #state{system = undefined, systems = [First | _]}) ->
    follow(start(First, State));
follow(State = #state{system = #{epoch := Epoch}, systems = Systems, version = Version}) ->
    case [System || System = #{epoch := E} <- Systems, E =:= Epoch] of
        [System = #{end_version := End}] when Version > End ->
            follow(rollback(End, State#state{system = System}));
        [#{end_version := End}] when Version =:= End ->
            %% The next epoch. The coordinated state has it, because the
            %% epoch `Epoch' has an end version.
            [Next | _] = [System || System = #{epoch := E} <- Systems, E > Epoch],
            follow(start(Next, State));
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
rollback(End, State = #state{changes = Changes, rollbacks = Rollbacks}) ->
    ok = atomics:put(State#state.published, 1, End),
    ok = remove_above(End, ets:last(Changes), State),
    State#state{version = End, buffer = gb_trees:empty(), rollbacks = Rollbacks + 1}.

%% Removes the versions above `End', from the highest version down. The
%% oldest version is at or below `End', so the table of the versions has a
%% row for each of them. The highest version of a key is in the row of the
%% key. After its removal, the next version of the key moves from the
%% table of the older versions to the row of the key.
remove_above(End, {Version, Key} = Row, State = #state{data = Data, history = History, changes = Changes}) when Version > End ->
    Previous = ets:prev(Changes, Row),
    true = ets:delete(Changes, Row),
    case ets:lookup(Data, Key) of
        [{Key, Version, _Change}] ->
            case ets:next_lookup(History, {Key, -Version}) of
                {{Key, Negative} = Older, [{Older, Change}]} ->
                    %% The row of the key changes before the older version
                    %% leaves its table, so a direct read finds the
                    %% version in one of the two.
                    true = ets:insert(Data, {Key, -Negative, Change}),
                    true = ets:delete(History, Older);
                _NoOlderVersionOfTheKey ->
                    true = ets:delete(Data, Key)
            end;
        _ ->
            true
    end,
    remove_above(End, Previous, State);
remove_above(_End, _RowAtOrBelowEndOrNoRow, _State) ->
    ok.

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
    case deadlift_log:ref(Log) of
        undefined ->
            State;
        Pid ->
            Number = maps:get(Log, Sent, 0) + 1,
            After = maps:get(Log, Cursors, Version),
            Requests1 = gen_server:send_request(Pid, {tail, After}, {Epoch, Log, Number}, Requests),
            State#state{requests = Requests1, sent = Sent#{Log => Number}}
    end.

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
                    apply_records(State1);
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

write({batch, Entries}, LSN, State) ->
    lists:foreach(fun(Entry) -> write_entry(Entry, LSN, State) end, Entries);
write(_NoTransaction, _LSN, _State) ->
    ok.

%% Applies the mutations of one transaction, in their order.
write_entry(Entry, LSN, State = #state{data = Data}) when is_list(Entry) ->
    lists:foreach(
        fun
            ({set, Key, Value}) ->
                put_version(Key, LSN, {set, Value}, ets:lookup(Data, Key), State);
            ({clear, Key}) ->
                clear_key(Key, LSN, ets:lookup(Data, Key), State);
            ({clear_range, Begin, End}) ->
                %% The rows of the range are read before the first change.
                lists:foreach(fun(Row = {Key, _Version, _Change}) -> clear_key(Key, LSN, [Row], State) end, rows(Begin, End, Data));
            (_Other) ->
                ok
        end,
        Entry
    );
write_entry(_NoTransaction, _LSN, _State) ->
    ok.

%% Adds the version `LSN' of `Key'. `Change' is `{set, Value}' or `clear'.
%% `Row' is the result of the lookup of the key. If a mutation of the same
%% record changed the key before, the new change replaces that one.
%% Otherwise the version of the row moves to the table of the older
%% versions. That table gets its row first, so a direct read finds the
%% version in one of the two tables.
put_version(Key, LSN, Change, Row, #state{data = Data, history = History, changes = Changes}) ->
    case Row of
        [{Key, Version, Earlier}] when Version < LSN -> true = ets:insert(History, {{Key, -Version}, Earlier});
        _SameRecordOrNoRow -> true
    end,
    true = ets:insert(Data, {Key, LSN, Change}),
    true = ets:insert(Changes, {{LSN, Key}}),
    ok.

%% A clear of a key without a value adds no version.
clear_key(Key, LSN, Row = [{Key, _Version, {set, _Value}}], State) ->
    put_version(Key, LSN, clear, Row, State);
clear_key(_Key, _LSN, _NoRowOrNoValue, _State) ->
    ok.

%% The rows of the keys `K' with `Begin =< K' and `K < End', in the order
%% of the keys. The function reads only these rows and the row after
%% them.
rows(Begin, End, Data) ->
    First =
        case ets:lookup(Data, Begin) of
            [Row] -> {Begin, [Row]};
            [] -> ets:next_lookup(Data, Begin)
        end,
    rows_until(First, End, Data).

rows_until({Key, [Row]}, End, Data) when Key < End ->
    [Row | rows_until(ets:next_lookup(Data, Key), End, Data)];
rows_until(_RowAtOrAfterEndOrEndOfTable, _End, _Data) ->
    [].

%% ---------------------------------------------------------------------------
%% MVCC window
%% ---------------------------------------------------------------------------

%% Raises the oldest version, and removes the versions that no read at or
%% above the oldest version returns.
advance_oldest(State = #state{window = infinity}) ->
    State;
advance_oldest(State = #state{version = Version, kcv = KCV, oldest = Oldest, window = Window}) ->
    case min(Version - Window, KCV) of
        New when New > Oldest ->
            %% The oldest version is published before the versions below
            %% it are removed. With the planted defect
            %% `check_window => false' it is not published.
            _ = State#state.check_window andalso atomics:put(State#state.published, 2, New),
            ok = remove_old(New, ets:first(State#state.changes), State),
            State#state{oldest = New};
        _ ->
            State
    end.

%% Handles the versions at or below `Oldest', from the lowest version up.
%% For the key of each such version, the function removes the versions
%% that no read at or above `Oldest' returns. The row of the version leaves
%% the table of the versions. The last version of a key at or below
%% `Oldest' stays in the data without such a row, until a later version of
%% the key is at or below a later oldest version.
remove_old(Oldest, {Version, Key} = Row, State = #state{data = Data, history = History, changes = Changes}) when Version =< Oldest ->
    Next = ets:next(Changes, Row),
    true = ets:delete(Changes, Row),
    %% No read at or above `Oldest' returns a version of the key below
    %% `Version'.
    ok = delete_older(ets:next(History, {Key, -Version}), Key, History),
    %% `Version' is the last version of the key at or below `Oldest', or a
    %% later row of this pass removes it. If it is a clear, the key has no
    %% value at `Version', and the row is not needed.
    case ets:lookup(Data, Key) of
        [{Key, Version, clear}] ->
            true = ets:delete(Data, Key);
        [{Key, Newest, _Change}] when Newest > Version ->
            case ets:lookup(History, {Key, -Version}) of
                [{Older, clear}] -> true = ets:delete(History, Older);
                _ -> true
            end;
        _ ->
            true
    end,
    remove_old(Oldest, Next, State);
remove_old(_Oldest, _RowAboveOldestOrNoRow, _State) ->
    ok.

delete_older({Key, _Negative} = Older, Key, History) ->
    Next = ets:next(History, Older),
    true = ets:delete(History, Older),
    delete_older(Next, Key, History);
delete_older(_RowOfAnotherKeyOrEndOfTable, _Key, _History) ->
    ok.

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

%% Answers a read at `Version', or keeps the read until the StorageServer
%% has the version.
read_at(_Query, Version, _From, State = #state{oldest = Oldest, check_window = true}) when Version < Oldest ->
    {reply, {error, transaction_too_old}, State};
read_at(Query, Version, _From, State = #state{version = Mine, wait = Wait}) when Version =< Mine; not Wait ->
    {reply, lookup(Query, Version, {State#state.data, State#state.history}), State};
read_at(Query, Version, From, State = #state{waiting = Waiting}) ->
    {noreply, State#state{waiting = [{From, Query, Version} | Waiting]}}.

answer_waiting(State = #state{version = Version, waiting = Waiting}) ->
    {Ready, Later} = lists:partition(fun({_From, _Query, V}) -> V =< Version end, Waiting),
    Tables = {State#state.data, State#state.history},
    lists:foreach(fun({From, Query, V}) -> gen_server:reply(From, lookup(Query, V, Tables)) end, Ready),
    State#state{waiting = Later}.

lookup({key, Key}, Version, {Data, History}) ->
    case ets:lookup(Data, Key) of
        [Row] -> visible(Row, Version, Data, History);
        [] -> not_found
    end;
lookup({range, Begin, End}, Version, {Data, History}) ->
    {ok, [
        {Key, Value, Written}
     || Row = {Key, _Newest, _Change} <- rows(Begin, End, Data), {ok, Value, Written} <- [visible(Row, Version, Data, History)]
    ]}.

%% The value of the last version of the key at or below `Version'. `Row'
%% is the row of the key, which has the newest version. An older version
%% is in the table of the older versions, with the key `{Key, -Version}',
%% so the first row at or after `{Key, -Version}' is the last version at
%% or below `Version'.
%%
%% A direct read runs in another process while the StorageServer changes
%% the tables. A rollback moves a version from the table of the older
%% versions to the row of the key. The read therefore reads the row of the
%% key again after the table of the older versions, and starts again if
%% the row has changed.
visible({_Key, Written, Change}, Version, _Data, _History) when Written =< Version ->
    value(Written, Change);
visible(Row = {Key, _Newest, _Change}, Version, Data, History) ->
    Older =
        case ets:lookup(History, {Key, -Version}) of
            [{_, Change}] ->
                value(Version, Change);
            [] ->
                case ets:next_lookup(History, {Key, -Version}) of
                    {{Key, Negative}, [{_, Change}]} -> value(-Negative, Change);
                    _NoVersionAtOrBelow -> not_found
                end
        end,
    case ets:lookup(Data, Key) of
        [Row] -> Older;
        [Changed] -> visible(Changed, Version, Data, History);
        [] -> not_found
    end.

value(Written, {set, Value}) -> {ok, Value, Written};
value(_Written, clear) -> not_found.

%% @doc Returns every version of every key, from the tables of `info/1' or
%% of `handle/1': a map from the key to the list of `{Version, Change}'
%% with the highest version first. `Change' is `{set, Value}' or `clear'.
%% The function is for tests and for the simulation. It reads every row.
-spec versions(#{data := ets:tid(), history := ets:tid(), _ => _}) -> #{term() => [{version(), {set, term()} | clear}]}.
versions(#{data := Data, history := History}) ->
    Older = lists:foldr(
        fun({{Key, Negative}, Change}, Acc) -> maps:update_with(Key, fun(List) -> [{-Negative, Change} | List] end, [{-Negative, Change}], Acc) end,
        #{},
        ets:tab2list(History)
    ),
    maps:from_list([{Key, [{Version, Change} | maps:get(Key, Older, [])]} || {Key, Version, Change} <- ets:tab2list(Data)]).
