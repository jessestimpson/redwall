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
%% The body of a record is a list of `{Key, Value}'. A record with another
%% body contains no write. The StorageServer keeps every version of every
%% key.
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
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The StorageServer keeps the data in memory, and keeps every version.
%%   A StorageServer of FoundationDB keeps the versions of the MVCC window
%%   in memory and writes older data to disk. This part is not in the
%%   paper.</li>
%% <li>One StorageServer has every key. FoundationDB divides the key space
%%   into shards, and a StorageServer reads the records with its tags.</li>
%% <li>The StorageServer reads the log systems from the coordinated state.
%%   A StorageServer of FoundationDB receives them from the
%%   ClusterController. This part is not in the paper.</li>
%% <li>The StorageServer does not tell a LogServer which records it has
%%   stored (the pop of FoundationDB).</li>
%% </ul>
-module(deadlift_storage).

-behaviour(gen_server).

-include("deadlift_eta.hrl").
-eta_observe({state, [version, data, rollbacks]}).

-export([start/1, start_link/1, stop/1]).
-export([read/3, read/4, read_with_version/4, info/1]).
-export([read_range/4, read_range/5, read_range_with_version/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([options/0]).

-type version() :: deadlift_cstate:version().

%% `rollback => false' plants a defect for the simulation: the
%% StorageServer keeps the writes above the end version of an epoch.
-type options() :: #{
    coordinators := deadlift_cstate:servers(),
    poll_ms => pos_integer(),
    cstate_timeout_ms => timeout(),
    rollback => boolean()
}.

-record(state, {
    coordinators :: deadlift_cstate:servers(),
    poll_ms :: pos_integer(),
    cstate_timeout_ms :: timeout(),
    rollback :: boolean(),
    %% The LSN of the last record that the StorageServer has applied.
    version = 0 :: version(),
    %% Key -> [{Version, Value}], the highest version first.
    data = #{} :: #{term() => [{version(), term()}]},
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
%% result is `{error, Reason}' if the StorageServer does not reach
%% `Version' in time, or is not alive.
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

-spec info(gen_server:server_ref()) ->
    #{version := version(), epoch := deadlift_cstate:epoch() | undefined, rollbacks := non_neg_integer()}.
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
        buffer = gb_trees:empty(),
        requests = gen_server:reqids_new()
    },
    self() ! poll,
    {ok, State}.

%% @private
handle_call({read, Key, Version}, From, State) ->
    read_at({key, Key}, Version, From, State);
handle_call({read_range, Begin, End, Version}, From, State) ->
    read_at({range, Begin, End}, Version, From, State);
handle_call(info, _From, State = #state{version = Version, system = System, rollbacks = Rollbacks}) ->
    Epoch =
        case System of
            #{epoch := E} -> E;
            undefined -> undefined
        end,
    {reply, #{version => Version, epoch => Epoch, rollbacks => Rollbacks}, State};
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
        {{reply, {ok, Records, Stopped}}, Label, Requests1} ->
            {noreply, received(Label, Records, Stopped, State#state{requests = Requests1})};
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
rollback(End, State = #state{data = Data, rollbacks = Rollbacks}) ->
    Data1 = maps:filtermap(
        fun(_Key, Versions) ->
            case [W || W = {Version, _Value} <- Versions, Version =< End] of
                [] -> false;
                Kept -> {true, Kept}
            end
        end,
        Data
    ),
    State#state{version = End, data = Data1, buffer = gb_trees:empty(), rollbacks = Rollbacks + 1}.

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
                    State1 = State#state{
                        version = LSN,
                        data = write(Body, LSN, State#state.data),
                        buffer = gb_trees:delete(LSN, Buffer)
                    },
                    apply_records(State1);
                _BodyNotReceivedOrPreviousRecordNotReceived ->
                    answer_waiting(State)
            end
    end.

write(Body, LSN, Data) when is_list(Body) ->
    lists:foldl(
        fun
            ({Key, Value}, Acc) -> maps:update_with(Key, fun(Versions) -> [{LSN, Value} | Versions] end, [{LSN, Value}], Acc);
            (_Other, Acc) -> Acc
        end,
        Data,
        Body
    );
write(_NoTransaction, _LSN, Data) ->
    Data.

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

%% Answers a read at `Version', or keeps the read until the StorageServer
%% has the version.
read_at(Query, Version, _From, State = #state{version = Mine}) when Version =< Mine ->
    {reply, lookup(Query, Version, State), State};
read_at(Query, Version, From, State = #state{waiting = Waiting}) ->
    {noreply, State#state{waiting = [{From, Query, Version} | Waiting]}}.

answer_waiting(State = #state{version = Version, waiting = Waiting}) ->
    {Ready, Later} = lists:partition(fun({_From, _Query, V}) -> V =< Version end, Waiting),
    lists:foreach(fun({From, Query, V}) -> gen_server:reply(From, lookup(Query, V, State)) end, Ready),
    State#state{waiting = Later}.

%% The StorageServer examines every key for a range. See "Limitations" in
%% README.md.
lookup({key, Key}, Version, State) ->
    lookup_key(Key, Version, State);
lookup({range, Begin, End}, Version, State = #state{data = Data}) ->
    Keys = lists:sort([Key || Key <- maps:keys(Data), Key >= Begin, Key < End]),
    {ok, [{Key, Value, Written} || Key <- Keys, {ok, Value, Written} <- [lookup_key(Key, Version, State)]]}.

lookup_key(Key, Version, #state{data = Data}) ->
    case [W || W = {Written, _Value} <- maps:get(Key, Data, []), Written =< Version] of
        [{Written, Value} | _] -> {ok, Value, Written};
        [] -> not_found
    end.
