%% @doc The LogServer.
%%
%% A LogServer stores the log records of one epoch. See sections 2.4.3 and
%% 2.4.4 of Zhou et al., "FoundationDB: A Distributed Unbundled
%% Transactional Key Value Store", SIGMOD 2021. `formal/Recovery.tla' models
%% the LogServer with the actions `Persist' and `LockLog'.
%%
%% == Records ==
%%
%% A Proxy sends every log record to every LogServer of the epoch. A record
%% has:
%%
%% <ul>
%% <li>the LSN of the record;</li>
%% <li>the previous LSN: the LSN that the Sequencer assigned before this
%%   LSN;</li>
%% <li>the Known Committed Version (KCV) of the Proxy;</li>
%% <li>a body, or the atom `empty'. The k LogServers that replicate the
%%   record receive the body. The other LogServers receive `empty'.</li>
%% </ul>
%%
%% A LogServer stores the records in the order of the LSNs. The LogServer
%% stores a record only when the previous LSN of the record is the Durable
%% Version (DV) of the LogServer. A record that arrives before the record
%% with the previous LSN waits. The LogServer answers a push after the
%% LogServer has stored the record.
%%
%% The LogServer keeps:
%%
%% <ul>
%% <li>the DV: the highest LSN that the LogServer has stored;</li>
%% <li>the KCV: the maximum of the KCVs of the stored records.</li>
%% </ul>
%%
%% == Lock ==
%%
%% The Sequencer of a later epoch locks the LogServer during a recovery.
%% A locked LogServer is stopped: it stores no more records, and it answers
%% `{error, stopped}' to every push, including the pushes that wait. The
%% answer to the lock contains the DV and the KCV. The Sequencer computes
%% the Recovery Version and the previous epoch's end version from these
%% answers.
%%
%% A LogServer that the Sequencer of epoch `N' has locked does not answer
%% the lock of an epoch below `N'. A stopped LogServer still answers
%% `peek/3', so that the Sequencer can copy records and StorageServers can
%% read records.
%%
%% == Departures from the paper ==
%%
%% A LogServer of deadlift keeps the records in memory. A LogServer that
%% stops loses the records. FoundationDB writes the records to disk. This
%% part is not in the paper.
%%
%% A LogServer that has stopped must not start again with the same
%% identifier. The new LogServer would answer a lock with a Durable Version
%% below the Durable Version of the stopped LogServer, and a recovery could
%% then discard an acknowledged record.
-module(deadlift_log).

-behaviour(gen_server).

-include("deadlift_eta.hrl").
-eta_observe({state, [epoch, dv, kcv, records, stopped_by]}).

-export([start/1, start_link/1, stop/1, ref/1]).
-export([replicas/3]).
-export([push/6, push/7, send_push/6, lock/2, lock/3, peek/3, peek/4, info/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-export_type([id/0, body/0, record/0, options/0, info/0]).

%% The identifier of a LogServer: the epoch, and the index of the LogServer
%% in the log system of the epoch, from 1. The coordinated state stores
%% these identifiers.
-type id() :: {deadlift_cstate:epoch(), pos_integer()}.

-type epoch() :: deadlift_cstate:epoch().
-type version() :: deadlift_cstate:version().

%% The body of a log record, or `empty' on a LogServer that does not
%% replicate the record.
-type body() :: {body, term()} | empty.

%% `{LSN, PreviousLSN, Body}'.
-type record() :: {version(), version(), body()}.

%% `start' is the DV of a new LogServer: 0 in the first epoch, and the
%% Recovery Version in a later epoch. `kcv' is the previous epoch's end
%% version. `records' are the records that the recovery copied from the
%% previous epoch.
%%
%% A LogServer with `id' registers with `deadlift_registry'. The epoch of
%% the identifier must be `epoch'.
-type options() :: #{
    epoch := epoch(),
    id => id(),
    start => version(),
    kcv => version(),
    records => [{version(), body()}]
}.

-type info() :: #{
    epoch := epoch(),
    dv := version(),
    kcv := version(),
    stopped_by := epoch() | undefined,
    waiting := non_neg_integer()
}.

-record(state, {
    epoch :: epoch(),
    dv :: version(),
    kcv :: version(),
    %% LSN -> {PreviousLSN, Body}
    records :: gb_trees:tree(version(), {version(), body()}),
    %% PreviousLSN -> {LSN, KCV, Body, Callers}: the pushes that wait for
    %% the record with the previous LSN.
    waiting = #{} :: #{version() => {version(), version(), body(), [gen_server:from()]}},
    stopped_by :: epoch() | undefined
}).

-define(TIMEOUT_MS, 5000).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts a LogServer without a link to the caller. A recovery uses
%% this function: the LogServers of an epoch outlive the process that
%% recruits them.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

%% @doc The process of the LogServer `Id', for the functions of this
%% module. The result is valid on the node of the LogServer.
%%
%% The result is the atom `undefined' if no LogServer with the identifier
%% runs. A `gen_server' call to `undefined' fails in the same way as a call
%% to a process that has stopped.
-spec ref(id()) -> pid() | undefined.
ref(Id) ->
    deadlift_registry:whereis({log, Id}).

%% @doc The LogServers that receive the body of the record `LSN', out of
%% the LogServers `Logs', with the replication degree `K'. The choice
%% rotates with the LSN.
%%
%% FoundationDB chooses the LogServers from the tags of the record.
%% deadlift has no tags yet. This part is not in the paper.
-spec replicas([Log], pos_integer(), version()) -> [Log].
replicas(Logs, K, LSN) ->
    M = length(Logs),
    [lists:nth((LSN + I) rem M + 1, Logs) || I <- lists:seq(0, K - 1)].

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Log) ->
    %% A call, and not `gen_server:stop/1'. `gen_server:stop/1' starts a
    %% process inside OTP, which the scheduler of eta does not own.
    gen_server:call(Log, stop).

%% @equiv push(Log, Epoch, Prev, LSN, KCV, Body, 5000)
-spec push(gen_server:server_ref(), epoch(), version(), version(), version(), body()) ->
    {ok, version()} | {error, term()}.
push(Log, Epoch, Prev, LSN, KCV, Body) ->
    push(Log, Epoch, Prev, LSN, KCV, Body, ?TIMEOUT_MS).

%% @doc Sends the record `LSN' to the LogServer. `Prev' is the previous LSN
%% and `KCV' is the KCV of the Proxy.
%%
%% The result `{ok, DV}' means that the LogServer has stored the record.
%% The other results are:
%%
%% <ul>
%% <li>`{error, stopped}': a Sequencer has locked the LogServer.</li>
%% <li>`{error, {epoch, Epoch}}': the LogServer belongs to another
%%   epoch.</li>
%% <li>`{error, {conflict, LSN}}': the LogServer has a different record
%%   with the same LSN or with the same previous LSN.</li>
%% </ul>
-spec push(gen_server:server_ref(), epoch(), version(), version(), version(), body(), timeout()) ->
    {ok, version()} | {error, term()}.
push(Log, Epoch, Prev, LSN, KCV, Body, Timeout) ->
    gen_server:call(Log, {push, Epoch, Prev, LSN, KCV, Body}, Timeout).

%% @doc Sends the record and returns at once. The caller reads the answer
%% with `gen_server:receive_response/2' or `gen_server:check_response/2'.
%% A Proxy uses this function to send one record to every LogServer.
-spec send_push(gen_server:server_ref(), epoch(), version(), version(), version(), body()) ->
    gen_server:request_id().
send_push(Log, Epoch, Prev, LSN, KCV, Body) ->
    gen_server:send_request(Log, {push, Epoch, Prev, LSN, KCV, Body}).

%% @equiv lock(Log, Epoch, 5000)
-spec lock(gen_server:server_ref(), epoch()) ->
    {ok, #{dv := version(), kcv := version()}} | {error, term()}.
lock(Log, Epoch) ->
    lock(Log, Epoch, ?TIMEOUT_MS).

%% @doc Stops the LogServer for the Sequencer of `Epoch'. `Epoch' must be
%% above the epoch of the LogServer. The result contains the DV and the KCV
%% of the LogServer. The result is `{error, {locked, Higher}}' if the
%% Sequencer of a higher epoch has locked the LogServer.
-spec lock(gen_server:server_ref(), epoch(), timeout()) ->
    {ok, #{dv := version(), kcv := version()}} | {error, term()}.
lock(Log, Epoch, Timeout) ->
    gen_server:call(Log, {lock, Epoch}, Timeout).

%% @equiv peek(Log, After, UpTo, 5000)
-spec peek(gen_server:server_ref(), version(), version() | infinity) -> {ok, [record()]}.
peek(Log, After, UpTo) ->
    peek(Log, After, UpTo, ?TIMEOUT_MS).

%% @doc Returns the stored records with `After < LSN =< UpTo', in the order
%% of the LSNs. The result includes the records with an `empty' body.
-spec peek(gen_server:server_ref(), version(), version() | infinity, timeout()) ->
    {ok, [record()]}.
peek(Log, After, UpTo, Timeout) ->
    gen_server:call(Log, {peek, After, UpTo}, Timeout).

-spec info(gen_server:server_ref()) -> info().
info(Log) ->
    gen_server:call(Log, info).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options = #{id := Id}) ->
    case deadlift_registry:register({log, Id}) of
        yes -> init1(Options);
        no -> {stop, {already_registered, Id}}
    end;
init(Options) ->
    init1(Options).

init1(Options = #{epoch := Epoch}) ->
    Start = maps:get(start, Options, 0),
    Copied = lists:keysort(1, maps:get(records, Options, [])),
    case [LSN || {LSN, _Body} <- Copied, LSN > Start] of
        [] ->
            {ok, #state{
                epoch = Epoch,
                dv = Start,
                kcv = maps:get(kcv, Options, 0),
                records = chain(Copied)
            }};
        Above ->
            {stop, {records_above_start, Above}}
    end.

%% The copied records, each with the LSN of the copied record before it as
%% the previous LSN.
chain(Copied) ->
    {Tree, _Last} = lists:foldl(
        fun({LSN, Body}, {Acc, Prev}) -> {gb_trees:insert(LSN, {Prev, Body}, Acc), LSN} end,
        {gb_trees:empty(), 0},
        Copied
    ),
    Tree.

%% @private
handle_call({push, Epoch, _Prev, _LSN, _KCV, _Body}, _From, State = #state{epoch = Mine}) when Epoch =/= Mine ->
    {reply, {error, {epoch, Mine}}, State};
handle_call({push, _Epoch, _Prev, _LSN, _KCV, _Body}, _From, State = #state{stopped_by = By}) when By =/= undefined ->
    {reply, {error, stopped}, State};
handle_call({push, _Epoch, Prev, LSN, KCV, Body}, From, State = #state{dv = DV}) when Prev =:= DV, LSN > DV ->
    State1 = store(Prev, LSN, KCV, Body, State),
    gen_server:reply(From, {ok, State1#state.dv}),
    {noreply, release(State1)};
handle_call({push, _Epoch, Prev, LSN, KCV, Body}, From, State = #state{dv = DV, waiting = Waiting}) when Prev > DV, LSN > Prev ->
    %% The record with the previous LSN has not arrived.
    case Waiting of
        #{Prev := {LSN, KCV, Body, Callers}} ->
            {noreply, State#state{waiting = Waiting#{Prev := {LSN, KCV, Body, [From | Callers]}}}};
        #{Prev := _Other} ->
            {reply, {error, {conflict, LSN}}, State};
        _ ->
            {noreply, State#state{waiting = Waiting#{Prev => {LSN, KCV, Body, [From]}}}}
    end;
handle_call({push, _Epoch, Prev, LSN, _KCV, Body}, _From, State = #state{dv = DV, records = Records}) ->
    %% The LSN is at or below the DV, or the previous LSN is below the DV.
    case gb_trees:lookup(LSN, Records) of
        {value, {Prev, Body}} ->
            %% The same record, sent a second time.
            {reply, {ok, DV}, State};
        _ ->
            {reply, {error, {conflict, LSN}}, State}
    end;
handle_call({lock, Epoch}, _From, State = #state{epoch = Mine}) when Epoch =< Mine ->
    {reply, {error, {epoch, Mine}}, State};
handle_call({lock, Epoch}, _From, State = #state{stopped_by = By}) when By =/= undefined, By > Epoch ->
    {reply, {error, {locked, By}}, State};
handle_call({lock, Epoch}, _From, State = #state{dv = DV, kcv = KCV, waiting = Waiting}) ->
    maps:foreach(
        fun(_Prev, {_LSN, _KCV, _Body, Callers}) ->
            [gen_server:reply(Caller, {error, stopped}) || Caller <- Callers]
        end,
        Waiting
    ),
    {reply, {ok, #{dv => DV, kcv => KCV}}, State#state{stopped_by = Epoch, waiting = #{}}};
handle_call({peek, After, UpTo}, _From, State = #state{records = Records}) ->
    {reply, {ok, range(gb_trees:next(gb_trees:iterator_from(After + 1, Records)), UpTo)}, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(info, _From, State) ->
    #state{epoch = Epoch, dv = DV, kcv = KCV, stopped_by = By, waiting = Waiting} = State,
    Info = #{epoch => Epoch, dv => DV, kcv => KCV, stopped_by => By, waiting => map_size(Waiting)},
    {reply, Info, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

store(Prev, LSN, KCV, Body, State = #state{kcv = Mine, records = Records}) ->
    State#state{
        dv = LSN,
        kcv = max(Mine, KCV),
        records = gb_trees:insert(LSN, {Prev, Body}, Records)
    }.

%% Stores the waiting records whose previous LSN is now the DV.
release(State = #state{dv = DV, waiting = Waiting}) ->
    case maps:take(DV, Waiting) of
        {{LSN, KCV, Body, Callers}, Waiting1} ->
            State1 = store(DV, LSN, KCV, Body, State#state{waiting = Waiting1}),
            [gen_server:reply(Caller, {ok, LSN}) || Caller <- Callers],
            release(State1);
        error ->
            State
    end.

range({LSN, {Prev, Body}, Iterator}, UpTo) when UpTo =:= infinity; LSN =< UpTo ->
    [{LSN, Prev, Body} | range(gb_trees:next(Iterator), UpTo)];
range(_, _UpTo) ->
    [].
