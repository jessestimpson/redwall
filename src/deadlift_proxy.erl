%% @doc The Proxy.
%%
%% A Proxy commits log records for clients. See sections 2.4.1 and 2.4.3 of
%% Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
%% Value Store", SIGMOD 2021. `formal/Recovery.tla' models the Proxy with
%% the actions `Assign' and `Ack'.
%%
%% == Commit ==
%%
%% <ol>
%% <li>The Proxy asks the Sequencer for a commit version. The answer is an
%%   LSN and the previous LSN.</li>
%% <li>The Proxy sends the record to every LogServer of the epoch, with the
%%   LSN, the previous LSN and the Known Committed Version (KCV) of the
%%   Proxy. The k LogServers that replicate the record receive the body.
%%   The other LogServers receive an empty body.</li>
%% <li>When every LogServer of the epoch has answered, the Proxy answers
%%   the client with `{ok, LSN}', and raises its KCV to the LSN.</li>
%% </ol>
%%
%% The Proxy waits for every LogServer, and not only for the k LogServers
%% with the body. A recovery computes the Recovery Version as the minimum of
%% the Durable Versions of the LogServers that answer. The config
%% `RecoveryAckReplicasOnly' of the model shows that a Proxy that waits for
%% k LogServers lets a recovery discard an acknowledged record.
%%
%% The Proxy handles several commits at the same time. It does not wait for
%% the answer to one commit before it starts the next commit.
%%
%% == Failure ==
%%
%% If a LogServer answers a push with an error, or is not alive, the Proxy
%% cannot know whether a later recovery keeps the record. The Proxy answers
%% `{error, commit_unknown_result}' to every client that waits, and stops.
%% A LogServer answers with an error after the Sequencer of a later epoch
%% has locked it, so the Proxy of an ended epoch stops at its next commit.
%%
%% `commit/2' also returns `{error, commit_unknown_result}' when the Proxy
%% does not answer in time or is not alive.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Proxy has no Resolver. It commits every record without a
%%   conflict check.</li>
%% <li>The Proxy sends one record for each commit. FoundationDB collects
%%   the transactions of a short time into one batch with one commit
%%   version.</li>
%% <li>The Proxy chooses the LogServers that receive the body from the LSN
%%   (`deadlift_log:replicas/3'). FoundationDB chooses them from the tags of
%%   the record.</li>
%% <li>The Proxy does not give read versions yet.</li>
%% </ul>
-module(deadlift_proxy).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1]).
-export([commit/2, commit/3, info/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-type version() :: deadlift_cstate:version().

%% `ack => replicas' plants a defect for the simulation: the Proxy answers
%% the client when the k LogServers with the body have answered. The config
%% `RecoveryAckReplicasOnly' of the model has the same defect. The default
%% is `all'.
-type options() :: #{sequencer := gen_server:server_ref(), ack => all | replicas}.

-record(state, {
    sequencer :: gen_server:server_ref(),
    epoch :: deadlift_cstate:epoch() | undefined,
    logs = [] :: [deadlift_log:id()],
    replication = 1 :: pos_integer(),
    ack = all :: all | replicas,
    kcv = 0 :: version(),
    %% The pushes without an answer. The label of a request is its LSN.
    requests :: gen_server:request_id_collection(),
    %% LSN -> {Client, the number of LogServers that have not answered}
    waiting = #{} :: #{version() => {gen_server:from(), pos_integer()}}
}).

-define(TIMEOUT_MS, 5000).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts a Proxy for the epoch of `sequencer', without a link to the
%% caller.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Proxy) ->
    gen_server:stop(Proxy).

%% @equiv commit(Proxy, Body, 5000)
-spec commit(gen_server:server_ref(), term()) -> {ok, version()} | {error, commit_unknown_result}.
commit(Proxy, Body) ->
    commit(Proxy, Body, ?TIMEOUT_MS).

%% @doc Commits a record with the body `Body'. The result `{ok, LSN}' means
%% that every LogServer of the epoch has stored the record, and that no
%% recovery discards it. After `{error, commit_unknown_result}' the record
%% is committed or is not committed.
-spec commit(gen_server:server_ref(), term(), timeout()) ->
    {ok, version()} | {error, commit_unknown_result}.
commit(Proxy, Body, Timeout) ->
    try
        gen_server:call(Proxy, {commit, Body}, Timeout)
    catch
        exit:_ -> {error, commit_unknown_result}
    end.

-spec info(gen_server:server_ref()) ->
    #{epoch := deadlift_cstate:epoch(), kcv := version(), waiting := non_neg_integer()}.
info(Proxy) ->
    gen_server:call(Proxy, info).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options = #{sequencer := Sequencer}) ->
    {ok, #state{sequencer = Sequencer, ack = maps:get(ack, Options, all), requests = gen_server:reqids_new()}, {continue, start}}.

%% @private
%% The Proxy starts with the previous epoch's end version as its KCV.
handle_continue(start, State = #state{sequencer = Sequencer}) ->
    try deadlift_sequencer:recovered(Sequencer) of
        {ok, Recovered} ->
            #{
                epoch := Epoch,
                log_system := #{logs := Logs, replication := K},
                previous_epoch_end_version := PEV
            } = Recovered,
            {noreply, State#state{epoch = Epoch, logs = Logs, replication = K, kcv = PEV}}
    catch
        exit:Reason ->
            {stop, {shutdown, {sequencer, Reason}}, State}
    end.

%% @private
handle_call({commit, Body}, From, State = #state{sequencer = Sequencer, epoch = Epoch}) ->
    try deadlift_sequencer:commit_version(Sequencer) of
        {ok, Epoch, Prev, LSN} ->
            %% The Proxy sends to a LogServer only if the LogServer runs.
            %% A request to a process that does not run would be answered
            %% by a `DOWN' message from the VM.
            Pids = [{Log, deadlift_log:ref(Log)} || Log <- State#state.logs],
            case lists:keymember(undefined, 2, Pids) of
                false ->
                    {noreply, push(From, Prev, LSN, Body, Pids, State)};
                true ->
                    Waiting = State#state.waiting,
                    fail({shutdown, {log, LSN, noproc}}, State#state{waiting = Waiting#{LSN => {From, 1}}})
            end
    catch
        exit:Reason ->
            %% The Sequencer has assigned no version to this commit, or the
            %% Proxy has not received the version. No LogServer has the
            %% record.
            fail({shutdown, {sequencer, Reason}}, State#state{waiting = (State#state.waiting)#{0 => {From, 1}}})
    end;
handle_call(info, _From, State = #state{epoch = Epoch, kcv = KCV, waiting = Waiting}) ->
    {reply, #{epoch => Epoch, kcv => KCV, waiting => map_size(Waiting)}, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info(Message, State = #state{requests = Requests}) ->
    case gen_server:check_response(Message, Requests, true) of
        {{reply, {ok, _DV}}, LSN, Requests1} ->
            {noreply, stored(LSN, State#state{requests = Requests1})};
        {{reply, {error, Reason}}, LSN, Requests1} ->
            fail({shutdown, {log, LSN, Reason}}, State#state{requests = Requests1});
        {{error, {Reason, _Log}}, LSN, Requests1} ->
            fail({shutdown, {log, LSN, Reason}}, State#state{requests = Requests1});
        _NoRequestOrNoReply ->
            {noreply, State}
    end.

%% Sends the record to every LogServer of the epoch.
push(From, Prev, LSN, Body, Pids, State) ->
    #state{epoch = Epoch, logs = Logs, replication = K, kcv = KCV, requests = Requests, waiting = Waiting} = State,
    Replicas = deadlift_log:replicas(Logs, K, LSN),
    Requests1 = lists:foldl(
        fun({Log, Pid}, Acc) ->
            Sent =
                case lists:member(Log, Replicas) of
                    true -> {body, Body};
                    false -> empty
                end,
            gen_server:send_request(Pid, {push, Epoch, Prev, LSN, KCV, Sent}, LSN, Acc)
        end,
        Requests,
        Pids
    ),
    Needed =
        case State#state.ack of
            all -> length(Logs);
            replicas -> K
        end,
    State#state{requests = Requests1, waiting = Waiting#{LSN => {From, Needed}}}.

%% One LogServer has stored the record `LSN'.
stored(LSN, State = #state{kcv = KCV, waiting = Waiting}) ->
    case Waiting of
        #{LSN := {From, 1}} ->
            gen_server:reply(From, {ok, LSN}),
            State#state{kcv = max(KCV, LSN), waiting = maps:remove(LSN, Waiting)};
        #{LSN := {From, N}} ->
            State#state{waiting = Waiting#{LSN := {From, N - 1}}};
        _ ->
            %% The Proxy has answered the client. This case occurs only
            %% with the planted defect `ack => replicas'.
            State
    end.

fail(Reason, State = #state{waiting = Waiting}) ->
    maps:foreach(
        fun(_LSN, {From, _N}) -> gen_server:reply(From, {error, commit_unknown_result}) end,
        Waiting
    ),
    {stop, Reason, State#state{waiting = #{}}}.
