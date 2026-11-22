%% @doc The Proxy.
%%
%% A Proxy commits log records for clients. See sections 2.4.1 and 2.4.3 of
%% Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
%% Value Store", SIGMOD 2021. `formal/Recovery.tla' models the Proxy with
%% the actions `Assign' and `Ack'.
%%
%% == Read version ==
%%
%% For `read_version/1', the Proxy sends two kinds of request at the same
%% time:
%%
%% <ul>
%% <li>The Proxy asks the Sequencer for the read version: the highest
%%   commit version that a Proxy has reported as committed.</li>
%% <li>The Proxy asks every LogServer of the epoch that runs whether the
%%   epoch is live (`deadlift_log:confirm/2').</li>
%% </ul>
%%
%% The Proxy answers the client when the Sequencer and k LogServers have
%% answered, where k is the replication degree. FoundationDB has the same
%% rule (`getLiveCommittedVersion' in `fdbserver/GrvProxyServer.actor.cpp'
%% and `confirmEpochLive' in `fdbserver/TagPartitionedLogSystem.actor.cpp').
%% The paper does not state it.
%%
%% The confirmation gives this guarantee: the read version is at or above
%% the commit version of every transaction that was acknowledged before
%% the client asked, in any epoch. The reason has three parts, for a Proxy
%% of epoch `E' and m LogServers:
%%
%% <ul>
%% <li>A recovery needs answers to its lock from more than m - k
%%   LogServers of epoch `E'. At least one of the k LogServers that
%%   confirmed is among them. That LogServer answered the lock after its
%%   confirmation, and so after the client asked. A commit of a later
%%   epoch is therefore acknowledged after the client asked.</li>
%% <li>A commit of epoch `E' that was acknowledged before the client asked
%%   was reported to the Sequencer before the Proxy asked the
%%   Sequencer.</li>
%% <li>A commit of an earlier epoch has a commit version below the first
%%   commit version of epoch `E'.</li>
%% </ul>
%%
%% If a LogServer answers that it is stopped, or more than m - k LogServers
%% of the epoch do not run, the epoch has ended or cannot commit, and the
%% Proxy stops.
%%
%% == Commit ==
%%
%% <ol>
%% <li>The Proxy asks the Sequencer for a commit version. The answer is an
%%   LSN and the previous LSN.</li>
%% <li>The Proxy sends the read version, the read keys and the write keys
%%   of the transaction to the Resolver of the epoch. If the Resolver
%%   answers `conflict' or `too_old', the Proxy answers the client with
%%   `{error, not_committed}' or `{error, transaction_too_old}'.</li>
%% <li>The Proxy sends a record to every LogServer of the epoch, with the
%%   LSN, the previous LSN and the Known Committed Version (KCV) of the
%%   Proxy. The k LogServers that replicate the record receive the body.
%%   The other LogServers receive an empty body.</li>
%% <li>When every LogServer of the epoch has answered, the Proxy raises its
%%   KCV to the LSN, and reports the LSN to the Sequencer as committed.</li>
%% <li>When the Sequencer has answered, the Proxy answers the client with
%%   `{ok, LSN}'.</li>
%% </ol>
%%
%% The Proxy also sends a record for a transaction that the Resolver did
%% not admit. The body of that record is `no_transaction'. A LogServer
%% stores a record only after the record with the previous LSN, so every
%% LSN that the Sequencer assigns must reach the LogServers. FoundationDB
%% sends the commit version of a batch to the LogServers in the same way
%% when every transaction of the batch has a conflict.
%%
%% Step 4 gives the order that a read version needs: the Sequencer knows
%% that an LSN is committed before any client does. A client that starts
%% after an answer therefore gets a read version at or above that LSN.
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
%% If the Resolver, a LogServer or the Sequencer answers with an error, or
%% is not alive, the Proxy cannot know whether a later recovery keeps the
%% record. The Proxy answers `{error, commit_unknown_result}' to every
%% client that waits, and stops. A LogServer answers with an error after
%% the Sequencer of a later epoch has locked it, so the Proxy of an ended
%% epoch stops at its next commit.
%%
%% `commit/2' also returns `{error, commit_unknown_result}' when the Proxy
%% does not answer in time or is not alive.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Proxy sends one record for each commit. FoundationDB collects
%%   the transactions of a short time into one batch with one commit
%%   version.</li>
%% <li>The Proxy chooses the LogServers that receive the body from the LSN
%%   (`deadlift_log:replicas/3'). FoundationDB chooses them from the tags of
%%   the record.</li>
%% <li>A Proxy of deadlift commits and gives read versions. FoundationDB
%%   has CommitProxies and GrvProxies.</li>
%% <li>The Proxy confirms the epoch for each request of a read version.
%%   FoundationDB confirms once for a batch of requests.</li>
%% </ul>
-module(deadlift_proxy).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1]).
-export([commit/2, commit/3, read_version/1, read_version/2, info/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([transaction/0]).

-type version() :: deadlift_cstate:version().

%% `ack => replicas' plants a defect for the simulation: the Proxy answers
%% the client when the k LogServers with the body have answered. The config
%% `RecoveryAckReplicasOnly' of the model has the same defect. The default
%% is `all'.
%%
%% `report => false' plants a second defect: the Proxy answers the client
%% and does not report the LSN to the Sequencer. The read version then does
%% not follow the commits.
%%
%% `confirm => false' plants a third defect: the Proxy gives a read version
%% without the confirmation that the epoch is live.
-type options() :: #{
    sequencer := gen_server:server_ref(),
    ack => all | replicas,
    report => boolean(),
    confirm => boolean()
}.

%% A transaction. `reads' are the keys that the transaction read at
%% `read_version'. `writes' are the keys and the values that the
%% transaction writes. The default of `read_version' is 0, and the default
%% of the lists is `[]'.
-type transaction() :: #{
    read_version => version(),
    reads => [term()],
    writes => [{term(), term()}]
}.

%% A commit that the Proxy has not finished.
-record(commit, {
    %% The client, or `none' after the Proxy has answered it.
    from :: gen_server:from() | none,
    prev :: version(),
    body :: term(),
    %% The number of LogServers that have not answered.
    left = 0 :: non_neg_integer()
}).

%% A request for a read version that the Proxy has not answered.
-record(read, {
    from :: gen_server:from(),
    %% The read version of the Sequencer, or `undefined' before the answer.
    version :: version() | undefined,
    %% The number of confirmations that the Proxy still needs.
    needed :: non_neg_integer(),
    %% The number of LogServers that do not run or did not answer.
    unreachable = 0 :: non_neg_integer()
}).

-record(state, {
    sequencer :: gen_server:server_ref(),
    epoch :: deadlift_cstate:epoch() | undefined,
    logs = [] :: [deadlift_log:id()],
    replication = 1 :: pos_integer(),
    ack = all :: all | replicas,
    report = true :: boolean(),
    confirm = true :: boolean(),
    kcv = 0 :: version(),
    %% The requests without an answer. The label of a request is
    %% `{resolve, LSN}', `{push, LSN}', `{report, LSN}', `{live, Id}' or
    %% `{confirm, Id}'. `Id' is the number of a request for a read version.
    requests :: gen_server:request_id_collection(),
    commits = #{} :: #{version() => #commit{}},
    reads = #{} :: #{pos_integer() => #read{}},
    next_read = 1 :: pos_integer()
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

%% @equiv commit(Proxy, Transaction, 5000)
-spec commit(gen_server:server_ref(), transaction() | term()) ->
    {ok, version()} | {error, not_committed | transaction_too_old | commit_unknown_result}.
commit(Proxy, Transaction) ->
    commit(Proxy, Transaction, ?TIMEOUT_MS).

%% @doc Commits a transaction. The result `{ok, LSN}' means that every
%% LogServer of the epoch has stored the record, and that no recovery
%% discards it. The other results are:
%%
%% <ul>
%% <li>`{error, not_committed}': a transaction that committed after the
%%   read version wrote a key that this transaction read.</li>
%% <li>`{error, transaction_too_old}': the read version is below the MVCC
%%   window.</li>
%% <li>`{error, commit_unknown_result}': the transaction is committed or is
%%   not committed.</li>
%% </ul>
%%
%% A term that is not a map is committed as the body of a record, without
%% read keys and without write keys. It has no conflict with any
%% transaction.
-spec commit(gen_server:server_ref(), transaction() | term(), timeout()) ->
    {ok, version()} | {error, not_committed | transaction_too_old | commit_unknown_result}.
commit(Proxy, Transaction, Timeout) ->
    try
        gen_server:call(Proxy, {commit, Transaction}, Timeout)
    catch
        exit:_ -> {error, commit_unknown_result}
    end.

%% @equiv read_version(Proxy, 5000)
-spec read_version(gen_server:server_ref()) -> {ok, version()} | {error, term()}.
read_version(Proxy) ->
    read_version(Proxy, ?TIMEOUT_MS).

%% @doc Returns a read version for a new transaction. The read version is
%% at or above the commit version of every transaction that was
%% acknowledged before the call.
-spec read_version(gen_server:server_ref(), timeout()) -> {ok, version()} | {error, term()}.
read_version(Proxy, Timeout) ->
    try
        gen_server:call(Proxy, read_version, Timeout)
    catch
        exit:Reason -> {error, Reason}
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
    State = #state{
        sequencer = Sequencer,
        ack = maps:get(ack, Options, all),
        report = maps:get(report, Options, true),
        confirm = maps:get(confirm, Options, true),
        requests = gen_server:reqids_new()
    },
    {ok, State, {continue, start}}.

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
handle_call({commit, Transaction}, From, State = #state{sequencer = Sequencer, epoch = Epoch}) ->
    {Resolved, Body} = split(Transaction),
    try deadlift_sequencer:commit_version(Sequencer) of
        {ok, Epoch, Prev, LSN} ->
            Commit = #commit{from = From, prev = Prev, body = Body},
            State1 = State#state{commits = (State#state.commits)#{LSN => Commit}},
            %% The Proxy sends to a process only if the process runs. A
            %% request to a process that does not run would be answered by
            %% a `DOWN' message from the VM.
            case deadlift_resolver:ref(Epoch) of
                undefined ->
                    fail({shutdown, {resolver, LSN, noproc}}, State1);
                Resolver ->
                    Request = {resolve, Epoch, Prev, LSN, Resolved},
                    Requests = gen_server:send_request(Resolver, Request, {resolve, LSN}, State1#state.requests),
                    {noreply, State1#state{requests = Requests}}
            end
    catch
        exit:Reason ->
            %% The Sequencer has assigned no version to this commit, or the
            %% Proxy has not received the version. No LogServer has the
            %% record.
            Commit0 = #commit{from = From, prev = 0, body = Body},
            fail({shutdown, {sequencer, Reason}}, State#state{commits = (State#state.commits)#{0 => Commit0}})
    end;
handle_call(read_version, From, State = #state{sequencer = Sequencer, epoch = Epoch, logs = Logs, replication = K}) ->
    #state{requests = Requests, reads = Reads, next_read = Id} = State,
    Requests1 = gen_server:send_request(Sequencer, read_version, {live, Id}, Requests),
    %% The Proxy sends to a LogServer only if the LogServer runs.
    Pids =
        case State#state.confirm of
            true -> [deadlift_log:ref(Log) || Log <- Logs];
            false -> []
        end,
    Running = [Pid || Pid <- Pids, Pid =/= undefined],
    Requests2 = lists:foldl(
        fun(Pid, Acc) -> gen_server:send_request(Pid, {confirm, Epoch}, {confirm, Id}, Acc) end,
        Requests1,
        Running
    ),
    Needed =
        case State#state.confirm of
            true -> K;
            %% The planted defect `confirm => false'.
            false -> 0
        end,
    Read = #read{from = From, needed = Needed, unreachable = length(Pids) - length(Running)},
    State1 = State#state{requests = Requests2, reads = Reads#{Id => Read}, next_read = Id + 1},
    read_progress(Id, noproc, State1);
handle_call(info, _From, State = #state{epoch = Epoch, kcv = KCV, commits = Commits}) ->
    {reply, #{epoch => Epoch, kcv => KCV, waiting => map_size(Commits)}, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info(Message, State = #state{requests = Requests}) ->
    case gen_server:check_response(Message, Requests, true) of
        {{reply, Reply}, Label, Requests1} ->
            answered(Label, Reply, State#state{requests = Requests1});
        {{error, {Reason, _Process}}, {confirm, Id}, Requests1} ->
            State1 = State#state{requests = Requests1},
            case State1#state.reads of
                #{Id := Read = #read{unreachable = N}} ->
                    Reads = (State1#state.reads)#{Id := Read#read{unreachable = N + 1}},
                    read_progress(Id, Reason, State1#state{reads = Reads});
                _ ->
                    {noreply, State1}
            end;
        {{error, {Reason, _Process}}, {live, _Id}, Requests1} ->
            fail({shutdown, {sequencer, read_version, Reason}}, State#state{requests = Requests1});
        {{error, {Reason, _Process}}, {Kind, LSN}, Requests1} ->
            fail({shutdown, {role(Kind), LSN, Reason}}, State#state{requests = Requests1});
        _NoRequestOrNoReply ->
            {noreply, State}
    end.

role(resolve) -> resolver;
role(push) -> log;
role(report) -> sequencer.

%% The part of a transaction that the Resolver needs, and the body of the
%% record.
split(Transaction) when is_map(Transaction) ->
    Writes = maps:get(writes, Transaction, []),
    Resolved = #{
        read_version => maps:get(read_version, Transaction, 0),
        reads => maps:get(reads, Transaction, []),
        writes => [Key || {Key, _Value} <- Writes]
    },
    {Resolved, Writes};
split(Body) ->
    {#{read_version => 0, reads => [], writes => []}, Body}.

%% Answers the request for a read version `Id' when the Sequencer and
%% enough LogServers have answered. Stops the Proxy when more than m - k
%% LogServers of the epoch do not run: the epoch cannot be confirmed, and
%% cannot commit. `Reason' is the reason of the last LogServer that the
%% Proxy did not reach.
read_progress(Id, Reason, State = #state{logs = Logs, replication = K, reads = Reads}) ->
    case Reads of
        #{Id := #read{unreachable = N}} when N > length(Logs) - K ->
            fail({shutdown, {log, read_version, Reason}}, State);
        #{Id := #read{from = From, version = Version, needed = 0}} when Version =/= undefined ->
            gen_server:reply(From, {ok, Version}),
            {noreply, State#state{reads = maps:remove(Id, Reads)}};
        _ ->
            {noreply, State}
    end.

%% The answer to a request.
answered({live, Id}, {ok, Version}, State = #state{reads = Reads}) ->
    case Reads of
        #{Id := Read} -> read_progress(Id, noproc, State#state{reads = Reads#{Id := Read#read{version = Version}}});
        _ -> {noreply, State}
    end;
answered({confirm, Id}, ok, State = #state{reads = Reads}) ->
    case Reads of
        #{Id := Read = #read{needed = N}} when N > 0 ->
            read_progress(Id, noproc, State#state{reads = Reads#{Id := Read#read{needed = N - 1}}});
        _ ->
            %% The Proxy has answered the client, or has enough
            %% confirmations.
            {noreply, State}
    end;
answered({confirm, _Id}, {error, Reason}, State) ->
    %% A Sequencer of a later epoch has locked the LogServer. The epoch of
    %% the Proxy has ended.
    fail({shutdown, {log, read_version, Reason}}, State);
answered({resolve, LSN}, committed, State) ->
    push(LSN, State);
answered({resolve, LSN}, Rejected, State = #state{commits = Commits}) when Rejected =:= conflict; Rejected =:= too_old ->
    #{LSN := Commit = #commit{from = From}} = Commits,
    Error =
        case Rejected of
            conflict -> not_committed;
            too_old -> transaction_too_old
        end,
    gen_server:reply(From, {error, Error}),
    %% The LSN still goes to the LogServers, without a transaction.
    push(LSN, State#state{commits = Commits#{LSN := Commit#commit{from = none, body = no_transaction}}});
answered({resolve, LSN}, {error, Reason}, State) ->
    fail({shutdown, {resolver, LSN, Reason}}, State);
answered({push, LSN}, {ok, _DV}, State) ->
    stored(LSN, State);
answered({push, LSN}, {error, Reason}, State) ->
    fail({shutdown, {log, LSN, Reason}}, State);
answered({report, LSN}, ok, State = #state{commits = Commits}) ->
    case maps:take(LSN, Commits) of
        {#commit{from = From}, Commits1} ->
            gen_server:reply(From, {ok, LSN}),
            {noreply, State#state{commits = Commits1}};
        error ->
            {noreply, State}
    end.

%% Sends the record to every LogServer of the epoch.
push(LSN, State) ->
    #state{epoch = Epoch, logs = Logs, replication = K, kcv = KCV, requests = Requests, commits = Commits} = State,
    #{LSN := Commit = #commit{prev = Prev, body = Body}} = Commits,
    Pids = [{Log, deadlift_log:ref(Log)} || Log <- Logs],
    case lists:keymember(undefined, 2, Pids) of
        true ->
            fail({shutdown, {log, LSN, noproc}}, State);
        false ->
            Replicas = deadlift_log:replicas(Logs, K, LSN),
            Requests1 = lists:foldl(
                fun({Log, Pid}, Acc) ->
                    Sent =
                        case lists:member(Log, Replicas) of
                            true -> {body, Body};
                            false -> empty
                        end,
                    gen_server:send_request(Pid, {push, Epoch, Prev, LSN, KCV, Sent}, {push, LSN}, Acc)
                end,
                Requests,
                Pids
            ),
            Needed =
                case State#state.ack of
                    all -> length(Logs);
                    replicas -> K
                end,
            {noreply, State#state{requests = Requests1, commits = Commits#{LSN := Commit#commit{left = Needed}}}}
    end.

%% One LogServer has stored the record `LSN'.
stored(LSN, State = #state{sequencer = Sequencer, kcv = KCV, requests = Requests, commits = Commits}) ->
    case Commits of
        #{LSN := #commit{from = none, left = 1}} ->
            %% A record without a transaction. No client waits.
            {noreply, State#state{kcv = max(KCV, LSN), commits = maps:remove(LSN, Commits)}};
        #{LSN := #commit{from = From, left = 1}} when not State#state.report ->
            %% The planted defect `report => false'.
            gen_server:reply(From, {ok, LSN}),
            {noreply, State#state{kcv = max(KCV, LSN), commits = maps:remove(LSN, Commits)}};
        #{LSN := Commit = #commit{left = 1}} ->
            %% The Sequencer learns that the LSN is committed before the
            %% client does.
            Requests1 = gen_server:send_request(Sequencer, {committed, LSN}, {report, LSN}, Requests),
            State1 = State#state{
                kcv = max(KCV, LSN),
                requests = Requests1,
                commits = Commits#{LSN := Commit#commit{left = 0}}
            },
            {noreply, State1};
        #{LSN := Commit = #commit{left = N}} when N > 1 ->
            {noreply, State#state{commits = Commits#{LSN := Commit#commit{left = N - 1}}}};
        _ ->
            %% The Proxy has counted enough answers. This case occurs only
            %% with the planted defect `ack => replicas'.
            {noreply, State}
    end.

fail(Reason, State) ->
    {stop, Reason, unknown(State)}.

unknown(State = #state{commits = Commits}) ->
    maps:foreach(
        fun
            (_LSN, #commit{from = none}) -> ok;
            (_LSN, #commit{from = From}) -> gen_server:reply(From, {error, commit_unknown_result})
        end,
        Commits
    ),
    State#state{commits = #{}}.
