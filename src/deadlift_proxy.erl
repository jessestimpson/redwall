%% @doc The Proxy.
%%
%% A Proxy commits log records for clients. See sections 2.4.1 and 2.4.3 of
%% Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
%% Value Store", SIGMOD 2021. `formal/Recovery.tla' models the Proxy with
%% the actions `Assign' and `Ack', and a request for a read version with
%% `ReadStart', `ReadConfirm', `ReadSeq' and `ReadDone'.
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
%% == Batch ==
%%
%% The Proxy collects the transactions that arrive in a short time into
%% one batch. A batch has one commit version and is one log record (paper,
%% section 2.4.1). The Proxy closes a batch in two cases:
%%
%% <ul>
%% <li>`batch_ms' milliseconds have passed since the first transaction of
%%   the batch arrived. The default is 1 ms. With 0, the batch has the
%%   transactions that are in the message queue of the Proxy at that
%%   time.</li>
%% <li>The batch has `batch_max' transactions. The default is 1,000.</li>
%% </ul>
%%
%% The order of the transactions in a batch is the order of their arrival.
%% The index of a transaction is its position in the batch, from 0.
%% `commit_with_index/3' returns the index. The pair of the commit version
%% and the index gives the order of all committed transactions.
%% FoundationDB uses the same pair for a versionstamp.
%%
%% The CommitProxy of FoundationDB closes a batch after a time, at a number
%% of transactions, or at a number of bytes (`commitBatcher' in
%% `fdbserver/CommitProxyServer.actor.cpp'). The time of FoundationDB
%% changes with the load. deadlift has a fixed time and no limit in bytes.
%%
%% == Commit ==
%%
%% <ol>
%% <li>When the Proxy closes a batch, it asks the Sequencer for a commit
%%   version. The answer is an LSN and the previous LSN.</li>
%% <li>The Proxy sends the read version, the read keys, the read ranges and
%%   the write keys of each transaction of the batch to the Resolver of the
%%   epoch, in one request. For a transaction with the answer `conflict' or
%%   `too_old', the Proxy answers the client with `{error, not_committed}'
%%   or `{error, transaction_too_old}'.</li>
%% <li>The Proxy sends a record to every LogServer of the epoch, with the
%%   LSN, the previous LSN and the Known Committed Version (KCV) of the
%%   Proxy. The body is `{batch, Entries}', with one entry for each
%%   transaction of the batch in the order of the batch. The entry of an
%%   admitted transaction has the mutations of the transaction: sets of
%%   keys, clears of keys and clears of key ranges. The entry of
%%   another transaction is `no_transaction'. The k LogServers that
%%   replicate the record receive the body. The other LogServers receive
%%   an empty body.</li>
%% <li>When every LogServer of the epoch has answered, the Proxy raises its
%%   KCV to the LSN, and reports the LSN to the Sequencer as committed. The
%%   Proxy also reports the LSN of a record without an admitted
%%   transaction.</li>
%% <li>When the Sequencer has answered, the Proxy answers the client of
%%   each admitted transaction with `{ok, LSN}'.</li>
%% </ol>
%%
%% The Proxy also sends the record when the Resolver admitted no
%% transaction of the batch. A LogServer stores a record only after the
%% record with the previous LSN, so every LSN that the Sequencer assigns
%% must reach the LogServers.
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
%% The Proxy handles several batches at the same time. It does not wait
%% for the answer to one batch before it closes the next batch.
%%
%% == Failure ==
%%
%% The Sequencer answers `{error, versions_in_flight}' when too many
%% versions are assigned and not committed (see {@link deadlift_sequencer}).
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
%% <li>The time of a batch is fixed, and a batch has no limit in bytes.
%%   The time of a batch of FoundationDB changes with the load.</li>
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
-export([commit/2, commit/3, commit_with_index/3, read_version/1, read_version/2, info/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([transaction/0, mutation/0]).

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
    confirm => boolean(),
    %% The time of a batch in milliseconds, and the highest number of
    %% transactions in a batch.
    batch_ms => non_neg_integer(),
    batch_max => pos_integer()
}.

%% A transaction. `reads' are the keys that the transaction read at
%% `read_version'. `read_ranges' are the key ranges that the transaction
%% read at `read_version': the range `{Begin, End}' has every key `K' with
%% `Begin =< K' and `K < End'. `writes' are the keys and the values that
%% the transaction writes. `mutations' are further changes, which the
%% StorageServer applies in their order after `writes'. The default of
%% `read_version' is 0, and the default of the lists is `[]'.
-type transaction() :: #{
    read_version => version(),
    reads => [term()],
    read_ranges => [deadlift_resolver:key_range()],
    writes => [{term(), term()}],
    mutations => [mutation()]
}.

%% A change of the database. `{set, Key, Value}' writes a value.
%% `{clear, Key}' removes a key. `{clear_range, Begin, End}' removes every
%% key `K' with `Begin =< K' and `K < End'. The entry of a transaction in a
%% log record is a list of mutations.
-type mutation() :: {set, term(), term()} | {clear, term()} | {clear_range, term(), term()}.

%% A batch that the Proxy has closed and has not finished.
-record(commit, {
    %% The clients that the Proxy has not answered, each with the index of
    %% its transaction in the batch.
    clients :: [{non_neg_integer(), gen_server:from()}],
    prev :: version(),
    %% The entries of the batch, in the order of the batch.
    entries :: [term()],
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
    batch_ms = 1 :: non_neg_integer(),
    batch_max = 1000 :: pos_integer(),
    %% The open batch: the client, the part for the Resolver and the entry
    %% of each transaction, with the last transaction first.
    batch = [] :: [{gen_server:from(), deadlift_resolver:transaction(), term()}],
    batch_timer :: reference() | undefined,
    reads = #{} :: #{pos_integer() => #read{}},
    next_read = 1 :: pos_integer()
}).

-define(TIMEOUT_MS, 5000).
%% The Proxy looks for the names of its Resolver and of its LogServers 100
%% times at an interval of 10 ms.
-define(AWAIT_NAMES_TRIES, 100).
-define(AWAIT_NAMES_MS, 10).

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
    case commit_with_index(Proxy, Transaction, Timeout) of
        {ok, LSN, _Index} -> {ok, LSN};
        Error -> Error
    end.

%% @doc Commits as `commit/3' does. The result also has the index of the
%% transaction in its batch. Of two committed transactions with one LSN,
%% the transaction with the lower index is the earlier one.
-spec commit_with_index(gen_server:server_ref(), transaction() | term(), timeout()) ->
    {ok, version(), non_neg_integer()} | {error, not_committed | transaction_too_old | commit_unknown_result}.
commit_with_index(Proxy, Transaction, Timeout) ->
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
        batch_ms = maps:get(batch_ms, Options, 1),
        batch_max = maps:get(batch_max, Options, 1000),
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
            ok = await_names(Epoch, Logs, ?AWAIT_NAMES_TRIES),
            {noreply, State#state{epoch = Epoch, logs = Logs, replication = K, kcv = PEV}}
    catch
        exit:Reason ->
            {stop, {shutdown, {sequencer, Reason}}, State}
    end.

%% Waits until the registry of this node has the name of the Resolver and
%% of every LogServer of the epoch, for at most 1 second. A process on
%% another node registers on that node, and the registry of this node
%% receives the name in a message (see {@link deadlift_registry}). After
%% the time, the Proxy continues. A commit then stops the Proxy if the
%% Resolver or a LogServer has no name.
await_names(_Epoch, _Logs, 0) ->
    ok;
await_names(Epoch, Logs, Tries) ->
    case lists:member(undefined, [deadlift_resolver:ref(Epoch) | [deadlift_log:ref(Log) || Log <- Logs]]) of
        false ->
            ok;
        true ->
            ok = timer:sleep(?AWAIT_NAMES_MS),
            await_names(Epoch, Logs, Tries - 1)
    end.

%% @private
handle_call({commit, Transaction}, From, State = #state{batch = Batch, batch_ms = Ms, batch_max = Max}) ->
    {Resolved, Entry} = split(Transaction),
    State1 = State#state{batch = [{From, Resolved, Entry} | Batch]},
    case length(Batch) + 1 of
        Max ->
            close_batch(State1);
        1 ->
            {noreply, State1#state{batch_timer = erlang:send_after(Ms, self(), close_batch)}};
        _ ->
            {noreply, State1}
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
handle_info(close_batch, State = #state{batch = []}) ->
    %% The batch of this timer was closed at `batch_max' transactions.
    {noreply, State};
handle_info(close_batch, State) ->
    close_batch(State);
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

%% Closes the open batch: asks the Sequencer for the commit version, and
%% sends the transactions to the Resolver.
close_batch(State0 = #state{sequencer = Sequencer, epoch = Epoch, batch = Batch0, batch_timer = Timer}) ->
    _ = is_reference(Timer) andalso erlang:cancel_timer(Timer),
    Batch = lists:reverse(Batch0),
    State = State0#state{batch = [], batch_timer = undefined},
    Clients = lists:enumerate(0, [From || {From, _Resolved, _Entry} <- Batch]),
    Entries = [Entry || {_From, _Resolved, Entry} <- Batch],
    try deadlift_sequencer:commit_version(Sequencer) of
        {ok, Epoch, Prev, LSN} ->
            Commit = #commit{clients = Clients, prev = Prev, entries = Entries},
            State1 = State#state{commits = (State#state.commits)#{LSN => Commit}},
            %% The Proxy sends to a process only if the process runs. A
            %% request to a process that does not run would be answered by
            %% a `DOWN' message from the VM.
            case deadlift_resolver:ref(Epoch) of
                undefined ->
                    fail({shutdown, {resolver, LSN, noproc}}, State1);
                Resolver ->
                    Request = {resolve, Epoch, Prev, LSN, [Resolved || {_From, Resolved, _Entry} <- Batch]},
                    Requests = gen_server:send_request(Resolver, Request, {resolve, LSN}, State1#state.requests),
                    {noreply, State1#state{requests = Requests}}
            end;
        {error, Reason} ->
            %% The Sequencer has assigned no version to this batch.
            Commit0 = #commit{clients = Clients, prev = 0, entries = Entries},
            fail({shutdown, {sequencer, Reason}}, State#state{commits = (State#state.commits)#{0 => Commit0}})
    catch
        exit:Reason ->
            %% The Sequencer has assigned no version to this batch, or the
            %% Proxy has not received the version. No LogServer has the
            %% record.
            Commit1 = #commit{clients = Clients, prev = 0, entries = Entries},
            fail({shutdown, {sequencer, Reason}}, State#state{commits = (State#state.commits)#{0 => Commit1}})
    end.

role(resolve) -> resolver;
role(push) -> log;
role(report) -> sequencer.

%% The part of a transaction that the Resolver needs, and the body of the
%% record.
split(Transaction) when is_map(Transaction) ->
    Mutations = [{set, Key, Value} || {Key, Value} <- maps:get(writes, Transaction, [])] ++ maps:get(mutations, Transaction, []),
    %% A set and a clear of a key are writes of the key for the Resolver. A
    %% clear of a range is a write range.
    Resolved = #{
        read_version => maps:get(read_version, Transaction, 0),
        reads => maps:get(reads, Transaction, []),
        read_ranges => maps:get(read_ranges, Transaction, []),
        writes => lists:append([
            case Mutation of
                {set, Key, _Value} -> [Key];
                {clear, Key} -> [Key];
                {clear_range, _Begin, _End} -> []
            end
         || Mutation <- Mutations
        ]),
        write_ranges => [{Begin, End} || {clear_range, Begin, End} <- Mutations]
    },
    {Resolved, Mutations};
split(Entry) ->
    {#{read_version => 0, reads => [], writes => []}, Entry}.

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
answered({resolve, LSN}, Results, State = #state{commits = Commits}) when is_list(Results) ->
    #{LSN := Commit = #commit{clients = Clients, entries = Entries}} = Commits,
    %% The Proxy answers the client of each transaction that the Resolver
    %% did not admit. The entry of that transaction has no write.
    Resolved = lists:zipwith3(
        fun
            (Client, Entry, committed) ->
                {[Client], Entry};
            ({_Index, From}, _Entry, Rejected) ->
                Error =
                    case Rejected of
                        conflict -> not_committed;
                        too_old -> transaction_too_old
                    end,
                gen_server:reply(From, {error, Error}),
                {[], no_transaction}
        end,
        Clients,
        Entries,
        Results
    ),
    {Admitted, Entries1} = lists:unzip(Resolved),
    %% The LSN goes to the LogServers also when no transaction is admitted.
    push(LSN, State#state{commits = Commits#{LSN := Commit#commit{clients = lists:append(Admitted), entries = Entries1}}});
answered({resolve, LSN}, {error, Reason}, State) ->
    fail({shutdown, {resolver, LSN, Reason}}, State);
answered({push, LSN}, {ok, _DV}, State) ->
    stored(LSN, State);
answered({push, LSN}, {error, Reason}, State) ->
    fail({shutdown, {log, LSN, Reason}}, State);
answered({report, LSN}, ok, State = #state{commits = Commits}) ->
    case maps:take(LSN, Commits) of
        {#commit{clients = Clients}, Commits1} ->
            ok = reply_committed(LSN, Clients),
            {noreply, State#state{commits = Commits1}};
        error ->
            {noreply, State}
    end.

reply_committed(LSN, Clients) ->
    lists:foreach(fun({Index, From}) -> gen_server:reply(From, {ok, LSN, Index}) end, Clients).

%% Sends the record to every LogServer of the epoch.
push(LSN, State) ->
    #state{epoch = Epoch, logs = Logs, replication = K, kcv = KCV, requests = Requests, commits = Commits} = State,
    #{LSN := Commit = #commit{prev = Prev, entries = Entries}} = Commits,
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
                            true -> {body, {batch, Entries}};
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
        #{LSN := #commit{clients = Clients, left = 1}} when not State#state.report ->
            %% The planted defect `report => false'.
            ok = reply_committed(LSN, Clients),
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

%% Answers every client that waits: the clients of the closed batches and
%% the clients of the open batch.
unknown(State = #state{commits = Commits, batch = Batch}) ->
    Waiting =
        [From || #commit{clients = Clients} <- maps:values(Commits), {_Index, From} <- Clients] ++
            [From || {From, _Resolved, _Entry} <- Batch],
    lists:foreach(fun(From) -> gen_server:reply(From, {error, commit_unknown_result}) end, Waiting),
    State#state{commits = #{}, batch = []}.
