%% @doc The process of an epoch: the Sequencer, the Resolver and the Proxy.
%%
%% One process does the work of three roles of the paper for one epoch.
%% See sections 2.3.1, 2.4.1, 2.4.2 and 2.4.3 of Zhou et al.,
%% "FoundationDB: A Distributed Unbundled Transactional Key Value Store",
%% SIGMOD 2021. `formal/Recovery.tla' models the roles with the recovery
%% actions, with `RecoveryTxn', `Accept', `Assign' and `Ack', and a request
%% for a read version with `ReadStart', `ReadConfirm', `ReadSeq' and
%% `ReadDone'. `formal/Resolver.tla' models the conflict check.
%%
%% <ul>
%% <li>As the Sequencer, the process performs the recovery at its start
%%   ({@link deadlift_recovery}), commits the recovery transaction, assigns
%%   the commit versions and keeps the read version. The rules for the
%%   versions are in {@link deadlift_sequencer}.</li>
%% <li>As the Resolver, the process checks each batch for conflicts with
%%   the functions of {@link deadlift_resolver}, on a history that it
%%   owns.</li>
%% <li>As the Proxy, the process collects the transactions of clients into
%%   batches, sends the records to the LogServers, and gives read
%%   versions.</li>
%% </ul>
%%
%% == One process ==
%%
%% FoundationDB has one process for each role, because a process of
%% FoundationDB runs on one thread, and separate processes are its way to
%% use several processors. The three roles have the same lifetime: they
%% start with an epoch, and the failure of each of them starts a recovery.
%% In deadlift they are one process:
%%
%% <ul>
%% <li>The process assigns the commit version of a batch and checks the
%%   batch in one step. The checks are therefore in the order of the
%%   versions, without a protocol for the order.</li>
%% <li>The process raises the read version when the LogServers have stored
%%   a record, before it answers the client. No report to another process
%%   is needed.</li>
%% <li>A commit has one round trip between nodes, to the LogServers.</li>
%% </ul>
%%
%% One process is one place for the conflict check. FoundationDB can add
%% Proxies and Resolvers on more machines. This part is not in the paper.
%%
%% == Read version ==
%%
%% For `read_version/1', the process does two things:
%%
%% <ul>
%% <li>It takes its read version: the highest commit version that every
%%   LogServer of the epoch has stored.</li>
%% <li>It asks every LogServer of the epoch that runs whether the epoch is
%%   live (`deadlift_log:confirm/2').</li>
%% </ul>
%%
%% The requests of several clients share these messages. While the Proxy
%% waits for the answers of one confirmation, the requests that arrive
%% wait. When the Proxy has answered the clients of that confirmation, it
%% starts one confirmation for all the requests that wait. A request does
%% not use a confirmation that started before the request arrived, because
%% the read version of that confirmation can be below a commit that was
%% acknowledged in between. The GrvProxy of FoundationDB also answers a
%% batch of requests with one read version. The option
%% `read_version_share => false' gives each request its own messages.
%%
%% The process answers the client when k LogServers have answered, where k
%% is the replication degree. FoundationDB has the same
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
%%   raised the read version of the process before the process took
%%   it.</li>
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
%% section 2.4.1). The Proxy closes a batch in three cases:
%%
%% <ul>
%% <li>Fewer than `batch_depth' earlier batches are in progress, and none
%%   of them waits for the Resolver. The default of `batch_depth' is 1. A
%%   transaction that arrives at an idle Proxy is then a batch of its
%%   own, and does not wait for a timer. While a batch is in progress,
%%   the transactions that arrive form the next batch, and the Proxy
%%   closes that batch when the LogServers have stored the earlier
%%   one. The option `batch_idle => false' removes this case.</li>
%% <li>`batch_ms' milliseconds have passed since the first transaction of
%%   the batch arrived. The default is 1 ms. With 0, the batch has the
%%   transactions that are in the message queue of the Proxy at that
%%   time.</li>
%% <li>The batch has `batch_max' transactions. The default is 1,000.</li>
%% </ul>
%%
%% The first case is not taken from FoundationDB, which changes the time
%% of a batch with the load. This part is not in the paper.
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
%% <li>When the process closes a batch, it assigns the commit version: an
%%   LSN, with the last LSN as the previous LSN.</li>
%% <li>The process checks the read version, the read keys, the read ranges
%%   and the write keys of each transaction of the batch
%%   (`deadlift_resolver:resolve/4'). For a transaction with the result
%%   `conflict' or `too_old', the process answers the client with
%%   `{error, not_committed}' or `{error, transaction_too_old}'.</li>
%% <li>The process sends a record to every LogServer of the epoch, with
%%   the LSN, the previous LSN and its Known Committed Version (KCV). The
%%   body is `{batch, Entries}', with one entry for each transaction of
%%   the batch in the order of the batch. The entry of an admitted
%%   transaction has the mutations of the transaction: sets of keys,
%%   clears of keys and clears of key ranges. The entry of another
%%   transaction is `no_transaction'. The k LogServers that replicate the
%%   record receive the body. The other LogServers receive an empty
%%   body.</li>
%% <li>When every LogServer of the epoch has answered, the process raises
%%   its KCV and its read version to the LSN.</li>
%% <li>The process then answers the client of each admitted transaction
%%   with `{ok, LSN}'.</li>
%% </ol>
%%
%% The process also sends the record when no transaction of the batch is
%% admitted. A LogServer stores a record only after the record with the
%% previous LSN, so every assigned LSN must reach the LogServers.
%%
%% Steps 4 and 5 give the order that a read version needs: the read
%% version is at or above an LSN before any client knows that the LSN is
%% committed. A client that starts after an answer therefore gets a read
%% version at or above that LSN.
%%
%% The process waits for every LogServer, and not only for the k
%% LogServers with the body. A recovery computes the Recovery Version as
%% the minimum of the Durable Versions of the LogServers that answer. The
%% config `RecoveryAckReplicasOnly' of the model shows that a process that
%% waits for k LogServers lets a recovery discard an acknowledged record.
%%
%% == Failure ==
%%
%% If the recovery fails, the process stops with the reason
%% `{shutdown, {recovery, Reason}}'. A request that arrives during the
%% recovery waits for its end.
%%
%% The process assigns no version that is `max_versions_in_flight' or more
%% above the read version (see {@link deadlift_sequencer}). In that case it
%% stops.
%%
%% If a LogServer answers with an error, or is not alive, the process
%% cannot know whether a later recovery keeps the record. The process
%% answers `{error, commit_unknown_result}' to every client that waits,
%% and stops. A LogServer answers with an error after the process of a
%% later epoch has locked it, so the process of an ended epoch stops at
%% its next commit.
%%
%% `commit/2' also returns `{error, commit_unknown_result}' when the
%% process does not answer in time or is not alive.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Sequencer, the Resolver and the Proxy are one process. See "One
%%   process" above.</li>
%% <li>The time of a batch is fixed, and a batch has no limit in bytes.
%%   The time of a batch of FoundationDB changes with the load.</li>
%% <li>The process chooses the LogServers that receive the body from the
%%   LSN (`deadlift_log:replicas/3'). FoundationDB chooses them from the
%%   tags of the record.</li>
%% <li>The process commits and gives read versions. FoundationDB has
%%   CommitProxies and GrvProxies.</li>
%% </ul>
-module(deadlift_epoch).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1, recovered/1, recovered/2]).
-export([commit/2, commit/3, commit_with_index/3, read_version/1, read_version/2, info/1, reset_stats/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([options/0]).

-export_type([transaction/0, mutation/0]).

-type version() :: deadlift_cstate:version().

%% `ack => replicas' plants a defect for the simulation: the Proxy answers
%% the client when the k LogServers with the body have answered. The config
%% `RecoveryAckReplicasOnly' of the model has the same defect. The default
%% is `all'.
%%
%% `report => false' plants a second defect: the process answers the
%% client and does not raise its read version. The read version then does
%% not follow the commits.
%%
%% `confirm => false' plants a third defect: the Proxy gives a read version
%% without the confirmation that the epoch is live.
%%
%% The options of {@link deadlift_recovery}, of
%% {@link deadlift_sequencer:defaults/1} and of
%% {@link deadlift_resolver:resolve/4} are options of the process too.
-type options() :: #{
    coordinators := deadlift_cstate:servers(),
    logs := pos_integer(),
    replication := pos_integer(),
    _ => _,
    ack => all | replicas,
    report => boolean(),
    confirm => boolean(),
    %% The time of a batch in milliseconds, and the highest number of
    %% transactions in a batch.
    batch_ms => non_neg_integer(),
    batch_max => pos_integer(),
    %% Whether the Proxy closes a batch at once when fewer than
    %% `batch_depth' batches are in progress. The defaults are `true' and
    %% 1.
    batch_idle => boolean(),
    batch_depth => pos_integer(),
    %% Whether the requests for a read version that arrive during a
    %% confirmation share the next confirmation. The default is `true'.
    read_version_share => boolean()
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
    left = 0 :: non_neg_integer(),
    %% The time of the last step of the batch, in microseconds.
    time = 0 :: integer()
}).

%% A request for a read version that the Proxy has not answered.
-record(read, {
    %% The clients that this confirmation answers.
    from :: [gen_server:from()],
    %% The time of the start, in microseconds.
    time = 0 :: integer(),
    %% The read version at the start of the confirmation.
    version :: version() | undefined,
    %% The number of confirmations that the Proxy still needs.
    needed :: non_neg_integer(),
    %% The number of LogServers that do not run or did not answer.
    unreachable = 0 :: non_neg_integer()
}).

-record(state, {
    options :: map(),
    %% The result of the recovery, for `recovered/2'.
    recovered :: deadlift_recovery:result() | undefined,
    %% The state of the Sequencer: the last commit version, the time of
    %% its assignment in microseconds, and the read version, which is the
    %% highest commit version that every LogServer has stored.
    last = 0 :: version(),
    last_time = 0 :: integer(),
    live = 0 :: version(),
    %% The state of the Resolver.
    history :: deadlift_resolver:history() | undefined,
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
    batch_idle = true :: boolean(),
    batch_depth = 1 :: pos_integer(),
    %% The time at which the first transaction of the open batch arrived.
    batch_opened = 0 :: integer(),
    read_version_share = true :: boolean(),
    %% The number of the confirmation in progress that later requests for
    %% a read version wait for, and those requests, newest first.
    read_round :: pos_integer() | undefined,
    read_queue = [] :: [gen_server:from()],
    %% Sums for `info/1': numbers of batches and transactions, and times
    %% in microseconds.
    stats = #{} :: #{atom() => non_neg_integer()},
    reads = #{} :: #{pos_integer() => #read{}},
    next_read = 1 :: pos_integer()
}).

-define(TIMEOUT_MS, 5000).
-define(RECOVERY_TIMEOUT_MS, 10000).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts the process of a new epoch, without a link to the caller.
%% The function returns before the recovery ends. If the recovery fails,
%% the process stops with the reason `{shutdown, {recovery, Reason}}'.
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

%% @doc Returns the epoch, the KCV, the number of batches in progress, and
%% sums since the start of the Proxy in `stats'. The sums are the numbers
%% of batches (`batches'), transactions (`transactions'), confirmations
%% for read versions (`read_rounds') and answered requests for a read
%% version (`read_versions'), and times in microseconds: from the arrival
%% of the first transaction of a batch to the close of the batch
%% (`wait_us'), for the commit version (`version_us'), for the Resolver
%% (`resolve_us'), for the LogServers (`push_us'), for the report to the
%% Sequencer (`report_us'), and for the confirmations (`read_us'). For each
%% time, `stats' also has the longest single time, for example
%% `resolve_max_us'.
-spec info(gen_server:server_ref()) ->
    #{epoch := deadlift_cstate:epoch(), kcv := version(), waiting := non_neg_integer(), stats := #{atom() => non_neg_integer()}}.
info(Proxy) ->
    gen_server:call(Proxy, info).

%% @doc Sets the sums of `info/1' to 0.
-spec reset_stats(gen_server:server_ref()) -> ok.
reset_stats(Proxy) ->
    gen_server:call(Proxy, reset_stats).

%% @equiv recovered(Proxy, 10000)
-spec recovered(gen_server:server_ref()) -> {ok, deadlift_recovery:result()}.
recovered(Proxy) ->
    recovered(Proxy, ?RECOVERY_TIMEOUT_MS).

%% @doc Returns the result of the recovery: the epoch, the log system, the
%% Recovery Version, the previous epoch's end version, and the first commit
%% version of the epoch (`recovery_transaction_version'). The call waits
%% for the end of the recovery. It exits if the recovery fails.
-spec recovered(gen_server:server_ref(), timeout()) -> {ok, deadlift_recovery:result()}.
recovered(Proxy, Timeout) ->
    gen_server:call(Proxy, recovered, Timeout).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options) ->
    State = #state{
        options = Options,
        ack = maps:get(ack, Options, all),
        report = maps:get(report, Options, true),
        confirm = maps:get(confirm, Options, true),
        batch_ms = maps:get(batch_ms, Options, 1),
        batch_max = maps:get(batch_max, Options, 1000),
        batch_idle = maps:get(batch_idle, Options, true),
        batch_depth = maps:get(batch_depth, Options, 1),
        read_version_share = maps:get(read_version_share, Options, true),
        requests = gen_server:reqids_new()
    },
    {ok, State, {continue, recover}}.

%% @private
%% The work of the Sequencer at the start of an epoch: the recovery and
%% the recovery transaction. A request that arrives in this time waits.
handle_continue(recover, State = #state{options = Options}) ->
    maybe
        {ok, Recovered} ?= deadlift_recovery:recover(Options),
        #{
            epoch := Epoch,
            recovery_version := RV,
            previous_epoch_end_version := PEV,
            log_system := #{logs := Logs, replication := K}
        } = Recovered,
        First = deadlift_sequencer:first_version(RV, Options),
        ok ?= recovery_transaction(Recovered, First),
        %% The history of the Resolver has no transaction. The recovery
        %% transaction gives it the first version of the epoch, so a
        %% transaction with a read version of the previous epoch is too
        %% old.
        #{mvcc_window := Window} = deadlift_sequencer:defaults(Options),
        {committed, History} = deadlift_resolver:check(
            #{read_version => 0, reads => [], writes => []}, First, deadlift_resolver:new(Window)
        ),
        {noreply, State#state{
            recovered = Recovered#{recovery_transaction_version => First},
            epoch = Epoch,
            logs = Logs,
            replication = K,
            %% The process starts with the previous epoch's end version as
            %% its KCV.
            kcv = PEV,
            last = First,
            last_time = now_us(),
            live = First,
            history = History
        }}
    else
        {error, Reason} ->
            {stop, {shutdown, {recovery, Reason}}, State}
    end.

%% Commits the recovery transaction: the record `First' with the previous
%% LSN `RV'. The KCV of the record is the previous epoch's end version.
recovery_transaction(Recovered, First) ->
    #{
        epoch := Epoch,
        recovery_version := RV,
        previous_epoch_end_version := PEV,
        log_system := #{logs := Logs, replication := K}
    } = Recovered,
    Replicas = deadlift_log:replicas(Logs, K, First),
    try
        lists:foreach(
            fun(Log) ->
                Sent =
                    case lists:member(Log, Replicas) of
                        true -> {body, recovery_transaction};
                        false -> empty
                    end,
                Pid = deadlift_log:ref(Log),
                _ = is_pid(Pid) orelse exit(noproc),
                {ok, _DV} = gen_server:call(Pid, {push, Epoch, RV, First, PEV, Sent}, ?RECOVERY_TIMEOUT_MS)
            end,
            Logs
        )
    catch
        exit:{Reason, {_Module, call, _Arguments}} -> {error, {recovery_transaction, Reason}};
        Class:Reason when Class =:= exit; Class =:= error -> {error, {recovery_transaction, Reason}}
    end.

%% @private
handle_call({commit, Transaction}, From, State = #state{batch = Batch, batch_ms = Ms, batch_max = Max}) ->
    {Resolved, Entry} = split(Transaction),
    State1 = State#state{batch = [{From, Resolved, Entry} | Batch]},
    case length(Batch) + 1 of
        Max ->
            close_batch(State1);
        1 ->
            case idle(State1) of
                true -> close_batch(State1#state{batch_opened = now_us()});
                false -> {noreply, State1#state{batch_opened = now_us(), batch_timer = erlang:send_after(Ms, self(), close_batch)}}
            end;
        _ ->
            {noreply, State1}
    end;
handle_call(read_version, From, State = #state{read_round = Round, read_queue = Queue}) when Round =/= undefined ->
    %% A confirmation is in progress. Its read version can be below a
    %% commit that was acknowledged before this request, so this request
    %% waits for the next confirmation.
    {noreply, State#state{read_queue = [From | Queue]}};
handle_call(read_version, From, State) ->
    confirm([From], State);
handle_call(reset_stats, _From, State) ->
    {reply, ok, State#state{stats = #{}}};
handle_call(recovered, _From, State = #state{recovered = Recovered}) ->
    {reply, {ok, Recovered}, State};
handle_call(info, _From, State = #state{epoch = Epoch, kcv = KCV, commits = Commits}) ->
    {reply, #{epoch => Epoch, kcv => KCV, waiting => map_size(Commits), stats => State#state.stats}, State}.

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
        {{error, {Reason, _Process}}, {push, LSN}, Requests1} ->
            fail({shutdown, {log, LSN, Reason}}, State#state{requests = Requests1});
        _NoRequestOrNoReply ->
            {noreply, State}
    end.

%% Closes the open batch. The process assigns the commit version, as the
%% Sequencer of the paper does, and checks the transactions for conflicts,
%% as the Resolver does. It then sends the record to the LogServers.
close_batch(State0 = #state{options = Options, batch = Batch0, batch_timer = Timer, last = Last, live = Live}) ->
    _ = is_reference(Timer) andalso erlang:cancel_timer(Timer),
    Batch = lists:reverse(Batch0),
    Closed = now_us(),
    State = add(
        #{batches => 1, transactions => length(Batch), wait_us => Closed - State0#state.batch_opened},
        State0#state{batch = [], batch_timer = undefined}
    ),
    Clients = lists:enumerate(0, [From || {From, _Resolved, _Entry} <- Batch]),
    Entries = [Entry || {_From, _Resolved, Entry} <- Batch],
    case deadlift_sequencer:next_version(Last, State#state.last_time, Closed, Live, Options) of
        {ok, LSN} ->
            {Results, History} = deadlift_resolver:resolve([Resolved || {_From, Resolved, _Entry} <- Batch], LSN, State#state.history, Options),
            Resolved = now_us(),
            %% The process answers the client of each transaction that the
            %% Resolver did not admit. The entry of that transaction has
            %% no write.
            Checked = lists:zipwith3(
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
            {Admitted, Entries1} = lists:unzip(Checked),
            Commit = #commit{clients = lists:append(Admitted), prev = Last, entries = Entries1, time = Resolved},
            State1 = add(#{resolve_us => Resolved - Closed}, State#state{
                last = LSN,
                last_time = Closed,
                history = History,
                commits = (State#state.commits)#{LSN => Commit}
            }),
            %% The LSN goes to the LogServers also when no transaction is
            %% admitted.
            push(LSN, State1);
        {error, Reason} ->
            %% Too many versions are assigned and not committed. No version
            %% is assigned to this batch.
            Commit0 = #commit{clients = Clients, prev = 0, entries = Entries},
            fail({shutdown, {sequencer, Reason}}, State#state{commits = (State#state.commits)#{0 => Commit0}})
    end.

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

%% Starts a confirmation for the clients `Froms': takes the read version,
%% and asks the LogServers whether the epoch is live. The read version is
%% the highest commit version that every LogServer has stored. The process
%% raises it before it answers the client of a commit, so the read version
%% is at or above every commit that was acknowledged before this point.
confirm(Froms, State = #state{epoch = Epoch, logs = Logs, replication = K, live = Live}) ->
    #state{requests = Requests1, reads = Reads, next_read = Id} = State,
    %% The process sends to a LogServer only if the LogServer runs.
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
    Read = #read{from = Froms, time = now_us(), version = Live, needed = Needed, unreachable = length(Pids) - length(Running)},
    Round =
        case State#state.read_version_share of
            true -> Id;
            false -> undefined
        end,
    State1 = State#state{requests = Requests2, reads = Reads#{Id => Read}, next_read = Id + 1, read_round = Round},
    read_progress(Id, noproc, State1).

%% Answers the request for a read version `Id' when the Sequencer and
%% enough LogServers have answered. Stops the Proxy when more than m - k
%% LogServers of the epoch do not run: the epoch cannot be confirmed, and
%% cannot commit. `Reason' is the reason of the last LogServer that the
%% Proxy did not reach.
read_progress(Id, Reason, State = #state{logs = Logs, replication = K, reads = Reads}) ->
    case Reads of
        #{Id := #read{unreachable = N}} when N > length(Logs) - K ->
            fail({shutdown, {log, read_version, Reason}}, State);
        #{Id := #read{from = Froms, time = Time, version = Version, needed = 0}} when Version =/= undefined ->
            lists:foreach(fun(From) -> gen_server:reply(From, {ok, Version}) end, Froms),
            State1 = add(#{read_rounds => 1, read_versions => length(Froms), read_us => now_us() - Time}, State#state{reads = maps:remove(Id, Reads)}),
            case State1 of
                #state{read_round = Id, read_queue = []} ->
                    {noreply, State1#state{read_round = undefined}};
                #state{read_round = Id, read_queue = Queue} ->
                    %% The requests that arrived during this confirmation
                    %% share the next one.
                    confirm(lists:reverse(Queue), State1#state{read_round = undefined, read_queue = []});
                _ ->
                    {noreply, State1}
            end;
        _ ->
            {noreply, State}
    end.

%% The answer to a request.
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
answered({push, LSN}, {ok, _DV}, State) ->
    stored(LSN, State);
answered({push, LSN}, {error, Reason}, State) ->
    fail({shutdown, {log, LSN, Reason}}, State).

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

%% One LogServer has stored the record `LSN'. When the last one has, the
%% process raises the read version and then answers the clients. A client
%% that starts after an answer therefore gets a read version at or above
%% the LSN.
stored(LSN, State = #state{kcv = KCV, live = Live, commits = Commits}) ->
    case Commits of
        #{LSN := #commit{clients = Clients, left = 1, time = Time}} ->
            Live1 =
                case State#state.report of
                    true -> max(Live, LSN);
                    %% The planted defect `report => false': the read
                    %% version does not follow the commits.
                    false -> Live
                end,
            ok = reply_committed(LSN, Clients),
            State1 = State#state{kcv = max(KCV, LSN), live = Live1, commits = maps:remove(LSN, Commits)},
            close_if_idle(add(#{push_us => now_us() - Time}, State1));
        #{LSN := Commit = #commit{left = N}} when N > 1 ->
            {noreply, State#state{commits = Commits#{LSN := Commit#commit{left = N - 1}}}};
        _ ->
            %% The process has counted enough answers. This case occurs
            %% only with the planted defect `ack => replicas'.
            {noreply, State}
    end.

%% Whether the Proxy closes the open batch at once: no earlier batch
%% waits for the Resolver, and fewer than `batch_depth' batches are in
%% progress.
idle(#state{batch_idle = Idle, commits = Commits, batch_depth = Depth}) ->
    Idle andalso map_size(Commits) < Depth.

%% Closes the open batch, which has the transactions that arrived while
%% earlier batches were in progress, if the Proxy is idle now.
close_if_idle(State = #state{batch = [_ | _]}) ->
    case idle(State) of
        true -> close_batch(State);
        false -> {noreply, State}
    end;
close_if_idle(State) ->
    {noreply, State}.

now_us() ->
    erlang:monotonic_time(microsecond).

%% Adds to the sums of `info/1', and keeps the longest single time.
add(Sums, State = #state{stats = Stats}) ->
    Stats1 = maps:merge_with(fun(_Key, A, B) -> A + B end, Stats, Sums),
    Stats2 = maps:fold(
        fun(Key, Value, Acc) ->
            case max_key(Key) of
                none -> Acc;
                MaxKey -> Acc#{MaxKey => max(Value, maps:get(MaxKey, Acc, 0))}
            end
        end,
        Stats1,
        Sums
    ),
    State#state{stats = Stats2}.

max_key(wait_us) -> wait_max_us;
max_key(version_us) -> version_max_us;
max_key(resolve_us) -> resolve_max_us;
max_key(push_us) -> push_max_us;
max_key(report_us) -> report_max_us;
max_key(read_us) -> read_max_us;
max_key(_Count) -> none.

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
