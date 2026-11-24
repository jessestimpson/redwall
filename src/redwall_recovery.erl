%% @doc The recovery of the transaction system.
%%
%% A new Sequencer performs the recovery before it assigns a version. See
%% section 2.4.4 of Zhou et al., "FoundationDB: A Distributed Unbundled
%% Transactional Key Value Store", SIGMOD 2021, and
%% `design/recovery-internals.md' in the FoundationDB repository.
%% `formal/Recovery.tla' models the recovery with the actions `LockCState',
%% `LockLog', `Recruit' and `WriteCState'.
%%
%% == Steps ==
%%
%% <ol>
%% <li>Read the coordinated state and lock it. The lock raises the recovery
%%   count by 1. The new count is the epoch of the Sequencer.</li>
%% <li>Lock every LogServer of the previous epoch. A locked LogServer stores
%%   no more records, and answers with its Durable Version (DV) and its
%%   Known Committed Version (KCV). The recovery needs more than `m - k'
%%   answers, where `m' is the number of LogServers of the previous epoch
%%   and `k' is the replication degree.</li>
%% <li>Compute the previous epoch's end version (PEV) as the maximum of the
%%   KCVs, and the Recovery Version (RV) as the minimum of the DVs.</li>
%% <li>Read the records with `PEV &lt; LSN =&lt; RV' from the LogServers that
%%   answered.</li>
%% <li>Recruit the LogServers of the new epoch. Each new LogServer starts
%%   with the DV `RV', the KCV `PEV' and the records of step 4.</li>
%% <li>Write the new log system to the coordinated state. The write succeeds
%%   only if the recovery count is still the epoch of the Sequencer.</li>
%% </ol>
%%
%% The first recovery finds no log system in the coordinated state. It skips
%% steps 2 to 4, and uses 0 for the PEV and for the RV.
%%
%% == Failure ==
%%
%% A recovery that fails after step 1 has raised the recovery count. The
%% next recovery uses a higher epoch. A recovery that fails after step 2 has
%% stopped LogServers of the previous epoch. The previous epoch then
%% acknowledges no more commits. Both effects are safe: the model allows a
%% Sequencer to stop at any step.
%%
%% A recovery that fails after step 5 stops the LogServers that it
%% recruited.
%%
%% == Reset ==
%%
%% A LogServer of redwall keeps its records in memory. When `replication'
%% LogServers of the previous epoch do not run, a record of that epoch can
%% be on no LogServer, and a recovery cannot end the epoch: fewer
%% LogServers than it needs answer the lock. After the stop of every node
%% of a cluster, no LogServer of the previous epoch runs.
%%
%% The recovery then resets the database, if the LogServers of the
%% previous epoch keep their records in memory. The log system of the
%% coordinated state has this in the key `durability', from
%% `redwall_log:durability/0'. redwall has LogServers in memory only, so a
%% reset is the default. A LogServer with a disk would have its records
%% when it starts again, and the recovery would wait for it. The option
%% `reset_after_log_loss' replaces the default: with `false' the recovery
%% does not reset.
%%
%% <ol>
%% <li>The recovery asks the node of each LogServer of the previous epoch
%%   whether the process runs (`redwall_log:lost/2'). A LogServer counts
%%   only if its node answers that it does not run. A node that does not
%%   answer can be cut off, and its LogServer can run.</li>
%% <li>If at least `replication' LogServers are lost in this way, the
%%   recovery writes a reset to the coordinated state
%%   (`redwall_cstate:reset/4'): the coordinated state then has no log
%%   system, and a generation that is 1 higher.</li>
%% <li>The recovery continues as the first recovery of a database. The
%%   versions start again.</li>
%% <li>A StorageServer that reads the higher generation removes its data
%%   and reads the log of the new epoch.</li>
%% </ol>
%%
%% The data of the database is lost by a reset, also the data of the
%% records that a LogServer still has, and the data of a StorageServer
%% that runs. A reset is reported in three places: the recovery writes a
%% warning to the log, the ClusterController sends the event `{reset,
%% Epoch}' to its `notify' function, and `redwall_status:status/1' has the
%% number of resets in the key `generation'. Without a reset, the recovery
%% fails with `too_few_replies', and the cluster has no epoch.
%% FoundationDB has no reset. Its LogServers write to disk, and a cluster
%% that has lost them needs an operator. This part is not in the paper.
%%
%% The Resolver rejects a transaction with a read version at or above its
%% commit version, so a transaction of the time before a reset does not
%% commit after it.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The recovery copies each record of step 4 to every new LogServer.
%%   FoundationDB copies a record to the new LogServers that replicate the
%%   tags of the record.</li>
%% <li>The recovery waits for the answer of every old LogServer until
%%   `lock_timeout_ms' has passed. This part is not in the paper.</li>
%% <li>The recovery has no recovery transaction and no transaction state
%%   store.</li>
%% </ul>
-module(redwall_recovery).

-include("redwall_eta.hrl").

-export([recover/1]).

-export_type([options/0, result/0]).

-type version() :: redwall_cstate:version().

%% `logs' is the number of LogServers of the new epoch. `recruit' starts
%% one LogServer. The default is `redwall_log:start/1'.
%%
%% `recruit' receives the options of the LogServer, with the identifier of
%% the LogServer in `id'. The LogServer must register with that
%% identifier.
-type options() :: #{
    coordinators := redwall_cstate:servers(),
    logs := pos_integer(),
    replication := pos_integer(),
    recruit => fun((redwall_log:options()) -> {ok, term()} | {error, term()}),
    lock_timeout_ms => non_neg_integer(),
    %% The options of the Sequencer. See `redwall_sequencer'.
    recruit_resolver => fun((redwall_resolver:options()) -> {ok, term()} | {error, term()}),
    versions_per_second => non_neg_integer(),
    max_versions_in_flight => pos_integer(),
    epoch_gap => pos_integer(),
    max_read_life => pos_integer(),
    mvcc_window => pos_integer() | infinity,
    cstate_timeout_ms => timeout()
}.

-type result() :: #{
    epoch := redwall_cstate:epoch(),
    log_system := redwall_cstate:log_system(),
    recovery_version := version(),
    previous_epoch_end_version := version(),
    %% Whether the recovery has reset the database. See "Reset".
    reset := boolean(),
    %% Added by the Sequencer.
    recovery_transaction_version => version()
}.

-define(LOCK_TIMEOUT_MS, 1000).
-define(CSTATE_TIMEOUT_MS, 5000).

%% @doc Performs a recovery. The result gives the epoch and the log system
%% of the caller. The caller assigns the LSNs above `recovery_version'.
%%
%% The errors are:
%%
%% <ul>
%% <li>`{error, {count, Count}}': another Sequencer locked the coordinated
%%   state during this recovery.</li>
%% <li>`{error, {locked, Epoch}}': the Sequencer of a higher epoch locked a
%%   LogServer of the previous epoch.</li>
%% <li>`{error, {too_few_replies, Replies, Needed}}': fewer old LogServers
%%   answered than the recovery needs.</li>
%% <li>`{error, {copy_failed, Logs}}': the LogServers `Logs' answered the
%%   lock and did not answer the read of their records, and a body is
%%   missing.</li>
%% <li>`{error, {missing_body, LSN}}': the recovery read every LogServer that
%%   answered the lock, and none of them has the body of a record that the
%%   recovery must copy.</li>
%% <li>`{error, {recruit, Reason}}': a new LogServer did not start.</li>
%% <li>`{error, {write_unknown, Reason}}': the write of the new log system
%%   to the coordinated state had no answer. The coordinated state has the
%%   new log system or does not have it. The recovery does not stop the
%%   new LogServers in this case.</li>
%% <li>`{error, Reason}': an operation on the coordinated state failed.</li>
%% </ul>
-spec recover(options()) -> {ok, result()} | {error, term()}.
recover(Options = #{coordinators := Coordinators, logs := M, replication := K}) when
    is_integer(M), is_integer(K), K >= 1, K =< M
->
    Timeout = maps:get(cstate_timeout_ms, Options, ?CSTATE_TIMEOUT_MS),
    Owner = make_ref(),
    maybe
        {ok, #{count := Count}} ?= redwall_cstate:read(Coordinators, Timeout),
        {ok, Epoch, CState} ?= redwall_cstate:lock(Coordinators, Count, Owner, Timeout),
        {ok, Ended} ?= end_previous_epoch(Epoch, CState, Options#{owner => Owner}),
        #{pev := PEV, rv := RV, records := Records, end_version := EndVersion} = Ended,
        {ok, Logs} ?= recruit(M, #{epoch => Epoch, start => RV, kcv => PEV, records => Records}, Options),
        LogSystem = #{epoch => Epoch, logs => Logs, replication => K, durability => redwall_log:durability()},
        case redwall_cstate:write(Coordinators, Epoch, Owner, LogSystem, EndVersion, Timeout) of
            ok ->
                {ok, #{
                    epoch => Epoch,
                    log_system => LogSystem,
                    recovery_version => RV,
                    previous_epoch_end_version => PEV,
                    %% Whether the recovery has reset the database.
                    reset => maps:get(reset, Ended, false)
                }};
            {error, Reason} when Reason =:= badarg; element(1, Reason) =:= count ->
                %% The coordinated state refused the write. No log system
                %% names the new LogServers.
                stop_logs(Logs),
                {error, Reason};
            {error, Reason} ->
                %% The write has no answer, so the coordinated state can
                %% have the new log system. The new LogServers continue:
                %% if the coordinated state names them, the next recovery
                %% needs them.
                {error, {write_unknown, Reason}}
        end
    end.

%% Steps 2 to 4.
end_previous_epoch(_Epoch, #{current := undefined}, _Options) ->
    {ok, #{pev => 0, rv => 0, records => [], end_version => undefined}};
end_previous_epoch(Epoch, #{current := Previous = #{logs := OldLogs, replication := OldK}}, Options) ->
    Needed = length(OldLogs) - OldK + 1,
    maybe
        LockTimeoutMs = maps:get(lock_timeout_ms, Options, ?LOCK_TIMEOUT_MS),
        {ok, Replies} ?= lock_logs(OldLogs, Epoch, LockTimeoutMs),
        true ?= length(Replies) >= Needed orelse {error, {too_few_replies, length(Replies), Needed}},
        PEV = lists:max([KCV || {_Log, #{kcv := KCV}} <- Replies]),
        RV = lists:min([DV || {_Log, #{dv := DV}} <- Replies]),
        {ok, Records} ?= copy([Log || {Log, _} <- Replies], PEV, RV),
        {ok, #{pev => PEV, rv => RV, records => Records, end_version => RV}}
    else
        {error, {too_few_replies, _Count, Needed}} = Error ->
            reset(Epoch, Previous, Error, Options);
        Other ->
            Other
    end.

%% Resets the database after the loss of the LogServers of the previous
%% epoch `Previous', if those LogServers keep their records in memory.
%% See "Reset" in the module documentation. `Error' is the result without
%% a reset.
reset(Epoch, Previous = #{epoch := Lost, logs := OldLogs, replication := OldK}, Error, Options = #{coordinators := Coordinators}) ->
    Timeout = maps:get(cstate_timeout_ms, Options, ?CSTATE_TIMEOUT_MS),
    %% The records of a LogServer in memory are lost when its process
    %% stops. A LogServer with a disk would have its records when it
    %% starts again, so the recovery would wait for it.
    InMemory = maps:get(durability, Previous, memory) =:= memory,
    case maps:get(reset_after_log_loss, Options, InMemory) andalso length([Log || Log <- OldLogs, redwall_log:lost(Log, Timeout)]) of
        Count when is_integer(Count), Count >= OldK ->
            case redwall_cstate:reset(Coordinators, Epoch, maps:get(owner, Options), Timeout) of
                ok ->
                    logger:warning(
                        "redwall: the database is reset. ~b of ~b LogServers of the epoch ~b do not run, and each record is on ~b of them. "
                        "The LogServers keep their records in memory, so records of the epoch are lost. The epoch ~b starts without data.",
                        [Count, length(OldLogs), Lost, OldK, Epoch]
                    ),
                    {ok, #{pev => 0, rv => 0, records => [], end_version => undefined, reset => true}};
                {error, Reason} ->
                    {error, {reset, Reason}}
            end;
        _NotAllowedOrTooFew ->
            Error
    end.

%% Sends a lock to every old LogServer, and collects the answers until
%% every LogServer has answered or the time has passed. A LogServer that is
%% not alive answers with an error at once.
lock_logs(OldLogs, Epoch, TimeoutMs) ->
    Requests = lists:foldl(
        fun(Log, Acc) ->
            %% A LogServer that does not run gives no answer.
            case redwall_log:ref(Log) of
                undefined -> Acc;
                Pid -> gen_server:send_request(Pid, {lock, Epoch}, Log, Acc)
            end
        end,
        gen_server:reqids_new(),
        OldLogs
    ),
    %% The time limit is a timer message, and not a time-out of
    %% `gen_server:receive_response/3'. A timer that this module starts is on
    %% the virtual clock in a simulation.
    Timer = erlang:start_timer(TimeoutMs, self(), lock_timeout),
    {Result, Others} = collect_locks(Requests, Timer, [], []),
    _ = erlang:cancel_timer(Timer),
    receive
        {timeout, Timer, lock_timeout} -> ok
    after 0 -> ok
    end,
    %% The messages that are not answers to a lock go back to the mailbox,
    %% in the order of their arrival. A request to the Sequencer that
    %% arrives during the recovery is such a message.
    lists:foreach(fun(Message) -> self() ! Message end, lists:reverse(Others)),
    Result.

%% `gen_server:check_response/3' is the only way to recognise an answer, so
%% this function takes every message out of the mailbox.
collect_locks(Requests, Timer, Replies, Others) ->
    case gen_server:reqids_size(Requests) of
        0 ->
            {{ok, Replies}, Others};
        _ ->
            receive
                {timeout, Timer, lock_timeout} ->
                    {{ok, Replies}, Others};
                Message ->
                    case gen_server:check_response(Message, Requests, true) of
                        {{reply, {ok, Reply}}, Log, Requests1} ->
                            collect_locks(Requests1, Timer, [{Log, Reply} | Replies], Others);
                        {{reply, {error, {locked, Higher}}}, _Log, _Requests1} ->
                            {{error, {locked, Higher}}, Others};
                        {_Error, _Log, Requests1} ->
                            collect_locks(Requests1, Timer, Replies, Others);
                        _NoReply ->
                            collect_locks(Requests, Timer, Replies, [Message | Others])
                    end
            end
    end.

%% Reads the records with `PEV < LSN =< RV' from the LogServers that
%% answered the lock. Every one of these LogServers has a DV at or above
%% the RV, so every one of them has every LSN of the range. The body of a
%% record is on the LogServers that replicate the record.
copy(_Logs, PEV, RV) when RV =< PEV ->
    {ok, []};
%%
%% A LogServer can fail between its answer to the lock and the read. If a
%% body is then missing, the error is `copy_failed', and a later recovery
%% can succeed. The error is `missing_body' only if the recovery read every
%% LogServer that answered the lock. `NoLoss' of the model states that this
%% case does not occur.
copy(Logs, PEV, RV) ->
    {Bodies, Failed} = lists:foldl(
        fun(Log, {Acc, Fail}) ->
            try redwall_log:ref(Log) =/= undefined andalso redwall_log:peek(redwall_log:ref(Log), PEV, RV) of
                {ok, Records} ->
                    {lists:foldl(fun merge/2, Acc, Records), Fail};
                false ->
                    {Acc, [Log | Fail]}
            catch
                exit:_ -> {Acc, [Log | Fail]}
            end
        end,
        {#{}, []},
        Logs
    ),
    Sorted = lists:keysort(1, maps:to_list(Bodies)),
    case {[LSN || {LSN, empty} <- Sorted], Failed} of
        {[], _} when Sorted =/= [] -> {ok, Sorted};
        {_, [_ | _]} -> {error, {copy_failed, lists:reverse(Failed)}};
        {[], []} -> {error, {missing_body, RV}};
        {[LSN | _], []} -> {error, {missing_body, LSN}}
    end.

merge({LSN, _Prev, empty}, Acc) ->
    maps:merge(#{LSN => empty}, Acc);
merge({LSN, _Prev, Body}, Acc) ->
    Acc#{LSN => Body}.

%% Step 5.
recruit(M, LogOptions, Options) ->
    Recruit = maps:get(recruit, Options, fun redwall_log:start/1),
    recruit(1, M, LogOptions, Recruit, []).

recruit(Index, M, _LogOptions, _Recruit, Logs) when Index > M ->
    {ok, lists:reverse(Logs)};
recruit(Index, M, LogOptions = #{epoch := Epoch}, Recruit, Logs) ->
    Id = {Epoch, Index},
    case Recruit(LogOptions#{id => Id}) of
        {ok, Started} when is_pid(Started) ->
            %% The entry of the LogServer in the log system has the
            %% process. The coordinated state then has the process of each
            %% LogServer.
            recruit(Index + 1, M, LogOptions, Recruit, [{Id, Started} | Logs]);
        {ok, Other} ->
            stop_logs(Logs),
            {error, {recruit, {not_a_process, Other}}};
        {error, Reason} ->
            stop_logs(Logs),
            {error, {recruit, Reason}}
    end.

stop_logs(Logs) ->
    lists:foreach(
        fun(Log) ->
            try
                redwall_log:ref(Log) =/= undefined andalso redwall_log:stop(redwall_log:ref(Log))
            catch
                exit:_ -> ok
            end
        end,
        Logs
    ).
