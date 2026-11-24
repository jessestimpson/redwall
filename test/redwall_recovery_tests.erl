-module(redwall_recovery_tests).

-include_lib("eunit/include/eunit.hrl").

%% Each test runs on a new peace cluster of three servers on one node.
recovery_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Ids) -> {timeout, 30, fun() -> first_recovery(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> second_recovery(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> one_log_down(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> two_logs_down(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> two_recoveries_at_once(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> sequencer(Ids) end} end
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(redwall),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([redwall_r, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), Opts),
    {ok, #{count := 0}} = await_read(Ids, 50),
    Ids.

%% Stops the applications of the test.
cleanup(_Ids) ->
    ok = application:stop(redwall),
    ok = application:stop(peace).

%% Reads until the cluster has a leader.
await_read(_Ids, 0) ->
    {error, no_leader};
await_read(Ids, N) ->
    case redwall_cstate:read(Ids, 1000) of
        {ok, State} ->
            {ok, State};
        _ ->
            timer:sleep(100),
            await_read(Ids, N - 1)
    end.

options(Ids) ->
    %% `versions_per_second => 0' makes each commit version the previous
    %% one plus 1.
    #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0}.

%% Pushes the record `LSN' of epoch `Epoch' to `Logs'. The LogServers in
%% `Replicas' receive the body.
push(Epoch, Logs, Replicas, Prev, LSN, KCV) ->
    [
        {ok, LSN} = redwall_log:push(
            redwall_log:ref(Log),
            Epoch,
            Prev,
            LSN,
            KCV,
            case lists:member(Log, Replicas) of
                true -> {body, {Epoch, LSN}};
                false -> empty
            end
        )
     || Log <- Logs
    ],
    ok.

first_recovery(Ids) ->
    {ok, Result} = redwall_recovery:recover(options(Ids)),
    #{epoch := 1, log_system := LogSystem, recovery_version := 0, previous_epoch_end_version := 0} = Result,
    #{epoch := 1, logs := Logs, replication := 2} = LogSystem,
    ?assertEqual(3, length(Logs)),
    [?assertMatch(#{epoch := 1, dv := 0, kcv := 0, stopped_by := undefined}, redwall_log:info(redwall_log:ref(L))) || L <- Logs],
    ?assertMatch({ok, #{count := 1, current := LogSystem, old := []}}, redwall_cstate:read(Ids)).

%% Epoch 1 has acknowledged the LSNs 1 and 2. LSN 3 is on every LogServer
%% and is not acknowledged. LSN 4 is on two LogServers. The recovery keeps
%% the LSNs up to 3, and copies the LSNs 2 and 3.
second_recovery(Ids) ->
    {ok, #{log_system := Old = #{logs := [L1, L2, L3]}}} = redwall_recovery:recover(options(Ids)),
    ok = push(1, [L1, L2, L3], [L1, L2], 0, 1, 0),
    ok = push(1, [L1, L2, L3], [L2, L3], 1, 2, 1),
    ok = push(1, [L1, L2, L3], [L1, L3], 2, 3, 1),
    ok = push(1, [L1, L2], [L1, L2], 3, 4, 1),
    {ok, Result} = redwall_recovery:recover(options(Ids)),
    #{epoch := 2, log_system := New, recovery_version := 3, previous_epoch_end_version := 1} = Result,
    #{epoch := 2, logs := NewLogs} = New,
    [
        begin
            ?assertMatch(#{epoch := 2, dv := 3, kcv := 1, stopped_by := undefined}, redwall_log:info(redwall_log:ref(L))),
            ?assertEqual({ok, [{2, 0, {body, {1, 2}}}, {3, 2, {body, {1, 3}}}]}, redwall_log:peek(redwall_log:ref(L), 0, infinity))
        end
     || L <- NewLogs
    ],
    [?assertMatch(#{stopped_by := 2}, redwall_log:info(redwall_log:ref(L))) || L <- [L1, L2, L3]],
    ?assertEqual({error, stopped}, redwall_log:push(redwall_log:ref(L3), 1, 3, 4, 1, empty)),
    ?assertEqual(
        {ok, #{count => 2, owner => maps:get(owner, element(2, redwall_cstate:read(Ids))), current => New, old => [Old#{end_version => 3}], controller => {0, undefined}, storages => #{}, generation => 0}},
        redwall_cstate:read(Ids)
    ),
    %% The new epoch accepts the LSN after the Recovery Version.
    ok = push(2, NewLogs, tl(NewLogs), 3, 4, 1).

%% With 3 LogServers and a replication degree of 2, the recovery needs 2
%% answers. The LogServer that is down has the body of LSN 1. The other
%% LogServer with the body answers.
one_log_down(Ids) ->
    {ok, #{log_system := #{logs := [L1, L2, L3]}}} = redwall_recovery:recover(options(Ids)),
    ok = push(1, [L1, L2, L3], [L1, L2], 0, 1, 0),
    ok = push(1, [L1, L2, L3], [L1, L3], 1, 2, 1),
    ok = redwall_log:stop(redwall_log:ref(L1)),
    {ok, Result} = redwall_recovery:recover(options(Ids)),
    #{epoch := 2, log_system := #{logs := [N1 | _]}, recovery_version := 2, previous_epoch_end_version := 1} = Result,
    ?assertEqual({ok, [{2, 0, {body, {1, 2}}}]}, redwall_log:peek(redwall_log:ref(N1), 0, infinity)).

%% One answer is not enough. The failed recovery has raised the recovery
%% count and has stopped the LogServer that answered.
two_logs_down(Ids) ->
    {ok, #{log_system := Old = #{logs := [L1, L2, L3]}}} = redwall_recovery:recover(options(Ids)),
    ok = redwall_log:stop(redwall_log:ref(L1)),
    ok = redwall_log:stop(redwall_log:ref(L2)),
    ?assertEqual({error, {too_few_replies, 1, 2}}, redwall_recovery:recover(options(Ids))),
    ?assertMatch(#{stopped_by := 2}, redwall_log:info(redwall_log:ref(L3))),
    ?assertMatch({ok, #{count := 2, current := Old, old := []}}, redwall_cstate:read(Ids)).

%% Sequencer A locks the coordinated state and the old LogServers. Before
%% A writes the coordinated state, Sequencer B performs a whole recovery.
%% A does not write, and stops the LogServers that it recruited.
two_recoveries_at_once(Ids) ->
    {ok, #{log_system := #{logs := OldLogs}}} = redwall_recovery:recover(options(Ids)),
    ok = push(1, OldLogs, tl(OldLogs), 0, 1, 0),
    Self = self(),
    Recruit = fun(LogOptions) ->
        {ok, Log} = redwall_log:start(LogOptions),
        Self ! {recruited, Log},
        case get(b_done) of
            undefined ->
                put(b_done, true),
                {ok, B} = redwall_recovery:recover(options(Ids)),
                Self ! {b, B};
            true ->
                ok
        end,
        {ok, Log}
    end,
    ?assertEqual({error, {count, 3}}, redwall_recovery:recover((options(Ids))#{recruit => Recruit})),
    erase(b_done),
    B =
        receive
            {b, Result} -> Result
        after 0 -> error(no_recovery_b)
        end,
    #{epoch := 3, log_system := New, recovery_version := 1, previous_epoch_end_version := 0} = B,
    ?assertMatch({ok, #{count := 3, current := New}}, redwall_cstate:read(Ids)),
    [?assertMatch(#{stopped_by := 3}, redwall_log:info(redwall_log:ref(L))) || L <- OldLogs],
    Recruited = (fun Collect() ->
        receive
            {recruited, Log} -> [Log | Collect()]
        after 0 -> []
        end
    end)(),
    ?assertEqual(3, length(Recruited)),
    [?assertNot(is_process_alive(L)) || L <- Recruited].

%% The processes of two epochs, one after the other. Each process commits
%% the recovery transaction of its epoch, and assigns the LSNs above it.
sequencer(Ids) ->
    {ok, P1} = redwall_epoch:start_link(options(Ids)),
    %% The recovery transaction of epoch 1 has the LSN 1.
    {ok, #{epoch := 1, log_system := #{logs := Logs}, recovery_transaction_version := 1}} = redwall_epoch:recovered(P1),
    [?assertMatch(#{dv := 1, kcv := 0}, redwall_log:info(redwall_log:ref(L))) || L <- Logs],
    ?assertEqual(
        2,
        length([L || L <- Logs, {ok, [{1, 0, {body, recovery_transaction}}]} =:= redwall_log:peek(redwall_log:ref(L), 0, infinity)])
    ),
    ?assertEqual({ok, 1}, redwall_epoch:read_version(P1)),
    ?assertEqual({ok, 2}, redwall_epoch:commit(P1, a)),
    ?assertEqual({ok, 3}, redwall_epoch:commit(P1, b)),
    {ok, P2} = redwall_epoch:start_link(options(Ids)),
    %% The recovery transaction of epoch 2 has the LSN 1003: the Recovery
    %% Version plus the versions in flight.
    ?assertMatch({ok, #{epoch := 2, recovery_version := 3, recovery_transaction_version := 1003}}, redwall_epoch:recovered(P2)),
    ?assertEqual({ok, 1003}, redwall_epoch:read_version(P2)),
    ?assertEqual({ok, 1004}, redwall_epoch:commit(P2, c)),
    %% The LogServers of the ended epoch store no record.
    ?assertEqual({error, stopped}, redwall_log:push(redwall_log:ref(hd(Logs)), 1, 3, 4, 0, empty)),
    %% A process whose recovery fails stops.
    process_flag(trap_exit, true),
    {ok, P3} = redwall_epoch:start_link((options(Ids))#{recruit => fun(_) -> {error, none} end}),
    receive
        {'EXIT', P3, Reason} -> ?assertEqual({shutdown, {recovery, {recruit, none}}}, Reason)
    after 5000 -> error(process_did_not_stop)
    end,
    process_flag(trap_exit, false),
    unlink(P1),
    unlink(P2),
    exit(P1, kill),
    exit(P2, kill).

%% The rules of the Sequencer for the versions.
versions_test() ->
    Options = #{versions_per_second => 0},
    %% The first version of an epoch is 1, or the Recovery Version plus the
    %% versions in flight, or plus `epoch_gap'.
    ?assertEqual(1, redwall_sequencer:first_version(0, Options)),
    ?assertEqual(1007, redwall_sequencer:first_version(7, Options)),
    ?assertEqual(100000007, redwall_sequencer:first_version(7, #{})),
    ?assertEqual(8, redwall_sequencer:first_version(7, #{epoch_gap => 1})),
    %% Without a clock, each version is the previous one plus 1.
    ?assertEqual({ok, 11}, redwall_sequencer:next_version(10, 0, 5000000, 10, Options)),
    %% No version is 1,000 or more above the read version.
    ?assertEqual({ok, 1009}, redwall_sequencer:next_version(1008, 0, 0, 10, Options)),
    ?assertEqual({error, versions_in_flight}, redwall_sequencer:next_version(1009, 0, 0, 10, Options)),
    %% With the clock, a version adds the versions of the time since the
    %% last version: at least 1, and at most the versions of 5 seconds.
    ?assertEqual({ok, 11}, redwall_sequencer:next_version(10, 1000, 1000, 10, #{})),
    ?assertEqual({ok, 20010}, redwall_sequencer:next_version(10, 1000, 21000, 10, #{})),
    ?assertEqual({ok, 5000010}, redwall_sequencer:next_version(10, 0, 60000000, 10, #{})).

%% The coordinated state applies the write of the new log system, and the
%% answer does not arrive. The recovery does not stop the new LogServers,
%% because the coordinated state names them. The next recovery locks them
%% and succeeds.
write_without_answer_test_() ->
    {timeout, 30, fun() ->
        {ok, CState} = redwall_cstate_local:start(#{}),
        Stub = spawn(fun() -> forward(CState) end),
        Options = #{logs => 3, replication => 2, lock_timeout_ms => 200, cstate_timeout_ms => 100},
        try
            ?assertMatch({error, {write_unknown, {timeout, _}}}, redwall_recovery:recover(Options#{coordinators => {local, Stub}})),
            {ok, #{count := 1, current := #{epoch := 1, logs := Logs}}} = redwall_cstate:read({local, CState}),
            ?assertEqual(3, length([Log || Log <- Logs, redwall_log:ref(Log) =/= undefined])),
            ?assertMatch(
                {ok, #{epoch := 2, recovery_version := 0}},
                redwall_recovery:recover(Options#{coordinators => {local, CState}})
            ),
            [?assertMatch(#{stopped_by := 2}, redwall_log:info(redwall_log:ref(Log))) || Log <- Logs],
            %% A refused write stops the new LogServers. The recovery
            %% below locks the count 2, and another recovery raises the
            %% count before its write.
            Started = ets:new(started, [public, bag]),
            Recruit = fun(LogOptions) ->
                {ok, Pid} = redwall_log:start(LogOptions),
                _ =
                    case ets:insert_new(Started, {raised, true}) of
                        true -> {ok, 4, _} = redwall_cstate:lock({local, CState}, 3, other);
                        false -> ok
                    end,
                true = ets:insert(Started, {log, Pid}),
                {ok, Pid}
            end,
            ?assertEqual({error, {count, 4}}, redwall_recovery:recover(Options#{coordinators => {local, CState}, recruit => Recruit})),
            ?assertEqual([false, false, false], [is_process_alive(Pid) || {log, Pid} <- ets:lookup(Started, log)])
        after
            exit(Stub, kill),
            exit(CState, kill)
        end
    end}.

%% Sends each request to the coordinated state `CState' and returns its
%% answer, except for a write of a log system: the coordinated state
%% applies the write, and the caller gets no answer.
forward(CState) ->
    receive
        {'$gen_call', From, Request} ->
            Reply = gen_server:call(CState, Request),
            case Request of
                {propose, {write, _, _, _, _}} -> ok;
                _ -> gen_server:reply(From, Reply)
            end,
            forward(CState)
    end.
