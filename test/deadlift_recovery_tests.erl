-module(deadlift_recovery_tests).

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
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_r, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), Opts),
    {ok, #{count := 0}} = await_read(Ids, 50),
    Ids.

%% The stop of deadlift empties the registry, so that the next test can use
%% the identifiers of the LogServers again.
cleanup(_Ids) ->
    ok = application:stop(deadlift),
    ok = application:stop(peace).

%% Reads until the cluster has a leader.
await_read(_Ids, 0) ->
    {error, no_leader};
await_read(Ids, N) ->
    case deadlift_cstate:read(Ids, 1000) of
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
        {ok, LSN} = deadlift_log:push(
            deadlift_log:ref(Log),
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
    {ok, Result} = deadlift_recovery:recover(options(Ids)),
    #{epoch := 1, log_system := LogSystem, recovery_version := 0, previous_epoch_end_version := 0} = Result,
    #{epoch := 1, logs := Logs, replication := 2} = LogSystem,
    ?assertEqual(3, length(Logs)),
    [?assertMatch(#{epoch := 1, dv := 0, kcv := 0, stopped_by := undefined}, deadlift_log:info(deadlift_log:ref(L))) || L <- Logs],
    ?assertMatch({ok, #{count := 1, current := LogSystem, old := []}}, deadlift_cstate:read(Ids)).

%% Epoch 1 has acknowledged the LSNs 1 and 2. LSN 3 is on every LogServer
%% and is not acknowledged. LSN 4 is on two LogServers. The recovery keeps
%% the LSNs up to 3, and copies the LSNs 2 and 3.
second_recovery(Ids) ->
    {ok, #{log_system := Old = #{logs := [L1, L2, L3]}}} = deadlift_recovery:recover(options(Ids)),
    ok = push(1, [L1, L2, L3], [L1, L2], 0, 1, 0),
    ok = push(1, [L1, L2, L3], [L2, L3], 1, 2, 1),
    ok = push(1, [L1, L2, L3], [L1, L3], 2, 3, 1),
    ok = push(1, [L1, L2], [L1, L2], 3, 4, 1),
    {ok, Result} = deadlift_recovery:recover(options(Ids)),
    #{epoch := 2, log_system := New, recovery_version := 3, previous_epoch_end_version := 1} = Result,
    #{epoch := 2, logs := NewLogs} = New,
    [
        begin
            ?assertMatch(#{epoch := 2, dv := 3, kcv := 1, stopped_by := undefined}, deadlift_log:info(deadlift_log:ref(L))),
            ?assertEqual({ok, [{2, 0, {body, {1, 2}}}, {3, 2, {body, {1, 3}}}]}, deadlift_log:peek(deadlift_log:ref(L), 0, infinity))
        end
     || L <- NewLogs
    ],
    [?assertMatch(#{stopped_by := 2}, deadlift_log:info(deadlift_log:ref(L))) || L <- [L1, L2, L3]],
    ?assertEqual({error, stopped}, deadlift_log:push(deadlift_log:ref(L3), 1, 3, 4, 1, empty)),
    ?assertEqual(
        {ok, #{count => 2, owner => maps:get(owner, element(2, deadlift_cstate:read(Ids))), current => New, old => [Old#{end_version => 3}]}},
        deadlift_cstate:read(Ids)
    ),
    %% The new epoch accepts the LSN after the Recovery Version.
    ok = push(2, NewLogs, tl(NewLogs), 3, 4, 1).

%% With 3 LogServers and a replication degree of 2, the recovery needs 2
%% answers. The LogServer that is down has the body of LSN 1. The other
%% LogServer with the body answers.
one_log_down(Ids) ->
    {ok, #{log_system := #{logs := [L1, L2, L3]}}} = deadlift_recovery:recover(options(Ids)),
    ok = push(1, [L1, L2, L3], [L1, L2], 0, 1, 0),
    ok = push(1, [L1, L2, L3], [L1, L3], 1, 2, 1),
    ok = deadlift_log:stop(deadlift_log:ref(L1)),
    {ok, Result} = deadlift_recovery:recover(options(Ids)),
    #{epoch := 2, log_system := #{logs := [N1 | _]}, recovery_version := 2, previous_epoch_end_version := 1} = Result,
    ?assertEqual({ok, [{2, 0, {body, {1, 2}}}]}, deadlift_log:peek(deadlift_log:ref(N1), 0, infinity)).

%% One answer is not enough. The failed recovery has raised the recovery
%% count and has stopped the LogServer that answered.
two_logs_down(Ids) ->
    {ok, #{log_system := Old = #{logs := [L1, L2, L3]}}} = deadlift_recovery:recover(options(Ids)),
    ok = deadlift_log:stop(deadlift_log:ref(L1)),
    ok = deadlift_log:stop(deadlift_log:ref(L2)),
    ?assertEqual({error, {too_few_replies, 1, 2}}, deadlift_recovery:recover(options(Ids))),
    ?assertMatch(#{stopped_by := 2}, deadlift_log:info(deadlift_log:ref(L3))),
    ?assertMatch({ok, #{count := 2, current := Old, old := []}}, deadlift_cstate:read(Ids)).

%% Sequencer A locks the coordinated state and the old LogServers. Before
%% A writes the coordinated state, Sequencer B performs a whole recovery.
%% A does not write, and stops the LogServers that it recruited.
two_recoveries_at_once(Ids) ->
    {ok, #{log_system := #{logs := OldLogs}}} = deadlift_recovery:recover(options(Ids)),
    ok = push(1, OldLogs, tl(OldLogs), 0, 1, 0),
    Self = self(),
    Recruit = fun(LogOptions) ->
        {ok, Log} = deadlift_log:start(LogOptions),
        Self ! {recruited, Log},
        case get(b_done) of
            undefined ->
                put(b_done, true),
                {ok, B} = deadlift_recovery:recover(options(Ids)),
                Self ! {b, B};
            true ->
                ok
        end,
        {ok, Log}
    end,
    ?assertEqual({error, {count, 3}}, deadlift_recovery:recover((options(Ids))#{recruit => Recruit})),
    erase(b_done),
    B =
        receive
            {b, Result} -> Result
        after 0 -> error(no_recovery_b)
        end,
    #{epoch := 3, log_system := New, recovery_version := 1, previous_epoch_end_version := 0} = B,
    ?assertMatch({ok, #{count := 3, current := New}}, deadlift_cstate:read(Ids)),
    [?assertMatch(#{stopped_by := 3}, deadlift_log:info(deadlift_log:ref(L))) || L <- OldLogs],
    Recruited = (fun Collect() ->
        receive
            {recruited, Log} -> [Log | Collect()]
        after 0 -> []
        end
    end)(),
    ?assertEqual(3, length(Recruited)),
    [?assertNot(is_process_alive(L)) || L <- Recruited].

%% Two Sequencers, one after the other. The second Sequencer assigns the
%% LSNs above the Recovery Version.
sequencer(Ids) ->
    {ok, S1} = deadlift_sequencer:start_link(options(Ids)),
    ?assertEqual({ok, 1, 0, 1}, deadlift_sequencer:commit_version(S1)),
    ?assertEqual({ok, 1, 1, 2}, deadlift_sequencer:commit_version(S1)),
    {ok, #{epoch := 1, log_system := #{logs := Logs}}} = deadlift_sequencer:recovered(S1),
    ok = push(1, Logs, tl(Logs), 0, 1, 0),
    {ok, S2} = deadlift_sequencer:start_link(options(Ids)),
    ?assertEqual({ok, 2, 1, 2}, deadlift_sequencer:commit_version(S2)),
    ?assertMatch({ok, #{epoch := 2, recovery_version := 1}}, deadlift_sequencer:recovered(S2)),
    %% The Sequencer of the ended epoch still assigns versions. The
    %% LogServers of the ended epoch store no record.
    ?assertEqual({ok, 1, 2, 3}, deadlift_sequencer:commit_version(S1)),
    ?assertEqual({error, stopped}, deadlift_log:push(deadlift_log:ref(hd(Logs)), 1, 1, 2, 0, empty)),
    %% A Sequencer whose recovery fails stops.
    process_flag(trap_exit, true),
    {ok, S3} = deadlift_sequencer:start_link((options(Ids))#{recruit => fun(_) -> {error, none} end}),
    receive
        {'EXIT', S3, Reason} -> ?assertEqual({shutdown, {recovery, {recruit, none}}}, Reason)
    after 5000 -> error(sequencer_did_not_stop)
    end,
    process_flag(trap_exit, false),
    ok = deadlift_sequencer:stop(S1),
    ok = deadlift_sequencer:stop(S2).
