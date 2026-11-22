-module(deadlift_proxy_tests).

-include_lib("eunit/include/eunit.hrl").

%% Each test runs on a new peace cluster of three servers on one node, with
%% 3 LogServers and a replication degree of 2.
proxy_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Ids) -> {timeout, 30, fun() -> commit(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> commits_at_once(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> recovery_keeps_commits(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> log_down(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> sequencer_down(Ids) end} end
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_p, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), Opts),
    {ok, #{count := 0}} = await_read(Ids, 50),
    Ids.

cleanup(_Ids) ->
    ok = application:stop(deadlift),
    ok = application:stop(peace).

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

%% Starts a Sequencer and a Proxy for a new epoch.
epoch(Ids) ->
    {ok, Sequencer} = deadlift_sequencer:start(#{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200}),
    {ok, Proxy} = deadlift_proxy:start(#{sequencer => Sequencer}),
    {ok, #{log_system := #{logs := Logs}}} = deadlift_sequencer:recovered(Sequencer),
    {Sequencer, Proxy, Logs}.

info(Log) ->
    deadlift_log:info(deadlift_log:ref(Log)).

%% The bodies of the records on the LogServer `Log', as `{LSN, Body}'.
bodies(Log) ->
    {ok, Records} = deadlift_log:peek(deadlift_log:ref(Log), 0, infinity),
    [{LSN, Body} || {LSN, _Prev, {body, Body}} <- Records].

%% Returns the reason of the stop of a process. `Monitor' is a monitor that
%% the test started before the stop.
await_down(Monitor) ->
    receive
        {'DOWN', Monitor, process, _Pid, Reason} -> Reason
    after 5000 -> error(still_alive)
    end.

commit(Ids) ->
    {_Sequencer, Proxy, Logs} = epoch(Ids),
    ?assertEqual({ok, 1}, deadlift_proxy:commit(Proxy, a)),
    ?assertEqual({ok, 2}, deadlift_proxy:commit(Proxy, b)),
    ?assertEqual({ok, 3}, deadlift_proxy:commit(Proxy, c)),
    ?assertEqual(#{epoch => 1, kcv => 3, waiting => 0}, deadlift_proxy:info(Proxy)),
    %% Every LogServer has every LSN. The record 3 carried the KCV 2.
    [?assertMatch(#{dv := 3, kcv := 2}, info(L)) || L <- Logs],
    %% Each body is on 2 of the 3 LogServers.
    All = lists:append([bodies(L) || L <- Logs]),
    [?assertEqual(2, length([x || {LSN, B} <- All, LSN =:= N, B =:= Body])) || {N, Body} <- [{1, a}, {2, b}, {3, c}]],
    ?assertEqual(6, length(All)).

%% 50 clients commit at the same time. Each client gets its own LSN.
commits_at_once(Ids) ->
    {_Sequencer, Proxy, Logs} = epoch(Ids),
    Self = self(),
    Clients = [spawn_link(fun() -> Self ! {self(), deadlift_proxy:commit(Proxy, N)} end) || N <- lists:seq(1, 50)],
    LSNs = [
        receive
            {Client, {ok, LSN}} -> LSN
        after 5000 -> error(no_answer)
        end
     || Client <- Clients
    ],
    ?assertEqual(lists:seq(1, 50), lists:sort(LSNs)),
    ?assertMatch(#{kcv := 50, waiting := 0}, deadlift_proxy:info(Proxy)),
    [?assertMatch(#{dv := 50}, info(L)) || L <- Logs].

%% A recovery keeps every record that the Proxy acknowledged. The Proxy of
%% the ended epoch stops at its next commit.
recovery_keeps_commits(Ids) ->
    {_S1, P1, Logs1} = epoch(Ids),
    [{ok, N} = deadlift_proxy:commit(P1, {one, N}) || N <- [1, 2, 3]],
    {S2, P2, Logs2} = epoch(Ids),
    ?assertMatch(
        {ok, #{epoch := 2, recovery_version := 3, previous_epoch_end_version := 2}},
        deadlift_sequencer:recovered(S2)
    ),
    [?assertMatch(#{stopped_by := 2}, info(L)) || L <- Logs1],
    %% The recovery copied the record 3, which is above the PEV.
    [?assertEqual([{3, {one, 3}}], bodies(L)) || L <- Logs2],
    Monitor = erlang:monitor(process, P1),
    ?assertEqual({error, commit_unknown_result}, deadlift_proxy:commit(P1, {one, 4})),
    ?assertEqual({shutdown, {log, 4, stopped}}, await_down(Monitor)),
    ?assertEqual({ok, 4}, deadlift_proxy:commit(P2, {two, 4})),
    ?assertEqual(#{epoch => 2, kcv => 4, waiting => 0}, deadlift_proxy:info(P2)).

%% One LogServer is not alive. The Proxy cannot acknowledge a commit.
log_down(Ids) ->
    {_Sequencer, Proxy, [L1 | _]} = epoch(Ids),
    {ok, 1} = deadlift_proxy:commit(Proxy, a),
    ok = deadlift_log:stop(deadlift_log:ref(L1)),
    Monitor = erlang:monitor(process, Proxy),
    ?assertEqual({error, commit_unknown_result}, deadlift_proxy:commit(Proxy, b)),
    ?assertEqual({shutdown, {log, 2, noproc}}, await_down(Monitor)),
    ?assertEqual({error, commit_unknown_result}, deadlift_proxy:commit(Proxy, c)).

sequencer_down(Ids) ->
    {Sequencer, Proxy, _Logs} = epoch(Ids),
    {ok, 1} = deadlift_proxy:commit(Proxy, a),
    ok = deadlift_sequencer:stop(Sequencer),
    Monitor = erlang:monitor(process, Proxy),
    ?assertEqual({error, commit_unknown_result}, deadlift_proxy:commit(Proxy, b)),
    ?assertMatch({shutdown, {sequencer, _}}, await_down(Monitor)).
