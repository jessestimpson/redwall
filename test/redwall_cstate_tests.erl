-module(redwall_cstate_tests).

-include_lib("eunit/include/eunit.hrl").

-define(META, #{index => 1}).

logs(Epoch) ->
    #{epoch => Epoch, logs => [{log, Epoch, N} || N <- [1, 2, 3]], replication => 2}.

apply_all(Values) ->
    lists:foldl(
        fun(Value, {State, _Reply}) -> redwall_cstate:apply(?META, Value, State) end,
        {redwall_cstate:init([]), undefined},
        Values
    ).

%% The first Sequencer locks the empty coordinated state and writes the log
%% system of epoch 1.
first_epoch_test() ->
    S0 = redwall_cstate:init([]),
    ?assertEqual(#{count => 0, owner => undefined, current => undefined, old => [], controller => {0, undefined}}, S0),
    {S1, {ok, 1, S1}} = redwall_cstate:apply(?META, {lock, 0, a}, S0),
    {S2, ok} = redwall_cstate:apply(?META, {write, 1, a, logs(1), undefined}, S1),
    ?assertEqual(#{count => 1, owner => a, current => logs(1), old => [], controller => {0, undefined}}, S2).

%% A recovery ends the previous epoch at the Recovery Version.
recovery_test() ->
    {S, ok} = apply_all([
        {lock, 0, a},
        {write, 1, a, logs(1), undefined},
        {lock, 1, b},
        {write, 2, b, logs(2), 7}
    ]),
    ?assertEqual(
        #{count => 2, owner => b, current => logs(2), old => [(logs(1))#{end_version => 7}], controller => {0, undefined}},
        S
    ).

%% Two Sequencers read the count 1. The second lock fails.
lock_is_compare_and_set_test() ->
    {S, {ok, 2, _}} = apply_all([{lock, 0, a}, {write, 1, a, logs(1), undefined}, {lock, 1, b}]),
    ?assertEqual({S, {error, {count, 2}}}, redwall_cstate:apply(?META, {lock, 1, c}, S)).

%% Sequencer b locks. Sequencer c locks after it. The write of b fails, and
%% the write of c succeeds. This is the race of the config `RecoveryNoCas'.
write_is_compare_and_set_test() ->
    {S, {ok, 3, _}} = apply_all([
        {lock, 0, a},
        {write, 1, a, logs(1), undefined},
        {lock, 1, b},
        {lock, 2, c}
    ]),
    ?assertEqual({S, {error, {count, 3}}}, redwall_cstate:apply(?META, {write, 2, b, logs(2), 7}, S)),
    {S1, ok} = redwall_cstate:apply(?META, {write, 3, c, logs(3), 5}, S),
    ?assertEqual([(logs(1))#{end_version => 5}], maps:get(old, S1)),
    ?assertEqual(logs(3), maps:get(current, S1)).

%% A Sequencer cannot write an epoch that another Sequencer owns.
write_needs_the_owner_test() ->
    {S, {ok, 2, _}} = apply_all([{lock, 0, a}, {write, 1, a, logs(1), undefined}, {lock, 1, b}]),
    ?assertEqual({S, {error, {count, 2}}}, redwall_cstate:apply(?META, {write, 2, x, logs(2), 7}, S)).

%% A value that is proposed again after a timeout is applied twice. The
%% second application gives the same reply and does not change the state.
values_are_idempotent_test() ->
    {S1, R1 = {ok, 1, _}} = apply_all([{lock, 0, a}]),
    ?assertEqual({S1, R1}, redwall_cstate:apply(?META, {lock, 0, a}, S1)),
    {S2, ok} = apply_all([{lock, 0, a}, {write, 1, a, logs(1), undefined}, {lock, 1, b}, {write, 2, b, logs(2), 7}]),
    ?assertEqual({S2, ok}, redwall_cstate:apply(?META, {write, 2, b, logs(2), 7}, S2)).

%% A write must carry its own epoch, and the end version of a previous epoch
%% if there is one.
bad_writes_test() ->
    {S1, _} = apply_all([{lock, 0, a}]),
    ?assertEqual({S1, {error, badarg}}, redwall_cstate:apply(?META, {write, 1, a, logs(2), undefined}, S1)),
    ?assertEqual({S1, {error, badarg}}, redwall_cstate:apply(?META, {write, 1, a, logs(1), 3}, S1)),
    {S2, _} = apply_all([{lock, 0, a}, {write, 1, a, logs(1), undefined}, {lock, 1, b}]),
    ?assertEqual({S2, {error, badarg}}, redwall_cstate:apply(?META, {write, 2, b, logs(2), undefined}, S2)),
    ?assertEqual({S2, {error, badarg}}, redwall_cstate:apply(?META, unknown, S2)).

%% The machine on a peace cluster of three servers on one node. Two
%% Sequencers recover at the same time, and one of them writes.
cluster_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(peace),
            Tag = erlang:unique_integer([positive]),
            [{list_to_atom(lists:concat([redwall_t, Tag, "_", N])), node()} || N <- [1, 2, 3]]
        end,
        fun(_Ids) -> ok = application:stop(peace) end,
        fun(Ids) ->
            {timeout, 30, fun() ->
                Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
                {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), Opts),
                {ok, #{count := 0}} = await_read(Ids, 50),
                {ok, 1, _} = redwall_cstate:lock(Ids, 0, a),
                ok = redwall_cstate:write(Ids, 1, a, logs(1), undefined),
                %% Sequencers b and c both read the count 1.
                {ok, #{count := 1, current := Current}} = redwall_cstate:read(Ids),
                ?assertEqual(logs(1), Current),
                {ok, 2, _} = redwall_cstate:lock(Ids, 1, b),
                ?assertEqual({error, {count, 2}}, redwall_cstate:lock(Ids, 1, c)),
                %% Sequencer c reads again and locks. Sequencer b then
                %% cannot write.
                {ok, 3, _} = redwall_cstate:lock(Ids, 2, c),
                ?assertEqual({error, {count, 3}}, redwall_cstate:write(Ids, 2, b, logs(2), 7)),
                ok = redwall_cstate:write(Ids, 3, c, logs(3), 5),
                ?assertEqual(
                    {ok, #{count => 3, owner => c, current => logs(3), old => [(logs(1))#{end_version => 5}], controller => {0, undefined}}},
                    redwall_cstate:read(Ids)
                )
            end}
        end}.

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

%% Two candidates read the term 0. The second election fails. An election
%% does not change the recovery count.
elect_is_compare_and_set_test() ->
    A = self(),
    B = spawn(fun() -> ok end),
    S0 = redwall_cstate:init([]),
    {S1, {ok, 1}} = redwall_cstate:apply(?META, {elect, 0, A}, S0),
    ?assertMatch(#{controller := {1, A}, count := 0}, S1),
    ?assertEqual({S1, {error, {term, 1, A}}}, redwall_cstate:apply(?META, {elect, 0, B}, S1)),
    %% The same value, applied a second time.
    ?assertEqual({S1, {ok, 1}}, redwall_cstate:apply(?META, {elect, 0, A}, S1)),
    {S2, {ok, 2}} = redwall_cstate:apply(?META, {elect, 1, B}, S1),
    ?assertMatch(#{controller := {2, B}}, S2).
