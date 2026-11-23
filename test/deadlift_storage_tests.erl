-module(deadlift_storage_tests).

-include_lib("eunit/include/eunit.hrl").

%% Each test runs on a new peace cluster of three servers on one node, with
%% 3 LogServers and a replication degree of 2.
storage_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Ids) -> {timeout, 30, fun() -> reads(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> read_waits(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> batch(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> window(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> clears(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> key_order(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> epochs(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> rollback(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> starts_late(Ids) end} end
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_s, Tag, "_", N])), node()} || N <- [1, 2, 3]],
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

%% Starts a Sequencer and a Proxy for a new epoch. Each commit version is
%% the previous one plus 1.
epoch(Ids) ->
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Sequencer} = deadlift_sequencer:start(Options),
    {ok, Proxy} = deadlift_proxy:start(#{sequencer => Sequencer}),
    {ok, #{log_system := #{logs := Logs}}} = deadlift_sequencer:recovered(Sequencer),
    {Proxy, Logs}.

storage(Ids) ->
    {ok, Storage} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5}),
    Storage.

%% Waits until the StorageServer has the version `Version'.
await_version(Storage, Version) ->
    await_version(Storage, Version, 500).

await_version(Storage, Version, 0) ->
    error({version, Version, deadlift_storage:info(Storage)});
await_version(Storage, Version, N) ->
    case deadlift_storage:info(Storage) of
        #{version := Version} ->
            ok;
        _ ->
            timer:sleep(10),
            await_version(Storage, Version, N - 1)
    end.

%% A read returns the last write at or below the read version.
reads(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = deadlift_proxy:commit(Proxy, #{writes => [{x, 1}, {y, 1}]}),
    {ok, 3} = deadlift_proxy:commit(Proxy, #{writes => [{x, 2}]}),
    %% A record without a transaction, and a rejected transaction.
    {ok, 4} = deadlift_proxy:commit(Proxy, blind),
    {error, not_committed} = deadlift_proxy:commit(Proxy, #{read_version => 2, reads => [x], writes => [{x, 3}]}),
    %% The rejected transaction has the LSN 5. The client receives the
    %% answer before the LogServers store the record.
    {ok, ReadVersion} = deadlift_proxy:read_version(Proxy),
    ?assert(lists:member(ReadVersion, [4, 5]), ReadVersion),
    ?assertEqual({ok, 2}, deadlift_storage:read(Storage, x, ReadVersion)),
    ?assertEqual({ok, 2, 3}, deadlift_storage:read_with_version(Storage, x, ReadVersion, 5000)),
    ?assertEqual({ok, 1}, deadlift_storage:read(Storage, y, ReadVersion)),
    ?assertEqual(not_found, deadlift_storage:read(Storage, z, ReadVersion)),
    %% Reads of ranges. The range has its first key and does not have its
    %% last key.
    ?assertEqual({ok, [{x, 2}, {y, 1}]}, deadlift_storage:read_range(Storage, a, z, ReadVersion)),
    ?assertEqual({ok, [{x, 2, 3}]}, deadlift_storage:read_range_with_version(Storage, x, y, ReadVersion, 5000)),
    ?assertEqual({ok, [{x, 1}, {y, 1}]}, deadlift_storage:read_range(Storage, x, z, 2)),
    ?assertEqual({ok, []}, deadlift_storage:read_range(Storage, a, x, ReadVersion)),
    ?assertEqual({ok, []}, deadlift_storage:read_range(Storage, a, z, 1)),
    %% Reads at earlier versions.
    ?assertEqual({ok, 1}, deadlift_storage:read(Storage, x, 2)),
    ?assertEqual(not_found, deadlift_storage:read(Storage, x, 1)),
    ok = await_version(Storage, 5),
    ?assertMatch(#{version := 5, epoch := 1, rollbacks := 0}, deadlift_storage:info(Storage)).

%% Clears of a key and of a key range. A read at a version before the clear
%% returns the value.
clears(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    {ok, Storage} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5, mvcc_window => 4}),
    {ok, 2} = deadlift_proxy:commit(Proxy, #{writes => [{a, 1}, {b, 1}, {c, 1}, {d, 1}]}),
    {ok, 3} = deadlift_proxy:commit(Proxy, #{mutations => [{clear, a}]}),
    %% The range has `b' and `c', and does not have `d'.
    {ok, 4} = deadlift_proxy:commit(Proxy, #{mutations => [{clear_range, b, d}, {clear, z}]}),
    %% The mutations of a transaction are applied in their order: `c' has
    %% the value 5, and `d' has no value.
    {ok, 5} = deadlift_proxy:commit(Proxy, #{mutations => [{clear_range, a, z}, {set, c, 5}, {set, d, 5}, {clear, d}]}),
    ?assertEqual({ok, [{a, 1}, {b, 1}, {c, 1}, {d, 1}]}, deadlift_storage:read_range(Storage, a, z, 2)),
    ?assertEqual({ok, [{b, 1}, {c, 1}, {d, 1}]}, deadlift_storage:read_range(Storage, a, z, 3)),
    ?assertEqual(not_found, deadlift_storage:read(Storage, a, 3)),
    ?assertEqual({ok, [{d, 1}]}, deadlift_storage:read_range(Storage, a, z, 4)),
    ?assertEqual({ok, [{c, 5}]}, deadlift_storage:read_range(Storage, a, z, 5)),
    ?assertEqual({ok, 5, 5}, deadlift_storage:read_with_version(Storage, c, 5, 5000)),
    %% A transaction that read `b' at the version 3 has a conflict with the
    %% clear of the range at the version 4.
    ?assertEqual({error, not_committed}, deadlift_proxy:commit(Proxy, #{read_version => 3, reads => [b], writes => [{e, 1}]})),
    ?assertEqual({error, not_committed}, deadlift_proxy:commit(Proxy, #{read_version => 2, reads => [a], writes => [{e, 1}]})),
    %% The MVCC window removes a key that has no value at the oldest
    %% version.
    [{ok, _} = deadlift_proxy:commit(Proxy, #{writes => [{c, N}]}) || N <- lists:seq(8, 14)],
    ok = await_version(Storage, 14),
    ?assertMatch(#{oldest := 10}, deadlift_storage:info(Storage)),
    Data = all_data(Storage),
    ?assertEqual([c], maps:keys(Data)).

%% The order of the keys is the order of Erlang terms: a number is below an
%% atom, an atom is below a tuple, and a tuple is below a binary. A read of
%% a range returns the keys in this order.
key_order(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    Storage = storage(Ids),
    Keys = [<<"b">>, {t, 2}, 7, b, <<"a">>, {t, 1}, a, 3.5, <<"a", 0>>],
    {ok, 2} = deadlift_proxy:commit(Proxy, #{writes => [{Key, Key} || Key <- Keys]}),
    Sorted = lists:sort(Keys),
    ?assertEqual([3.5, 7, a, b, {t, 1}, {t, 2}, <<"a">>, <<"a", 0>>, <<"b">>], Sorted),
    ?assertEqual({ok, [{Key, Key} || Key <- Sorted]}, deadlift_storage:read_range(Storage, 0, <<"z">>, 2)),
    ?assertEqual({ok, [{b, b}, {{t, 1}, {t, 1}}]}, deadlift_storage:read_range(Storage, b, {t, 2}, 2)),
    %% The range from the binary `a' to the binary `b' has `a' and the
    %% binary `a' with a zero byte.
    ?assertEqual({ok, [{<<"a">>, <<"a">>}, {<<"a", 0>>, <<"a", 0>>}]}, deadlift_storage:read_range(Storage, <<"a">>, <<"b">>, 2)),
    {ok, 3} = deadlift_proxy:commit(Proxy, #{mutations => [{clear_range, a, {t, 2}}]}),
    ?assertEqual({ok, [{3.5, 3.5}, {7, 7}, {{t, 2}, {t, 2}}]}, deadlift_storage:read_range(Storage, 0, <<>>, 3)).

%% A StorageServer with an MVCC window of 10 versions. The commit `N' has
%% the LSN `N + 1' and writes `{x, N}'. The first commit also writes `y'.
window(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    {ok, Storage} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5, mvcc_window => 10}),
    {ok, 2} = deadlift_proxy:commit(Proxy, #{writes => [{x, 1}, {y, 1}]}),
    [{ok, _} = deadlift_proxy:commit(Proxy, #{writes => [{x, N}]}) || N <- lists:seq(2, 30)],
    ok = await_version(Storage, 31),
    %% The LogServers have the Known Committed Version 30 with the record
    %% 31. The oldest version is 31 - 10.
    ?assertMatch(#{version := 31, oldest := 21}, deadlift_storage:info(Storage)),
    ?assertEqual({ok, 30}, deadlift_storage:read(Storage, x, 31)),
    ?assertEqual({ok, 20, 21}, deadlift_storage:read_with_version(Storage, x, 21, 5000)),
    ?assertEqual({error, transaction_too_old}, deadlift_storage:read(Storage, x, 20)),
    ?assertEqual({error, transaction_too_old}, deadlift_storage:read_range(Storage, a, z, 20)),
    %% The StorageServer keeps the last version at or below the oldest
    %% version: the write of `y' at the LSN 2.
    ?assertEqual({ok, 1, 2}, deadlift_storage:read_with_version(Storage, y, 31, 5000)),
    ?assertEqual({ok, [{x, 22}, {y, 1}]}, deadlift_storage:read_range(Storage, a, z, 23)),
    %% The StorageServer has 11 versions of `x': the versions 22 to 31 and
    %% the version 21.
    #{data := #{x := Versions, y := [{2, {set, 1}}]}} = sys_state(Storage),
    ?assertEqual(lists:seq(31, 21, -1), [Version || {Version, _Value} <- Versions]).

%% The versions that the StorageServer keeps for each key.
sys_state(Storage) ->
    #{data => all_data(Storage)}.

%% The rows of the ETS table of the data, as a map.
all_data(Storage) ->
    #{data := Table} = deadlift_storage:info(Storage),
    maps:from_list(ets:tab2list(Table)).

%% Two transactions of one batch write the key `x'. A read returns the
%% write of the transaction with the higher index.
batch(Ids) ->
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Sequencer} = deadlift_sequencer:start(Options),
    {ok, Proxy} = deadlift_proxy:start(#{sequencer => Sequencer, batch_ms => 300, batch_idle => false}),
    Storage = storage(Ids),
    Self = self(),
    Clients = [
        spawn_link(fun() -> Self ! {self(), deadlift_proxy:commit_with_index(Proxy, #{writes => [{x, N}, {N, N}]}, 5000)} end)
     || N <- [1, 2, 3]
    ],
    Answers = [
        receive
            {Client, Answer} -> Answer
        after 5000 -> error(no_answer)
        end
     || Client <- Clients
    ],
    ?assertEqual([{ok, 2, 0}, {ok, 2, 1}, {ok, 2, 2}], lists:sort(Answers)),
    [Last] = [N || {N, {ok, 2, 2}} <- lists:zip([1, 2, 3], Answers)],
    ?assertEqual({ok, Last, 2}, deadlift_storage:read_with_version(Storage, x, 2, 5000)),
    ?assertEqual({ok, [{1, 1}, {2, 2}, {3, 3}]}, deadlift_storage:read_range(Storage, 1, 4, 2)),
    ?assertEqual(not_found, deadlift_storage:read(Storage, x, 1)).

%% A read at a version that the StorageServer does not have waits for the
%% version.
read_waits(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    Storage = storage(Ids),
    ok = await_version(Storage, 1),
    ?assertMatch({error, {timeout, _}}, deadlift_storage:read(Storage, x, 2, 100)),
    Self = self(),
    Reader = spawn_link(fun() -> Self ! {self(), deadlift_storage:read(Storage, x, 2)} end),
    {ok, 2} = deadlift_proxy:commit(Proxy, #{writes => [{x, 1}]}),
    receive
        {Reader, Result} -> ?assertEqual({ok, 1}, Result)
    after 5000 -> error(no_answer)
    end.

%% The StorageServer follows the records from one epoch to the next.
epochs(Ids) ->
    {P1, _} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = deadlift_proxy:commit(P1, #{writes => [{x, 1}]}),
    {P2, _} = epoch(Ids),
    %% The recovery transaction of epoch 2 has the LSN 1002.
    {ok, 1002} = deadlift_proxy:read_version(P2),
    ?assertEqual({ok, 1}, deadlift_storage:read(Storage, x, 1002)),
    {ok, 1003} = deadlift_proxy:commit(P2, #{read_version => 1002, reads => [x], writes => [{x, 2}]}),
    {P3, _} = epoch(Ids),
    {ok, 2004} = deadlift_proxy:commit(P3, #{writes => [{y, 3}]}),
    ?assertEqual({ok, 2}, deadlift_storage:read(Storage, x, 2004)),
    ?assertEqual({ok, 3}, deadlift_storage:read(Storage, y, 2004)),
    ?assertMatch(#{version := 2004, epoch := 3, rollbacks := 0}, deadlift_storage:info(Storage)).

%% The record 3 of epoch 1 is on 2 of the 3 LogServers. The StorageServer
%% applies it. The recovery discards it, because the Recovery Version is 2.
%% The StorageServer removes the write.
rollback(Ids) ->
    {P1, [L1, L2, _L3]} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = deadlift_proxy:commit(P1, #{writes => [{x, 1}]}),
    [{ok, 3} = deadlift_log:push(deadlift_log:ref(L), 1, 2, 3, 2, {body, [{x, discarded}]}) || L <- [L1, L2]],
    ok = await_version(Storage, 3),
    {P2, _} = epoch(Ids),
    %% The Recovery Version is 2. The recovery transaction of epoch 2 has
    %% the LSN 1002.
    {ok, 1002} = deadlift_proxy:read_version(P2),
    ?assertEqual({ok, 1, 2}, deadlift_storage:read_with_version(Storage, x, 1002, 5000)),
    {ok, 1003} = deadlift_proxy:commit(P2, #{writes => [{y, 2}]}),
    ?assertEqual({ok, 1}, deadlift_storage:read(Storage, x, 1003)),
    ?assertMatch(#{version := 1003, epoch := 2, rollbacks := 1}, deadlift_storage:info(Storage)).

%% A StorageServer that starts in the third epoch reads the records of the
%% ended epochs from their LogServers.
starts_late(Ids) ->
    {P1, _} = epoch(Ids),
    {ok, 2} = deadlift_proxy:commit(P1, #{writes => [{x, 1}]}),
    {ok, 3} = deadlift_proxy:commit(P1, #{writes => [{y, 1}]}),
    {P2, _} = epoch(Ids),
    {ok, 1004} = deadlift_proxy:commit(P2, #{writes => [{x, 2}]}),
    {P3, _} = epoch(Ids),
    {ok, 2005} = deadlift_proxy:commit(P3, #{writes => [{z, 3}]}),
    Storage = storage(Ids),
    ?assertEqual({ok, 2}, deadlift_storage:read(Storage, x, 2005)),
    ?assertEqual({ok, 1}, deadlift_storage:read(Storage, y, 2005)),
    ?assertEqual({ok, 3}, deadlift_storage:read(Storage, z, 2005)),
    ?assertEqual({ok, 1}, deadlift_storage:read(Storage, x, 3)).
