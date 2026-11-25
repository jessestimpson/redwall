-module(redwall_storage_tests).

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
        fun(Ids) -> {timeout, 30, fun() -> starts_late(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> many_versions(Ids) end} end,
        fun(Ids) -> {timeout, 60, fun() -> copy(Ids) end} end,
        fun(Ids) -> {timeout, 60, fun() -> pop(Ids) end} end,
        fun(Ids) -> {timeout, 60, fun() -> reset(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> committed_at_epoch_end(Ids) end} end,
        fun(Ids) -> {timeout, 60, fun() -> watches(Ids) end} end,
        fun(Ids) -> {timeout, 60, fun() -> watch_limits(Ids) end} end
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(redwall),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([redwall_s, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), Opts),
    {ok, #{count := 0}} = await_read(Ids, 50),
    Ids.

cleanup(_Ids) ->
    ok = application:stop(redwall),
    ok = application:stop(peace).

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

%% Starts the process of a new epoch. Each commit version is
%% the previous one plus 1.
epoch(Ids) ->
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Proxy} = redwall_epoch:start(Options),
    {ok, #{log_system := #{logs := Logs}}} = redwall_epoch:recovered(Proxy),
    {Proxy, Logs}.

storage(Ids) ->
    {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5}),
    Storage.

%% Waits until the StorageServer has the version `Version'.
await_version(Storage, Version) ->
    await_version(Storage, Version, 500).

await_version(Storage, Version, 0) ->
    error({version, Version, redwall_storage:info(Storage)});
await_version(Storage, Version, N) ->
    case redwall_storage:info(Storage) of
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
    {ok, 2} = redwall_epoch:commit(Proxy, #{writes => [{x, 1}, {y, 1}]}),
    {ok, 3} = redwall_epoch:commit(Proxy, #{writes => [{x, 2}]}),
    %% A record without a transaction, and a rejected transaction.
    {ok, 4} = redwall_epoch:commit(Proxy, blind),
    {error, not_committed} = redwall_epoch:commit(Proxy, #{read_version => 2, reads => [x], writes => [{x, 3}]}),
    %% The rejected transaction has the LSN 5. The client receives the
    %% answer before the LogServers store the record.
    {ok, ReadVersion} = redwall_epoch:read_version(Proxy),
    ?assert(lists:member(ReadVersion, [4, 5]), ReadVersion),
    ?assertEqual({ok, 2}, redwall_storage:read(Storage, x, ReadVersion)),
    ?assertEqual({ok, 2, 3}, redwall_storage:read_with_version(Storage, x, ReadVersion, 5000)),
    ?assertEqual({ok, 1}, redwall_storage:read(Storage, y, ReadVersion)),
    ?assertEqual(not_found, redwall_storage:read(Storage, z, ReadVersion)),
    %% Reads of ranges. The range has its first key and does not have its
    %% last key.
    ?assertEqual({ok, [{x, 2}, {y, 1}]}, redwall_storage:read_range(Storage, a, z, ReadVersion)),
    ?assertEqual({ok, [{x, 2, 3}]}, redwall_storage:read_range_with_version(Storage, x, y, ReadVersion, 5000)),
    ?assertEqual({ok, [{x, 1}, {y, 1}]}, redwall_storage:read_range(Storage, x, z, 2)),
    ?assertEqual({ok, []}, redwall_storage:read_range(Storage, a, x, ReadVersion)),
    ?assertEqual({ok, []}, redwall_storage:read_range(Storage, a, z, 1)),
    %% Reads at earlier versions.
    ?assertEqual({ok, 1}, redwall_storage:read(Storage, x, 2)),
    ?assertEqual(not_found, redwall_storage:read(Storage, x, 1)),
    ok = await_version(Storage, 5),
    ?assertMatch(#{version := 5, epoch := 1, rollbacks := 0}, redwall_storage:info(Storage)).

%% Clears of a key and of a key range. A read at a version before the clear
%% returns the value.
clears(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, mvcc_window => 4}),
    {ok, 2} = redwall_epoch:commit(Proxy, #{writes => [{a, 1}, {b, 1}, {c, 1}, {d, 1}]}),
    {ok, 3} = redwall_epoch:commit(Proxy, #{mutations => [{clear, a}]}),
    %% The range has `b' and `c', and does not have `d'.
    {ok, 4} = redwall_epoch:commit(Proxy, #{mutations => [{clear_range, b, d}, {clear, z}]}),
    %% The mutations of a transaction are applied in their order: `c' has
    %% the value 5, and `d' has no value.
    {ok, 5} = redwall_epoch:commit(Proxy, #{mutations => [{clear_range, a, z}, {set, c, 5}, {set, d, 5}, {clear, d}]}),
    ?assertEqual({ok, [{a, 1}, {b, 1}, {c, 1}, {d, 1}]}, redwall_storage:read_range(Storage, a, z, 2)),
    ?assertEqual({ok, [{b, 1}, {c, 1}, {d, 1}]}, redwall_storage:read_range(Storage, a, z, 3)),
    ?assertEqual(not_found, redwall_storage:read(Storage, a, 3)),
    ?assertEqual({ok, [{d, 1}]}, redwall_storage:read_range(Storage, a, z, 4)),
    ?assertEqual({ok, [{c, 5}]}, redwall_storage:read_range(Storage, a, z, 5)),
    ?assertEqual({ok, 5, 5}, redwall_storage:read_with_version(Storage, c, 5, 5000)),
    %% A transaction that read `b' at the version 3 has a conflict with the
    %% clear of the range at the version 4.
    ?assertEqual({error, not_committed}, redwall_epoch:commit(Proxy, #{read_version => 3, reads => [b], writes => [{e, 1}]})),
    ?assertEqual({error, not_committed}, redwall_epoch:commit(Proxy, #{read_version => 2, reads => [a], writes => [{e, 1}]})),
    %% The MVCC window removes a key that has no value at the oldest
    %% version.
    [{ok, _} = redwall_epoch:commit(Proxy, #{writes => [{c, N}]}) || N <- lists:seq(8, 14)],
    ok = await_version(Storage, 14),
    ?assertMatch(#{oldest := 10}, redwall_storage:info(Storage)),
    Data = all_data(Storage),
    ?assertEqual([c], maps:keys(Data)).

%% One key has 300 versions, with a clear at each tenth version, and a
%% second key has one version. The row of a key has its newest version,
%% and the older versions are in a second table. A read at each version
%% returns the value of that version, by message and in the process of the
%% caller, for a key and for a range.
many_versions(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = redwall_epoch:commit(Proxy, #{writes => [{a, first}]}),
    Last = lists:foldl(
        fun
            (N, _) when N rem 10 =:= 0 ->
                {ok, LSN} = redwall_epoch:commit(Proxy, #{mutations => [{clear, x}]}),
                LSN;
            (N, _) ->
                {ok, LSN} = redwall_epoch:commit(Proxy, #{writes => [{x, N}]}),
                LSN
        end,
        0,
        lists:seq(3, 302)
    ),
    ?assertEqual(302, Last),
    ok = await_version(Storage, 302),
    Info = #{data := Data, history := History} = redwall_storage:info(Storage),
    ?assertEqual([{a, 2, {set, first}}, {x, 302, {set, 302}}], ets:tab2list(Data)),
    ?assertEqual(299, ets:info(History, size)),
    ?assertEqual([{V, case V rem 10 of 0 -> clear; _ -> {set, V} end} || V <- lists:seq(302, 3, -1)], maps:get(x, redwall_storage:versions(Info))),
    {ok, Handle} = redwall_storage:handle(Storage),
    lists:foreach(
        fun(Version) ->
            Expected =
                case Version of
                    2 -> not_found;
                    _ when Version rem 10 =:= 0 -> not_found;
                    _ -> {ok, Version, Version}
                end,
            ?assertEqual(Expected, redwall_storage:read_with_version(Storage, x, Version, 5000)),
            ?assertEqual({ok, Expected}, redwall_storage:read_direct(Handle, {key, x}, Version)),
            Range = [{a, first, 2} | [{x, Version, Version} || Expected =/= not_found]],
            ?assertEqual({ok, Range}, redwall_storage:read_range_with_version(Storage, a, z, Version, 5000)),
            ?assertEqual({ok, {ok, Range}}, redwall_storage:read_direct(Handle, {range, a, z}, Version))
        end,
        lists:seq(2, 302)
    ).

%% The order of the keys is the order of Erlang terms: a number is below an
%% atom, an atom is below a tuple, and a tuple is below a binary. A read of
%% a range returns the keys in this order.
key_order(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    Storage = storage(Ids),
    Keys = [<<"b">>, {t, 2}, 7, b, <<"a">>, {t, 1}, a, 3.5, <<"a", 0>>],
    {ok, 2} = redwall_epoch:commit(Proxy, #{writes => [{Key, Key} || Key <- Keys]}),
    Sorted = lists:sort(Keys),
    ?assertEqual([3.5, 7, a, b, {t, 1}, {t, 2}, <<"a">>, <<"a", 0>>, <<"b">>], Sorted),
    ?assertEqual({ok, [{Key, Key} || Key <- Sorted]}, redwall_storage:read_range(Storage, 0, <<"z">>, 2)),
    ?assertEqual({ok, [{b, b}, {{t, 1}, {t, 1}}]}, redwall_storage:read_range(Storage, b, {t, 2}, 2)),
    %% The range from the binary `a' to the binary `b' has `a' and the
    %% binary `a' with a zero byte.
    ?assertEqual({ok, [{<<"a">>, <<"a">>}, {<<"a", 0>>, <<"a", 0>>}]}, redwall_storage:read_range(Storage, <<"a">>, <<"b">>, 2)),
    {ok, 3} = redwall_epoch:commit(Proxy, #{mutations => [{clear_range, a, {t, 2}}]}),
    ?assertEqual({ok, [{3.5, 3.5}, {7, 7}, {{t, 2}, {t, 2}}]}, redwall_storage:read_range(Storage, 0, <<>>, 3)).

%% A StorageServer with an MVCC window of 10 versions. The commit `N' has
%% the LSN `N + 1' and writes `{x, N}'. The first commit also writes `y'.
window(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, mvcc_window => 10}),
    {ok, 2} = redwall_epoch:commit(Proxy, #{writes => [{x, 1}, {y, 1}]}),
    [{ok, _} = redwall_epoch:commit(Proxy, #{writes => [{x, N}]}) || N <- lists:seq(2, 30)],
    ok = await_version(Storage, 31),
    %% The LogServers have the Known Committed Version 30 with the record
    %% 31. The oldest version is 31 - 10.
    ?assertMatch(#{version := 31, oldest := 21}, redwall_storage:info(Storage)),
    ?assertEqual({ok, 30}, redwall_storage:read(Storage, x, 31)),
    ?assertEqual({ok, 20, 21}, redwall_storage:read_with_version(Storage, x, 21, 5000)),
    ?assertEqual({error, transaction_too_old}, redwall_storage:read(Storage, x, 20)),
    ?assertEqual({error, transaction_too_old}, redwall_storage:read_range(Storage, a, z, 20)),
    %% The StorageServer keeps the last version at or below the oldest
    %% version: the write of `y' at the LSN 2.
    ?assertEqual({ok, 1, 2}, redwall_storage:read_with_version(Storage, y, 31, 5000)),
    ?assertEqual({ok, [{x, 22}, {y, 1}]}, redwall_storage:read_range(Storage, a, z, 23)),
    %% The StorageServer has 11 versions of `x': the versions 22 to 31 and
    %% the version 21.
    #{data := #{x := Versions, y := [{2, {set, 1}}]}} = sys_state(Storage),
    ?assertEqual(lists:seq(31, 21, -1), [Version || {Version, _Value} <- Versions]).

%% The versions that the StorageServer keeps for each key.
sys_state(Storage) ->
    #{data => all_data(Storage)}.

%% The rows of the ETS table of the data, as a map.
all_data(Storage) ->
    redwall_storage:versions(redwall_storage:info(Storage)).

%% Two transactions of one batch write the key `x'. A read returns the
%% write of the transaction with the higher index.
batch(Ids) ->
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Proxy} = redwall_epoch:start(Options#{batch_ms => 300, batch_idle => false}),
    Storage = storage(Ids),
    Self = self(),
    Clients = [
        spawn_link(fun() -> Self ! {self(), redwall_epoch:commit_with_index(Proxy, #{writes => [{x, N}, {N, N}]}, 5000)} end)
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
    ?assertEqual({ok, Last, 2}, redwall_storage:read_with_version(Storage, x, 2, 5000)),
    ?assertEqual({ok, [{1, 1}, {2, 2}, {3, 3}]}, redwall_storage:read_range(Storage, 1, 4, 2)),
    ?assertEqual(not_found, redwall_storage:read(Storage, x, 1)).

%% A read at a version that the StorageServer does not have waits for the
%% version.
read_waits(Ids) ->
    {Proxy, _Logs} = epoch(Ids),
    Storage = storage(Ids),
    ok = await_version(Storage, 1),
    ?assertMatch({error, {timeout, _}}, redwall_storage:read(Storage, x, 2, 100)),
    Self = self(),
    Reader = spawn_link(fun() -> Self ! {self(), redwall_storage:read(Storage, x, 2)} end),
    {ok, 2} = redwall_epoch:commit(Proxy, #{writes => [{x, 1}]}),
    receive
        {Reader, Result} -> ?assertEqual({ok, 1}, Result)
    after 5000 -> error(no_answer)
    end.

%% The StorageServer follows the records from one epoch to the next.
epochs(Ids) ->
    {P1, _} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = redwall_epoch:commit(P1, #{writes => [{x, 1}]}),
    {P2, _} = epoch(Ids),
    %% The recovery transaction of epoch 2 has the LSN 1002.
    {ok, 1002} = redwall_epoch:read_version(P2),
    ?assertEqual({ok, 1}, redwall_storage:read(Storage, x, 1002)),
    {ok, 1003} = redwall_epoch:commit(P2, #{read_version => 1002, reads => [x], writes => [{x, 2}]}),
    {P3, _} = epoch(Ids),
    {ok, 2004} = redwall_epoch:commit(P3, #{writes => [{y, 3}]}),
    ?assertEqual({ok, 2}, redwall_storage:read(Storage, x, 2004)),
    ?assertEqual({ok, 3}, redwall_storage:read(Storage, y, 2004)),
    ?assertMatch(#{version := 2004, epoch := 3, rollbacks := 0}, redwall_storage:info(Storage)).

%% The record 3 of epoch 1 is on 2 of the 3 LogServers. The StorageServer
%% applies it. The recovery discards it, because the Recovery Version is 2.
%% The StorageServer removes the write.
rollback(Ids) ->
    {P1, [L1, L2, _L3]} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = redwall_epoch:commit(P1, #{writes => [{x, 1}]}),
    [{ok, 3} = redwall_log:push(redwall_log:ref(L), 1, 2, 3, 2, {body, [{x, discarded}]}) || L <- [L1, L2]],
    ok = await_version(Storage, 3),
    {P2, _} = epoch(Ids),
    %% The Recovery Version is 2. The recovery transaction of epoch 2 has
    %% the LSN 1002.
    {ok, 1002} = redwall_epoch:read_version(P2),
    ?assertEqual({ok, 1, 2}, redwall_storage:read_with_version(Storage, x, 1002, 5000)),
    {ok, 1003} = redwall_epoch:commit(P2, #{writes => [{y, 2}]}),
    ?assertEqual({ok, 1}, redwall_storage:read(Storage, x, 1003)),
    ?assertMatch(#{version := 1003, epoch := 2, rollbacks := 1}, redwall_storage:info(Storage)).

%% The end version of an ended epoch is a Known Committed Version of the
%% StorageServer. The LogServers send the Known Committed Version of a
%% record with the next record, so after the last record of an epoch the
%% stored version is below the version. After the start of the next
%% epoch, without a commit, the stored version is the end version of the
%% ended epoch, and a new StorageServer copies the data at that version.
committed_at_epoch_end(Ids) ->
    {P1, _} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = redwall_epoch:commit(P1, #{writes => [{x, 1}]}),
    {ok, 3} = redwall_epoch:commit(P1, #{writes => [{x, 2}]}),
    ok = await_version(Storage, 3),
    {ok, #{stored := Before}} = redwall_storage:stored(Storage, 1000),
    ?assert(Before < 3, Before),
    {P2, _} = epoch(Ids),
    {ok, 1003} = redwall_epoch:read_version(P2),
    ok = await_version(Storage, 1003),
    {ok, #{stored := After}} = redwall_storage:stored(Storage, 1000),
    ?assertEqual(3, After),
    {ok, Second} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, peers => [Storage]}),
    ok = await_version(Second, 1003),
    ?assertMatch(#{copies := 1}, redwall_storage:info(Second)),
    ?assertEqual({ok, 2}, redwall_storage:read(Second, x, 1003)).

%% Starts a process that sends a watch to the StorageServer, and sends
%% the answer to the caller.
watcher(Storage, Key, Value, Version) ->
    Self = self(),
    spawn_link(fun() -> Self ! {self(), redwall_storage:watch_value(Storage, Key, Value, Version, 10000)} end).

answer(Watcher) ->
    receive
        {Watcher, Answer} -> Answer
    after 5000 -> error(no_answer)
    end.

no_answer(Watcher) ->
    receive
        {Watcher, Answer} -> error({answer, Answer})
    after 100 -> ok
    end.

%% Waits until the StorageServer keeps `Count' watches.
await_watches(Storage, Count) ->
    await_watches(Storage, Count, 500).

await_watches(Storage, Count, 0) ->
    error({watches, Count, redwall_storage:info(Storage)});
await_watches(Storage, Count, N) ->
    case redwall_storage:info(Storage) of
        #{watches := Count} ->
            ok;
        _ ->
            timer:sleep(10),
            await_watches(Storage, Count, N - 1)
    end.

%% A watch is answered when the value of its key differs from its value
%% at a version of the StorageServer at or above its version.
watches(Ids) ->
    {P1, [L1, L2, _L3]} = epoch(Ids),
    Storage = storage(Ids),
    {ok, 2} = redwall_epoch:commit(P1, #{writes => [{x, w}]}),
    ok = await_version(Storage, 2),
    %% The value differs at once: another value, a key with a value for a
    %% watch without one, and a key without a value for a watch with one.
    ?assertEqual({ok, 2}, redwall_storage:watch_value(Storage, x, {ok, other}, 2, 1000)),
    ?assertEqual({ok, 2}, redwall_storage:watch_value(Storage, x, not_found, 0, 1000)),
    ?assertEqual({ok, 2}, redwall_storage:watch_value(Storage, y, {ok, 1}, 2, 1000)),
    %% Three watches whose value does not differ. The third one has a
    %% version that the StorageServer has not applied.
    A = watcher(Storage, x, {ok, w}, 2),
    B = watcher(Storage, y, not_found, 2),
    C = watcher(Storage, x, {ok, w}, 4),
    ok = await_watches(Storage, 3),
    %% A set of the key to the same value answers no watch.
    {ok, 3} = redwall_epoch:commit(P1, #{writes => [{x, w}]}),
    ok = await_version(Storage, 3),
    [ok = no_answer(Watcher) || Watcher <- [A, B, C]],
    %% A clear of a key range answers the watches on the keys of the range
    %% that had a value.
    {ok, 4} = redwall_epoch:commit(P1, #{mutations => [{clear_range, x, z}]}),
    ?assertEqual({ok, 4}, answer(A)),
    ?assertEqual({ok, 4}, answer(C)),
    ok = no_answer(B),
    {ok, 5} = redwall_epoch:commit(P1, #{writes => [{y, 1}]}),
    ?assertEqual({ok, 5}, answer(B)),
    ?assertMatch(#{watches := 0, watches_fired := 6, watches_timed_out := 0}, redwall_storage:info(Storage)),
    %% A clear of the key answers a watch with a value.
    D = watcher(Storage, y, {ok, 1}, 5),
    ok = await_watches(Storage, 1),
    {ok, 6} = redwall_epoch:commit(P1, #{mutations => [{clear, y}]}),
    ?assertEqual({ok, 6}, answer(D)),
    %% The record 9 of epoch 1 is on 2 of the 3 LogServers. It sets `x'
    %% back to the value of the watch, so the watch is not answered. The
    %% recovery discards the record. After the rollback the value differs,
    %% and the StorageServer answers the watch.
    {ok, 7} = redwall_epoch:commit(P1, #{writes => [{x, w}]}),
    {ok, 8} = redwall_epoch:commit(P1, #{writes => [{x, z}]}),
    [{ok, 9} = redwall_log:push(redwall_log:ref(L), 1, 8, 9, 8, {body, {batch, [[{set, x, w}]]}}) || L <- [L1, L2]],
    ok = await_version(Storage, 9),
    ?assertEqual({ok, w}, redwall_storage:read(Storage, x, 9)),
    E = watcher(Storage, x, {ok, w}, 7),
    ok = await_watches(Storage, 1),
    ok = no_answer(E),
    {P2, _} = epoch(Ids),
    ?assertEqual({ok, 8}, answer(E)),
    {ok, ReadVersion} = redwall_epoch:read_version(P2),
    ?assertEqual({ok, z}, redwall_storage:read(Storage, x, ReadVersion)),
    ?assertMatch(#{watches := 0, rollbacks := 1}, redwall_storage:info(Storage)).

%% The time limit of a watch and the highest number of watches.
watch_limits(Ids) ->
    {P1, _Logs} = epoch(Ids),
    {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, watch_timeout_ms => 300, max_watches => 2}),
    {ok, 2} = redwall_epoch:commit(P1, #{writes => [{x, w}]}),
    ok = await_version(Storage, 2),
    %% The value does not differ in the time limit. The same holds for a
    %% watch with a version that the StorageServer does not reach.
    ?assertEqual({error, timed_out}, redwall_storage:watch_value(Storage, x, {ok, w}, 2, 5000)),
    ?assertEqual({error, timed_out}, redwall_storage:watch_value(Storage, x, {ok, w}, 50, 5000)),
    ?assertMatch(#{watches := 0, watches_timed_out := 2}, redwall_storage:info(Storage)),
    %% With 2 watches, the StorageServer answers `watch_cancelled' to a
    %% watch whose value does not differ, and answers a watch whose value
    %% differs.
    A = watcher(Storage, x, {ok, w}, 2),
    B = watcher(Storage, x, {ok, w}, 50),
    ok = await_watches(Storage, 2),
    ?assertEqual({error, watch_cancelled}, redwall_storage:watch_value(Storage, x, {ok, w}, 2, 5000)),
    ?assertEqual({error, watch_cancelled}, redwall_storage:watch_value(Storage, x, {ok, w}, 60, 5000)),
    ?assertEqual({ok, 2}, redwall_storage:watch_value(Storage, x, {ok, other}, 2, 5000)),
    ?assertEqual({error, timed_out}, answer(A)),
    ?assertEqual({error, timed_out}, answer(B)),
    %% A StorageServer that is not alive.
    ok = redwall_storage:stop(Storage),
    ?assertMatch({error, _}, redwall_storage:watch_value(Storage, x, {ok, w}, 2, 1000)).

%% A StorageServer that starts in the third epoch reads the records of the
%% ended epochs from their LogServers.
starts_late(Ids) ->
    {P1, _} = epoch(Ids),
    {ok, 2} = redwall_epoch:commit(P1, #{writes => [{x, 1}]}),
    {ok, 3} = redwall_epoch:commit(P1, #{writes => [{y, 1}]}),
    {P2, _} = epoch(Ids),
    {ok, 1004} = redwall_epoch:commit(P2, #{writes => [{x, 2}]}),
    {P3, _} = epoch(Ids),
    {ok, 2005} = redwall_epoch:commit(P3, #{writes => [{z, 3}]}),
    Storage = storage(Ids),
    ?assertEqual({ok, 2}, redwall_storage:read(Storage, x, 2005)),
    ?assertEqual({ok, 1}, redwall_storage:read(Storage, y, 2005)),
    ?assertEqual({ok, 3}, redwall_storage:read(Storage, z, 2005)),
    ?assertEqual({ok, 1}, redwall_storage:read(Storage, x, 3)).

%% A StorageServer with peers copies the data from a peer that has data,
%% and reads the log from the version of the copy.
copy(Ids) ->
    {Proxy, Logs} = epoch(Ids),
    First = storage(Ids),
    %% A StorageServer whose peer has no data reads the log from the
    %% start.
    {ok, FromLog} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, peers => [First]}),
    %% 2,500 keys in 5 transactions, which is more than 2 pages of a copy,
    %% then clears and a second version of a key.
    lists:foreach(
        fun(N) ->
            {ok, _} = redwall_epoch:commit(Proxy, #{mutations => [{set, {k, I}, N} || I <- lists:seq(N * 500 + 1, N * 500 + 500)]})
        end,
        lists:seq(0, 4)
    ),
    {ok, _} = redwall_epoch:commit(Proxy, #{mutations => [{clear, {k, 7}}, {set, {k, 8}, new}, {clear_range, {k, 100}, {k, 200}}]}),
    {ok, Last} = redwall_epoch:commit(Proxy, #{mutations => [{set, a, 1}]}),
    ok = await_version(First, Last),
    ok = await_version(FromLog, Last),
    ?assertMatch(#{copies := 0, copying := false}, redwall_storage:info(FromLog)),
    All = fun(Storage, Version) -> redwall_storage:read_range(Storage, 0, <<"z">>, Version) end,
    {ok, Rows} = All(First, Last),
    ?assertEqual(2500 - 1 - 100 + 1, length(Rows)),
    ?assertEqual({ok, Rows}, All(FromLog, Last)),
    %% A StorageServer whose peer has data copies it. The first peer of
    %% its list does not run.
    Dead = spawn(fun() -> ok end),
    {ok, Second} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, peers => fun() -> [Dead, First] end}),
    ok = await_version(Second, Last),
    %% The version of the copy is the Known Committed Version of the
    %% peer, which is the last version or the version before it. The
    %% StorageServer reads the later record from the log.
    #{copies := 1, copying := false, oldest := Copied} = redwall_storage:info(Second),
    ?assert(Copied =:= Last orelse Copied =:= Last - 1, Copied),
    ?assertEqual({ok, Rows}, All(Second, Last)),
    ?assertEqual(not_found, redwall_storage:read(Second, {k, 7}, Last)),
    ?assertEqual({ok, new}, redwall_storage:read(Second, {k, 8}, Last)),
    %% A read below the version of the copy is too old.
    ?assertEqual({error, transaction_too_old}, redwall_storage:read(Second, a, Copied - 1)),
    ?assertEqual({ok, 0}, redwall_storage:read(First, {k, 8}, Last - 2)),
    %% The copy ends, so the first StorageServer raises its oldest
    %% version again. Both StorageServers apply the later records.
    {ok, _} = redwall_epoch:commit(Proxy, #{mutations => [{set, {k, 8}, newer}, {clear, a}]}),
    {ok, Later} = redwall_epoch:commit(Proxy, #{mutations => [{set, b, 2}]}),
    ok = await_version(Second, Later),
    ok = await_version(First, Later),
    ?assertEqual(All(First, Later), All(Second, Later)),
    ?assertEqual({ok, newer}, redwall_storage:read(Second, {k, 8}, Later)),
    ?assertEqual({ok, new}, redwall_storage:read(Second, {k, 8}, Last)),
    ?assertEqual(not_found, redwall_storage:read(Second, a, Later)),
    %% Every LogServer stops. A new StorageServer has no record to read,
    %% and has the data from the copy.
    exit(Proxy, kill),
    [exit(Pid, kill) || {_Id, Pid} <- Logs],
    {ok, Third} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, peers => [Second]}),
    %% The version of the copy is the Known Committed Version of the
    %% peer. The record after it is on no LogServer.
    ok = await_copy(Third, 500),
    #{copies := 1, version := Reached} = redwall_storage:info(Third),
    ?assert(Reached =:= Later orelse Reached =:= Later - 1, Reached),
    ?assertEqual(All(First, Reached), All(Third, Reached)),
    %% A StorageServer without a peer that answers waits.
    {ok, Alone} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, peers => [Dead]}),
    timer:sleep(500),
    ?assertMatch(#{version := 0, copying := true}, redwall_storage:info(Alone)),
    [exit(Storage, kill) || Storage <- [First, FromLog, Second, Third, Alone]].

%% The stored version of a StorageServer, and the StorageServers that
%% start after the LogServers have discarded the records at or below that
%% version.
pop(Ids) ->
    {Proxy, Logs} = epoch(Ids),
    First = storage(Ids),
    {ok, _} = redwall_epoch:commit(Proxy, #{mutations => [{set, a, 1}]}),
    {ok, _} = redwall_epoch:commit(Proxy, #{mutations => [{set, b, 2}]}),
    {ok, Last} = redwall_epoch:commit(Proxy, #{mutations => [{set, c, 3}]}),
    ok = await_version(First, Last),
    %% The stored version is at or below the Known Committed Version,
    %% which is the last version or the version before it.
    {ok, #{generation := 0, stored := Stored, version := Last}} = redwall_storage:stored(First, 1000),
    ?assert(Stored =:= Last orelse Stored =:= Last - 1, Stored),
    [ok = redwall_log:pop(Pid, Stored) || {_Id, Pid} <- Logs],
    [?assertMatch(#{popped := Stored}, redwall_log:info(Pid)) || {_Id, Pid} <- Logs],
    %% The StorageServer with the discarded records applies the later
    %% records.
    {ok, Later} = redwall_epoch:commit(Proxy, #{mutations => [{set, d, 4}]}),
    ok = await_version(First, Later),
    %% A StorageServer without data and without a peer reads the log from
    %% the start. The LogServers do not have the first records, so it
    %% applies no record.
    Alone = storage(Ids),
    timer:sleep(300),
    ?assertMatch(#{version := 0, copies := 0}, redwall_storage:info(Alone)),
    ?assertMatch({ok, #{stored := 0}}, redwall_storage:stored(Alone, 1000)),
    %% A StorageServer whose only peer has no data reads the log from the
    %% start, and applies no record either.
    Peers = atomics:new(1, []),
    PeersFun = fun() ->
        case atomics:get(Peers, 1) of
            0 -> [Alone];
            1 -> [First]
        end
    end,
    {ok, Second} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, peers => PeersFun}),
    timer:sleep(300),
    ?assertMatch(#{version := 0, copies := 0}, redwall_storage:info(Second)),
    %% With a peer that has the data, it copies the data after the next
    %% answer `popped' of a LogServer, and reads the log from the version
    %% of the copy.
    ok = atomics:put(Peers, 1, 1),
    ok = await_copy(Second, 500),
    ok = await_version(Second, Later),
    ?assertEqual({ok, 1}, redwall_storage:read(Second, a, Later)),
    ?assertEqual({ok, 4}, redwall_storage:read(Second, d, Later)),
    {ok, Next} = redwall_epoch:commit(Proxy, #{mutations => [{set, e, 5}]}),
    ok = await_version(Second, Next),
    ok = await_version(First, Next),
    ?assertEqual(redwall_storage:read_range(First, 0, <<"z">>, Next), redwall_storage:read_range(Second, 0, <<"z">>, Next)),
    exit(Proxy, kill),
    [exit(Storage, kill) || Storage <- [First, Alone, Second]].

%% Waits until the StorageServer has finished a copy.
await_copy(Storage, 0) ->
    error({no_copy, redwall_storage:info(Storage)});
await_copy(Storage, N) ->
    case redwall_storage:info(Storage) of
        #{copies := 1, copying := false} ->
            ok;
        _ ->
            timer:sleep(10),
            await_copy(Storage, N - 1)
    end.

%% After the loss of the LogServers of an epoch, a recovery resets the
%% database, and the StorageServer removes its data. The LogServers keep
%% their records in memory, so the reset is the default.
reset(Ids) ->
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {Proxy1, Logs1} = epoch(Ids),
    Storage = storage(Ids),
    {ok, _} = redwall_epoch:commit(Proxy1, #{mutations => [{set, a, 1}, {set, b, 1}]}),
    {ok, V1} = redwall_epoch:commit(Proxy1, #{mutations => [{set, a, 2}]}),
    ok = await_version(Storage, V1),
    ?assertEqual({ok, 2}, redwall_storage:read(Storage, a, V1)),
    %% One LogServer of 3 stops. The recovery ends the epoch without a
    %% reset.
    exit(element(2, hd(Logs1)), kill),
    exit(Proxy1, kill),
    {ok, Proxy2} = redwall_epoch:start(Options),
    {ok, #{reset := false, epoch := 2, log_system := #{logs := Logs2, durability := memory}}} = redwall_epoch:recovered(Proxy2),
    ?assertMatch({ok, #{generation := 0, old := [_]}}, redwall_cstate:read(Ids)),
    {ok, V2} = redwall_epoch:commit(Proxy2, #{mutations => [{set, a, 3}]}),
    ok = await_version(Storage, V2),
    ?assertEqual({ok, 3}, redwall_storage:read(Storage, a, V2)),
    %% Every LogServer of the epoch 2 stops, as after the stop of every
    %% node. With the option `reset_after_log_loss => false', the recovery
    %% fails.
    exit(Proxy2, kill),
    [exit(Pid, kill) || {_Id, Pid} <- Logs2],
    {ok, Proxy3} = redwall_epoch:start(Options#{reset_after_log_loss => false}),
    ?assertExit({{shutdown, {recovery, {too_few_replies, 0, 2}}}, _}, redwall_epoch:recovered(Proxy3)),
    ?assertMatch({ok, #{generation := 0, current := #{epoch := 2}}}, redwall_cstate:read(Ids)),
    %% Without the option, the recovery resets the database: the
    %% coordinated state has the new log system only, and the next
    %% generation.
    {ok, Proxy4} = redwall_epoch:start(Options),
    {ok, #{reset := true, epoch := 4, recovery_version := 0}} = redwall_epoch:recovered(Proxy4),
    ?assertMatch({ok, #{generation := 1, old := [], current := #{epoch := 4}}}, redwall_cstate:read(Ids)),
    %% The StorageServer removes its data, and applies the records of the
    %% new epoch. The versions start again.
    {ok, V4} = redwall_epoch:commit(Proxy4, #{mutations => [{set, c, 4}]}),
    ok = await_version(Storage, V4),
    ?assertMatch(#{resets := 1, rollbacks := 0}, redwall_storage:info(Storage)),
    ?assertEqual({ok, 4}, redwall_storage:read(Storage, c, V4)),
    ?assertEqual(not_found, redwall_storage:read(Storage, a, V4)),
    ?assertEqual({ok, [{c, 4}]}, redwall_storage:read_range(Storage, 0, <<"z">>, V4)),
    %% A transaction of the time before the reset has a read version above
    %% the commit versions of the new epoch. The Resolver rejects it.
    ?assert(V2 > V4, {V2, V4}),
    ?assertEqual({error, transaction_too_old}, redwall_epoch:commit(Proxy4, #{read_version => V2, reads => [a], mutations => [{set, a, old}]})),
    {ok, V5} = redwall_epoch:commit(Proxy4, #{read_version => V4, reads => [a], mutations => [{set, a, new}]}),
    ok = await_version(Storage, V5),
    ?assertEqual({ok, new}, redwall_storage:read(Storage, a, V5)),
    %% A new StorageServer reads the log of the new epoch only.
    Second = storage(Ids),
    ok = await_version(Second, V5),
    ?assertEqual({ok, [{a, new}, {c, 4}]}, redwall_storage:read_range(Second, 0, <<"z">>, V5)),
    [exit(Pid, kill) || Pid <- [Storage, Second, Proxy3, Proxy4]].
