-module(deadlift_resolver_tests).

-include_lib("eunit/include/eunit.hrl").

txn(ReadVersion, Reads, Writes) ->
    #{read_version => ReadVersion, reads => Reads, writes => Writes}.

%% Algorithm 1 of the paper.
algorithm_test() ->
    H0 = deadlift_resolver:new(infinity),
    {committed, H1} = deadlift_resolver:check(txn(0, [a], [a, b]), 10, H0),
    %% A read of `a' at version 5 is below the write at version 10.
    {conflict, H2} = deadlift_resolver:check(txn(5, [a], [c]), 11, H1),
    %% A transaction with a conflict records no write.
    {committed, H3} = deadlift_resolver:check(txn(5, [c], []), 12, H2),
    %% A read at the version of the write, or above it, has no conflict.
    {committed, H4} = deadlift_resolver:check(txn(10, [a, b], [a]), 13, H3),
    {conflict, H5} = deadlift_resolver:check(txn(12, [x, a], []), 14, H4),
    %% A transaction without read keys has no conflict.
    {committed, _} = deadlift_resolver:check(txn(0, [], [a]), 15, H5).

txn(ReadVersion, Reads, ReadRanges, Writes, WriteRanges) ->
    (txn(ReadVersion, Reads, Writes))#{read_ranges => ReadRanges, write_ranges => WriteRanges}.

%% Read ranges. The range `{Begin, End}' has `Begin' and does not have
%% `End'.
read_range_test() ->
    H0 = deadlift_resolver:new(infinity),
    {committed, H1} = deadlift_resolver:check(txn(0, [], [c]), 10, H0),
    {conflict, H2} = deadlift_resolver:check(txn(5, [], [{a, d}], [x], []), 11, H1),
    {conflict, H3} = deadlift_resolver:check(txn(5, [], [{c, d}], [], []), 12, H2),
    %% `c' is the end of the range, and is not in the range.
    {committed, H4} = deadlift_resolver:check(txn(5, [], [{a, c}], [], []), 13, H3),
    {committed, H5} = deadlift_resolver:check(txn(5, [], [{d, z}], [], []), 14, H4),
    %% A range without a key has no conflict.
    {committed, H6} = deadlift_resolver:check(txn(5, [], [{d, a}, {c, c}], [], []), 15, H5),
    %% A read version at the version of the write has no conflict.
    {committed, H7} = deadlift_resolver:check(txn(10, [], [{a, z}], [], []), 16, H6),
    %% The transaction with a conflict at version 11 recorded no write.
    {committed, _} = deadlift_resolver:check(txn(5, [x], []), 17, H7).

%% A transaction reads a range that has no value for the key `b'. A write
%% of `b' by another transaction is a conflict.
absent_key_test() ->
    H0 = deadlift_resolver:new(infinity),
    {committed, H1} = deadlift_resolver:check(txn(0, [], [b]), 10, H0),
    {conflict, _} = deadlift_resolver:check(txn(9, [], [{a, c}], [d], []), 11, H1).

%% Write ranges. A later write of a part of a range keeps the version of
%% the other parts.
write_range_test() ->
    H0 = deadlift_resolver:new(infinity),
    {committed, H1} = deadlift_resolver:check(txn(0, [], [], [], [{b, f}]), 10, H0),
    {committed, H2} = deadlift_resolver:check(txn(0, [], [d]), 20, H1),
    {committed, H3} = deadlift_resolver:check(txn(0, [], [], [], [{e, h}]), 30, H2),
    %% The versions are: 10 from `b' to `d', 20 for `d', 10 between `d' and
    %% `e', 30 from `e' to `h', and 0 for the other keys.
    lists:foreach(
        fun({Key, Version}) ->
            ?assertMatch({committed, _}, deadlift_resolver:check(txn(Version, [Key], []), 31, H3), Key),
            [?assertMatch({conflict, _}, deadlift_resolver:check(txn(Version - 1, [Key], []), 31, H3), Key) || Version > 0]
        end,
        [{a, 0}, {b, 10}, {c, 10}, {d, 20}, {da, 10}, {e, 30}, {g, 30}, {h, 0}, {z, 0}]
    ),
    %% A read range has a conflict with the highest version in the range.
    {committed, _} = deadlift_resolver:check(txn(30, [], [{a, z}], [], []), 31, H3),
    {conflict, _} = deadlift_resolver:check(txn(29, [], [{a, z}], [], []), 31, H3),
    {committed, _} = deadlift_resolver:check(txn(20, [], [{a, e}], [], []), 31, H3),
    {conflict, _} = deadlift_resolver:check(txn(19, [], [{c, e}], [], []), 31, H3),
    {committed, _} = deadlift_resolver:check(txn(0, [], [{h, z}, {a, b}], [], []), 31, H3).

%% The check with ranges gives the result of a check that keeps one version
%% for each key, for 3000 random transactions over 6 keys.
random_test() ->
    Keys = [1, 2, 3, 4, 5, 6],
    Rand0 = rand:seed_s(exsss, {1, 2, 3}),
    lists:foldl(
        fun(Version, {Rand1, History, Model}) ->
            {[RV, K1, K2, K3, K4, K5, K6], Rand2} = lists:mapfoldl(fun(N, R) -> rand:uniform_s(N, R) end, Rand1, [12, 7, 7, 7, 7, 6, 6]),
            ReadVersion = max(0, Version - RV),
            Txn = txn(ReadVersion, [K5], [{min(K1, K2), max(K1, K2)}], [K6], [{min(K3, K4), max(K3, K4)}]),
            Read = [K5 | [K || K <- Keys, K >= min(K1, K2), K < max(K1, K2)]],
            Written = [K6 | [K || K <- Keys, K >= min(K3, K4), K < max(K3, K4)]],
            Expected =
                case lists:any(fun(K) -> maps:get(K, Model, 0) > ReadVersion end, Read) of
                    true -> conflict;
                    false -> committed
                end,
            {Result, History1} = deadlift_resolver:check(Txn, Version, History),
            ?assertEqual(Expected, Result, {Version, Txn, Model}),
            Model1 =
                case Result of
                    committed -> lists:foldl(fun(K, M) -> M#{K => Version} end, Model, Written);
                    conflict -> Model
                end,
            {Rand2, History1, Model1}
        end,
        {Rand0, deadlift_resolver:new(infinity), #{}},
        lists:seq(1, 3000)
    ),
    ok.

%% The MVCC window removes the versions of ranges.
window_range_test() ->
    H0 = deadlift_resolver:new(100),
    {committed, H1} = deadlift_resolver:check(txn(0, [], [], [], [{a, m}]), 50, H0),
    {committed, H2} = deadlift_resolver:check(txn(0, [], [f]), 120, H1),
    %% The oldest version is 200 - 100 = 100. The write of the range at
    %% version 50 is removed, and the write of `f' at version 120 is kept.
    {committed, H3} = deadlift_resolver:check(txn(0, [], [z]), 200, H2),
    {conflict, H4} = deadlift_resolver:check(txn(110, [], [{a, m}], [], []), 201, H3),
    {committed, H5} = deadlift_resolver:check(txn(120, [], [{a, m}], [], []), 202, H4),
    {committed, H6} = deadlift_resolver:check(txn(119, [], [{a, f}, {g, m}], [], []), 203, H5),
    {too_old, _} = deadlift_resolver:check(txn(99, [], [{a, b}], [], []), 204, H6).

%% The MVCC window, with the rule of FoundationDB.
window_test() ->
    H0 = deadlift_resolver:new(100),
    {committed, H1} = deadlift_resolver:check(txn(0, [], [a]), 50, H0),
    %% The oldest version is 150 - 100 = 50. A read version of 49 is too
    %% old. A read version of 50 is not.
    {too_old, H2} = deadlift_resolver:check(txn(49, [b], []), 150, H1),
    {committed, H3} = deadlift_resolver:check(txn(50, [a], []), 150, H2),
    %% A transaction without read keys is never too old.
    {committed, H4} = deadlift_resolver:check(txn(0, [], [b]), 152, H3),
    %% The write of `a' at version 50 leaves the window at version 152. A
    %% read version below the window is too old, so the lost entry cannot
    %% hide a conflict.
    {too_old, H5} = deadlift_resolver:check(txn(40, [a], []), 153, H4),
    {committed, _} = deadlift_resolver:check(txn(60, [a], []), 154, H5).

%% The Resolver of a new epoch has no history. The first commit version of
%% the epoch is more than the window above the last version of the previous
%% epoch, so a read version of the previous epoch is too old.
new_epoch_test() ->
    Window = 5000000,
    LastEpochEnd = 123,
    First = LastEpochEnd + 100000000,
    H0 = deadlift_resolver:new(Window),
    {too_old, H1} = deadlift_resolver:check(txn(LastEpochEnd, [a], [a]), First, H0),
    {committed, _} = deadlift_resolver:check(txn(First, [a], [a]), First + 1, H1).

%% The process handles the versions in their order.
order_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(deadlift),
            ok
        end,
        fun(_) -> ok = application:stop(deadlift) end,
        fun() ->
            {ok, Resolver} = deadlift_resolver:start_link(#{epoch => 7, version => 100}),
            ?assertEqual(Resolver, deadlift_resolver:ref(7)),
            ?assertEqual({error, {epoch, 7}}, gen_server:call(Resolver, {resolve, 6, 100, 101, txn(0, [], [])})),
            %% The request for version 102 arrives before the request for
            %% version 101, and waits.
            R2 = gen_server:send_request(Resolver, {resolve, 7, 101, 102, txn(100, [a], [])}),
            ?assertEqual(timeout, gen_server:wait_response(R2, 50)),
            ?assertEqual(committed, gen_server:call(Resolver, {resolve, 7, 100, 101, txn(100, [], [a])})),
            %% The read of `a' at version 100 is below the write at 101.
            ?assertEqual({reply, conflict}, gen_server:receive_response(R2, 1000)),
            ?assertEqual({error, {handled, 101}}, gen_server:call(Resolver, {resolve, 7, 100, 101, txn(100, [], [a])})),
            ok = deadlift_resolver:stop(Resolver)
        end}.
