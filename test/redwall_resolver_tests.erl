-module(redwall_resolver_tests).

-include_lib("eunit/include/eunit.hrl").

txn(ReadVersion, Reads, Writes) ->
    #{read_version => ReadVersion, reads => Reads, writes => Writes}.

%% Algorithm 1 of the paper.
algorithm_test() ->
    H0 = redwall_resolver:new(infinity),
    {committed, H1} = redwall_resolver:check(txn(0, [a], [a, b]), 10, H0),
    %% A read of `a' at version 5 is below the write at version 10.
    {conflict, H2} = redwall_resolver:check(txn(5, [a], [c]), 11, H1),
    %% A transaction with a conflict records no write.
    {committed, H3} = redwall_resolver:check(txn(5, [c], []), 12, H2),
    %% A read at the version of the write, or above it, has no conflict.
    {committed, H4} = redwall_resolver:check(txn(10, [a, b], [a]), 13, H3),
    {conflict, H5} = redwall_resolver:check(txn(12, [x, a], []), 14, H4),
    %% A transaction without read keys has no conflict.
    {committed, _} = redwall_resolver:check(txn(0, [], [a]), 15, H5).

txn(ReadVersion, Reads, ReadRanges, Writes, WriteRanges) ->
    (txn(ReadVersion, Reads, Writes))#{read_ranges => ReadRanges, write_ranges => WriteRanges}.

%% Read ranges. The range `{Begin, End}' has `Begin' and does not have
%% `End'.
read_range_test() ->
    H0 = redwall_resolver:new(infinity),
    {committed, H1} = redwall_resolver:check(txn(0, [], [c]), 10, H0),
    {conflict, H2} = redwall_resolver:check(txn(5, [], [{a, d}], [x], []), 11, H1),
    {conflict, H3} = redwall_resolver:check(txn(5, [], [{c, d}], [], []), 12, H2),
    %% `c' is the end of the range, and is not in the range.
    {committed, H4} = redwall_resolver:check(txn(5, [], [{a, c}], [], []), 13, H3),
    {committed, H5} = redwall_resolver:check(txn(5, [], [{d, z}], [], []), 14, H4),
    %% A range without a key has no conflict.
    {committed, H6} = redwall_resolver:check(txn(5, [], [{d, a}, {c, c}], [], []), 15, H5),
    %% A read version at the version of the write has no conflict.
    {committed, H7} = redwall_resolver:check(txn(10, [], [{a, z}], [], []), 16, H6),
    %% The transaction with a conflict at version 11 recorded no write.
    {committed, _} = redwall_resolver:check(txn(5, [x], []), 17, H7).

%% A transaction reads a range that has no value for the key `b'. A write
%% of `b' by another transaction is a conflict.
absent_key_test() ->
    H0 = redwall_resolver:new(infinity),
    {committed, H1} = redwall_resolver:check(txn(0, [], [b]), 10, H0),
    {conflict, _} = redwall_resolver:check(txn(9, [], [{a, c}], [d], []), 11, H1).

%% Write ranges. A later write of a part of a range keeps the version of
%% the other parts.
write_range_test() ->
    H0 = redwall_resolver:new(infinity),
    {committed, H1} = redwall_resolver:check(txn(0, [], [], [], [{b, f}]), 10, H0),
    {committed, H2} = redwall_resolver:check(txn(0, [], [d]), 20, H1),
    {committed, H3} = redwall_resolver:check(txn(0, [], [], [], [{e, h}]), 30, H2),
    %% The versions are: 10 from `b' to `d', 20 for `d', 10 between `d' and
    %% `e', 30 from `e' to `h', and 0 for the other keys.
    lists:foreach(
        fun({Key, Version}) ->
            ?assertMatch({committed, _}, redwall_resolver:check(txn(Version, [Key], []), 31, H3), Key),
            [?assertMatch({conflict, _}, redwall_resolver:check(txn(Version - 1, [Key], []), 31, H3), Key) || Version > 0]
        end,
        [{a, 0}, {b, 10}, {c, 10}, {d, 20}, {da, 10}, {e, 30}, {g, 30}, {h, 0}, {z, 0}]
    ),
    %% A read range has a conflict with the highest version in the range.
    {committed, _} = redwall_resolver:check(txn(30, [], [{a, z}], [], []), 31, H3),
    {conflict, _} = redwall_resolver:check(txn(29, [], [{a, z}], [], []), 31, H3),
    {committed, _} = redwall_resolver:check(txn(20, [], [{a, e}], [], []), 31, H3),
    {conflict, _} = redwall_resolver:check(txn(19, [], [{c, e}], [], []), 31, H3),
    {committed, _} = redwall_resolver:check(txn(0, [], [{h, z}, {a, b}], [], []), 31, H3).

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
            {Result, History1} = redwall_resolver:check(Txn, Version, History),
            ?assertEqual(Expected, Result, {Version, Txn, Model}),
            Model1 =
                case Result of
                    committed -> lists:foldl(fun(K, M) -> M#{K => Version} end, Model, Written);
                    conflict -> Model
                end,
            {Rand2, History1, Model1}
        end,
        {Rand0, redwall_resolver:new(infinity), #{}},
        lists:seq(1, 3000)
    ),
    ok.

%% The MVCC window removes the versions of ranges.
window_range_test() ->
    H0 = redwall_resolver:new(100),
    {committed, H1} = redwall_resolver:check(txn(0, [], [], [], [{a, m}]), 50, H0),
    {committed, H2} = redwall_resolver:check(txn(0, [], [f]), 120, H1),
    %% The oldest version is 200 - 100 = 100. The write of the range at
    %% version 50 is removed, and the write of `f' at version 120 is kept.
    {committed, H3} = redwall_resolver:check(txn(0, [], [z]), 200, H2),
    {conflict, H4} = redwall_resolver:check(txn(110, [], [{a, m}], [], []), 201, H3),
    {committed, H5} = redwall_resolver:check(txn(120, [], [{a, m}], [], []), 202, H4),
    {committed, H6} = redwall_resolver:check(txn(119, [], [{a, f}, {g, m}], [], []), 203, H5),
    {too_old, _} = redwall_resolver:check(txn(99, [], [{a, b}], [], []), 204, H6).

%% The MVCC window, with the rule of FoundationDB.
window_test() ->
    H0 = redwall_resolver:new(100),
    {committed, H1} = redwall_resolver:check(txn(0, [], [a]), 50, H0),
    %% The oldest version is 150 - 100 = 50. A read version of 49 is too
    %% old. A read version of 50 is not.
    {too_old, H2} = redwall_resolver:check(txn(49, [b], []), 150, H1),
    {committed, H3} = redwall_resolver:check(txn(50, [a], []), 150, H2),
    %% A transaction without read keys is never too old.
    {committed, H4} = redwall_resolver:check(txn(0, [], [b]), 152, H3),
    %% The write of `a' at version 50 leaves the window at version 152. A
    %% read version below the window is too old, so the lost entry cannot
    %% hide a conflict.
    {too_old, H5} = redwall_resolver:check(txn(40, [a], []), 153, H4),
    {committed, _} = redwall_resolver:check(txn(60, [a], []), 154, H5).

%% The Resolver of a new epoch has no history. The first commit version of
%% the epoch is more than the window above the last version of the previous
%% epoch, so a read version of the previous epoch is too old.
new_epoch_test() ->
    Window = 5000000,
    LastEpochEnd = 123,
    First = LastEpochEnd + 100000000,
    H0 = redwall_resolver:new(Window),
    {too_old, H1} = redwall_resolver:check(txn(LastEpochEnd, [a], [a]), First, H0),
    {committed, _} = redwall_resolver:check(txn(First, [a], [a]), First + 1, H1).

%% The transactions of a batch have one commit version. A transaction has a
%% conflict with an earlier transaction of the batch that was admitted.
batch_test() ->
    H0 = redwall_resolver:new(infinity),
    {[committed, conflict, committed, conflict, committed], H1} = redwall_resolver:check_batch(
        [
            txn(5, [a], [a]),
            %% The first transaction wrote `a'.
            txn(5, [a], [b]),
            %% The second transaction was not admitted, so its write of `b'
            %% gives no conflict.
            txn(5, [b], [c]),
            txn(5, [], [{b, d}], [d], []),
            %% A transaction without read keys has no conflict.
            txn(5, [], [a, c])
        ],
        10,
        H0
    ),
    %% After the batch, `a' and `c' have the version 10, and `b' and `d'
    %% have no version.
    {[committed, committed, conflict, conflict], _} = redwall_resolver:check_batch(
        [txn(9, [b], []), txn(9, [d], []), txn(9, [a], []), txn(9, [c], [])],
        11,
        H1
    ).

%% `resolve/4' checks a batch as `check_batch/3' does. Each flag plants one
%% defect.
resolve_test() ->
    New = fun() ->
        {committed, History} = redwall_resolver:check(txn(0, [], [], [a], [{m, p}]), 10, redwall_resolver:new(infinity)),
        History
    end,
    Batch = [
        %% A read of `a' below the write at 10.
        txn(5, [a], [b]),
        %% A read of a range with `n' below the clear of the range at 10.
        txn(5, [], [{n, o}], [c], []),
        %% A read of `d', which the next transaction of the batch reads
        %% after this one wrote it.
        txn(10, [d], [d]),
        txn(10, [d], [e]),
        %% A clear of a range, and a read of a key of the range in a later
        %% batch.
        txn(10, [], [], [], [{x, z}])
    ],
    Later = [txn(10, [y], [])],
    Run = fun(Flags) ->
        {First, H1} = redwall_resolver:resolve(Batch, 11, New(), Flags),
        {Second, _} = redwall_resolver:resolve(Later, 12, H1, Flags),
        {First, Second}
    end,
    ?assertEqual({[conflict, conflict, committed, conflict, committed], [conflict]}, Run(#{})),
    ?assertEqual({[committed, committed, committed, committed, committed], [committed]}, Run(#{check => false})),
    ?assertEqual({[conflict, committed, committed, conflict, committed], [conflict]}, Run(#{check_ranges => false})),
    ?assertEqual({[conflict, conflict, committed, committed, committed], [conflict]}, Run(#{check_batch => false})),
    ?assertEqual({[conflict, conflict, committed, conflict, committed], [committed]}, Run(#{check_write_ranges => false})).

%% The Resolver removes the versions below the oldest version. A removed
%% version becomes 0, and a bound with the version 0 after a bound with
%% the version 0, or after no bound, is removed.
expire_test() ->
    Empty = #{read_version => 0, reads => [], writes => []},
    Rows = fun(History, Table) -> ets:tab2list(maps:get(Table, History)) end,
    H0 = redwall_resolver:new(80),
    {committed, H1} = redwall_resolver:check(txn(0, [], [], [], [{b, f}]), 10, H0),
    {committed, H2} = redwall_resolver:check(txn(0, [], [d]), 20, H1),
    ?assertEqual([{{b, 0}, 10}, {{d, 0}, 20}, {{d, 1}, 10}, {{f, 0}, 0}], Rows(H2, last_commit)),
    ?assertEqual([{{10, {b, 0}}}, {{10, {d, 1}}}, {{20, {d, 0}}}], Rows(H2, versions)),
    %% The oldest version is 15. The bound of `b' has no bound before it
    %% and is removed. The bound after `d' ends the version 20, so it
    %% stays with the version 0, and the bound of `f' after it is removed.
    {committed, H3} = redwall_resolver:check(Empty, 95, H2),
    ?assertEqual([{{d, 0}, 20}, {{d, 1}, 0}], Rows(H3, last_commit)),
    ?assertEqual([{{20, {d, 0}}}], Rows(H3, versions)),
    ?assertMatch({committed, _}, redwall_resolver:check(txn(16, [b, c, e], []), 96, H3)),
    ?assertMatch({conflict, _}, redwall_resolver:check(txn(19, [d], []), 96, H3)),
    ?assertMatch({committed, _}, redwall_resolver:check(txn(20, [d], []), 96, H3)),
    ?assertMatch({too_old, _}, redwall_resolver:check(txn(15, [d], []), 96, H3)),
    %% The oldest version is 21. Both tables are empty.
    {committed, H4} = redwall_resolver:check(Empty, 101, H3),
    ?assertEqual([], Rows(H4, last_commit)),
    ?assertEqual([], Rows(H4, versions)).

%% `random_test/0' with an MVCC window of 8 versions. The model keeps the
%% last version of each key and removes nothing. The tables of the
%% Resolver stay small, and the table of the versions has one row for each
%% bound with a version above 0.
random_window_test() ->
    Keys = [1, 2, 3, 4, 5, 6],
    Window = 8,
    Rand0 = rand:seed_s(exsss, {4, 5, 6}),
    {_, _, _, Counts} = lists:foldl(
        fun(Version, {Rand1, History, Model, Seen}) ->
            {[RV, K1, K2, K3, K4, K5, K6], Rand2} = lists:mapfoldl(fun(N, R) -> rand:uniform_s(N, R) end, Rand1, [12, 7, 7, 7, 7, 6, 6]),
            ReadVersion = max(0, Version - RV),
            Txn = txn(ReadVersion, [K5], [{min(K1, K2), max(K1, K2)}], [K6], [{min(K3, K4), max(K3, K4)}]),
            Read = [K5 | [K || K <- Keys, K >= min(K1, K2), K < max(K1, K2)]],
            Written = [K6 | [K || K <- Keys, K >= min(K3, K4), K < max(K3, K4)]],
            Expected =
                case {ReadVersion < Version - Window, lists:any(fun(K) -> maps:get(K, Model, 0) > ReadVersion end, Read)} of
                    {true, _} -> too_old;
                    {false, true} -> conflict;
                    {false, false} -> committed
                end,
            {Result, History1} = redwall_resolver:check(Txn, Version, History),
            ?assertEqual(Expected, Result, {Version, Txn, Model}),
            Bounds = ets:tab2list(maps:get(last_commit, History1)),
            ?assert(length(Bounds) =< 13, Bounds),
            ?assertEqual(
                lists:sort([{{V, Bound}} || {Bound, V} <- Bounds, V > 0]),
                ets:tab2list(maps:get(versions, History1))
            ),
            %% No version is below the oldest version.
            ?assertEqual([], [Bound || {Bound, V} <- Bounds, V > 0, V < Version - Window]),
            Model1 =
                case Result of
                    committed -> lists:foldl(fun(K, M) -> M#{K => Version} end, Model, Written);
                    _ -> Model
                end,
            {Rand2, History1, Model1, Seen#{Result => maps:get(Result, Seen, 0) + 1}}
        end,
        {Rand0, redwall_resolver:new(Window), #{}, #{}},
        lists:seq(1, 5000)
    ),
    %% The run has each result.
    [?assert(maps:get(Result, Counts, 0) > 500, Counts) || Result <- [committed, conflict, too_old]].
