-module(deadlift_tests).

-include_lib("eunit/include/eunit.hrl").

%% Each test runs on a new peace cluster of three servers on one node, with
%% one epoch, 3 LogServers, a replication degree of 2, one Proxy and one
%% StorageServer.
client_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Db) -> {timeout, 30, fun() -> basics(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> own_writes(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> conflict(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> range_conflict(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> snapshot(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> retry(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> two_processes(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> lifecycle(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> too_old(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> replicas(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> replica_behind(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> select(Db) end} end,
        fun(Db) -> {timeout, 60, fun() -> select_random(Db) end} end
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_c, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), Opts),
    {ok, #{count := 0}} = await_read(Ids, 50),
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Proxy} = deadlift_epoch:start(Options),
    {ok, Storage} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5, mvcc_window => 20}),
    deadlift:open(#{proxy => Proxy, storage => Storage}).

cleanup(_Db) ->
    ok = application:stop(deadlift),
    ok = application:stop(peace).

await_read(_Ids, 0) ->
    error(no_leader);
await_read(Ids, N) ->
    case deadlift_cstate:read(Ids, 1000) of
        {ok, State} ->
            {ok, State};
        _ ->
            timer:sleep(100),
            await_read(Ids, N - 1)
    end.

%% `select/4' evaluates a match spec on a key range. The result is the
%% result of `ets:match_spec_run/2' on the result of `get_range/3', for a
%% read from the ETS tables and for a read by message.
select(Db = {deadlift_database, Servers}) ->
    Message = {deadlift_database, Servers#{direct => false}},
    ok = deadlift:transactional(Db, fun(Tx) ->
        lists:foreach(
            fun(I) ->
                Status =
                    case I rem 10 of
                        0 -> active;
                        _ -> idle
                    end,
                ok = deadlift:set(Tx, {user, I}, #{status => Status, n => I, blob => binary:copy(<<0>>, 100)})
            end,
            lists:seq(1, 300)
        ),
        deadlift:set(Tx, {zone, 1}, #{status => active, n => 0})
    end),
    Active = [{{{user, '$1'}, #{status => active, n => '$2'}}, [{'>', '$1', 100}], [{{'$1', '$2'}}]}],
    Specs = [
        Active,
        %% The whole row, and the variables of the head.
        [{{{user, '$1'}, #{status => active}}, [{'<', '$1', 40}], ['$_']}],
        [{{{user, '$1'}, #{n => '$2'}}, [{'=:=', {'rem', '$2', 50}, 0}], ['$$']}],
        %% Two clauses.
        [{{{user, 7}, '_'}, [], [seven]}, {{'$1', #{status => active}}, [], ['$1']}],
        %% No row matches.
        [{{'_', #{status => deleted}}, [], [true]}]
    ],
    Compare = fun(D, Begin, End) ->
        deadlift:transactional(D, fun(Tx) ->
            Rows = deadlift:get_range(Tx, Begin, End),
            lists:foreach(
                fun(Spec) ->
                    Expected = ets:match_spec_run(Rows, ets:match_spec_compile(Spec)),
                    ?assertEqual(Expected, deadlift:select(Tx, Begin, End, Spec), Spec),
                    ?assertEqual(lists:sublist(Expected, 3), deadlift:select(Tx, Begin, End, Spec, [{limit, 3}]), Spec)
                end,
                Specs
            ),
            length(Rows)
        end)
    end,
    [?assertEqual(Count, Compare(D, Begin, End)) || D <- [Db, Message], {Begin, End, Count} <- [{{user, 0}, {user, 999}, 300}, {{user, 50}, {user, 150}, 100}, {a, z, 0}, {{user, 0}, {zone, 9}, 301}]],
    ?assertEqual([{I, I} || I <- lists:seq(110, 300, 10)], deadlift:transactional(Db, fun(Tx) -> deadlift:select(Tx, {user, 0}, {user, 999}, Active) end)),
    %% The function sees the sets and the clears of its transaction.
    lists:foreach(
        fun(D) ->
            {Limited, All} = deadlift:transactional(D, fun(Tx) ->
                ok = deadlift:set(Tx, {user, 105}, #{status => active, n => 105}),
                ok = deadlift:clear(Tx, {user, 120}),
                ok = deadlift:clear_range(Tx, {user, 125}, {user, 145}),
                ok = deadlift:set(Tx, {user, 130}, #{status => idle, n => 130}),
                {deadlift:select(Tx, {user, 0}, {user, 999}, Active, [{limit, 2}]), deadlift:select(Tx, {user, 0}, {user, 999}, Active)}
            end),
            ?assertEqual([{105, 105}, {110, 110}], Limited),
            ?assertEqual([{105, 105}, {110, 110}, {150, 150}], lists:sublist(All, 3))
        end,
        [Db, Message]
    ),
    ok = deadlift:transactional(Db, fun(Tx) ->
        ok = deadlift:clear(Tx, {user, 105}),
        ok = deadlift:set(Tx, {user, 120}, #{status => active, n => 120}),
        lists:foreach(fun(I) -> deadlift:set(Tx, {user, I}, #{status => active, n => I}) end, [130, 140])
    end),
    %% A write of another transaction to a key of the range that does not
    %% match is a conflict.
    Tx1 = deadlift:create_transaction(Db),
    ?assertEqual(20, length(deadlift:select(Tx1, {user, 0}, {user, 999}, Active))),
    ok = deadlift:set(Tx1, other, 1),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, {user, 101}, #{status => idle, n => 101}) end),
    ?assertError({erlfdb_error, 1020}, deadlift:wait(deadlift:commit(Tx1))),
    %% A select of a snapshot gives no conflict.
    Tx2 = deadlift:create_transaction(Db),
    ?assertEqual(20, length(deadlift:select(deadlift:snapshot(Tx2), {user, 0}, {user, 999}, Active))),
    ok = deadlift:set(Tx2, other, 2),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, {user, 102}, #{status => idle, n => 102}) end),
    ok = deadlift:wait(deadlift:commit(Tx2)),
    %% A term that is not a match spec.
    Tx3 = deadlift:create_transaction(Db),
    ?assertError(badarg, deadlift:select(Tx3, a, z, [{no_spec}])),
    ?assertError(badarg, deadlift:select(Tx3, a, z, no_list)).

%% 400 random transactions set and clear some of 30 keys. After each one,
%% `select/4' at the newest version and at an older version of the MVCC
%% window gives the result of `ets:match_spec_run/2' on `get_range/3', for
%% a read from the ETS tables and for a read by message.
select_random(Db = {deadlift_database, Servers}) ->
    Message = {deadlift_database, Servers#{direct => false}},
    Specs = [
        [{{'$1', '$2'}, [{'>', '$2', 500}], [{{'$1', '$2'}}]}],
        [{{'$1', '$2'}, [{'=:=', {'rem', '$1', 3}, 0}], ['$2']}],
        [{'_', [], ['$_']}]
    ],
    Check = fun(D, ReadVersion) ->
        Tx = deadlift:create_transaction(D),
        _ = is_integer(ReadVersion) andalso deadlift:set_read_version(Tx, ReadVersion),
        try
            Rows = deadlift:get_range(Tx, 5, 25),
            [?assertEqual(ets:match_spec_run(Rows, ets:match_spec_compile(Spec)), deadlift:select(Tx, 5, 25, Spec)) || Spec <- Specs],
            ok
        catch
            %% The older version left the MVCC window.
            error:{erlfdb_error, 1007} -> too_old
        end
    end,
    Rand0 = rand:seed_s(exsss, {7, 8, 9}),
    {_, Checks} = lists:foldl(
        fun(_N, {Rand1, Count}) ->
            {[Kind, K1, K2, Value, Back], Rand2} = lists:mapfoldl(fun(Max, R) -> rand:uniform_s(Max, R) end, Rand1, [10, 30, 30, 1000, 15]),
            ok = deadlift:transactional(Db, fun(Tx) ->
                case Kind of
                    1 -> deadlift:clear(Tx, K1);
                    2 -> deadlift:clear_range(Tx, min(K1, K2), max(K1, K2));
                    _ -> deadlift:set(Tx, K1, Value)
                end
            end),
            Newest = deadlift:transactional(Db, fun(Tx) -> deadlift:get_read_version(Tx) end),
            Results = [Check(D, V) || D <- [Db, Message], V <- [newest, max(1, Newest - Back)]],
            {Rand2, Count + length([ok || ok <- Results])}
        end,
        {Rand0, 0},
        lists:seq(1, 400)
    ),
    %% Most checks are inside the MVCC window.
    ?assert(Checks > 1000, Checks).

%% Sets, reads, a range read, clears and the versions of a transaction.
basics(Db) ->
    ok = deadlift:transactional(Db, fun(Tx) ->
        ?assertEqual(not_found, deadlift:wait(deadlift:get(Tx, a))),
        ok = deadlift:set(Tx, a, 1),
        ok = deadlift:set(Tx, b, 2),
        deadlift:set(Tx, c, 3)
    end),
    ?assertEqual(
        {1, [{a, 1}, {b, 2}, {c, 3}], [{b, 2}]},
        deadlift:transactional(Db, fun(Tx) ->
            {deadlift:wait(deadlift:get(Tx, a)), deadlift:get_range(Tx, a, z), deadlift:get_range(Tx, b, c)}
        end)
    ),
    ok = deadlift:transactional(Db, fun(Tx) ->
        ok = deadlift:clear(Tx, a),
        deadlift:clear_range(Tx, c, z)
    end),
    ?assertEqual([{b, 2}], deadlift:transactional(Db, fun(Tx) -> deadlift:get_range(Tx, a, z) end)),
    %% A transaction without `transactional/2'. The recovery transaction has
    %% the LSN 1, and the two commits above have the LSNs 2 and 3.
    Tx = deadlift:create_transaction(Db),
    ?assert(deadlift:is_read_only(Tx)),
    ?assertEqual(3, deadlift:get_read_version(Tx)),
    ok = deadlift:set(Tx, d, 4),
    ?assertNot(deadlift:is_read_only(Tx)),
    ok = deadlift:wait(deadlift:commit(Tx)),
    ?assertEqual(4, deadlift:get_committed_version(Tx)),
    ?assertEqual(<<4:64, 0:16>>, deadlift:get_versionstamp(Tx)),
    %% A transaction without a mutation sends no commit.
    Tx2 = deadlift:create_transaction(Db),
    ?assertEqual(4, deadlift:wait(deadlift:get(Tx2, d))),
    ok = deadlift:commit(Tx2),
    ?assertEqual(-1, deadlift:get_committed_version(Tx2)).

%% A read returns the writes of the same transaction, and a range read
%% merges them with the stored keys.
own_writes(Db) ->
    ok = deadlift:transactional(Db, fun(Tx) -> [deadlift:set(Tx, K, stored) || K <- [a, b, c, d, e]], ok end),
    deadlift:transactional(Db, fun(Tx) ->
        ok = deadlift:set(Tx, b, mine),
        ok = deadlift:clear(Tx, c),
        ok = deadlift:set(Tx, bb, new),
        ?assertEqual(mine, deadlift:get(Tx, b)),
        ?assertEqual(not_found, deadlift:get(Tx, c)),
        ?assertEqual(new, deadlift:get(Tx, bb)),
        ?assertEqual([{a, stored}, {b, mine}, {bb, new}, {d, stored}, {e, stored}], deadlift:get_range(Tx, a, z)),
        %% A clear of a range, and a set of a key of the range after it.
        ok = deadlift:clear_range(Tx, b, e),
        ?assertEqual(not_found, deadlift:get(Tx, b)),
        ?assertEqual(not_found, deadlift:get(Tx, d)),
        ok = deadlift:set(Tx, d, again),
        ?assertEqual(again, deadlift:get(Tx, d)),
        ?assertEqual([{a, stored}, {d, again}, {e, stored}], deadlift:get_range(Tx, a, z))
    end),
    ?assertEqual(
        [{a, stored}, {d, again}, {e, stored}],
        deadlift:transactional(Db, fun(Tx) -> deadlift:get_range(Tx, a, z) end)
    ).

%% Two transactions read and write one key. The second commit has a
%% conflict, with the error code 1020 of FoundationDB.
conflict(Db) ->
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, x, 0) end),
    Tx1 = deadlift:create_transaction(Db),
    Tx2 = deadlift:create_transaction(Db),
    0 = deadlift:get(Tx1, x),
    0 = deadlift:get(Tx2, x),
    ok = deadlift:set(Tx1, x, 1),
    ok = deadlift:set(Tx2, x, 2),
    ok = deadlift:commit(Tx1),
    ?assertError({erlfdb_error, 1020}, deadlift:commit(Tx2)),
    %% A transaction that wrote without a read has no conflict.
    Tx3 = deadlift:create_transaction(Db),
    Tx4 = deadlift:create_transaction(Db),
    ok = deadlift:set(Tx3, x, 3),
    ok = deadlift:set(Tx4, x, 4),
    ok = deadlift:commit(Tx3),
    ok = deadlift:commit(Tx4),
    ?assertEqual(4, deadlift:transactional(Db, fun(Tx) -> deadlift:get(Tx, x) end)).

%% A transaction reads a range without the key `m'. Another transaction
%% writes `m'. The first transaction has a conflict.
range_conflict(Db) ->
    Tx1 = deadlift:create_transaction(Db),
    ?assertEqual([], deadlift:get_range(Tx1, a, z)),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, m, 1) end),
    ok = deadlift:set(Tx1, other, 1),
    ?assertError({erlfdb_error, 1020}, deadlift:commit(Tx1)),
    %% A clear of a range is a conflict for a reader of a key of the range.
    Tx2 = deadlift:create_transaction(Db),
    1 = deadlift:get(Tx2, m),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:clear_range(Tx, a, z) end),
    ok = deadlift:set(Tx2, other, 2),
    ?assertError({erlfdb_error, 1020}, deadlift:commit(Tx2)),
    %% A read conflict key that the transaction adds without a read.
    Tx3 = deadlift:create_transaction(Db),
    _ = deadlift:get_read_version(Tx3),
    ok = deadlift:add_read_conflict_key(Tx3, k),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, k, 1) end),
    ok = deadlift:set(Tx3, other, 3),
    ?assertError({erlfdb_error, 1020}, deadlift:commit(Tx3)).

%% A read of a snapshot adds no read conflict key.
snapshot(Db) ->
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, x, 0) end),
    Tx1 = deadlift:create_transaction(Db),
    Snapshot = deadlift:snapshot(Tx1),
    ?assertEqual(0, deadlift:get(Snapshot, x)),
    ?assertEqual([{x, 0}], deadlift:get_range(Snapshot, a, z)),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, x, 1) end),
    ok = deadlift:set(Tx1, y, 1),
    ok = deadlift:commit(Tx1),
    %% The snapshot has the writes of its transaction.
    Tx2 = deadlift:create_transaction(Db),
    ok = deadlift:set(Tx2, x, mine),
    ?assertEqual(mine, deadlift:get(deadlift:snapshot(Tx2), x)).

%% `transactional/2' runs the function again after a conflict. 20 processes
%% add 1 to one key 10 times each.
retry(Db) ->
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, counter, 0) end),
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            Attempts = lists:sum([
                begin
                    Count = counters:new(1, []),
                    ok = deadlift:transactional(Db, fun(Tx) ->
                        counters:add(Count, 1, 1),
                        deadlift:set(Tx, counter, deadlift:get(Tx, counter) + 1)
                    end),
                    counters:get(Count, 1)
                end
             || _ <- lists:seq(1, 10)
            ]),
            Self ! {self(), Attempts}
        end)
     || _ <- lists:seq(1, 20)
    ],
    Attempts = lists:sum([
        receive
            {Pid, N} -> N
        after 25000 -> error(no_answer)
        end
     || Pid <- Pids
    ]),
    ?assertEqual(200, deadlift:transactional(Db, fun(Tx) -> deadlift:get(Tx, counter) end)),
    %% At least one transaction had a conflict and ran again.
    ?assert(Attempts > 200, Attempts).

%% A second process uses the transaction of the first process.
two_processes(Db) ->
    Tx = deadlift:create_transaction(Db),
    ok = deadlift:set(Tx, a, 1),
    Self = self(),
    Pid = spawn_link(fun() ->
        ok = deadlift:set(Tx, b, 2),
        Self ! {self(), deadlift:get(Tx, a)}
    end),
    receive
        {Pid, Value} -> ?assertEqual(1, Value)
    after 5000 -> error(no_answer)
    end,
    ?assertEqual(2, deadlift:get(Tx, b)),
    ok = deadlift:commit(Tx),
    ?assertEqual([{a, 1}, {b, 2}], deadlift:transactional(Db, fun(T) -> deadlift:get_range(T, a, z) end)).

%% Reset, cancel and the errors that `on_error/2' does not accept.
lifecycle(Db) ->
    Tx = deadlift:create_transaction(Db),
    ok = deadlift:set(Tx, a, 1),
    ok = deadlift:reset(Tx),
    ?assert(deadlift:is_read_only(Tx)),
    ?assertEqual(not_found, deadlift:get(Tx, a)),
    ok = deadlift:cancel(Tx),
    ?assertError({erlfdb_error, 1025}, deadlift:get(Tx, a)),
    ?assertError({erlfdb_error, 1025}, deadlift:set(Tx, a, 1)),
    %% `on_error/2' raises a code that is not a reason to run again.
    ?assertError({erlfdb_error, 1025}, deadlift:on_error(Tx, 1025)),
    ?assertError({erlfdb_error, 2004}, deadlift:on_error(Tx, {erlfdb_error, 2004})),
    %% `on_error/2' resets the transaction for a conflict.
    Tx2 = deadlift:create_transaction(Db),
    ok = deadlift:set(Tx2, a, 1),
    ok = deadlift:on_error(Tx2, 1020),
    ?assert(deadlift:is_read_only(Tx2)),
    %% An exception of the function that is not an error of deadlift leaves
    %% `transactional/2', and the transaction is not committed.
    ?assertError(
        my_error,
        deadlift:transactional(Db, fun(T) ->
            deadlift:set(T, a, 1),
            error(my_error)
        end)
    ),
    ?assertEqual(not_found, deadlift:transactional(Db, fun(T) -> deadlift:get(T, a) end)).

%% The StorageServer has an MVCC window of 20 versions. A transaction with
%% an old read version has the error 1007 for a read, and `transactional/2'
%% runs the function again with a new read version.
too_old(Db) ->
    Tx = deadlift:create_transaction(Db),
    Old = deadlift:get_read_version(Tx),
    [ok = deadlift:transactional(Db, fun(T) -> deadlift:set(T, x, N) end) || N <- lists:seq(1, 40)],
    %% A read at a new read version returns when the StorageServer has
    %% applied the 40 records.
    ?assertEqual(40, deadlift:transactional(Db, fun(T) -> deadlift:get(T, x) end)),
    ?assertError({erlfdb_error, 1007}, deadlift:get(Tx, x)),
    ?assertError({erlfdb_error, 1007}, deadlift:get_range(Tx, a, z)),
    Count = counters:new(1, []),
    ?assertEqual(
        40,
        deadlift:transactional(Db, fun(T) ->
            counters:add(Count, 1, 1),
            _ = counters:get(Count, 1) =:= 1 andalso deadlift:set_read_version(T, Old),
            deadlift:get(T, x)
        end)
    ),
    ?assertEqual(2, counters:get(Count, 1)).

%% Three StorageServers. A read has the same result with direct reads and
%% with reads by message. The reads continue after the stop of two of the
%% three StorageServers, and fail with 1037 after the stop of the third.
replicas({deadlift_database, Servers = #{storage := First}}) ->
    Options = #{coordinators => coordinators(First), poll_ms => 5, mvcc_window => 20},
    {ok, Second} = deadlift_storage:start(Options),
    {ok, Third} = deadlift_storage:start(Options),
    Direct = deadlift:open(Servers#{storage := [First, Second, Third]}),
    Message = deadlift:open(Servers#{storage := fun() -> [First, Second, Third] end, direct => false}),
    Read = fun(Db) -> deadlift:transactional(Db, fun(Tx) -> {deadlift:get(Tx, a), deadlift:get_range(Tx, a, z)} end) end,
    %% Several transactions, so that the reads start at each of the three.
    lists:foreach(
        fun(N) ->
            ok = deadlift:transactional(Direct, fun(Tx) -> deadlift:set(Tx, a, N) end),
            ?assertEqual({N, [{a, N}]}, Read(Direct)),
            ?assertEqual({N, [{a, N}]}, Read(Message))
        end,
        lists:seq(1, 6)
    ),
    ok = deadlift_storage:stop(First),
    ok = deadlift_storage:stop(Third),
    lists:foreach(
        fun(N) ->
            ok = deadlift:transactional(Direct, fun(Tx) -> deadlift:set(Tx, a, N) end),
            ?assertEqual({N, [{a, N}]}, Read(Direct)),
            ?assertEqual({N, [{a, N}]}, Read(Message))
        end,
        lists:seq(7, 12)
    ),
    ok = deadlift_storage:stop(Second),
    Tx = deadlift:create_transaction(Direct),
    ?assertError({erlfdb_error, 1037}, deadlift:get(Tx, a)),
    ?assertError({erlfdb_error, 1037}, deadlift:get(deadlift:create_transaction(Message), a)),
    %% A database without a StorageServer.
    None = deadlift:open(Servers#{storage := fun() -> [] end}),
    ?assertError({erlfdb_error, 1037}, deadlift:get(deadlift:create_transaction(None), a)).

%% A second StorageServer that does not apply records: its coordinated
%% state has no epoch. A read does not wait for it when the first
%% StorageServer has the read version. With the second StorageServer
%% alone, the read has the error 1009 after 5 seconds.
replica_behind({deadlift_database, Servers = #{storage := First}}) ->
    {ok, Empty} = deadlift_cstate_local:start(#{}),
    {ok, Behind} = deadlift_storage:start(#{coordinators => {local, Empty}, poll_ms => 5}),
    Db = deadlift:open(Servers#{storage := [First, Behind]}),
    Message = deadlift:open(Servers#{storage := [Behind, First], direct => false}),
    lists:foreach(
        fun(N) ->
            ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, a, N) end),
            ?assertEqual(N, deadlift:transactional(Db, fun(Tx) -> deadlift:get(Tx, a) end))
        end,
        lists:seq(1, 4)
    ),
    %% By message, the read waits for one half of 5 seconds at the
    %% StorageServer that is behind, when that one is the first of the
    %% order.
    ?assertEqual(4, deadlift:transactional(Message, fun(Tx) -> deadlift:get(Tx, a) end)),
    Alone = deadlift:open(Servers#{storage := Behind}),
    Tx = deadlift:create_transaction(Alone),
    ok = deadlift:set_read_version(Tx, deadlift:transactional(Db, fun(T) -> deadlift:get_read_version(T) end)),
    ?assertError({erlfdb_error, 1009}, deadlift:get(Tx, a)).

%% The coordinators of a StorageServer.
coordinators(Storage) ->
    element(2, sys:get_state(Storage)).

%% The options `retry_delay_ms' and `max_retry_delay_ms' of `open/1' set
%% the first backoff and the highest backoff of `on_error/2'. The test
%% needs no cluster, because `on_error/2' sends no message.
retry_delay_test() ->
    Elapsed = fun(Options, Errors) ->
        Db = deadlift:open(Options#{proxy => self(), storage => self()}),
        Tx = deadlift:create_transaction(Db),
        Start = erlang:monotonic_time(millisecond),
        [ok = deadlift:on_error(Tx, 1020) || _ <- lists:seq(1, Errors)],
        erlang:monotonic_time(millisecond) - Start
    end,
    %% Without a backoff, 50 errors take no wait.
    ?assert(Elapsed(#{retry_delay_ms => 0}, 50) < 50),
    %% The backoff starts at 1 ms and stays at or below 2 ms. With the
    %% default of 1,000 ms for the highest backoff, 30 errors wait for
    %% several seconds.
    ?assert(Elapsed(#{retry_delay_ms => 1, max_retry_delay_ms => 2}, 30) < 150),
    %% The default of the first backoff is 10 ms. 6 errors have backoffs of
    %% 10 ms to 320 ms, and the client waits for a random part of each.
    Default = Elapsed(#{}, 6),
    ?assert(Default > 20, Default).

%% A profile is a set of client options. `open/1' takes the options of the
%% profile, and an option of `open/1' replaces the option of the profile.
profile_test() ->
    ?assertEqual(#{direct => true, retry_delay_ms => 10, max_retry_delay_ms => 1000}, deadlift:profile(default)),
    ?assertEqual(#{direct => true, retry_delay_ms => 1, max_retry_delay_ms => 100}, deadlift:profile(ets)),
    Servers = #{proxy => self(), storage => self()},
    ?assertEqual({deadlift_database, maps:merge(deadlift:profile(default), Servers)}, deadlift:open(Servers)),
    ?assertEqual({deadlift_database, maps:merge(deadlift:profile(ets), Servers)}, deadlift:open(Servers#{profile => ets})),
    {deadlift_database, Options} = deadlift:open(Servers#{profile => ets, retry_delay_ms => 3, direct => false}),
    ?assertEqual(#{proxy => self(), storage => self(), direct => false, retry_delay_ms => 3, max_retry_delay_ms => 100}, Options),
    ?assertError(function_clause, deadlift:open(Servers#{profile => unknown})),
    %% With the profile `ets', 30 errors wait for at most 100 ms each, and
    %% for a random part of it.
    Tx = deadlift:create_transaction(deadlift:open(Servers#{profile => ets})),
    Start = erlang:monotonic_time(millisecond),
    [ok = deadlift:on_error(Tx, 1020) || _ <- lists:seq(1, 30)],
    ?assert(erlang:monotonic_time(millisecond) - Start < 3100).
