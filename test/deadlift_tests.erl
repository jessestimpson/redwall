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
        fun(Db) -> {timeout, 60, fun() -> select_random(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> mapped_range(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> mapped_range_conflicts(Db) end} end,
        fun(Db) -> {timeout, 60, fun() -> mapped_range_random(Db) end} end
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

pack(Tuple) -> deadlift_tuple:pack(Tuple).

%% The layout of the tests of FoundationDB (`bindings/c/test/unit/
%% unit_tests.cpp', `GetMappedRange'): an index row has the key
%% `{Prefix, <<"INDEX">>, IndexKey, Id}' and an empty value, and a record
%% has 3 rows with the keys `{Prefix, <<"RECORD">>, Id, Split}'.
index_key(I) -> pack({<<"p">>, <<"INDEX">>, <<"index-", (integer_to_binary(I))/binary>>, I}).
record_key(I, Split) -> pack({<<"p">>, <<"RECORD">>, I, Split}).
record_value(I, Split) -> pack({<<"data-", (integer_to_binary(I))/binary>>, Split}).

load_records(Db, Ids) ->
    deadlift:transactional(Db, fun(Tx) ->
        lists:foreach(
            fun(I) ->
                ok = deadlift:set(Tx, index_key(I), pack({})),
                [ok = deadlift:set(Tx, record_key(I, Split), record_value(I, Split)) || Split <- [0, 1, 2]]
            end,
            Ids
        )
    end).

%% `get_mapped_range/5' with the mapper of FoundationDB, for a read from
%% the ETS tables and for a read by message.
mapped_range(Db = {deadlift_database, Servers}) ->
    Message = {deadlift_database, Servers#{direct => false}},
    Ids = lists:seq(10, 27),
    ok = load_records(Db, Ids),
    %% A record without an index row, an index row without a record, and
    %% rows that are not tuples.
    ok = deadlift:transactional(Db, fun(Tx) ->
        ok = deadlift:set(Tx, index_key(99), pack({})),
        [ok = deadlift:set(Tx, record_key(50, Split), record_value(50, Split)) || Split <- [0, 1, 2]],
        ok = deadlift:set(Tx, pack({<<"q">>, 1}), pack({<<"p">>, <<"RECORD">>, 10})),
        ok = deadlift:set(Tx, pack({<<"q">>, 2}), no_tuple),
        ok = deadlift:set(Tx, {term, key}, pack({}))
    end),
    {Begin, End} = {pack({<<"p">>, <<"INDEX">>}), pack({<<"p">>, <<"INDEY">>})},
    Mapper = {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, <<"{...}">>},
    Record = fun(I) -> [{record_key(I, Split), record_value(I, Split)} || Split <- [0, 1, 2]] end,
    Prefix = fun(I) -> pack({<<"p">>, <<"RECORD">>, I}) end,
    Second = fun(I) -> deadlift_mapper:range(range, Prefix(I)) end,
    ?assertEqual({Prefix(10), Prefix(11)}, Second(10)),
    Row = fun
        (99) -> {{index_key(99), <<>>}, Second(99), []};
        (I) -> {{index_key(I), <<>>}, Second(I), Record(I)}
    end,
    Read = fun(D, From, To, M, Options) -> deadlift:transactional(D, fun(Tx) -> deadlift:get_mapped_range(Tx, From, To, M, Options) end) end,
    lists:foreach(
        fun(D) ->
            All = deadlift:transactional(D, fun(Tx) -> deadlift:get_mapped_range(Tx, Begin, End, Mapper) end),
            ?assertEqual([Row(I) || I <- Ids ++ [99]], All),
            %% The mapper in its packed form.
            ?assertEqual(All, Read(D, Begin, End, pack(Mapper), [])),
            ?assertEqual(All, Read(D, Begin, End, Mapper, [{limit, 0}, {streaming_mode, want_all}])),
            %% A limit, and the next call after the last key.
            Page1 = Read(D, Begin, End, Mapper, [{limit, 7}]),
            ?assertEqual([Row(I) || I <- lists:seq(10, 16)], Page1),
            {{Last, _}, _, _} = lists:last(Page1),
            ?assertEqual([Row(I) || I <- lists:seq(17, 21)], Read(D, <<Last/binary, 0>>, End, Mapper, [{limit, 5}])),
            ?assertEqual([], Read(D, pack({<<"none">>}), pack({<<"nonf">>}), Mapper, [])),
            %% A mapper without `{...}': the second read is a read of one
            %% key.
            Point = {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, 1},
            ?assertEqual(
                [{{index_key(I), <<>>}, {record_key(I, 1), <<(record_key(I, 1))/binary, 0>>}, [{record_key(I, 1), record_value(I, 1)}]} || I <- [10, 11]],
                Read(D, Begin, End, Point, [{limit, 2}])
            ),
            ?assertEqual({{index_key(99), <<>>}, {record_key(99, 1), <<(record_key(99, 1))/binary, 0>>}, []}, lists:last(Read(D, Begin, End, Point, []))),
            %% The mapped key from the value of the row.
            ?assertEqual(
                [{{pack({<<"q">>, 1}), pack({<<"p">>, <<"RECORD">>, 10})}, {Prefix(10), Prefix(11)}, Record(10)}],
                Read(D, pack({<<"q">>, 1}), pack({<<"q">>, 2}), {<<"{V[0]}">>, <<"{V[1]}">>, <<"{V[2]}">>, <<"{...}">>}, [])
            ),
            %% A mapper of literals reads rows that are not tuples.
            ?assertEqual(
                [{{{term, key}, <<>>}, {Prefix(10), Prefix(11)}, Record(10)}],
                Read(D, {term, a}, {term, z}, {<<"p">>, <<"RECORD">>, 10, <<"{...}">>}, [])
            ),
            %% The errors of the mapper.
            Q = {pack({<<"q">>}), pack({<<"r">>})},
            Error = fun({From, To}, M) ->
                Tx = deadlift:create_transaction(D),
                try deadlift:get_mapped_range(Tx, From, To, M) of
                    Rows -> {ok, Rows}
                catch
                    error:{erlfdb_error, Code} ->
                        ?assertError({erlfdb_error, Code}, deadlift:on_error(Tx, Code)),
                        Code
                end
            end,
            ?assertEqual(2043, Error(Q, <<"no tuple">>)),
            ?assertEqual(2032, Error(Q, {<<"{...}">>, <<"x">>})),
            ?assertEqual(2030, Error(Q, {<<"{K[7]}">>})),
            ?assertEqual(2030, Error(Q, {<<"{K[x]}">>})),
            ?assertEqual(2042, Error(Q, {<<"{K[0]}">>, <<"{V[0]}">>})),
            ?assertEqual(2041, Error({{term, a}, {term, z}}, {<<"{K[0]}">>})),
            %% A range without rows gives no error of a row.
            ?assertEqual({ok, []}, Error({pack({<<"none">>}), pack({<<"nonf">>})}, {<<"{K[7]}">>})),
            Tx = deadlift:create_transaction(D),
            ?assertError({unsupported, term_mapper}, deadlift:get_mapped_range(Tx, Begin, End, {term, anything})),
            ?assertError({unsupported, reverse}, deadlift:get_mapped_range(Tx, Begin, End, Mapper, [{reverse, true}])),
            ?assertError({erlfdb_error, 2108}, deadlift:get_mapped_range(Tx, Begin, End, Mapper, [{snapshot, true}])),
            ?assertError({erlfdb_error, 2108}, deadlift:get_mapped_range(deadlift:snapshot(Tx), Begin, End, Mapper)),
            ?assertError(badarg, deadlift:get_mapped_range(Tx, Begin, End, Mapper, [{limit, -1}])),
            ?assertError(badarg, deadlift:get_mapped_range(Tx, Begin, End, "list")),
            ?assertError({invalid_tuple_term, an_atom}, deadlift:get_mapped_range(Tx, Begin, End, {an_atom, <<"{...}">>}))
        end,
        [Db, Message]
    ).

%% The read conflict ranges of `get_mapped_range/5', and the error for a
%% write of the same transaction.
mapped_range_conflicts(Db) ->
    ok = load_records(Db, lists:seq(10, 19)),
    {Begin, End} = {pack({<<"p">>, <<"INDEX">>}), pack({<<"p">>, <<"INDEY">>})},
    Range = {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, <<"{...}">>},
    Point = {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, 1},
    %% `Other' runs in another transaction between the read and the commit
    %% of a transaction. The result is `ok' or the error code of the
    %% commit.
    Commit = fun(Mapper, Options, Other) ->
        Tx = deadlift:create_transaction(Db),
        _ = deadlift:get_mapped_range(Tx, Begin, End, Mapper, Options),
        ok = deadlift:set(Tx, other, 1),
        ok = deadlift:transactional(Db, Other),
        try
            deadlift:wait(deadlift:commit(Tx))
        catch
            error:{erlfdb_error, Code} -> Code
        end
    end,
    Set = fun(Key) -> fun(Tx) -> deadlift:set(Tx, Key, pack({<<"new">>})) end end,
    %% A new index row, a changed record row and a new record row.
    ?assertEqual(1020, Commit(Range, [], Set(index_key(5)))),
    ?assertEqual(1020, Commit(Range, [], Set(record_key(12, 2)))),
    ?assertEqual(1020, Commit(Range, [], Set(record_key(12, 7)))),
    ?assertEqual(1020, Commit(Range, [], fun(Tx) -> deadlift:clear(Tx, record_key(19, 0)) end)),
    %% A record that no index row names.
    ?assertEqual(ok, Commit(Range, [], Set(record_key(40, 0)))),
    ?assertEqual(ok, Commit(Range, [], Set(pack({<<"p">>, <<"RECORD">>})))),
    %% With a limit, the keys after the last row give no conflict.
    ?assertEqual(ok, Commit(Range, [{limit, 3}], Set(index_key(18)))),
    ?assertEqual(ok, Commit(Range, [{limit, 3}], Set(record_key(18, 0)))),
    ?assertEqual(1020, Commit(Range, [{limit, 3}], Set(index_key(11)))),
    ?assertEqual(1020, Commit(Range, [{limit, 3}], Set(record_key(12, 0)))),
    %% A second read of one key.
    ?assertEqual(1020, Commit(Point, [], Set(record_key(13, 1)))),
    ?assertEqual(ok, Commit(Point, [], Set(record_key(13, 0)))),
    %% The transaction has written or cleared a key of the range or of a
    %% second read.
    Own = fun(Mapper, Write) ->
        Tx = deadlift:create_transaction(Db),
        ok = Write(Tx),
        try deadlift:get_mapped_range(Tx, Begin, End, Mapper) of
            Rows -> {ok, length(Rows)}
        catch
            error:{erlfdb_error, Code} -> Code
        end
    end,
    ?assertEqual(2039, Own(Range, Set(index_key(14)))),
    ?assertEqual(2039, Own(Range, Set(record_key(14, 9)))),
    ?assertEqual(2039, Own(Range, fun(Tx) -> deadlift:clear(Tx, record_key(14, 0)) end)),
    ?assertEqual(2039, Own(Range, fun(Tx) -> deadlift:clear_range(Tx, record_key(15, 1), record_key(15, 2)) end)),
    ?assertEqual({ok, 11}, Own(Range, fun(Tx) -> deadlift:clear_range(Tx, record_key(40, 0), record_key(41, 0)) end)),
    ?assertEqual(2039, Own(Point, Set(record_key(14, 1)))),
    ?assertEqual({ok, 11}, Own(Point, Set(record_key(14, 0)))),
    ?assertEqual({ok, 11}, Own(Range, Set(record_key(40, 0)))).

%% 300 random transactions set and clear index rows and records. After
%% each one, `get_mapped_range/5' at the newest version and at an older
%% version of the MVCC window gives the result of `get_range/3' and of the
%% second reads with `get_range/3' and `get/2' at the same read version,
%% for a read from the ETS tables and for a read by message.
mapped_range_random(Db = {deadlift_database, Servers}) ->
    Message = {deadlift_database, Servers#{direct => false}},
    {Begin, End} = {pack({<<"p">>, <<"INDEX">>}), pack({<<"p">>, <<"INDEY">>})},
    Check = fun(D, ReadVersion, Limit) ->
        Mapped = deadlift:create_transaction(D),
        Plain = deadlift:create_transaction(D),
        try
            Version =
                case ReadVersion of
                    newest -> deadlift:get_read_version(Plain);
                    _ -> ReadVersion
                end,
            ok = deadlift:set_read_version(Mapped, Version),
            ok = deadlift:set_read_version(Plain, Version),
            Rows = lists:sublist(deadlift:get_range(Plain, Begin, End), Limit),
            ?assertEqual(
                [
                    begin
                        {_, _, _, I} = deadlift_tuple:unpack(Key),
                        Range = {From, To} = deadlift_mapper:range(range, pack({<<"p">>, <<"RECORD">>, I})),
                        {{Key, Value}, Range, deadlift:get_range(Plain, From, To)}
                    end
                 || {Key, Value} <- Rows
                ],
                deadlift:get_mapped_range(Mapped, Begin, End, {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, <<"{...}">>}, [{limit, Limit}])
            ),
            ?assertEqual(
                [
                    begin
                        {_, _, _, I} = deadlift_tuple:unpack(Key),
                        Second = record_key(I, 1),
                        {{Key, Value}, {Second, <<Second/binary, 0>>}, [{Second, Found} || Found <- [deadlift:get(Plain, Second)], Found =/= not_found]}
                    end
                 || {Key, Value} <- Rows
                ],
                deadlift:get_mapped_range(Mapped, Begin, End, {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, 1}, [{limit, Limit}])
            ),
            ok
        catch
            %% The older version left the MVCC window.
            error:{erlfdb_error, 1007} -> too_old
        end
    end,
    Rand0 = rand:seed_s(exsss, {4, 5, 6}),
    {_, Checks} = lists:foldl(
        fun(N, {Rand, Acc}) ->
            {R, Rand1} = rand:uniform_s(1 bsl 30, Rand),
            I = R rem 12,
            ok = deadlift:transactional(Db, fun(Tx) ->
                case (R bsr 8) rem 5 of
                    0 -> deadlift:clear(Tx, index_key(I));
                    1 -> deadlift:clear_range(Tx, record_key(I, 0), record_key(I, 2));
                    2 -> deadlift:set(Tx, index_key(I), pack({N}));
                    _ -> deadlift:set(Tx, record_key(I, (R bsr 12) rem 3), pack({N}))
                end
            end),
            Version = deadlift:transactional(Db, fun(Tx) -> deadlift:get_read_version(Tx) end),
            Results = [Check(D, V, Limit) || D <- [Db, Message], V <- [newest, Version - (R bsr 16) rem 15], Limit <- [(R bsr 20) rem 4 + 1, 100]],
            {Rand1, Results ++ Acc}
        end,
        {Rand0, []},
        lists:seq(1, 300)
    ),
    %% Most reads are inside the MVCC window.
    ?assert(length([ok || ok <- Checks]) > 1500, length([ok || ok <- Checks])).
