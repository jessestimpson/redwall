-module(redwall_tests).

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
        fun(Db) -> {timeout, 60, fun() -> mapped_range_random(Db) end} end,
        fun(Db) -> {timeout, 60, fun() -> idempotency(Db) end} end,
        fun(Db) -> {timeout, 30, fun() -> keys(Db) end} end
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(redwall),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([redwall_c, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), Opts),
    {ok, #{count := 0}} = await_read(Ids, 50),
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Proxy} = redwall_epoch:start(Options),
    {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5, mvcc_window => 20}),
    redwall:open(#{proxy => Proxy, storage => Storage}).

cleanup(_Db) ->
    ok = application:stop(redwall),
    ok = application:stop(peace).

await_read(_Ids, 0) ->
    error(no_leader);
await_read(Ids, N) ->
    case redwall_cstate:read(Ids, 1000) of
        {ok, State} ->
            {ok, State};
        _ ->
            timer:sleep(100),
            await_read(Ids, N - 1)
    end.

%% `select/4' evaluates a match spec on a key range. The result is the
%% result of `ets:match_spec_run/2' on the result of `get_range/3', for a
%% read from the ETS tables and for a read by message.
select(Db = {redwall_database, Servers}) ->
    Message = {redwall_database, Servers#{direct => false}},
    ok = redwall:transactional(Db, fun(Tx) ->
        lists:foreach(
            fun(I) ->
                Status =
                    case I rem 10 of
                        0 -> active;
                        _ -> idle
                    end,
                ok = redwall:set(Tx, {user, I}, #{status => Status, n => I, blob => binary:copy(<<0>>, 100)})
            end,
            lists:seq(1, 300)
        ),
        redwall:set(Tx, {zone, 1}, #{status => active, n => 0})
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
        redwall:transactional(D, fun(Tx) ->
            Rows = redwall:get_range(Tx, Begin, End),
            lists:foreach(
                fun(Spec) ->
                    Expected = ets:match_spec_run(Rows, ets:match_spec_compile(Spec)),
                    ?assertEqual(Expected, redwall:select(Tx, Begin, End, Spec), Spec),
                    ?assertEqual(lists:sublist(Expected, 3), redwall:select(Tx, Begin, End, Spec, [{limit, 3}]), Spec)
                end,
                Specs
            ),
            length(Rows)
        end)
    end,
    [?assertEqual(Count, Compare(D, Begin, End)) || D <- [Db, Message], {Begin, End, Count} <- [{{user, 0}, {user, 999}, 300}, {{user, 50}, {user, 150}, 100}, {a, z, 0}, {{user, 0}, {zone, 9}, 301}]],
    ?assertEqual([{I, I} || I <- lists:seq(110, 300, 10)], redwall:transactional(Db, fun(Tx) -> redwall:select(Tx, {user, 0}, {user, 999}, Active) end)),
    %% The function sees the sets and the clears of its transaction.
    lists:foreach(
        fun(D) ->
            {Limited, All} = redwall:transactional(D, fun(Tx) ->
                ok = redwall:set(Tx, {user, 105}, #{status => active, n => 105}),
                ok = redwall:clear(Tx, {user, 120}),
                ok = redwall:clear_range(Tx, {user, 125}, {user, 145}),
                ok = redwall:set(Tx, {user, 130}, #{status => idle, n => 130}),
                {redwall:select(Tx, {user, 0}, {user, 999}, Active, [{limit, 2}]), redwall:select(Tx, {user, 0}, {user, 999}, Active)}
            end),
            ?assertEqual([{105, 105}, {110, 110}], Limited),
            ?assertEqual([{105, 105}, {110, 110}, {150, 150}], lists:sublist(All, 3))
        end,
        [Db, Message]
    ),
    ok = redwall:transactional(Db, fun(Tx) ->
        ok = redwall:clear(Tx, {user, 105}),
        ok = redwall:set(Tx, {user, 120}, #{status => active, n => 120}),
        lists:foreach(fun(I) -> redwall:set(Tx, {user, I}, #{status => active, n => I}) end, [130, 140])
    end),
    %% A write of another transaction to a key of the range that does not
    %% match is a conflict.
    Tx1 = redwall:create_transaction(Db),
    ?assertEqual(20, length(redwall:select(Tx1, {user, 0}, {user, 999}, Active))),
    ok = redwall:set(Tx1, other, 1),
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, {user, 101}, #{status => idle, n => 101}) end),
    ?assertError({erlfdb_error, 1020}, redwall:wait(redwall:commit(Tx1))),
    %% A select of a snapshot gives no conflict.
    Tx2 = redwall:create_transaction(Db),
    ?assertEqual(20, length(redwall:select(redwall:snapshot(Tx2), {user, 0}, {user, 999}, Active))),
    ok = redwall:set(Tx2, other, 2),
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, {user, 102}, #{status => idle, n => 102}) end),
    ok = redwall:wait(redwall:commit(Tx2)),
    %% A term that is not a match spec.
    Tx3 = redwall:create_transaction(Db),
    ?assertError(badarg, redwall:select(Tx3, a, z, [{no_spec}])),
    ?assertError(badarg, redwall:select(Tx3, a, z, no_list)).

%% 400 random transactions set and clear some of 30 keys. After each one,
%% `select/4' at the newest version and at an older version of the MVCC
%% window gives the result of `ets:match_spec_run/2' on `get_range/3', for
%% a read from the ETS tables and for a read by message.
select_random(Db = {redwall_database, Servers}) ->
    Message = {redwall_database, Servers#{direct => false}},
    Specs = [
        [{{'$1', '$2'}, [{'>', '$2', 500}], [{{'$1', '$2'}}]}],
        [{{'$1', '$2'}, [{'=:=', {'rem', '$1', 3}, 0}], ['$2']}],
        [{'_', [], ['$_']}]
    ],
    Check = fun(D, ReadVersion) ->
        Tx = redwall:create_transaction(D),
        _ = is_integer(ReadVersion) andalso redwall:set_read_version(Tx, ReadVersion),
        try
            Rows = redwall:get_range(Tx, 5, 25),
            [?assertEqual(ets:match_spec_run(Rows, ets:match_spec_compile(Spec)), redwall:select(Tx, 5, 25, Spec)) || Spec <- Specs],
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
            ok = redwall:transactional(Db, fun(Tx) ->
                case Kind of
                    1 -> redwall:clear(Tx, K1);
                    2 -> redwall:clear_range(Tx, min(K1, K2), max(K1, K2));
                    _ -> redwall:set(Tx, K1, Value)
                end
            end),
            Newest = redwall:transactional(Db, fun(Tx) -> redwall:get_read_version(Tx) end),
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
    ok = redwall:transactional(Db, fun(Tx) ->
        ?assertEqual(not_found, redwall:wait(redwall:get(Tx, a))),
        ok = redwall:set(Tx, a, 1),
        ok = redwall:set(Tx, b, 2),
        redwall:set(Tx, c, 3)
    end),
    ?assertEqual(
        {1, [{a, 1}, {b, 2}, {c, 3}], [{b, 2}]},
        redwall:transactional(Db, fun(Tx) ->
            {redwall:wait(redwall:get(Tx, a)), redwall:get_range(Tx, a, z), redwall:get_range(Tx, b, c)}
        end)
    ),
    ok = redwall:transactional(Db, fun(Tx) ->
        ok = redwall:clear(Tx, a),
        redwall:clear_range(Tx, c, z)
    end),
    ?assertEqual([{b, 2}], redwall:transactional(Db, fun(Tx) -> redwall:get_range(Tx, a, z) end)),
    %% A transaction without `transactional/2'. The recovery transaction has
    %% the LSN 1, and the two commits above have the LSNs 2 and 3.
    Tx = redwall:create_transaction(Db),
    ?assert(redwall:is_read_only(Tx)),
    ?assertEqual(3, redwall:get_read_version(Tx)),
    ok = redwall:set(Tx, d, 4),
    ?assertNot(redwall:is_read_only(Tx)),
    ok = redwall:wait(redwall:commit(Tx)),
    ?assertEqual(4, redwall:get_committed_version(Tx)),
    ?assertEqual(<<4:64, 0:16>>, redwall:get_versionstamp(Tx)),
    %% A transaction without a mutation sends no commit.
    Tx2 = redwall:create_transaction(Db),
    ?assertEqual(4, redwall:wait(redwall:get(Tx2, d))),
    ok = redwall:commit(Tx2),
    ?assertEqual(-1, redwall:get_committed_version(Tx2)).

%% A read returns the writes of the same transaction, and a range read
%% merges them with the stored keys.
own_writes(Db) ->
    ok = redwall:transactional(Db, fun(Tx) -> [redwall:set(Tx, K, stored) || K <- [a, b, c, d, e]], ok end),
    redwall:transactional(Db, fun(Tx) ->
        ok = redwall:set(Tx, b, mine),
        ok = redwall:clear(Tx, c),
        ok = redwall:set(Tx, bb, new),
        ?assertEqual(mine, redwall:get(Tx, b)),
        ?assertEqual(not_found, redwall:get(Tx, c)),
        ?assertEqual(new, redwall:get(Tx, bb)),
        ?assertEqual([{a, stored}, {b, mine}, {bb, new}, {d, stored}, {e, stored}], redwall:get_range(Tx, a, z)),
        %% A clear of a range, and a set of a key of the range after it.
        ok = redwall:clear_range(Tx, b, e),
        ?assertEqual(not_found, redwall:get(Tx, b)),
        ?assertEqual(not_found, redwall:get(Tx, d)),
        ok = redwall:set(Tx, d, again),
        ?assertEqual(again, redwall:get(Tx, d)),
        ?assertEqual([{a, stored}, {d, again}, {e, stored}], redwall:get_range(Tx, a, z))
    end),
    ?assertEqual(
        [{a, stored}, {d, again}, {e, stored}],
        redwall:transactional(Db, fun(Tx) -> redwall:get_range(Tx, a, z) end)
    ).

%% Two transactions read and write one key. The second commit has a
%% conflict, with the error code 1020 of FoundationDB.
conflict(Db) ->
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, x, 0) end),
    Tx1 = redwall:create_transaction(Db),
    Tx2 = redwall:create_transaction(Db),
    0 = redwall:get(Tx1, x),
    0 = redwall:get(Tx2, x),
    ok = redwall:set(Tx1, x, 1),
    ok = redwall:set(Tx2, x, 2),
    ok = redwall:commit(Tx1),
    ?assertError({erlfdb_error, 1020}, redwall:commit(Tx2)),
    %% A transaction that wrote without a read has no conflict.
    Tx3 = redwall:create_transaction(Db),
    Tx4 = redwall:create_transaction(Db),
    ok = redwall:set(Tx3, x, 3),
    ok = redwall:set(Tx4, x, 4),
    ok = redwall:commit(Tx3),
    ok = redwall:commit(Tx4),
    ?assertEqual(4, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, x) end)).

%% A transaction reads a range without the key `m'. Another transaction
%% writes `m'. The first transaction has a conflict.
range_conflict(Db) ->
    Tx1 = redwall:create_transaction(Db),
    ?assertEqual([], redwall:get_range(Tx1, a, z)),
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, m, 1) end),
    ok = redwall:set(Tx1, other, 1),
    ?assertError({erlfdb_error, 1020}, redwall:commit(Tx1)),
    %% A clear of a range is a conflict for a reader of a key of the range.
    Tx2 = redwall:create_transaction(Db),
    1 = redwall:get(Tx2, m),
    ok = redwall:transactional(Db, fun(Tx) -> redwall:clear_range(Tx, a, z) end),
    ok = redwall:set(Tx2, other, 2),
    ?assertError({erlfdb_error, 1020}, redwall:commit(Tx2)),
    %% A read conflict key that the transaction adds without a read.
    Tx3 = redwall:create_transaction(Db),
    _ = redwall:get_read_version(Tx3),
    ok = redwall:add_read_conflict_key(Tx3, k),
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, k, 1) end),
    ok = redwall:set(Tx3, other, 3),
    ?assertError({erlfdb_error, 1020}, redwall:commit(Tx3)).

%% A read of a snapshot adds no read conflict key.
snapshot(Db) ->
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, x, 0) end),
    Tx1 = redwall:create_transaction(Db),
    Snapshot = redwall:snapshot(Tx1),
    ?assertEqual(0, redwall:get(Snapshot, x)),
    ?assertEqual([{x, 0}], redwall:get_range(Snapshot, a, z)),
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, x, 1) end),
    ok = redwall:set(Tx1, y, 1),
    ok = redwall:commit(Tx1),
    %% The snapshot has the writes of its transaction.
    Tx2 = redwall:create_transaction(Db),
    ok = redwall:set(Tx2, x, mine),
    ?assertEqual(mine, redwall:get(redwall:snapshot(Tx2), x)).

%% `transactional/2' runs the function again after a conflict. 20 processes
%% add 1 to one key 10 times each.
retry(Db) ->
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, counter, 0) end),
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            Attempts = lists:sum([
                begin
                    Count = counters:new(1, []),
                    ok = redwall:transactional(Db, fun(Tx) ->
                        counters:add(Count, 1, 1),
                        redwall:set(Tx, counter, redwall:get(Tx, counter) + 1)
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
    ?assertEqual(200, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, counter) end)),
    %% At least one transaction had a conflict and ran again.
    ?assert(Attempts > 200, Attempts).

%% A second process uses the transaction of the first process.
two_processes(Db) ->
    Tx = redwall:create_transaction(Db),
    ok = redwall:set(Tx, a, 1),
    Self = self(),
    Pid = spawn_link(fun() ->
        ok = redwall:set(Tx, b, 2),
        Self ! {self(), redwall:get(Tx, a)}
    end),
    receive
        {Pid, Value} -> ?assertEqual(1, Value)
    after 5000 -> error(no_answer)
    end,
    ?assertEqual(2, redwall:get(Tx, b)),
    ok = redwall:commit(Tx),
    ?assertEqual([{a, 1}, {b, 2}], redwall:transactional(Db, fun(T) -> redwall:get_range(T, a, z) end)).

%% Reset, cancel and the errors that `on_error/2' does not accept.
lifecycle(Db) ->
    Tx = redwall:create_transaction(Db),
    ok = redwall:set(Tx, a, 1),
    ok = redwall:reset(Tx),
    ?assert(redwall:is_read_only(Tx)),
    ?assertEqual(not_found, redwall:get(Tx, a)),
    ok = redwall:cancel(Tx),
    ?assertError({erlfdb_error, 1025}, redwall:get(Tx, a)),
    ?assertError({erlfdb_error, 1025}, redwall:set(Tx, a, 1)),
    %% `on_error/2' raises a code that is not a reason to run again.
    ?assertError({erlfdb_error, 1025}, redwall:on_error(Tx, 1025)),
    ?assertError({erlfdb_error, 2004}, redwall:on_error(Tx, {erlfdb_error, 2004})),
    %% `on_error/2' resets the transaction for a conflict.
    Tx2 = redwall:create_transaction(Db),
    ok = redwall:set(Tx2, a, 1),
    ok = redwall:on_error(Tx2, 1020),
    ?assert(redwall:is_read_only(Tx2)),
    %% An exception of the function that is not an error of redwall leaves
    %% `transactional/2', and the transaction is not committed.
    ?assertError(
        my_error,
        redwall:transactional(Db, fun(T) ->
            redwall:set(T, a, 1),
            error(my_error)
        end)
    ),
    ?assertEqual(not_found, redwall:transactional(Db, fun(T) -> redwall:get(T, a) end)).

%% The StorageServer has an MVCC window of 20 versions. A transaction with
%% an old read version has the error 1007 for a read, and `transactional/2'
%% runs the function again with a new read version.
too_old(Db) ->
    Tx = redwall:create_transaction(Db),
    Old = redwall:get_read_version(Tx),
    [ok = redwall:transactional(Db, fun(T) -> redwall:set(T, x, N) end) || N <- lists:seq(1, 40)],
    %% A read at a new read version returns when the StorageServer has
    %% applied the 40 records.
    ?assertEqual(40, redwall:transactional(Db, fun(T) -> redwall:get(T, x) end)),
    ?assertError({erlfdb_error, 1007}, redwall:get(Tx, x)),
    ?assertError({erlfdb_error, 1007}, redwall:get_range(Tx, a, z)),
    Count = counters:new(1, []),
    ?assertEqual(
        40,
        redwall:transactional(Db, fun(T) ->
            counters:add(Count, 1, 1),
            _ = counters:get(Count, 1) =:= 1 andalso redwall:set_read_version(T, Old),
            redwall:get(T, x)
        end)
    ),
    ?assertEqual(2, counters:get(Count, 1)).

%% Three StorageServers. A read has the same result with direct reads and
%% with reads by message. The reads continue after the stop of two of the
%% three StorageServers, and fail with 1037 after the stop of the third.
replicas({redwall_database, Servers = #{storage := First}}) ->
    Options = #{coordinators => coordinators(First), poll_ms => 5, mvcc_window => 20},
    {ok, Second} = redwall_storage:start(Options),
    {ok, Third} = redwall_storage:start(Options),
    Direct = redwall:open(Servers#{storage := [First, Second, Third]}),
    Message = redwall:open(Servers#{storage := fun() -> [First, Second, Third] end, direct => false}),
    Read = fun(Db) -> redwall:transactional(Db, fun(Tx) -> {redwall:get(Tx, a), redwall:get_range(Tx, a, z)} end) end,
    %% Several transactions, so that the reads start at each of the three.
    lists:foreach(
        fun(N) ->
            ok = redwall:transactional(Direct, fun(Tx) -> redwall:set(Tx, a, N) end),
            ?assertEqual({N, [{a, N}]}, Read(Direct)),
            ?assertEqual({N, [{a, N}]}, Read(Message))
        end,
        lists:seq(1, 6)
    ),
    ok = redwall_storage:stop(First),
    ok = redwall_storage:stop(Third),
    lists:foreach(
        fun(N) ->
            ok = redwall:transactional(Direct, fun(Tx) -> redwall:set(Tx, a, N) end),
            ?assertEqual({N, [{a, N}]}, Read(Direct)),
            ?assertEqual({N, [{a, N}]}, Read(Message))
        end,
        lists:seq(7, 12)
    ),
    ok = redwall_storage:stop(Second),
    Tx = redwall:create_transaction(Direct),
    ?assertError({erlfdb_error, 1037}, redwall:get(Tx, a)),
    ?assertError({erlfdb_error, 1037}, redwall:get(redwall:create_transaction(Message), a)),
    %% A database without a StorageServer.
    None = redwall:open(Servers#{storage := fun() -> [] end}),
    ?assertError({erlfdb_error, 1037}, redwall:get(redwall:create_transaction(None), a)).

%% A second StorageServer that does not apply records: its coordinated
%% state has no epoch. A read does not wait for it when the first
%% StorageServer has the read version. With the second StorageServer
%% alone, the read has the error 1009 after 5 seconds.
replica_behind({redwall_database, Servers = #{storage := First}}) ->
    {ok, Empty} = redwall_cstate_local:start(#{}),
    {ok, Behind} = redwall_storage:start(#{coordinators => {local, Empty}, poll_ms => 5}),
    Db = redwall:open(Servers#{storage := [First, Behind]}),
    Message = redwall:open(Servers#{storage := [Behind, First], direct => false}),
    lists:foreach(
        fun(N) ->
            ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, N) end),
            ?assertEqual(N, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end))
        end,
        lists:seq(1, 4)
    ),
    %% By message, the read waits for one half of 5 seconds at the
    %% StorageServer that is behind, when that one is the first of the
    %% order.
    ?assertEqual(4, redwall:transactional(Message, fun(Tx) -> redwall:get(Tx, a) end)),
    Alone = redwall:open(Servers#{storage := Behind}),
    Tx = redwall:create_transaction(Alone),
    ok = redwall:set_read_version(Tx, redwall:transactional(Db, fun(T) -> redwall:get_read_version(T) end)),
    ?assertError({erlfdb_error, 1009}, redwall:get(Tx, a)).

%% The coordinators of a StorageServer.
coordinators(Storage) ->
    element(2, sys:get_state(Storage)).

%% The options `retry_delay_ms' and `max_retry_delay_ms' of `open/1' set
%% the first backoff and the highest backoff of `on_error/2'. The test
%% needs no cluster, because `on_error/2' sends no message.
retry_delay_test() ->
    Elapsed = fun(Options, Errors) ->
        Db = redwall:open(Options#{proxy => self(), storage => self()}),
        Tx = redwall:create_transaction(Db),
        Start = erlang:monotonic_time(millisecond),
        [ok = redwall:on_error(Tx, 1020) || _ <- lists:seq(1, Errors)],
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
    ?assertEqual(#{direct => true, retry_delay_ms => 10, max_retry_delay_ms => 1000}, redwall:profile(default)),
    ?assertEqual(#{direct => true, retry_delay_ms => 1, max_retry_delay_ms => 100}, redwall:profile(ets)),
    Servers = #{proxy => self(), storage => self()},
    ?assertEqual({redwall_database, maps:merge(redwall:profile(default), Servers)}, redwall:open(Servers)),
    ?assertEqual({redwall_database, maps:merge(redwall:profile(ets), Servers)}, redwall:open(Servers#{profile => ets})),
    {redwall_database, Options} = redwall:open(Servers#{profile => ets, retry_delay_ms => 3, direct => false}),
    ?assertEqual(#{proxy => self(), storage => self(), direct => false, retry_delay_ms => 3, max_retry_delay_ms => 100}, Options),
    ?assertError(function_clause, redwall:open(Servers#{profile => unknown})),
    %% With the profile `ets', 30 errors wait for at most 100 ms each, and
    %% for a random part of it.
    Tx = redwall:create_transaction(redwall:open(Servers#{profile => ets})),
    Start = erlang:monotonic_time(millisecond),
    [ok = redwall:on_error(Tx, 1020) || _ <- lists:seq(1, 30)],
    ?assert(erlang:monotonic_time(millisecond) - Start < 3100).

pack(Tuple) -> redwall_tuple:pack(Tuple).

%% The layout of the tests of FoundationDB (`bindings/c/test/unit/
%% unit_tests.cpp', `GetMappedRange'): an index row has the key
%% `{Prefix, <<"INDEX">>, IndexKey, Id}' and an empty value, and a record
%% has 3 rows with the keys `{Prefix, <<"RECORD">>, Id, Split}'.
index_key(I) -> pack({<<"p">>, <<"INDEX">>, <<"index-", (integer_to_binary(I))/binary>>, I}).
record_key(I, Split) -> pack({<<"p">>, <<"RECORD">>, I, Split}).
record_value(I, Split) -> pack({<<"data-", (integer_to_binary(I))/binary>>, Split}).

load_records(Db, Ids) ->
    redwall:transactional(Db, fun(Tx) ->
        lists:foreach(
            fun(I) ->
                ok = redwall:set(Tx, index_key(I), pack({})),
                [ok = redwall:set(Tx, record_key(I, Split), record_value(I, Split)) || Split <- [0, 1, 2]]
            end,
            Ids
        )
    end).

%% `get_mapped_range/5' with the mapper of FoundationDB, for a read from
%% the ETS tables and for a read by message.
mapped_range(Db = {redwall_database, Servers}) ->
    Message = {redwall_database, Servers#{direct => false}},
    Ids = lists:seq(10, 27),
    ok = load_records(Db, Ids),
    %% A record without an index row, an index row without a record, and
    %% rows that are not tuples.
    ok = redwall:transactional(Db, fun(Tx) ->
        ok = redwall:set(Tx, index_key(99), pack({})),
        [ok = redwall:set(Tx, record_key(50, Split), record_value(50, Split)) || Split <- [0, 1, 2]],
        ok = redwall:set(Tx, pack({<<"q">>, 1}), pack({<<"p">>, <<"RECORD">>, 10})),
        ok = redwall:set(Tx, pack({<<"q">>, 2}), no_tuple),
        ok = redwall:set(Tx, {term, key}, pack({}))
    end),
    {Begin, End} = {pack({<<"p">>, <<"INDEX">>}), pack({<<"p">>, <<"INDEY">>})},
    Mapper = {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, <<"{...}">>},
    Record = fun(I) -> [{record_key(I, Split), record_value(I, Split)} || Split <- [0, 1, 2]] end,
    Prefix = fun(I) -> pack({<<"p">>, <<"RECORD">>, I}) end,
    Second = fun(I) -> redwall_mapper:range(range, Prefix(I)) end,
    ?assertEqual({Prefix(10), Prefix(11)}, Second(10)),
    Row = fun
        (99) -> {{index_key(99), <<>>}, Second(99), []};
        (I) -> {{index_key(I), <<>>}, Second(I), Record(I)}
    end,
    Read = fun(D, From, To, M, Options) -> redwall:transactional(D, fun(Tx) -> redwall:get_mapped_range(Tx, From, To, M, Options) end) end,
    lists:foreach(
        fun(D) ->
            All = redwall:transactional(D, fun(Tx) -> redwall:get_mapped_range(Tx, Begin, End, Mapper) end),
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
                Tx = redwall:create_transaction(D),
                try redwall:get_mapped_range(Tx, From, To, M) of
                    Rows -> {ok, Rows}
                catch
                    error:{erlfdb_error, Code} ->
                        ?assertError({erlfdb_error, Code}, redwall:on_error(Tx, Code)),
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
            Tx = redwall:create_transaction(D),
            ?assertError({unsupported, term_mapper}, redwall:get_mapped_range(Tx, Begin, End, {term, anything})),
            ?assertError({unsupported, reverse}, redwall:get_mapped_range(Tx, Begin, End, Mapper, [{reverse, true}])),
            ?assertError({erlfdb_error, 2108}, redwall:get_mapped_range(Tx, Begin, End, Mapper, [{snapshot, true}])),
            ?assertError({erlfdb_error, 2108}, redwall:get_mapped_range(redwall:snapshot(Tx), Begin, End, Mapper)),
            ?assertError(badarg, redwall:get_mapped_range(Tx, Begin, End, Mapper, [{limit, -1}])),
            ?assertError(badarg, redwall:get_mapped_range(Tx, Begin, End, "list")),
            ?assertError({invalid_tuple_term, an_atom}, redwall:get_mapped_range(Tx, Begin, End, {an_atom, <<"{...}">>}))
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
        Tx = redwall:create_transaction(Db),
        _ = redwall:get_mapped_range(Tx, Begin, End, Mapper, Options),
        ok = redwall:set(Tx, other, 1),
        ok = redwall:transactional(Db, Other),
        try
            redwall:wait(redwall:commit(Tx))
        catch
            error:{erlfdb_error, Code} -> Code
        end
    end,
    Set = fun(Key) -> fun(Tx) -> redwall:set(Tx, Key, pack({<<"new">>})) end end,
    %% A new index row, a changed record row and a new record row.
    ?assertEqual(1020, Commit(Range, [], Set(index_key(5)))),
    ?assertEqual(1020, Commit(Range, [], Set(record_key(12, 2)))),
    ?assertEqual(1020, Commit(Range, [], Set(record_key(12, 7)))),
    ?assertEqual(1020, Commit(Range, [], fun(Tx) -> redwall:clear(Tx, record_key(19, 0)) end)),
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
        Tx = redwall:create_transaction(Db),
        ok = Write(Tx),
        try redwall:get_mapped_range(Tx, Begin, End, Mapper) of
            Rows -> {ok, length(Rows)}
        catch
            error:{erlfdb_error, Code} -> Code
        end
    end,
    ?assertEqual(2039, Own(Range, Set(index_key(14)))),
    ?assertEqual(2039, Own(Range, Set(record_key(14, 9)))),
    ?assertEqual(2039, Own(Range, fun(Tx) -> redwall:clear(Tx, record_key(14, 0)) end)),
    ?assertEqual(2039, Own(Range, fun(Tx) -> redwall:clear_range(Tx, record_key(15, 1), record_key(15, 2)) end)),
    ?assertEqual({ok, 11}, Own(Range, fun(Tx) -> redwall:clear_range(Tx, record_key(40, 0), record_key(41, 0)) end)),
    ?assertEqual(2039, Own(Point, Set(record_key(14, 1)))),
    ?assertEqual({ok, 11}, Own(Point, Set(record_key(14, 0)))),
    ?assertEqual({ok, 11}, Own(Range, Set(record_key(40, 0)))).

%% 300 random transactions set and clear index rows and records. After
%% each one, `get_mapped_range/5' at the newest version and at an older
%% version of the MVCC window gives the result of `get_range/3' and of the
%% second reads with `get_range/3' and `get/2' at the same read version,
%% for a read from the ETS tables and for a read by message.
mapped_range_random(Db = {redwall_database, Servers}) ->
    Message = {redwall_database, Servers#{direct => false}},
    {Begin, End} = {pack({<<"p">>, <<"INDEX">>}), pack({<<"p">>, <<"INDEY">>})},
    Check = fun(D, ReadVersion, Limit) ->
        Mapped = redwall:create_transaction(D),
        Plain = redwall:create_transaction(D),
        try
            Version =
                case ReadVersion of
                    newest -> redwall:get_read_version(Plain);
                    _ -> ReadVersion
                end,
            ok = redwall:set_read_version(Mapped, Version),
            ok = redwall:set_read_version(Plain, Version),
            Rows = lists:sublist(redwall:get_range(Plain, Begin, End), Limit),
            ?assertEqual(
                [
                    begin
                        {_, _, _, I} = redwall_tuple:unpack(Key),
                        Range = {From, To} = redwall_mapper:range(range, pack({<<"p">>, <<"RECORD">>, I})),
                        {{Key, Value}, Range, redwall:get_range(Plain, From, To)}
                    end
                 || {Key, Value} <- Rows
                ],
                redwall:get_mapped_range(Mapped, Begin, End, {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, <<"{...}">>}, [{limit, Limit}])
            ),
            ?assertEqual(
                [
                    begin
                        {_, _, _, I} = redwall_tuple:unpack(Key),
                        Second = record_key(I, 1),
                        {{Key, Value}, {Second, <<Second/binary, 0>>}, [{Second, Found} || Found <- [redwall:get(Plain, Second)], Found =/= not_found]}
                    end
                 || {Key, Value} <- Rows
                ],
                redwall:get_mapped_range(Mapped, Begin, End, {<<"p">>, <<"RECORD">>, <<"{K[3]}">>, 1}, [{limit, Limit}])
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
            ok = redwall:transactional(Db, fun(Tx) ->
                case (R bsr 8) rem 5 of
                    0 -> redwall:clear(Tx, index_key(I));
                    1 -> redwall:clear_range(Tx, record_key(I, 0), record_key(I, 2));
                    2 -> redwall:set(Tx, index_key(I), pack({N}));
                    _ -> redwall:set(Tx, record_key(I, (R bsr 12) rem 3), pack({N}))
                end
            end),
            Version = redwall:transactional(Db, fun(Tx) -> redwall:get_read_version(Tx) end),
            Results = [Check(D, V, Limit) || D <- [Db, Message], V <- [newest, Version - (R bsr 16) rem 15], Limit <- [(R bsr 20) rem 4 + 1, 100]],
            {Rand1, Results ++ Acc}
        end,
        {Rand0, []},
        lists:seq(1, 300)
    ),
    %% Most reads are inside the MVCC window.
    ?assert(length([ok || ok <- Checks]) > 1500, length([ok || ok <- Checks])).

%% A process in the place of the Proxy for a test. It sends each request
%% to the Proxy `Real'. `Plan' is a function of the number of the commit:
%%
%% <ul>
%% <li>`forward': the client receives the answer of the Proxy.</li>
%% <li>`commit_and_stop': the Proxy commits the transaction, and this
%%   process stops without an answer. The client has an unknown
%%   result for a committed transaction.</li>
%% <li>`stop': this process stops before it sends the commit. The client
%%   has an unknown result for a transaction that is not committed.</li>
%% </ul>
relay(Real, Counter, Plan) ->
    receive
        {'$gen_call', From, {commit, Transaction}} ->
            case Plan(ets:update_counter(Counter, commits, 1, {commits, 0})) of
                forward ->
                    gen_server:reply(From, gen_server:call(Real, {commit, Transaction})),
                    relay(Real, Counter, Plan);
                commit_and_stop ->
                    {ok, _LSN, _Index} = gen_server:call(Real, {commit, Transaction}),
                    exit(stopped);
                stop ->
                    exit(stopped)
            end;
        {'$gen_call', From, Request} ->
            gen_server:reply(From, gen_server:call(Real, Request)),
            relay(Real, Counter, Plan);
        {'$gen_cast', Message} ->
            gen_server:cast(Real, Message),
            relay(Real, Counter, Plan)
    end.

%% A database whose Proxy is a `relay/3' process. A new process takes the
%% place of a stopped one.
relayed({redwall_database, Servers = #{proxy := Real}}, Counter, Plan, Options) ->
    Proxy = fun() ->
        case ets:lookup(Counter, relay) of
            [{relay, Pid}] when is_pid(Pid) ->
                case is_process_alive(Pid) of
                    true ->
                        Pid;
                    false ->
                        New = spawn(fun() -> relay(Real, Counter, Plan) end),
                        true = ets:insert(Counter, {relay, New}),
                        New
                end;
            [] ->
                New = spawn(fun() -> relay(Real, Counter, Plan) end),
                true = ets:insert(Counter, {relay, New}),
                New
        end
    end,
    redwall:open(maps:merge(Servers#{proxy => Proxy, retry_delay_ms => 1, max_retry_delay_ms => 5}, Options)).

%% A transaction with an idempotency id is applied once when its commit
%% has no answer. Without the id, `transactional/2' applies a committed
%% transaction a second time.
idempotency(Db = {redwall_database, #{proxy := Real}}) ->
    Counter = ets:new(counter, [public]),
    %% 2 commits in 7 are committed without an answer, and 1 commit in 7
    %% is not sent. The commits that determine a result are counted too.
    Plan = fun(N) ->
        case N rem 7 of
            1 -> commit_and_stop;
            4 -> commit_and_stop;
            5 -> stop;
            _ -> forward
        end
    end,
    Increment = fun(D, Key) ->
        redwall:transactional(D, fun(Tx) ->
            Value =
                case redwall:get(Tx, Key) of
                    not_found -> 0;
                    Found -> Found
                end,
            ok = redwall:set(Tx, Key, Value + 1),
            Value + 1
        end)
    end,
    Read = fun(Key) -> redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, Key) end) end,
    %% Without an id, a committed transaction runs again.
    Plain = relayed(Db, Counter, Plan, #{}),
    _ = [Increment(Plain, plain) || _ <- lists:seq(1, 100)],
    ?assert(Read(plain) > 110, Read(plain)),
    %% With an id for every transaction of the database.
    Safe = relayed(Db, Counter, Plan, #{automatic_idempotency => true}),
    Results = [Increment(Safe, safe) || _ <- lists:seq(1, 200)],
    ?assertEqual(lists:seq(1, 200), Results),
    ?assertEqual(200, Read(safe)),
    %% With the option of a transaction.
    lists:foreach(
        fun(N) ->
            Tx = redwall:create_transaction(Plain),
            ok = redwall:set_option(Tx, automatic_idempotency),
            Retry = fun Attempt() ->
                try
                    Value =
                        case redwall:get(Tx, option) of
                            not_found -> 0;
                            Found -> Found
                        end,
                    ok = redwall:set(Tx, option, Value + 1),
                    ok = redwall:commit(Tx)
                catch
                    error:{erlfdb_error, Code} ->
                        ok = redwall:on_error(Tx, Code),
                        Attempt()
                end
            end,
            ok = Retry(),
            %% The commit version of a transaction whose commit had no
            %% answer is the commit version of its record.
            ?assert(redwall:get_committed_version(Tx) > 0),
            ?assertEqual(N, Read(option))
        end,
        lists:seq(1, 50)
    ),
    %% A transaction without a read gets a read version at its commit.
    %% The versions of the test are below the MVCC window of the Resolver,
    %% so the test only checks the write.
    ok = redwall:transactional(Safe, fun(Tx) -> redwall:set(Tx, blind, 1) end),
    ?assertEqual(1, Read(blind)),
    %% The Proxy clears the records of the ids with its batches.
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, flush, 1) end),
    Records = redwall:transactional(Db, fun(Tx) ->
        redwall:get_range(Tx, redwall_idempotency:key({0, 0}), redwall_idempotency:key({1 bsl 62, 0}))
    end),
    ?assert(length(Records) < 5, Records),
    %% The mutation `set_versionstamp' sets a key to the commit version
    %% and the index of its transaction.
    {ok, LSN, Index} = redwall_epoch:commit_with_index(Real, #{mutations => [{set_versionstamp, stamp}, {set, other, 1}]}, 5000),
    ?assertEqual({LSN, Index}, Read(stamp)),
    %% The records below the time of the key `idmp_expired' can be
    %% removed. A commit without an answer and without a record then has
    %% a result that cannot be determined.
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, redwall_idempotency:expired_key(), 1 bsl 60) end),
    Stop = relayed(Db, ets:new(counter, [public]), fun(1) -> stop; (_) -> forward end, #{automatic_idempotency => true}),
    ?assertError({erlfdb_error, 1022}, Increment(Stop, fatal)),
    ?assertEqual(not_found, Read(fatal)),
    %% A commit without an answer and with a record is committed.
    Committed = relayed(Db, ets:new(counter, [public]), fun(1) -> commit_and_stop; (_) -> forward end, #{automatic_idempotency => true}),
    ?assertEqual(1, Increment(Committed, old_record)),
    ?assertEqual(1, Read(old_record)).

%% The key range of a prefix, for a list and for a binary.
key_range_test() ->
    ?assertEqual({[a], [a | <<>>]}, redwall_key:range([a])),
    ?assertEqual({[a, 1], [a, 1 | <<>>]}, redwall_key:range([a, 1])),
    ?assertEqual({[], <<>>}, redwall_key:range([])),
    ?assertEqual({<<"ab">>, <<"ac">>}, redwall_key:range(<<"ab">>)),
    ?assertEqual({<<1, 255, 255>>, <<2>>}, redwall_key:range(<<1, 255, 255>>)),
    ?assertError(badarg, redwall_key:range(<<255, 255>>)),
    ?assertError(badarg, redwall_key:range(<<>>)),
    ?assertError(badarg, redwall_key:range({a})),
    ?assertError(badarg, redwall_key:range([a | b])),
    %% The lists with a prefix are the lists of its range, for elements
    %% of every type.
    Elements = [-1, 0, 2.5, a, zz, {t, 1}, #{m => 1}, [], [n], "s", <<>>, <<"b">>, <<255, 255>>],
    Keys = [[P] || P <- Elements] ++ [[P, E] || P <- Elements, E <- Elements] ++ [[P, E, x] || P <- [a, {t, 1}], E <- Elements],
    lists:foreach(
        fun(Prefix) ->
            {Begin, End} = redwall_key:range(Prefix),
            ?assertEqual(
                lists:sort([Key || Key <- Keys, lists:prefix(Prefix, Key)]),
                lists:sort([Key || Key <- Keys, Key >= Begin, Key < End]),
                Prefix
            )
        end,
        [[]] ++ [[P] || P <- Elements] ++ [[a, E] || E <- Elements]
    ),
    %% A tuple, which is the form of a key of redwall itself, is below
    %% every list and every binary.
    ?assert(redwall_idempotency:key({1 bsl 62, 0}) < []),
    ?assert(redwall_idempotency:key({1 bsl 62, 0}) < <<>>).

%% The keys of a database with the option `keys', and the functions for a
%% prefix.
keys(Db = {redwall_database, Servers}) ->
    Lists = redwall:open(Servers#{keys => list, automatic_idempotency => true}),
    Binaries = redwall:open(Servers#{keys => binary}),
    ?assertError(badarg, redwall:open(Servers#{keys => tuple})),
    ok = redwall:transactional(Lists, fun(Tx) ->
        [ok = redwall:set(Tx, Key, Key) || Key <- [[users], [users, 1], [users, 1, name], [users, 2], [users, 10], [userz], [orders, 7], []]],
        ok
    end),
    ok = redwall:transactional(Binaries, fun(Tx) ->
        [ok = redwall:set(Tx, Key, Key) || Key <- [<<"a">>, <<"ab">>, <<"ab", 0>>, <<"ab", 255>>, <<"ac">>, <<"b">>]],
        ok
    end),
    %% A key of the test database without the option is any term.
    ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, {tuple, key}, 1) end),
    Keys = fun(D, Prefix) -> [Key || {Key, _Value} <- redwall:transactional(D, fun(Tx) -> redwall:get_range_startswith(Tx, Prefix) end)] end,
    ?assertEqual([[users], [users, 1], [users, 1, name], [users, 2], [users, 10]], Keys(Lists, [users])),
    ?assertEqual([[users, 1], [users, 1, name]], Keys(Lists, [users, 1])),
    ?assertEqual([[orders, 7]], Keys(Lists, [orders])),
    ?assertEqual([], Keys(Lists, [none])),
    %% Every list. The keys of the idempotency ids are tuples, which are
    %% not in the range.
    ?assertEqual([[], [orders, 7], [users], [users, 1], [users, 1, name], [users, 2], [users, 10], [userz]], Keys(Lists, [])),
    ?assertEqual([<<"ab">>, <<"ab", 0>>, <<"ab", 255>>], Keys(Binaries, <<"ab">>)),
    ?assertEqual([<<"a">>, <<"ab">>, <<"ab", 0>>, <<"ab", 255>>, <<"ac">>], Keys(Binaries, <<"a">>)),
    %% A clear of a prefix, and a read of the writes of the transaction.
    ?assertEqual(
        [[users], [users, 2], [users, 3], [users, 10]],
        redwall:transactional(Lists, fun(Tx) ->
            ok = redwall:clear_range_startswith(Tx, [users, 1]),
            ok = redwall:set(Tx, [users, 3], new),
            [Key || {Key, _} <- redwall:get_range_startswith(Tx, [users])]
        end)
    ),
    ?assertEqual([[users], [users, 2], [users, 3], [users, 10]], Keys(Lists, [users])),
    ok = redwall:transactional(Binaries, fun(Tx) -> redwall:clear_range_startswith(Tx, <<"ab">>) end),
    ?assertEqual([<<"a">>, <<"ac">>], Keys(Binaries, <<"a">>)),
    %% A function with a key of another kind raises `badarg'.
    Tx1 = redwall:create_transaction(Lists),
    Tx2 = redwall:create_transaction(Binaries),
    [
        begin
            ?assertError(badarg, redwall:set(Tx, Bad, 1)),
            ?assertError(badarg, redwall:get(Tx, Bad)),
            ?assertError(badarg, redwall:clear(Tx, Bad)),
            ?assertError(badarg, redwall:get_range(Tx, Bad, Good)),
            ?assertError(badarg, redwall:get_range(Tx, Good, Bad)),
            ?assertError(badarg, redwall:clear_range(Tx, Bad, Good)),
            ?assertError(badarg, redwall:select(Tx, Bad, Good, [{'_', [], ['$_']}])),
            ?assertError(badarg, redwall:add_read_conflict_key(Tx, Bad)),
            ?assertError(badarg, redwall:add_read_conflict_range(Tx, Good, Bad))
        end
     || {Tx, Good, Bads} <- [{Tx1, [a], [{a}, a, 1, <<"a">>, #{}]}, {Tx2, <<"a">>, [{a}, a, 1, [a], #{}]}], Bad <- Bads
    ],
    ?assertError(badarg, redwall:set(Tx1, [improper | list], 1)),
    ?assertEqual(true, redwall:is_read_only(Tx1)).
