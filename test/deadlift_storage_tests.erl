-module(deadlift_storage_tests).

-include_lib("eunit/include/eunit.hrl").

%% Each test runs on a new peace cluster of three servers on one node, with
%% 3 LogServers and a replication degree of 2.
storage_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Ids) -> {timeout, 30, fun() -> reads(Ids) end} end,
        fun(Ids) -> {timeout, 30, fun() -> read_waits(Ids) end} end,
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
