-module(deadlift_nodes_tests).

-include_lib("eunit/include/eunit.hrl").

%% The test uses two Erlang nodes on this computer: the node of the test
%% and a second node that the test starts with `peer'. The test starts
%% Erlang distribution with a short name, which needs `epmd'.
%%
%% The node of the test has the peace cluster, the Sequencer, the
%% LogServers, the Proxy and one StorageServer. The second node has a
%% second StorageServer.
two_nodes_test_() ->
    {timeout, 60, fun two_nodes/0}.

two_nodes() ->
    ok = distribute(),
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_n, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    Opts = #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}},
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), Opts),
    ok = await_read(Ids, 50),
    Options = #{coordinators => Ids, logs => 3, replication => 2, lock_timeout_ms => 200, versions_per_second => 0},
    {ok, Sequencer} = deadlift_sequencer:start(Options),
    {ok, Proxy} = deadlift_proxy:start(#{sequencer => Sequencer}),
    {ok, First} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5}),
    %% The test controls the second node through its standard input and
    %% output. The processes of the two nodes use Erlang distribution.
    Args = ["-setcookie", atom_to_list(erlang:get_cookie()), "-pa" | [Path || Path <- code:get_path(), filelib:is_dir(Path)]],
    {ok, Peer, Node} = peer:start_link(#{name => peer:random_name(), connection => standard_io, args => Args}),
    true = net_kernel:connect_node(Node),
    try
        {ok, _} = peer:call(Peer, application, ensure_all_started, [deadlift]),
        %% The StorageServer of the second node finds the LogServers of
        %% the first node in the registry of its own node.
        {ok, Second} = peer:call(Peer, deadlift_storage, start, [#{coordinators => Ids, poll_ms => 5}]),
        ?assertEqual(Node, node(Second)),
        Both = deadlift:open(#{proxy => Proxy, storage => [First, Second]}),
        Remote = deadlift:open(#{proxy => Proxy, storage => Second}),
        Read = fun(Tx) -> {deadlift:get(Tx, a), deadlift:get_range(Tx, a, z)} end,
        lists:foreach(
            fun(N) ->
                ok = deadlift:transactional(Both, fun(Tx) -> deadlift:set(Tx, a, N) end),
                ?assertEqual({N, [{a, N}]}, deadlift:transactional(Both, Read)),
                %% A read by message from the StorageServer of the second
                %% node.
                ?assertEqual({N, [{a, N}]}, deadlift:transactional(Remote, Read)),
                %% A client on the second node reads the ETS table of the
                %% StorageServer of that node.
                ?assertEqual({N, [{a, N}]}, peer:call(Peer, deadlift, transactional, [Remote, Read]))
            end,
            lists:seq(1, 6)
        ),
        %% A client on the second node commits through the Proxy of the
        %% first node.
        ok = peer:call(Peer, deadlift, transactional, [Remote, fun(Tx) -> deadlift:set(Tx, b, 1) end]),
        ?assertEqual({6, [{a, 6}, {b, 1}]}, deadlift:transactional(Both, Read)),
        %% After the stop of the StorageServer of the first node, the reads
        %% go to the second node.
        ok = deadlift_storage:stop(First),
        ok = deadlift:transactional(Both, fun(Tx) -> deadlift:set(Tx, a, 7) end),
        ?assertEqual({7, [{a, 7}, {b, 1}]}, deadlift:transactional(Both, Read)),
        %% After the stop of the second node, no StorageServer is alive.
        ok = peer:stop(Peer),
        ?assertError({erlfdb_error, 1037}, deadlift:get(deadlift:create_transaction(Both), a))
    after
        catch peer:stop(Peer),
        ok = application:stop(deadlift),
        ok = application:stop(peace),
        _ = net_kernel:stop()
    end.

%% The node of the test has the peace cluster, a worker of the class
%% `stateless' and a candidate for the ClusterController. The second node
%% has a worker of the class `log' and a second candidate.
controller_test_() ->
    {timeout, 60, fun controller/0}.

controller() ->
    ok = distribute(),
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_m, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}}),
    ok = await_read(Ids, 50),
    Args = ["-setcookie", atom_to_list(erlang:get_cookie()), "-pa" | [Path || Path <- code:get_path(), filelib:is_dir(Path)]],
    {ok, Peer, Node} = peer:start_link(#{name => peer:random_name(), connection => standard_io, args => Args}),
    true = net_kernel:connect_node(Node),
    Options = #{coordinators => Ids, poll_ms => 50, retry_ms => 20, min_workers => 2, sequencer => #{lock_timeout_ms => 200, versions_per_second => 0}},
    {ok, Worker} = deadlift_worker:start(#{coordinators => Ids, class => stateless, poll_ms => 50}),
    {ok, A} = deadlift_cluster_controller:start(Options),
    try
        {ok, _} = peer:call(Peer, application, ensure_all_started, [deadlift]),
        {ok, _} = peer:call(Peer, deadlift_worker, start, [#{coordinators => Ids, class => log, poll_ms => 50}]),
        %% The candidate of the first node is the leader. It recruits
        %% with the workers of both nodes. The Sequencer and the Proxy
        %% are on the first node. The leader uses each worker once before
        %% it uses a worker again, so 2 LogServers are on the second
        %% node and 1 is on the first node.
        ok = await(fun() -> epoch(deadlift_worker:db_info()) =:= 1 end, 300),
        ?assertMatch(#{role := leader, term := 1, workers := [_, _]}, deadlift_cluster_controller:info(A)),
        Info1 = #{sequencer := Sequencer1, proxies := [Proxy1]} = deadlift_worker:db_info(),
        ?assertEqual([node(), node()], [node(Sequencer1), node(Proxy1)]),
        ?assertEqual(lists:sort([Node, Node, node()]), lists:sort([node(peer:call(Peer, deadlift_log, ref, [{1, Index}])) || Index <- [1, 2, 3]])),
        %% The worker of the second node has the same information.
        ok = await(fun() -> peer:call(Peer, deadlift_worker, db_info, []) =:= Info1 end, 100),
        {ok, Storage} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5}),
        Db = deadlift:open(#{proxy => fun() -> hd(maps:get(proxies, deadlift_worker:db_info())) end, storage => Storage}),
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, a, 1) end),
        %% A candidate on the second node watches the leader.
        {ok, B} = peer:call(Peer, deadlift_cluster_controller, start, [Options]),
        ok = await(fun() -> peer:call(Peer, deadlift_cluster_controller, info, [B]) =:= #{role => candidate, term => 1, workers => [], db_info => undefined} end, 100),
        %% The leader stops. The candidate of the second node wins the
        %% election of term 2. Both workers register with it, and it
        %% recruits the epoch 2.
        exit(A, kill),
        ok = await(fun() -> epoch(deadlift_worker:db_info()) =:= 2 end, 300),
        ?assertMatch(#{role := leader, term := 2, workers := [_, _]}, peer:call(Peer, deadlift_cluster_controller, info, [B])),
        ?assertMatch(#{term := 2, controller := {2, B}}, maps:merge(deadlift_worker:db_info(), deadlift_worker:info())),
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, b, 2) end),
        ?assertEqual({1, 2}, deadlift:transactional(Db, fun(Tx) -> {deadlift:get(Tx, a), deadlift:get(Tx, b)} end)),
        %% A LogServer stops. The leader recruits the epoch 3.
        exit(peer:call(Peer, deadlift_log, ref, [{2, 1}]), kill),
        ok = await(fun() -> epoch(deadlift_worker:db_info()) =:= 3 end, 300),
        ?assertEqual({1, 2}, deadlift:transactional(Db, fun(Tx) -> {deadlift:get(Tx, a), deadlift:get(Tx, b)} end)),
        %% The second node stops, with the leader and 2 LogServers. A
        %% new candidate on the first node wins the election of term 3.
        %% It has one worker and needs two, so it does not recruit.
        {ok, C} = deadlift_cluster_controller:start(Options),
        ok = await(fun() -> maps:get(term, deadlift_cluster_controller:info(C)) =:= 2 end, 100),
        ?assertMatch(#{role := candidate, term := 2}, deadlift_cluster_controller:info(C)),
        ok = peer:stop(Peer),
        ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(C)) =:= leader end, 300),
        ok = await(fun() -> maps:get(workers, deadlift_cluster_controller:info(C)) =:= [node()] end, 300),
        ?assertMatch(#{term := 3, db_info := #{epoch := undefined}}, deadlift_cluster_controller:info(C)),
        ok = await(fun() -> deadlift_worker:db_info() =:= #{term => 3, epoch => undefined, sequencer => undefined, proxies => []} end, 100),
        exit(C, kill),
        ok = deadlift_storage:stop(Storage)
    after
        catch peer:stop(Peer),
        exit(A, kill),
        exit(Worker, kill),
        ok = application:stop(deadlift),
        ok = application:stop(peace),
        _ = net_kernel:stop()
    end.

epoch(#{epoch := Epoch, proxies := [_]}) -> Epoch;
epoch(_Info) -> undefined.

await(_Fun, 0) ->
    error(timeout);
await(Fun, Tries) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            await(Fun, Tries - 1)
    end.

%% Starts Erlang distribution on the node of the test.
distribute() ->
    _ = os:cmd("epmd -daemon"),
    Name = list_to_atom("deadlift_test_" ++ os:getpid()),
    case net_kernel:start(Name, #{name_domain => shortnames}) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end.

await_read(_Ids, 0) ->
    error(no_leader);
await_read(Ids, N) ->
    case deadlift_cstate:read(Ids, 1000) of
        {ok, _State} ->
            ok;
        _ ->
            timer:sleep(100),
            await_read(Ids, N - 1)
    end.
