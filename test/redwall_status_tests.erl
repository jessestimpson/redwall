-module(redwall_status_tests).

-include_lib("eunit/include/eunit.hrl").

%% The tests of `redwall_status:status/1'. The first test uses one Erlang
%% node. The second test uses two Erlang nodes on this computer, as
%% `redwall_nodes_tests' does, for the fault tolerance in nodes.

%% ---------------------------------------------------------------------------
%% One node
%% ---------------------------------------------------------------------------

%% One node has the coordinators, the worker, the ClusterController and
%% the StorageServer.
one_node_test_() ->
    {timeout, 60, fun one_node/0}.

one_node() ->
    {ok, _} = application:ensure_all_started(redwall),
    Ids = ids(redwall_status_a),
    Node = node(),
    %% No coordinator runs.
    None = redwall_status:status(#{coordinators => Ids, timeout => 200}),
    ?assertMatch(
        #{
            healthy := false,
            epoch := undefined,
            generation := undefined,
            controller := undefined,
            storage_engine := undefined,
            storage_durability := undefined,
            coordinators := #{total := 3, up := 0, majority := 2},
            logs := #{total := 0, up := 0, replication := 0},
            storages := #{total := 0, up := 0, current := 0},
            fault_tolerance := #{max_node_failures_without_losing_data := 0, max_node_failures_without_losing_availability := 0, safe_to_stop := []}
        },
        None
    ),
    ?assertEqual([coordinated_state_unavailable, {coordinator_down, Node}, {coordinator_down, Node}, {coordinator_down, Node}], maps:get(problems, None)),
    %% The coordinators run. The cluster has no ClusterController and no
    %% epoch.
    {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}}),
    ok = await_read(Ids, 50),
    Formed = redwall_status:status(#{coordinators => Ids}),
    ?assertMatch(#{healthy := false, epoch := undefined, generation := 0, controller := undefined, coordinators := #{up := 3}}, Formed),
    ?assertEqual([no_epoch, no_cluster_controller], maps:get(problems, Formed)),
    {ok, Worker} = redwall_worker:start(#{coordinators => Ids, poll_ms => 20}),
    {ok, Controller} = redwall_cluster_controller:start(#{coordinators => Ids, poll_ms => 20, retry_ms => 10, proxy => #{lock_timeout_ms => 200, versions_per_second => 0}}),
    {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5}),
    try
        ok = await(fun() -> epoch(redwall_worker:db_info()) =:= 1 end, 300),
        Options = #{coordinators => Ids, storages => [Storage]},
        Db = redwall:open(#{proxy => fun() -> hd(maps:get(proxies, redwall_worker:db_info())) end, storage => Storage}),
        ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 1) end),
        ?assertEqual(1, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
        %% The cluster is healthy. One worker has the 3 LogServers of the
        %% epoch. Every process is on one node, so no node can stop.
        Healthy = redwall_status:status(Options),
        ?assertMatch(
            #{
                healthy := true,
                problems := [],
                epoch := 1,
                generation := 0,
                controller := Node,
                storage_engine := ets,
                storage_durability := memory,
                coordinators := #{total := 3, up := 3, majority := 2},
                logs := #{total := 3, up := 3, replication := 2},
                workers := #{registered := 1, min := 1},
                storages := #{total := 1, up := 1, current := 1},
                fault_tolerance := #{max_node_failures_without_losing_data := 0, max_node_failures_without_losing_availability := 0, safe_to_stop := []}
            },
            Healthy
        ),
        ?assertMatch(#{Node := #{coordinator := up, worker := true, logs := 3, storage := #{current := true, copying := false}}}, maps:get(nodes, Healthy)),
        ?assertEqual([Node], maps:keys(maps:get(nodes, Healthy))),
        %% Without the option `storages', the StorageServers are the
        %% StorageServers of the coordinated state, which has none.
        ?assertMatch(#{healthy := true, storages := #{total := 0}, storage_engine := undefined}, redwall_status:status(#{coordinators => Ids})),
        %% The option `min_workers' is the number of workers that a
        %% ClusterController needs.
        ?assertMatch(#{healthy := false, problems := [{workers, 1, 2}], workers := #{registered := 1, min := 2}}, redwall_status:status(Options#{min_workers => 2})),
        %% A StorageServer stops.
        ok = redwall_storage:stop(Storage),
        ?assertMatch(
            #{healthy := false, problems := [{storage_down, Node}], storages := #{total := 1, up := 0, current := 0}, storage_engine := undefined},
            redwall_status:status(Options)
        ),
        ?assertMatch(#{Node := #{storage := down}}, maps:get(nodes, redwall_status:status(Options))),
        %% A coordinator stops. The other two are a majority.
        ok = peace:stop_server(hd(Ids)),
        %% The stopped coordinator can be the leader of the peace cluster.
        %% The coordinated state then has no answer until the other two
        %% have a leader.
        ok = await(fun() -> maps:get(epoch, redwall_status:status(#{coordinators => Ids})) =:= 1 end, 100),
        Degraded = redwall_status:status(#{coordinators => Ids}),
        ?assertMatch(#{coordinators := #{total := 3, up := 2, majority := 2}}, Degraded),
        ?assert(lists:member({coordinator_down, Node}, maps:get(problems, Degraded))),
        %% The ClusterController stops.
        exit(Controller, kill),
        ok = await(fun() -> not is_process_alive(Controller) end, 100),
        Without = redwall_status:status(#{coordinators => Ids}),
        ?assertMatch(#{healthy := false, controller := undefined, epoch := 1}, Without),
        ?assert(lists:member(no_cluster_controller, maps:get(problems, Without)))
    after
        exit(Controller, kill),
        exit(Worker, kill),
        catch redwall_storage:stop(Storage),
        ok = application:stop(redwall),
        ok = application:stop(peace)
    end.

%% ---------------------------------------------------------------------------
%% Two nodes
%% ---------------------------------------------------------------------------

%% The node of the test has the coordinators, a worker of the class
%% `stateless', the ClusterController and a StorageServer. The second node
%% has a worker of the class `log' and a StorageServer. The epoch has 2
%% LogServers, each on a node of its own.
two_nodes_test_() ->
    {timeout, 60, fun two_nodes/0}.

two_nodes() ->
    ok = distribute(),
    {ok, _} = application:ensure_all_started(redwall),
    Ids = ids(redwall_status_b),
    {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}}),
    ok = await_read(Ids, 50),
    Args = ["-setcookie", atom_to_list(erlang:get_cookie()), "-pa" | [Path || Path <- code:get_path(), filelib:is_dir(Path)]],
    {ok, Peer, Second} = peer:start_link(#{name => peer:random_name(), connection => standard_io, args => Args}),
    true = net_kernel:connect_node(Second),
    First = node(),
    {ok, Worker} = redwall_worker:start(#{coordinators => Ids, class => stateless, poll_ms => 50}),
    {ok, Controller} = redwall_cluster_controller:start(#{coordinators => Ids, poll_ms => 50, retry_ms => 20, min_workers => 2, proxy => #{lock_timeout_ms => 200, versions_per_second => 0}}),
    try
        {ok, _} = peer:call(Peer, application, ensure_all_started, [redwall]),
        {ok, _} = peer:call(Peer, redwall_worker, start, [#{coordinators => Ids, class => log, poll_ms => 50}]),
        ok = await(fun() -> epoch(redwall_worker:db_info()) =:= 1 end, 300),
        {ok, Storage1} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5}),
        {ok, Storage2} = peer:call(Peer, redwall_storage, start, [#{coordinators => Ids, poll_ms => 5}]),
        Db = redwall:open(#{proxy => fun() -> hd(maps:get(proxies, redwall_worker:db_info())) end, storage => [Storage1, Storage2]}),
        ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 1) end),
        Options = #{coordinators => Ids, storages => [Storage1, Storage2], min_workers => 2},
        ok = await(fun() -> maps:get(healthy, redwall_status:status(Options)) end, 100),
        Status = redwall_status:status(Options),
        ?assertMatch(
            #{
                epoch := 1,
                controller := First,
                logs := #{total := 2, up := 2},
                workers := #{registered := 2, min := 2},
                storages := #{total := 2, up := 2, current := 2},
                storage_engine := ets
            },
            Status
        ),
        ?assertEqual(lists:sort([First, Second]), lists:sort(maps:keys(maps:get(nodes, Status)))),
        ?assertMatch(#{First := #{coordinator := up, worker := true, logs := 1}, Second := #{coordinator := none, worker := true, logs := 1}}, maps:get(nodes, Status)),
        #{logs := #{replication := Replication}, fault_tolerance := Tolerance} = Status,
        %% One node can stop without a loss of data if each record is on
        %% the 2 LogServers. The StorageServer of the other node is
        %% current.
        Data =
            case Replication of
                2 -> 1;
                1 -> 0
            end,
        ?assertEqual(Data, maps:get(max_node_failures_without_losing_data, Tolerance)),
        %% With `min_workers' of 2, the stop of either node leaves 1
        %% worker, so no node can stop.
        ?assertEqual(0, maps:get(max_node_failures_without_losing_availability, Tolerance)),
        ?assertEqual([], maps:get(safe_to_stop, Tolerance)),
        %% With `min_workers' of 1, the second node can stop if no data is
        %% lost. The first node has every coordinator, so it cannot stop.
        #{fault_tolerance := Relaxed} = redwall_status:status(Options#{min_workers => 1}),
        ?assertEqual(0, maps:get(max_node_failures_without_losing_availability, Relaxed)),
        ?assertEqual([Second || Data =:= 1], maps:get(safe_to_stop, Relaxed)),
        %% The StorageServer of the second node stops. The stop of the
        %% first node then loses data.
        ok = peer:call(Peer, redwall_storage, stop, [Storage2]),
        Down = redwall_status:status(Options#{min_workers => 1}),
        ?assertMatch(#{healthy := false, problems := [{storage_down, Second}], storages := #{total := 2, up := 1, current := 1}}, Down),
        ?assertMatch(#{fault_tolerance := #{max_node_failures_without_losing_data := 0}}, Down),
        %% The second node stops. Its LogServer and its worker are gone.
        ok = peer:stop(Peer),
        Stopped = redwall_status:status(#{coordinators => Ids, storages => [Storage1], timeout => 300}),
        ?assertMatch(#{healthy := false, logs := #{total := 2, up := 1}}, Stopped),
        ?assert(lists:member({log_down, Second}, maps:get(problems, Stopped))),
        ok = redwall_storage:stop(Storage1)
    after
        catch peer:stop(Peer),
        exit(Controller, kill),
        exit(Worker, kill),
        ok = application:stop(redwall),
        ok = application:stop(peace),
        _ = net_kernel:stop()
    end.

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

ids(Prefix) ->
    Tag = erlang:unique_integer([positive]),
    [{list_to_atom(lists:concat([Prefix, Tag, "_", N])), node()} || N <- [1, 2, 3]].

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

await_read(_Ids, 0) ->
    error(no_leader);
await_read(Ids, N) ->
    case redwall_cstate:read(Ids, 1000) of
        {ok, _State} ->
            ok;
        _ ->
            timer:sleep(100),
            await_read(Ids, N - 1)
    end.

%% Starts Erlang distribution on the node of the test.
distribute() ->
    _ = os:cmd("epmd -daemon"),
    Name = list_to_atom("redwall_test_" ++ os:getpid()),
    case net_kernel:start(Name, #{name_domain => shortnames}) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end.
