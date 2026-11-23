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
