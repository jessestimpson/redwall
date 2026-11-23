%% @doc A benchmark of a deadlift cluster on one computer.
%%
%% `bench/run.sh' runs `main/0'. The benchmark is not a test: it has no
%% limits, and it does not check the results of the transactions. The
%% tests and the simulation check the safety of the system.
%%
%% == Cluster ==
%%
%% The benchmark starts 7 Erlang nodes with `peer', each with 2 scheduler
%% threads:
%%
%% <ul>
%% <li>3 nodes with a worker of the class `log'. Each receives one
%%   LogServer of an epoch.</li>
%% <li>3 nodes with a worker of the class `stateless'. They receive the
%%   Sequencer, the Resolver and the Proxy. 2 of them have a candidate for
%%   the ClusterController.</li>
%% <li>1 node with a StorageServer.</li>
%% </ul>
%%
%% The node of the benchmark has the peace cluster of 3 servers with the
%% coordinated state, and the clients. A client is one process that runs
%% one operation after another (a closed loop). Every role has its default
%% options.
%%
%% == Measurements ==
%%
%% <ul>
%% <li>`write': a transaction that sets one key, without a read.</li>
%% <li>`read_write': a transaction that reads one key by message and sets
%%   one key. The keys are 2 of 1,000,000 keys.</li>
%% <li>`conflict': `read_write' with 100 keys. The result has the number
%%   of attempts for each transaction.</li>
%% <li>`read_version': one read version from the Proxy.</li>
%% <li>`read': the read of one key at a read version that the client has.
%%   The clients are on the node of the benchmark and read by message, or
%%   on the node of the StorageServer and read the ETS table.</li>
%% <li>The time without an acknowledged commit after the kill of one
%%   process: a LogServer, the Resolver, the Proxy, the Sequencer and the
%%   ClusterController.</li>
%% </ul>
%%
%% The latency of an operation is the time of the whole call in the
%% client. The benchmark leaves out the operations of the first 500 ms of
%% each run.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>All nodes share the processors of one computer, and a message
%%   between two nodes has no network delay.</li>
%% <li>Every LogServer and the StorageServer keep their data in
%%   memory.</li>
%% <li>The clients of a run share 2 scheduler threads or the scheduler
%%   threads of the node of the benchmark, so many clients measure the
%%   node of the clients too.</li>
%% <li>A kill stops one process. The node of the process continues.</li>
%% </ul>
-module(deadlift_bench).

-export([main/0, main/1]).
-export([run/4, client/5]).

-define(KEYS, 1000000).
-define(WARMUP_US, 500000).
-define(PROXY, {?MODULE, proxy}).

%% @doc Runs the benchmark with a run time of 3 seconds for each row.
main() ->
    main(#{}).

%% @doc Runs the benchmark. `seconds' is the run time of each row.
%% `clients' is the list of the numbers of clients.
main(Options) ->
    Ms = round(1000 * maps:get(seconds, Options, 3)),
    Counts = maps:get(clients, Options, [1, 4, 16, 64, 256]),
    Cluster = #{storage := Storage, storage_node := StorageNode} = start_cluster(#{}),
    try
        #{proxies := [Proxy]} = db_info(Cluster),
        Db = deadlift:open(#{proxy => Proxy, storage => Storage, direct => false}),
        io:format("Erlang/OTP ~s, ~b scheduler threads on the node of the benchmark, ~b ms for each row~n~n", [
            erlang:system_info(otp_release), erlang:system_info(schedulers), Ms
        ]),
        ok = preload(Db),
        header(),
        [row(write, N, run(write, Db, N, Ms)) || N <- Counts],
        [row(read_write, N, run(read_write, Db, N, Ms)) || N <- Counts],
        row(conflict, 64, run(conflict, Db, 64, Ms)),
        [row(read_version, N, run(read_version, Db, N, Ms)) || N <- Counts],
        [row(read_by_message, N, run(read, Db, N, Ms)) || N <- [1, 16, 64]],
        Direct = deadlift:open(#{proxy => Proxy, storage => Storage}),
        [row(read_direct, N, erpc:call(StorageNode, ?MODULE, run, [read, Direct, N, Ms], infinity)) || N <- [1, 2, 16]],
        io:format("~nThe default options of the ClusterController (`retry_ms` 500, `poll_ms` 1000):~n~n"),
        failures(Cluster)
    after
        stop_cluster(Cluster)
    end,
    %% A second cluster, whose ClusterController recruits 35 ms after a
    %% failure. FoundationDB waits for that time
    %% (`ATTEMPT_RECRUITMENT_DELAY').
    Fast = start_cluster(#{retry_ms => 35}),
    try
        io:format("~n`retry_ms` 35:~n~n"),
        failures(Fast)
    after
        stop_cluster(Fast)
    end.

%% ---------------------------------------------------------------------------
%% Runs
%% ---------------------------------------------------------------------------

%% @private
%% Runs `Clients' clients for `Ms' milliseconds on this node. The result
%% has the number of operations after the warm-up, the time of that part
%% in microseconds, the sorted latencies in microseconds, and the number of
%% attempts.
run(Op, Db, Clients, Ms) ->
    Start = erlang:monotonic_time(microsecond),
    End = Start + Ms * 1000,
    Pids = [spawn_link(?MODULE, client, [self(), Op, Db, Start + ?WARMUP_US, End]) || _ <- lists:seq(1, Clients)],
    Results = [
        receive
            {Pid, Result} -> Result
        end
     || Pid <- Pids
    ],
    Latencies = lists:sort(lists:append([L || {L, _Attempts} <- Results])),
    #{
        count => length(Latencies),
        us => Ms * 1000 - ?WARMUP_US,
        latencies => Latencies,
        attempts => lists:sum([A || {_L, A} <- Results])
    }.

%% @private
client(Parent, Op, Db, Measure, End) ->
    Context =
        case Op of
            read ->
                %% One read version for every read of the client.
                Tx = deadlift:create_transaction(Db),
                _ = deadlift:get_read_version(Tx),
                Tx;
            _ ->
                Db
        end,
    Parent ! {self(), loop(Op, Context, Measure, End, [], 0)}.

loop(Op, Context, Measure, End, Latencies, Attempts) ->
    T0 = erlang:monotonic_time(microsecond),
    case T0 >= End of
        true ->
            {Latencies, Attempts};
        false ->
            Tried = op(Op, Context),
            T1 = erlang:monotonic_time(microsecond),
            case T0 >= Measure of
                true -> loop(Op, Context, Measure, End, [T1 - T0 | Latencies], Attempts + Tried);
                false -> loop(Op, Context, Measure, End, Latencies, Attempts)
            end
    end.

%% One operation. The result is the number of attempts.
op(write, Db) ->
    Key = rand:uniform(?KEYS),
    ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, Key, value()) end),
    1;
op(read_write, Db) ->
    read_write(Db, ?KEYS);
op(conflict, Db) ->
    read_write(Db, 100);
op(read_version, {deadlift_database, #{proxy := Proxy}}) ->
    {ok, _Version} = deadlift_proxy:read_version(Proxy),
    1;
op(read, Tx) ->
    _ = deadlift:wait(deadlift:get(Tx, rand:uniform(10000))),
    1.

read_write(Db, Keys) ->
    Read = rand:uniform(Keys),
    Write = rand:uniform(Keys),
    Counter = counters:new(1, []),
    ok = deadlift:transactional(Db, fun(Tx) ->
        ok = counters:add(Counter, 1, 1),
        _ = deadlift:wait(deadlift:get(Tx, Read)),
        deadlift:set(Tx, Write, value())
    end),
    counters:get(Counter, 1).

%% A value of 100 bytes.
value() ->
    <<0:800>>.

%% Writes the keys 1 to 10,000, which the reads use.
preload(Db) ->
    lists:foreach(
        fun(First) ->
            ok = deadlift:transactional(Db, fun(Tx) ->
                lists:foreach(fun(Key) -> ok = deadlift:set(Tx, Key, value()) end, lists:seq(First, First + 99))
            end)
        end,
        lists:seq(1, 10000, 100)
    ),
    %% The StorageServer has applied the last record when a read at a new
    %% read version returns.
    _ = deadlift:transactional(Db, fun(Tx) -> deadlift:wait(deadlift:get(Tx, 10000)) end),
    ok.

header() ->
    io:format("| Operation | Clients | Operations per second | p50 (us) | p99 (us) | p99.9 (us) | Attempts per operation |~n"),
    io:format("|---|---|---|---|---|---|---|~n").

row(Label, Clients, #{count := Count, us := Us, latencies := Latencies, attempts := Attempts}) ->
    io:format("| `~s` | ~b | ~b | ~b | ~b | ~b | ~.2f |~n", [
        Label,
        Clients,
        round(Count * 1000000 / Us),
        percentile(Latencies, Count, 0.5),
        percentile(Latencies, Count, 0.99),
        percentile(Latencies, Count, 0.999),
        Attempts / max(1, Count)
    ]).

percentile(_Latencies, 0, _P) ->
    0;
percentile(Latencies, Count, P) ->
    lists:nth(max(1, min(Count, round(P * Count + 0.5))), Latencies).

%% ---------------------------------------------------------------------------
%% Failures
%% ---------------------------------------------------------------------------

%% Kills one process of each kind, with 8 clients that write. Each client
%% reads the Proxy before each transaction from a value that this
%% function keeps current.
failures(Cluster = #{storage := Storage, coordinators := Ids}) ->
    ok = publish(Cluster),
    Refresher = spawn_link(fun() -> refresh(Cluster) end),
    Db = deadlift:open(#{proxy => fun() -> persistent_term:get(?PROXY) end, storage => Storage}),
    Collector = self(),
    Writers = [spawn_link(fun() -> writer(Collector, Db) end) || _ <- lists:seq(1, 8)],
    io:format("| Killed process | Time without an acknowledged commit (ms) | Epoch before | Epoch after |~n"),
    io:format("|---|---|---|---|~n"),
    lists:foreach(
        fun(Kind) ->
            ok = timer:sleep(2000),
            #{epoch := Epoch} = Info = db_info(Cluster),
            Pid = victim(Kind, Info, Cluster, Ids),
            _ = drain(),
            Killed = erlang:monotonic_time(microsecond),
            true = exit(Pid, kill),
            After = await_epoch(Cluster, Epoch, 300),
            ok = timer:sleep(1000),
            Acks = lists:sort([T || T <- drain(), T > Killed]),
            Gap =
                case Acks of
                    [] -> infinity;
                    _ -> lists:max([B - A || {A, B} <- lists:zip([Killed | lists:droplast(Acks)], Acks)])
                end,
            io:format("| ~s | ~p | ~p | ~p |~n", [Kind, ms(Gap), Epoch, After])
        end,
        [log, resolver, proxy, sequencer, cluster_controller]
    ),
    [unlink(Pid) || Pid <- [Refresher | Writers]],
    [exit(Pid, kill) || Pid <- [Refresher | Writers]],
    ok.

ms(infinity) -> infinity;
ms(Us) -> round(Us / 1000).

victim(log, #{epoch := Epoch}, #{log_nodes := [Node | _]}, _Ids) ->
    erpc:call(Node, deadlift_log, ref, [{Epoch, 1}]);
victim(resolver, #{epoch := Epoch}, #{stateless_nodes := [Node | _]}, _Ids) ->
    erpc:call(Node, deadlift_resolver, ref, [Epoch]);
victim(proxy, #{proxies := [Proxy]}, _Cluster, _Ids) ->
    Proxy;
victim(sequencer, #{sequencer := Sequencer}, _Cluster, _Ids) ->
    Sequencer;
victim(cluster_controller, _Info, _Cluster, Ids) ->
    {ok, #{controller := {_Term, Controller}}} = deadlift_cstate:read(Ids),
    Controller.

%% Waits until the workers have an epoch above `Epoch' with a Proxy, for
%% at most `Tries' times 100 ms.
await_epoch(_Cluster, _Epoch, 0) ->
    timeout;
await_epoch(Cluster, Epoch, Tries) ->
    case db_info(Cluster) of
        #{epoch := New, proxies := [_]} when is_integer(New), New > Epoch ->
            New;
        _ ->
            ok = timer:sleep(100),
            await_epoch(Cluster, Epoch, Tries - 1)
    end.

writer(Collector, Db) ->
    try
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, rand:uniform(?KEYS), value()) end),
        Collector ! {ack, erlang:monotonic_time(microsecond)}
    catch
        _:_ -> timer:sleep(5)
    end,
    writer(Collector, Db).

drain() ->
    receive
        {ack, T} -> [T | drain()]
    after 0 -> []
    end.

refresh(Cluster) ->
    ok = timer:sleep(10),
    ok = publish(Cluster),
    refresh(Cluster).

%% Stores the Proxy of the current epoch, or `undefined'.
publish(Cluster) ->
    Proxy =
        case db_info(Cluster) of
            #{proxies := [Pid]} -> Pid;
            _ -> undefined
        end,
    case persistent_term:get(?PROXY, none) of
        Proxy -> ok;
        _ -> persistent_term:put(?PROXY, Proxy)
    end.

%% ---------------------------------------------------------------------------
%% Cluster
%% ---------------------------------------------------------------------------

%% Starts the cluster. `Controller' has further options of the candidates
%% for the ClusterController.
start_cluster(Controller) ->
    _ = os:cmd("epmd -daemon"),
    _ = net_kernel:start(list_to_atom("deadlift_bench_" ++ os:getpid()), #{name_domain => shortnames}),
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_bench_c, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), #{storage => {peace_storage_mem, #{}}}),
    ok = await(fun() -> element(1, deadlift_cstate:read(Ids, 1000)) =:= ok end, 100),
    Peers = [start_peer() || _ <- lists:seq(1, 7)],
    Nodes = [Node || {_Peer, Node} <- Peers],
    {LogNodes, Rest} = lists:split(3, Nodes),
    {StatelessNodes, [StorageNode]} = lists:split(3, Rest),
    [{ok, _} = erpc:call(Node, deadlift_worker, start, [#{coordinators => Ids, class => log}]) || Node <- LogNodes],
    [{ok, _} = erpc:call(Node, deadlift_worker, start, [#{coordinators => Ids, class => stateless}]) || Node <- StatelessNodes],
    [
        {ok, _} = erpc:call(Node, deadlift_cluster_controller, start, [Controller#{coordinators => Ids, min_workers => 6}])
     || Node <- lists:sublist(StatelessNodes, 2)
    ],
    {ok, Storage} = erpc:call(StorageNode, deadlift_storage, start, [#{coordinators => Ids}]),
    Cluster = #{
        coordinators => Ids,
        peers => Peers,
        log_nodes => LogNodes,
        stateless_nodes => StatelessNodes,
        storage_node => StorageNode,
        storage => Storage
    },
    ok = await(fun() -> is_map_key(proxies, db_info(Cluster)) andalso maps:get(proxies, db_info(Cluster)) =/= [] end, 300),
    Cluster.

start_peer() ->
    Args = [
        "-setcookie",
        atom_to_list(erlang:get_cookie()),
        "+S",
        "2:2",
        "+sbwt",
        "none",
        "+sbwtdcpu",
        "none",
        "+sbwtdio",
        "none",
        "-pa"
        | [Path || Path <- code:get_path(), filelib:is_dir(Path)]
    ],
    {ok, Peer, Node} = peer:start_link(#{name => peer:random_name(), connection => standard_io, args => Args}),
    true = net_kernel:connect_node(Node),
    {ok, _} = erpc:call(Node, application, ensure_all_started, [deadlift]),
    {Peer, Node}.

stop_cluster(#{peers := Peers}) ->
    [catch peer:stop(Peer) || {Peer, _Node} <- Peers],
    ok.

%% The information about the database of the first worker that answers, or
%% an empty map.
db_info(#{stateless_nodes := Stateless, log_nodes := Logs}) ->
    db_info_of(Stateless ++ Logs).

db_info_of([]) ->
    #{};
db_info_of([Node | Nodes]) ->
    try erpc:call(Node, deadlift_worker, db_info, [], 1000) of
        Info when is_map(Info) -> Info;
        _ -> db_info_of(Nodes)
    catch
        _:_ -> db_info_of(Nodes)
    end.

await(_Fun, 0) ->
    error(timeout);
await(Fun, Tries) ->
    case Fun() of
        true ->
            ok;
        false ->
            ok = timer:sleep(100),
            await(Fun, Tries - 1)
    end.
