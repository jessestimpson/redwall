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
%% <li>3 nodes with a worker of the class `stateless'. One of them receives
%%   the process of an epoch, which does the work of the Sequencer, of the
%%   Resolver and of the Proxy. 2 of them have a candidate for the
%%   ClusterController.</li>
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
%% <li>`read_after_write': a client commits a write, and reads the key at
%%   the commit version. The row has the time of the read, which ends
%%   when the StorageServer has applied the record.</li>
%% <li>The mean times of the steps of a batch in the Proxy, from
%%   `deadlift_epoch:info/1'.</li>
%% <li>The time without an acknowledged commit after the kill of one
%%   process: a LogServer, the process of the epoch and the
%%   ClusterController.</li>
%% </ul>
%%
%% The latency of an operation is the time of the whole call in the
%% client. The benchmark leaves out the operations of the first 500 ms of
%% each run. A latency of 4,096 us or more is rounded down to 100 us.
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

-export([main/0, main/1, depth/1, conflicts/1]).
-export([run/4, client/5, watch/0]).

-define(KEYS, 1000000).
-define(WARMUP_US, 500000).
-define(BUCKETS, 14056).
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
        Writes = [with_stats(Proxy, N, fun() -> row(write, N, run(write, Db, N, Ms)) end) || N <- Counts],
        [row(read_write, N, run(read_write, Db, N, Ms)) || N <- Counts],
        row(conflict, 64, run(conflict, Db, 64, Ms)),
        Versions = [with_stats(Proxy, N, fun() -> row(read_version, N, run(read_version, Db, N, Ms)) end) || N <- Counts],
        [row(read_after_write, N, run(read_after_write, Db, N, Ms)) || N <- [1, 16]],
        [row(read_by_message, N, run(read, Db, N, Ms)) || N <- [1, 16, 64]],
        Direct = deadlift:open(#{proxy => Proxy, storage => Storage}),
        [row(read_direct, N, erpc:call(StorageNode, ?MODULE, run, [read, Direct, N, Ms], infinity)) || N <- [1, 2, 16]],
        breakdown(Writes, Versions),
        pauses(Cluster),
        io:format("~nThe default options of the ClusterController (`retry_ms` 500, `poll_ms` 1000):~n~n"),
        failures(Cluster)
    after
        stop_cluster(Cluster)
    end,
    %% A second cluster, whose ClusterController recruits 35 ms after a
    %% failure. FoundationDB waits 35 ms (`ATTEMPT_RECRUITMENT_DELAY').
    Fast = start_cluster(#{retry_ms => 35}),
    try
        io:format("~n`retry_ms` 35:~n~n"),
        failures(Fast)
    after
        stop_cluster(Fast)
    end.

%% @doc Measures `write' for several values of the option `batch_depth' of
%% the Proxy, with the Sequencer, the Resolver and the Proxy on one node
%% or on three nodes. Each value has a cluster of its own.
depth(Options) ->
    Ms = round(1000 * maps:get(seconds, Options, 3)),
    lists:foreach(
        fun(Depth) ->
            Cluster = #{storage := Storage} = start_cluster(#{proxy => #{batch_depth => Depth}}),
            try
                #{proxies := [Proxy]} = db_info(Cluster),
                Db = deadlift:open(#{proxy => Proxy, storage => Storage, direct => false}),
                io:format("~n`batch_depth` ~b:~n~n", [Depth]),
                header(),
                Writes = [with_stats(Proxy, N, fun() -> row(write, N, run(write, Db, N, Ms)) end) || N <- [1, 16, 64, 256]],
                breakdown(Writes, [])
            after
                stop_cluster(Cluster)
            end
        end,
        maps:get(depths, Options, [1, 2, 4, 8])
    ).

%% @doc Measures `conflict' for several values of the option
%% `retry_delay_ms' of the client, with 16, 64 and 256 clients. An element
%% `{profile, Profile}' of `delays' selects a profile of the client.
conflicts(Options) ->
    Ms = round(1000 * maps:get(seconds, Options, 3)),
    Cluster = #{storage := Storage} = start_cluster(#{}),
    try
        #{proxies := [Proxy]} = db_info(Cluster),
        lists:foreach(
            fun(Delay) ->
                Db =
                    case Delay of
                        {profile, Profile} -> deadlift:open(#{proxy => Proxy, storage => Storage, direct => false, profile => Profile});
                        _ -> deadlift:open(#{proxy => Proxy, storage => Storage, direct => false, retry_delay_ms => Delay})
                    end,
                io:format("~n`retry_delay_ms` ~p:~n~n", [Delay]),
                Errors = counters:new(6, [write_concurrency]),
                ok = persistent_term:put({?MODULE, errors}, Errors),
                header(),
                Rows = [
                    begin
                        Before = [counters:get(Errors, I) || I <- lists:seq(1, 6)],
                        Stats = with_stats(Proxy, N, fun() -> row(conflict, N, run(conflict, Db, N, Ms)) end),
                        {Stats, [counters:get(Errors, I) - B || {I, B} <- lists:enumerate(Before)]}
                    end
                 || N <- maps:get(clients, Options, [16, 64, 256])
                ],
                io:format("~n| Clients | 1020 not committed | 1007 too old | 1009 future version | 1021 unknown result | 1037 process behind | Other |~n|---|---|---|---|---|---|---|~n"),
                [io:format("| ~b | ~b | ~b | ~b | ~b | ~b | ~b |~n", [N | Counts]) || {{N, _}, Counts} <- Rows],
                breakdown([Stats || {Stats, _} <- Rows], [])
            end,
            maps:get(delays, Options, [10, 2, 1, 0])
        )
    after
        stop_cluster(Cluster)
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
    %% The clients count each latency in a shared array of counters. A
    %% list of the latencies would grow the heap of each client, and the
    %% garbage collections of the clients would be part of the result.
    Histogram = counters:new(?BUCKETS, [write_concurrency]),
    Start = erlang:monotonic_time(microsecond),
    End = Start + Ms * 1000,
    Pids = [spawn_link(?MODULE, client, [self(), Op, {Db, Histogram}, Start + ?WARMUP_US, End]) || _ <- lists:seq(1, Clients)],
    Attempts = lists:sum([
        receive
            {Pid, Result} -> Result
        end
     || Pid <- Pids
    ]),
    Latencies = [{value(Bucket), Count} || Bucket <- lists:seq(1, ?BUCKETS), Count <- [counters:get(Histogram, Bucket)], Count > 0],
    #{
        count => lists:sum([Count || {_Us, Count} <- Latencies]),
        us => Ms * 1000 - ?WARMUP_US,
        latencies => Latencies,
        attempts => Attempts
    }.

%% The bucket of a latency: one bucket for each microsecond below 4,096 us,
%% and one bucket for each 100 us from there to 1 s.
bucket(Us) when Us < 4096 -> max(0, Us) + 1;
bucket(Us) -> min(?BUCKETS, 4097 + (Us - 4096) div 100).

value(Bucket) when Bucket =< 4096 -> Bucket - 1;
value(Bucket) -> 4096 + (Bucket - 4097) * 100.

%% @private
client(Parent, Op, {Db, Histogram}, Measure, End) ->
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
    Parent ! {self(), loop(Op, Context, Measure, End, Histogram, 0)}.

loop(Op, Context, Measure, End, Histogram, Attempts) ->
    T0 = erlang:monotonic_time(microsecond),
    case T0 >= End of
        true ->
            Attempts;
        false ->
            {Us, Tried} =
                case op(Op, Context) of
                    {time, Time} -> {Time, 1};
                    Count -> {erlang:monotonic_time(microsecond) - T0, Count}
                end,
            case T0 >= Measure of
                true ->
                    ok = counters:add(Histogram, bucket(Us), 1),
                    loop(Op, Context, Measure, End, Histogram, Attempts + Tried);
                false ->
                    loop(Op, Context, Measure, End, Histogram, Attempts)
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
    {ok, _Version} = deadlift_epoch:read_version(Proxy),
    1;
op(read_after_write, Db) ->
    %% The loop measures this whole operation. The row of the table has
    %% the time of the read only, which `read_after_write/1' returns.
    read_after_write(Db);
op(read, Tx) ->
    _ = deadlift:wait(deadlift:get(Tx, rand:uniform(10000))),
    1.

read_write(Db, Keys) ->
    Read = rand:uniform(Keys),
    Write = rand:uniform(Keys),
    Tx = deadlift:create_transaction(Db),
    attempt(Tx, Read, Write, 1).

%% One attempt of the transaction, as `deadlift:transactional/2' runs it.
%% The function counts the error code of a failed attempt, for
%% `conflicts/1'.
attempt(Tx, Read, Write, Attempt) ->
    try
        _ = deadlift:wait(deadlift:get(Tx, Read)),
        ok = deadlift:set(Tx, Write, value()),
        ok = deadlift:wait(deadlift:commit(Tx)),
        {deadlift_transaction, Table} = Tx,
        true = ets:delete(Table),
        Attempt
    catch
        error:{erlfdb_error, Code} ->
            _ =
                case persistent_term:get({?MODULE, errors}, undefined) of
                    undefined -> ok;
                    Errors -> counters:add(Errors, error_index(Code), 1)
                end,
            ok = deadlift:wait(deadlift:on_error(Tx, Code)),
            attempt(Tx, Read, Write, Attempt + 1)
    end.

error_index(1020) -> 1;
error_index(1007) -> 2;
error_index(1009) -> 3;
error_index(1021) -> 4;
error_index(1037) -> 5;
error_index(_Code) -> 6.

%% Commits a write, and reads the key at the commit version. The read
%% returns when the StorageServer has applied the record. The result is
%% the time of the read in microseconds, as `{time, Us}'.
read_after_write(Db) ->
    Key = rand:uniform(?KEYS),
    Tx = deadlift:create_transaction(Db),
    ok = deadlift:set(Tx, Key, value()),
    ok = deadlift:wait(deadlift:commit(Tx)),
    Version = deadlift:get_committed_version(Tx),
    Read = deadlift:create_transaction(Db),
    ok = deadlift:set_read_version(Read, Version),
    T0 = erlang:monotonic_time(microsecond),
    _ = deadlift:wait(deadlift:get(Read, Key)),
    {time, erlang:monotonic_time(microsecond) - T0}.

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

%% Runs `Fun', and returns the number of clients with the sums that the
%% Proxy added in that time (`deadlift_epoch:info/1').
with_stats(Proxy, Clients, Fun) ->
    ok = deadlift_epoch:reset_stats(Proxy),
    _ = Fun(),
    #{stats := Stats} = deadlift_epoch:info(Proxy),
    {Clients, Stats}.

%% Prints the times of the steps of a batch in the Proxy, and the
%% confirmations for read versions.
breakdown(Writes, Versions) ->
    io:format("~nThe steps of a `write` batch in the Proxy, as the mean time of a batch:~n~n"),
    io:format("| Clients | Transactions per batch | Wait for the close (us) | Conflict check (us) | LogServers (us) |~n"),
    io:format("|---|---|---|---|---|~n"),
    lists:foreach(
        fun({Clients, Stats = #{batches := Batches, transactions := Transactions}}) ->
            Mean = fun(Key) -> round(maps:get(Key, Stats, 0) / max(1, Batches)) end,
            io:format("| ~b | ~.1f | ~b | ~b | ~b |~n", [Clients, Transactions / max(1, Batches), Mean(wait_us), Mean(resolve_us), Mean(push_us)])
        end,
        Writes
    ),
    io:format("~nThe longest single time of each step:~n~n"),
    io:format("| Clients | Wait for the close (us) | Conflict check (us) | LogServers (us) |~n"),
    io:format("|---|---|---|---|~n"),
    lists:foreach(
        fun({Clients, Stats}) ->
            io:format("| ~b | ~b | ~b | ~b |~n", [Clients | [maps:get(Key, Stats, 0) || Key <- [wait_max_us, resolve_max_us, push_max_us]]])
        end,
        Writes
    ),
    io:format("~nThe confirmations of the Proxy for `read_version`:~n~n"),
    io:format("| Clients | Requests per confirmation | Time of a confirmation (us) |~n"),
    io:format("|---|---|---|~n"),
    lists:foreach(
        fun({Clients, Stats}) ->
            Rounds = max(1, maps:get(read_rounds, Stats, 0)),
            io:format("| ~b | ~.1f | ~b |~n", [Clients, maps:get(read_versions, Stats, 0) / Rounds, round(maps:get(read_us, Stats, 0) / Rounds)])
        end,
        Versions
    ).

%% `Latencies' has `{Us, Count}' in the order of `Us'.
percentile(_Latencies, 0, _P) ->
    0;
percentile(Latencies, Count, P) ->
    rank(Latencies, max(1, min(Count, round(P * Count + 0.5)))).

rank([{Us, Count} | _], Rank) when Rank =< Count -> Us;
rank([{_Us, Count} | Rest], Rank) -> rank(Rest, Rank - Count).

%% ---------------------------------------------------------------------------
%% Pauses
%% ---------------------------------------------------------------------------

%% @private
%% A process that receives a message of the VM for each garbage
%% collection of 2 ms or more, and for each time of 2 ms or more for which
%% a process ran without a pause (`erlang:system_monitor/2').
watch() ->
    _ = erlang:system_monitor(self(), [{long_gc, 2}, {long_schedule, 2}]),
    watch(#{}).

watch(Seen) ->
    receive
        {monitor, Pid, Kind, Info} when is_pid(Pid) ->
            Ms = proplists:get_value(timeout, Info, 0),
            Key = {Kind, label(Pid)},
            {Count, Max, Sum} = maps:get(Key, Seen, {0, 0, 0}),
            watch(Seen#{Key => {Count + 1, max(Max, Ms), Sum + Ms}});
        {report, From} ->
            From ! {pauses, self(), Seen},
            watch(#{});
        _ ->
            watch(Seen)
    end.

%% The callback module of a `gen_server', or the first function of another
%% process.
label(Pid) ->
    case erlang:process_info(Pid, [dictionary, initial_call]) of
        [{dictionary, Dictionary}, {initial_call, Call}] ->
            case proplists:get_value('$initial_call', Dictionary) of
                {Module, _Function, _Arity} -> Module;
                undefined -> element(1, Call)
            end;
        undefined ->
            stopped
    end.

%% Prints the pauses that the watchers have seen since the start of the
%% cluster.
pauses(#{watchers := Watchers}) ->
    Seen = lists:append([
        begin
            Watcher ! {report, self()},
            receive
                {pauses, Watcher, Map} -> [{node(Watcher) =:= node(), Key, Value} || {Key, Value} <- maps:to_list(Map)]
            after 5000 -> []
            end
        end
     || Watcher <- Watchers
    ]),
    io:format("~nGarbage collections and uninterrupted runs of a process of 2 ms or more, since the start of the cluster:~n~n"),
    io:format("| Kind | Process | Count | Longest (ms) | Sum (ms) |~n|---|---|---|---|---|~n"),
    Merged = lists:foldl(
        fun({Local, {Kind, Label}, {Count, Max, Sum}}, Acc) ->
            Name =
                case Local of
                    true -> {Kind, {client_node, Label}};
                    false -> {Kind, Label}
                end,
            {C0, M0, S0} = maps:get(Name, Acc, {0, 0, 0}),
            Acc#{Name => {C0 + Count, max(M0, Max), S0 + Sum}}
        end,
        #{},
        Seen
    ),
    lists:foreach(
        fun({{Kind, Label}, {Count, Max, Sum}}) -> io:format("| ~s | `~0p` | ~b | ~b | ~b |~n", [Kind, Label, Count, Max, Sum]) end,
        lists:reverse(lists:keysort(2, [{K, V} || {K, V = {_, _, _}} <- maps:to_list(Merged)]))
    ).

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
        [log, proxy, cluster_controller]
    ),
    [unlink(Pid) || Pid <- [Refresher | Writers]],
    [exit(Pid, kill) || Pid <- [Refresher | Writers]],
    ok.

ms(infinity) -> infinity;
ms(Us) -> round(Us / 1000).

victim(log, _Info, _Cluster, Ids) ->
    %% The coordinated state has the process of each LogServer.
    {ok, #{current := #{logs := [{_Id, Pid} | _]}}} = deadlift_cstate:read(Ids),
    Pid;
victim(proxy, #{proxies := [Proxy]}, _Cluster, _Ids) ->
    Proxy;
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
    Watchers = [erpc:call(Node, erlang, spawn, [?MODULE, watch, []]) || Node <- [node() | Nodes]],
    Cluster = #{
        watchers => Watchers,
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
