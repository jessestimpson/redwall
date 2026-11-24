%% @doc The health of a cluster, and its fault tolerance in Erlang nodes.
%%
%% `status/1' reads the state of the processes of a cluster, and computes
%% how many nodes can stop. FoundationDB reports the same two numbers in
%% the `fault_tolerance' section of its status
%% (`max_zone_failures_without_losing_data' and
%% `max_zone_failures_without_losing_availability', `fdbserver/Status.actor.cpp').
%% The unit of redwall is the Erlang node.
%%
%% == What the function reads ==
%%
%% <ol>
%% <li>Each coordinator: whether its process runs on its node.</li>
%% <li>The coordinated state: the LogServers of the current epoch, the
%%   replication degree and the ClusterController.</li>
%% <li>Each LogServer of the current epoch: whether its process runs.</li>
%% <li>The ClusterController: the nodes of the registered workers, and the
%%   epoch that it has given to the workers.</li>
%% <li>Each StorageServer of the option `storages': its version, and
%%   whether it copies from another StorageServer.</li>
%% </ol>
%%
%% The function asks the processes one after another, so the result is not
%% the state of one moment. A call to a node that does not answer takes up
%% to `timeout' (default 1,000 ms).
%%
%% == Rules ==
%%
%% A StorageServer is current if it runs, does not copy, and has a version
%% within 5,000,000 versions of the highest version of a StorageServer,
%% which is 5 seconds.
%%
%% The cluster keeps its data after the stop of a set of nodes if both
%% conditions hold:
%%
%% <ul>
%% <li>Fewer than `replication' LogServers of the current epoch are
%%   stopped, with the LogServers that are stopped now. Each record is on
%%   `replication' LogServers, and a LogServer keeps its records in
%%   memory.</li>
%% <li>A current StorageServer is on another node. A record of an earlier
%%   epoch can be on a StorageServer only.</li>
%% </ul>
%%
%% The cluster stays available after the stop of a set of nodes if it
%% keeps its data and these conditions hold:
%%
%% <ul>
%% <li>A majority of the coordinators runs on the other nodes.</li>
%% <li>At least `min_workers' registered workers are on the other nodes,
%%   so that a ClusterController can recruit the next epoch.</li>
%% </ul>
%%
%% `max_node_failures_without_losing_data' is the highest number `F' such
%% that the cluster keeps its data after the stop of any `F' nodes.
%% `max_node_failures_without_losing_availability' is the same for the
%% second rule. `safe_to_stop' has the nodes with a process of the cluster
%% whose stop alone leaves the cluster available.
%%
%% `healthy' is `true' if the cluster has an epoch that its
%% ClusterController has given to the workers, every coordinator, every
%% LogServer of the epoch and every StorageServer runs, and every
%% StorageServer is current.
%%
%% == Use ==
%%
%% An operator who restarts the nodes of a cluster one after another stops
%% a node only if it is in `safe_to_stop', and waits until `healthy' is
%% `true' before the next node. A second stop before that can end two
%% LogServers of one epoch, and the cluster then cannot start an epoch.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The function knows only the StorageServers of the option
%%   `storages'. redwall has no list of the StorageServers of a
%%   cluster.</li>
%% <li>The function assumes that every node with a registered worker has a
%%   candidate for the ClusterController.</li>
%% <li>The rules are for the stop of nodes. A partition is not a part of
%%   the result.</li>
%% <li>The numbers are for stops at the same time. After the stop of one
%%   node, the cluster starts a new epoch, and the numbers change.</li>
%% <li>With more than 12 nodes, the function examines sets of up to 3
%%   nodes.</li>
%% </ul>
-module(redwall_status).

-include("redwall_eta.hrl").

-export([status/1]).
-export([registered/1, running/1]).
-export_type([options/0, status/0]).

-type options() :: #{
    coordinators := [peace:server_id()],
    storages => [gen_server:server_ref()],
    min_workers => pos_integer(),
    timeout => pos_integer()
}.

-type status() :: #{
    healthy := boolean(),
    epoch := redwall_cstate:epoch() | undefined,
    controller := node() | undefined,
    coordinators := #{total := non_neg_integer(), up := non_neg_integer(), majority := pos_integer()},
    logs := #{total := non_neg_integer(), up := non_neg_integer(), replication := non_neg_integer()},
    workers := #{registered := non_neg_integer(), min := pos_integer()},
    storages := #{total := non_neg_integer(), up := non_neg_integer(), current := non_neg_integer()},
    nodes := #{node() => map()},
    fault_tolerance := #{
        max_node_failures_without_losing_data := non_neg_integer(),
        max_node_failures_without_losing_availability := non_neg_integer(),
        safe_to_stop := [node()]
    },
    problems := [term()]
}.

%% The versions of 5 seconds: the MVCC window.
-define(CURRENT_LAG, 5000000).
-define(TIMEOUT_MS, 1000).

%% @doc Returns the health of the cluster. See the module documentation.
-spec status(options()) -> status().
status(Options = #{coordinators := Coordinators}) ->
    Timeout = maps:get(timeout, Options, ?TIMEOUT_MS),
    MinWorkers = maps:get(min_workers, Options, 1),
    CoordinatorsUp = [{Node, on_node(Node, registered, [Name], Timeout) =:= true} || {Name, Node} <- Coordinators],
    Majority = length(Coordinators) div 2 + 1,
    CState =
        case redwall_cstate:read(Coordinators, Timeout) of
            {ok, State} -> State;
            {error, _Reason} -> unavailable
        end,
    {Epoch, Logs, Replication} =
        case CState of
            #{current := #{epoch := E, logs := LogSystem, replication := K}} ->
                {E, [{node(Pid), alive(Pid, Timeout)} || {_Id, Pid} <- LogSystem], K};
            _ ->
                {undefined, [], 0}
        end,
    {Controller, Workers, Given} =
        case CState of
            #{controller := {_Term, Pid}} when is_pid(Pid) ->
                try redwall_cluster_controller:info(Pid) of
                    #{role := leader, workers := Registered, db_info := #{epoch := GivenEpoch}} -> {node(Pid), Registered, GivenEpoch};
                    #{role := leader, workers := Registered} -> {node(Pid), Registered, undefined};
                    _Candidate -> {undefined, [], undefined}
                catch
                    exit:_ -> {undefined, [], undefined}
                end;
            _ ->
                {undefined, [], undefined}
        end,
    Storages0 = [{node_of(Storage), storage(Storage)} || Storage <- maps:get(storages, Options, [])],
    Highest = lists:max([0 | [Version || {_Node, #{version := Version}} <- Storages0]]),
    Storages = [
        {Node,
            case Info of
                #{version := Version, copying := false} when Version > 0, Version >= Highest - ?CURRENT_LAG -> Info#{current => true};
                #{} -> Info#{current => false};
                down -> down
            end}
     || {Node, Info} <- Storages0
    ],
    Nodes = lists:usort(
        [Node || {Node, _} <- CoordinatorsUp] ++ [Node || {Node, _} <- Logs] ++ Workers ++ [Node || {Node, _} <- Storages]
    ),
    View = #{
        coordinators => CoordinatorsUp,
        majority => Majority,
        logs => Logs,
        replication => Replication,
        workers => Workers,
        min_workers => MinWorkers,
        storages => Storages,
        epoch => Epoch
    },
    Problems = problems(View, CState, Controller, Given),
    #{
        healthy => Problems =:= [],
        epoch => Epoch,
        controller => Controller,
        coordinators => #{total => length(Coordinators), up => length([up || {_, true} <- CoordinatorsUp]), majority => Majority},
        logs => #{total => length(Logs), up => length([up || {_, true} <- Logs]), replication => Replication},
        workers => #{registered => length(Workers), min => MinWorkers},
        storages => #{
            total => length(Storages),
            up => length([up || {_, #{}} <- Storages]),
            current => length([current || {_, #{current := true}} <- Storages])
        },
        nodes => maps:from_list([{Node, node_view(Node, View)} || Node <- Nodes]),
        fault_tolerance => #{
            max_node_failures_without_losing_data => max_failures(Nodes, fun(Stopped) -> keeps_data(Stopped, View) end),
            max_node_failures_without_losing_availability => max_failures(Nodes, fun(Stopped) -> available(Stopped, View) end),
            safe_to_stop => [Node || Node <- Nodes, runs(Node, View), available([Node], View)]
        },
        problems => Problems
    }.

%% ---------------------------------------------------------------------------
%% Rules
%% ---------------------------------------------------------------------------

%% The cluster keeps its data after the stop of the nodes `Stopped'.
keeps_data(Stopped, #{logs := Logs, replication := K, storages := Storages, epoch := Epoch}) ->
    Lost = length([lost || {Node, Up} <- Logs, not Up orelse lists:member(Node, Stopped)]),
    Current = [Node || {Node, #{current := true}} <- Storages, not lists:member(Node, Stopped)],
    Epoch =/= undefined andalso Lost < K andalso Current =/= [].

%% The cluster stays available after the stop of the nodes `Stopped'.
available(Stopped, View = #{coordinators := Coordinators, majority := Majority, workers := Workers, min_workers := MinWorkers}) ->
    Up = length([up || {Node, true} <- Coordinators, not lists:member(Node, Stopped)]),
    keeps_data(Stopped, View) andalso Up >= Majority andalso length(Workers -- Stopped) >= MinWorkers.

%% A process of the cluster runs on the node.
runs(Node, #{coordinators := Coordinators, logs := Logs, workers := Workers, storages := Storages}) ->
    lists:member({Node, true}, Coordinators) orelse lists:member({Node, true}, Logs) orelse lists:member(Node, Workers) orelse
        lists:any(fun({N, Info}) -> N =:= Node andalso is_map(Info) end, Storages).

%% The highest number `F' such that `Holds' is true for every set of `F'
%% nodes.
max_failures(Nodes, Holds) ->
    Limit =
        case length(Nodes) > 12 of
            true -> 3;
            false -> length(Nodes)
        end,
    max_failures(0, Limit, Nodes, Holds).

max_failures(F, Limit, _Nodes, _Holds) when F > Limit ->
    Limit;
max_failures(F, Limit, Nodes, Holds) ->
    case lists:all(Holds, subsets(F, Nodes)) of
        true -> max_failures(F + 1, Limit, Nodes, Holds);
        false -> max(0, F - 1)
    end.

%% The sets of `N' elements of the list.
subsets(0, _List) -> [[]];
subsets(_N, []) -> [];
subsets(N, [Head | Tail]) -> [[Head | Subset] || Subset <- subsets(N - 1, Tail)] ++ subsets(N, Tail).

%% The reasons for which the cluster is not healthy.
problems(View, CState, Controller, Given) ->
    #{coordinators := Coordinators, logs := Logs, storages := Storages, epoch := Epoch, workers := Workers, min_workers := MinWorkers} = View,
    [coordinated_state_unavailable || CState =:= unavailable] ++
        [{coordinator_down, Node} || {Node, false} <- Coordinators] ++
        [no_epoch || Epoch =:= undefined, CState =/= unavailable] ++
        [no_cluster_controller || Controller =:= undefined, CState =/= unavailable] ++
        [{epoch_not_recruited, Epoch} || Controller =/= undefined, Epoch =/= undefined, Given =/= Epoch] ++
        [{log_down, Node} || {Node, false} <- Logs] ++
        [{workers, length(Workers), MinWorkers} || Controller =/= undefined, length(Workers) < MinWorkers] ++
        [{storage_down, Node} || {Node, down} <- Storages] ++
        [{storage_copying, Node} || {Node, #{copying := true}} <- Storages] ++
        [{storage_behind, Node} || {Node, #{copying := false, current := false}} <- Storages].

%% The processes of one node.
node_view(Node, #{coordinators := Coordinators, logs := Logs, workers := Workers, storages := Storages}) ->
    #{
        coordinator =>
            case lists:keyfind(Node, 1, Coordinators) of
                {Node, true} -> up;
                {Node, false} -> down;
                false -> none
            end,
        worker => lists:member(Node, Workers),
        logs => length([up || {N, true} <- Logs, N =:= Node]),
        storage =>
            case lists:keyfind(Node, 1, Storages) of
                {Node, Info = #{}} -> maps:with([version, copying, current], Info);
                {Node, down} -> down;
                false -> none
            end
    }.

%% ---------------------------------------------------------------------------
%% Reads
%% ---------------------------------------------------------------------------

%% Calls a function of this module on a node. The result is `undefined'
%% if the node does not answer.
on_node(Node, Function, Arguments, _Timeout) when Node =:= node() ->
    apply(?MODULE, Function, Arguments);
on_node(Node, Function, Arguments, Timeout) ->
    try
        erpc:call(Node, ?MODULE, Function, Arguments, Timeout)
    catch
        _:_ -> undefined
    end.

alive(Pid, Timeout) ->
    on_node(node(Pid), running, [Pid], Timeout) =:= true.

%% @private
%% A process with the registered name runs on this node.
registered(Name) ->
    is_pid(whereis(Name)).

%% @private
%% The process runs on this node.
running(Pid) ->
    is_process_alive(Pid).

node_of(Pid) when is_pid(Pid) -> node(Pid);
node_of({Name, Node}) when is_atom(Name), is_atom(Node) -> Node;
node_of(Name) when is_atom(Name) -> node().

%% The version of a StorageServer and whether it copies, or `down'.
storage(Storage) ->
    try redwall_storage:info(Storage) of
        Info -> maps:with([version, copying], Info)
    catch
        exit:_ -> down
    end.
