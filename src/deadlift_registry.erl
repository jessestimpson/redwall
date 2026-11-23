%% @doc A registry of the processes of the connected Erlang nodes.
%%
%% The coordinated state identifies a LogServer with a stable identifier,
%% and not with a process identifier. This module finds the process that
%% has an identifier, on the local node or on a connected node.
%%
%% A process registers a name for itself with `register/1'. A caller finds
%% the process with `whereis/1', which reads an ETS table and sends no
%% message. The registry removes a name when the process stops.
%%
%% == Several nodes ==
%%
%% Each node has one registry process. The registries exchange the names:
%%
%% <ul>
%% <li>When a process registers a name, the registry of its node sends the
%%   name and the process to the registry of each connected node.</li>
%% <li>When a registry starts, it asks the registry of each connected node
%%   for the names of that node.</li>
%% <li>When a node connects, the registry sends the names of its own node
%%   to the registry of the new node. A registry without a name of its own
%%   node does not ask for the message about a new node.</li>
%% <li>A registry monitors each process in its table, and removes the name
%%   when the process stops.</li>
%% <li>When the connection to the node of a process ends, the registry
%%   keeps the name, because the process can be alive. A message to the
%%   process connects the nodes again. When the nodes are connected again,
%%   the registry of the other node sends its names, and the registry
%%   monitors the process again.</li>
%% <li>A process that starts another process on another node can add the
%%   name to the registry of its own node with `learn/2'. The process then
%%   does not wait for the message of the other registry.</li>
%% </ul>
%%
%% A StorageServer or a Proxy on one node finds a LogServer on another
%% node in this way.
%%
%% The module is not a `via' callback module. eta does not schedule a
%% `gen_server' that starts with a `via' name.
%%
%% In FoundationDB, a worker registers its roles with the
%% ClusterController, and the other roles ask the ClusterController.
%% deadlift has no ClusterController yet, and this registry has its place.
%% This part is not in the paper.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>A name reaches another node after `register/1' has returned. For
%%   that time, `whereis/1' on the other node returns `undefined'.</li>
%% <li>`register/1' refuses a name that a process of the local table has.
%%   Two processes on two nodes can register one name at the same time.
%%   Each registry then keeps the process that it learned first. The
%%   callers of deadlift register a LogServer identifier, which the
%%   Sequencer of one epoch creates.</li>
%% <li>A registry receives the names of the nodes to which its node is
%%   connected. The registry does not connect nodes. The nodes of an
%%   Erlang cluster are connected to each other by default
%%   (`connect_all' of the kernel application).</li>
%% <li>The registry keeps the name of a process on a node that has
%%   stopped. A message to such a process has no answer.</li>
%% <li>The registry loses every name when the registry process stops.</li>
%% </ul>
-module(deadlift_registry).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-compile({no_auto_import, [register/2, whereis/1]}).

-export([start/0, start/1, start_link/0]).
-export([register/1, learn/2, whereis/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, ?MODULE).

%% @doc Starts the registry without a link to the caller.
-spec start() -> {ok, pid()} | {error, term()}.
start() ->
    start(#{}).

%% @doc Starts the registry. `exchange => false' plants a defect for the
%% simulation: the registry sends no name to the registry of another node.
-spec start(#{exchange => boolean()}) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Options, []).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

%% @doc Registers `Name' for the calling process. The result is `no' if a
%% process that is alive has the name.
-spec register(term()) -> yes | no.
register(Name) ->
    gen_server:call(?MODULE, {register, Name, self()}).

%% @doc Adds the name `Name' of the process `Pid' to the registry of this
%% node. `Pid' has registered the name on its own node. The function does
%% nothing if the registry has the name.
-spec learn(term(), pid()) -> ok.
learn(Name, Pid) ->
    gen_server:call(?MODULE, {learn, Name, Pid}).

%% @doc Returns the process with the name `Name', or `undefined'.
-spec whereis(term()) -> pid() | undefined.
whereis(Name) ->
    try ets:lookup(?TABLE, Name) of
        [{Name, Pid, _Monitor}] ->
            %% The process can have stopped before the registry has
            %% received the `DOWN' message.
            %% `is_process_alive/1' raises an error for a process of
            %% another node. The registry removes such a process when it
            %% stops.
            try is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            catch
                error:badarg -> Pid
            end;
        [] ->
            undefined
    catch
        error:badarg -> undefined
    end.

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
%% The state has the map from a monitor to the name of the monitored
%% process.
init(Options) ->
    ?TABLE = ets:new(?TABLE, [named_table, protected, {read_concurrency, true}]),
    State = #{monitors => #{}, exchange => maps:get(exchange, Options, true), subscribed => false},
    ok = cast_all({names_wanted, node()}, State),
    {ok, State}.

%% @private
handle_call({register, Name, Pid}, _From, State) ->
    case whereis(Name) of
        undefined ->
            ok = cast_all({registered, Name, Pid}, State),
            {reply, yes, insert(Name, Pid, subscribe(State))};
        _Pid ->
            {reply, no, State}
    end;
handle_call({learn, Name, Pid}, _From, State) ->
    {reply, ok, learned(Name, Pid, State)}.

%% @private
%% A name from the registry of another node.
handle_cast({registered, Name, Pid}, State) ->
    {noreply, learned(Name, Pid, State)};
handle_cast({names_wanted, Node}, State) ->
    ok = send_names(Node, State),
    {noreply, State};
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', Monitor, process, _Pid, Reason}, State = #{monitors := Monitors}) ->
    case maps:take(Monitor, Monitors) of
        {Name, Monitors1} when Reason =:= noconnection ->
            %% The process can be alive. The name stays without a monitor.
            [{Name, Pid, Monitor}] = ets:lookup(?TABLE, Name),
            true = ets:insert(?TABLE, {Name, Pid, none}),
            {noreply, State#{monitors := Monitors1}};
        {Name, Monitors1} ->
            ets:delete(?TABLE, Name),
            {noreply, State#{monitors := Monitors1}};
        error ->
            {noreply, State}
    end;
handle_info({nodeup, Node}, State) ->
    ok = send_names(Node, State),
    {noreply, State};
handle_info(_Message, State) ->
    {noreply, State}.

%% Asks for a message when a node connects. A registry needs the message
%% only when a process of its own node has a name, because it sends only
%% such names to a node that connects.
subscribe(State = #{subscribed := true}) ->
    State;
subscribe(State) ->
    ok = net_kernel:monitor_nodes(true),
    State#{subscribed := true}.

%% Sends a message to the registry of each connected node.
cast_all(_Message, #{exchange := false}) ->
    ok;
cast_all(Message, _State) ->
    lists:foreach(fun(Node) -> gen_server:cast({?MODULE, Node}, Message) end, nodes()).

%% Adds a name that another node has. If the registry has the name for
%% the same process without a monitor, the registry monitors the process
%% again.
learned(Name, Pid, State) ->
    case ets:lookup(?TABLE, Name) of
        [{Name, Pid, none}] ->
            insert(Name, Pid, State);
        _ ->
            case whereis(Name) of
                undefined -> insert(Name, Pid, State);
                _Pid -> State
            end
    end.

insert(Name, Pid, State) ->
    State1 = #{monitors := Monitors} = remove(Name, State),
    Monitor = erlang:monitor(process, Pid),
    true = ets:insert(?TABLE, {Name, Pid, Monitor}),
    State1#{monitors := Monitors#{Monitor => Name}}.

%% Sends the names of the processes of this node to the registry of
%% `Node'.
send_names(_Node, #{exchange := false}) ->
    ok;
send_names(Node, _State) ->
    lists:foreach(
        fun({Name, Pid, _Monitor}) ->
            case node(Pid) =:= node() of
                true -> gen_server:cast({?MODULE, Node}, {registered, Name, Pid});
                false -> ok
            end
        end,
        ets:tab2list(?TABLE)
    ).

remove(Name, State = #{monitors := Monitors}) ->
    case ets:lookup(?TABLE, Name) of
        [{Name, _Pid, none}] ->
            ets:delete(?TABLE, Name),
            State;
        [{Name, _Pid, Monitor}] ->
            erlang:demonitor(Monitor, [flush]),
            ets:delete(?TABLE, Name),
            State#{monitors := maps:remove(Monitor, Monitors)};
        [] ->
            State
    end.
