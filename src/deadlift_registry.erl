%% @doc A registry of the processes of one node.
%%
%% The coordinated state identifies a LogServer with a stable identifier,
%% and not with a process identifier. This module finds the process that
%% has an identifier, on the local node.
%%
%% A process registers a name for itself with `register/1'. A caller finds
%% the process with `whereis/1', which reads an ETS table and sends no
%% message. The registry removes a name when the process stops.
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
%% <li>The registry knows the processes of the local node only.</li>
%% <li>The registry loses every name when the registry process stops.</li>
%% </ul>
-module(deadlift_registry).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-compile({no_auto_import, [register/2, whereis/1]}).

-export([start/0, start_link/0]).
-export([register/1, whereis/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, ?MODULE).

%% @doc Starts the registry without a link to the caller.
-spec start() -> {ok, pid()} | {error, term()}.
start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Registers `Name' for the calling process. The result is `no' if a
%% process that is alive has the name.
-spec register(term()) -> yes | no.
register(Name) ->
    gen_server:call(?MODULE, {register, Name, self()}).

%% @doc Returns the process with the name `Name', or `undefined'.
-spec whereis(term()) -> pid() | undefined.
whereis(Name) ->
    try ets:lookup(?TABLE, Name) of
        [{Name, Pid, _Monitor}] ->
            %% The process can have stopped before the registry has
            %% received the `DOWN' message.
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
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
init([]) ->
    ?TABLE = ets:new(?TABLE, [named_table, protected, {read_concurrency, true}]),
    {ok, #{}}.

%% @private
%% The state maps a monitor to the name of the monitored process.
handle_call({register, Name, Pid}, _From, Monitors) ->
    case whereis(Name) of
        undefined ->
            Monitors1 = remove(Name, Monitors),
            Monitor = erlang:monitor(process, Pid),
            true = ets:insert(?TABLE, {Name, Pid, Monitor}),
            {reply, yes, Monitors1#{Monitor => Name}};
        _Pid ->
            {reply, no, Monitors}
    end.

%% @private
handle_cast(_Message, Monitors) ->
    {noreply, Monitors}.

%% @private
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, Monitors) ->
    case maps:take(Monitor, Monitors) of
        {Name, Monitors1} ->
            ets:delete(?TABLE, Name),
            {noreply, Monitors1};
        error ->
            {noreply, Monitors}
    end;
handle_info(_Message, Monitors) ->
    {noreply, Monitors}.

remove(Name, Monitors) ->
    case ets:lookup(?TABLE, Name) of
        [{Name, _Pid, Monitor}] ->
            erlang:demonitor(Monitor, [flush]),
            ets:delete(?TABLE, Name),
            maps:remove(Monitor, Monitors);
        [] ->
            Monitors
    end.
