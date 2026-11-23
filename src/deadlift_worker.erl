%% @doc The worker.
%%
%% An Erlang node that can run roles of the transaction system has one
%% worker process. The worker registers with the ClusterController, and
%% starts a role on its node when the ClusterController or a Sequencer
%% asks for one. In FoundationDB, each `fdbserver' process is a worker
%% (`fdbserver/worker.actor.cpp'). The paper does not describe the worker.
%%
%% == Registration ==
%%
%% <ol>
%% <li>The worker reads the coordinated state, which has the term and the
%%   process of the ClusterController
%%   (see {@link deadlift_cluster_controller}).</li>
%% <li>The worker monitors the ClusterController and sends a registration
%%   with its own process, its node, its class and the term.</li>
%% <li>The ClusterController answers with the information about the
%%   database (see `db_info/0').</li>
%% <li>The worker reads the coordinated state again every `poll_ms'
%%   (default 1000 ms), and at once when the ClusterController stops or
%%   the connection to its node ends. While no ClusterController has
%%   answered, the worker reads after 10 ms, and doubles the time with
%%   each read up to `poll_ms'. A worker therefore registers within some
%%   10 ms of the election of a ClusterController.</li>
%% <li>The worker sends a registration after each read at the interval,
%%   also when the ClusterController has answered. The ClusterController
%%   answers each registration with the information about the database.
%%   A worker that missed a message therefore receives the information
%%   within `poll_ms'.</li>
%% </ol>
%%
%% A separate process performs each read of the coordinated state, with a
%% limit of `cstate_timeout_ms' (default 1000 ms). The worker answers a
%% request for a role in that time.
%%
%% == Candidate ==
%%
%% With the option `candidate', the worker starts a candidate for the
%% ClusterController on its node ({@link deadlift_cluster_controller}),
%% with the options of the map and the coordinators of the worker. The two
%% processes are linked: when one of them stops, the other one stops too.
%% A node then has one process to start and to supervise. In FoundationDB,
%% the ClusterController also runs in a worker.
%%
%% == Roles ==
%%
%% `start_role/3' starts a LogServer or the process of an epoch
%% ({@link deadlift_epoch}) on the node of the worker. The worker does not link to the role and
%% does not monitor it. The ClusterController monitors the roles of an
%% epoch.
%%
%% == Information about the database ==
%%
%% The ClusterController sends the information about the database to each
%% registered worker when the information changes. The worker keeps it in
%% an ETS table, and `db_info/0' reads the table without a message. The
%% information is a map:
%%
%% <ul>
%% <li>`term': the term of the ClusterController that sent it.</li>
%% <li>`epoch': the epoch that accepts commits, or `undefined'.</li>
%% <li>`proxies': the process of the epoch, in a list. A client sends its
%%   commits and its requests for a read version to it.</li>
%% </ul>
%%
%% In FoundationDB this information is `ServerDBInfo', and the part for
%% clients is `ClientDBInfo'.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The worker reads the coordinated state at an interval. A worker of
%%   FoundationDB waits for a change with a request that the coordinator
%%   answers late.</li>
%% <li>The roles of deadlift do not read the information about the
%%   database. They read the process of each LogServer from the
%%   coordinated state.</li>
%% <li>A worker has one of three classes and no locality. See
%%   {@link deadlift_cluster_controller}.</li>
%% </ul>
-module(deadlift_worker).

-behaviour(gen_server).

-include("deadlift_eta.hrl").
-eta_observe({state, [controller, registered, db_info]}).

-export([start/1, start_link/1, stop/0, stop/1]).
-export([start_role/3, db_info/0, db_info/1, info/0, info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0, role/0, class/0, db_info/0]).

-type role() :: log | proxy.

%% The ClusterController prefers a worker of the class `log' for a
%% LogServer, and a worker of the class `stateless' for the process of an
%% epoch.
%% The default is `unset'. FoundationDB has these and further process
%% classes.
-type class() :: log | stateless | unset.

%% `roles' replaces the function that starts a role. The default starts
%% the role with `start/1' of its module. A test uses the option to record
%% the roles.
-type options() :: #{
    coordinators := deadlift_cstate:servers(),
    %% The registered name of the worker and the name of its ETS table.
    %% The default is `deadlift_worker'. A node that runs the workers of
    %% several clusters gives each worker a name.
    name => atom(),
    class => class(),
    %% Further options of a candidate for the ClusterController on this
    %% node. Without the option, the node has no candidate.
    candidate => map(),
    roles => fun((role(), map()) -> {ok, pid()} | {error, term()}),
    poll_ms => pos_integer(),
    cstate_timeout_ms => pos_integer()
}.

-type db_info() :: #{
    term := non_neg_integer(),
    epoch := deadlift_cstate:epoch() | undefined,
    proxies := [pid()]
}.

-define(ROLE_TIMEOUT_MS, 5000).
%% The first time between two polls while no ClusterController has
%% answered. The time doubles with each poll, up to `poll_ms'.
-define(FAST_MS, 10).

-record(state, {
    options :: options(),
    %% The candidate for the ClusterController that the worker started.
    candidate :: pid() | undefined,
    %% The term and the process of the ClusterController that the worker
    %% read last.
    controller = {0, undefined} :: {non_neg_integer(), pid() | undefined},
    monitor :: reference() | undefined,
    %% Whether the ClusterController of the term has answered.
    registered = false :: boolean(),
    %% The monitor of the process that reads the coordinated state, and
    %% whether the worker sends a registration after the read also to the
    %% ClusterController that it knows.
    reader :: reference() | undefined,
    again = false :: boolean(),
    %% The timer of the next poll, and the time until the next poll while
    %% no ClusterController has answered.
    timer :: reference() | undefined,
    fast_ms = ?FAST_MS :: pos_integer(),
    %% A copy of the information in the ETS table.
    db_info :: db_info() | undefined
}).

%% @doc Starts the worker of this node without a link to the caller.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start({local, maps:get(name, Options, ?MODULE)}, ?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link({local, maps:get(name, Options, ?MODULE)}, ?MODULE, Options, []).

%% @doc Stops the worker of this node. The roles of the node continue.
-spec stop() -> ok.
stop() ->
    stop(?MODULE).

%% @doc Stops the worker with the name `Name' of this node.
-spec stop(atom()) -> ok.
stop(Name) ->
    gen_server:stop(Name).

%% @doc Starts a role on the node of the worker `Worker'. The result is
%% `{error, Reason}' if the worker does not answer in 5 seconds.
-spec start_role(pid(), role(), map()) -> {ok, pid()} | {error, term()}.
start_role(Worker, Role, Options) ->
    try
        gen_server:call(Worker, {start_role, Role, Options}, ?ROLE_TIMEOUT_MS)
    catch
        exit:{Reason, {gen_server, call, _}} -> {error, {worker, Reason}};
        exit:Reason -> {error, {worker, Reason}}
    end.

%% @doc Returns the information about the database that the worker of this
%% node received last, or `undefined'. The function sends no message.
-spec db_info() -> db_info() | undefined.
db_info() ->
    db_info(?MODULE).

%% @doc Returns the information about the database of the worker with the
%% name `Name' of this node.
-spec db_info(atom()) -> db_info() | undefined.
db_info(Name) ->
    try ets:lookup(Name, db_info) of
        [{db_info, Info}] -> Info;
        [] -> undefined
    catch
        error:badarg -> undefined
    end.

%% @doc Returns the term and the process of the ClusterController that the
%% worker of this node knows, whether that ClusterController has answered
%% the registration, and the candidate that the worker started.
-spec info() -> #{controller := {non_neg_integer(), pid() | undefined}, registered := boolean(), candidate := pid() | undefined}.
info() ->
    info(?MODULE).

%% @doc `info/0' for the worker with the name `Name' of this node.
-spec info(atom()) -> #{controller := {non_neg_integer(), pid() | undefined}, registered := boolean(), candidate := pid() | undefined}.
info(Name) ->
    gen_server:call(Name, info).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options = #{coordinators := Coordinators}) ->
    Name = maps:get(name, Options, ?MODULE),
    Name = ets:new(Name, [named_table, protected, {read_concurrency, true}]),
    %% The worker handles the stop of the candidate, and stops the
    %% candidate in `terminate/2'.
    process_flag(trap_exit, true),
    Candidate =
        case Options of
            #{candidate := CandidateOptions} ->
                {ok, Pid} = deadlift_cluster_controller:start_link(CandidateOptions#{coordinators => Coordinators}),
                Pid;
            _ ->
                undefined
        end,
    self() ! poll,
    {ok, #state{options = Options, candidate = Candidate}}.

%% @private
terminate(_Reason, #state{candidate = Candidate}) ->
    _ = is_pid(Candidate) andalso exit(Candidate, shutdown),
    ok.

%% @private
handle_call({start_role, Role, RoleOptions}, _From, State = #state{options = Options}) ->
    Start = maps:get(roles, Options, fun start_default/2),
    Reply =
        try
            Start(Role, RoleOptions)
        catch
            Class:Reason -> {error, {Class, Reason}}
        end,
    {reply, Reply, State};
handle_call(info, _From, State = #state{controller = Controller, registered = Registered, candidate = Candidate}) ->
    {reply, #{controller => Controller, registered => Registered, candidate => Candidate}, State}.

%% @private
%% The information about the database, from the ClusterController of
%% `Term'. It is also the answer to a registration.
handle_cast({db_info, Info = #{term := Term}}, State = #state{controller = {Term, _Pid}}) ->
    true = ets:insert(maps:get(name, State#state.options, ?MODULE), {db_info, Info}),
    {noreply, State#state{registered = true, db_info = Info}};
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info({'EXIT', Candidate, Reason}, State = #state{candidate = Candidate}) ->
    %% The candidate stopped. The worker stops too, so that a supervisor
    %% starts both again.
    {stop, {candidate, Reason}, State#state{candidate = undefined}};
handle_info(poll, State) ->
    {noreply, find(schedule(State#state{again = true}))};
handle_info({'DOWN', Reader, process, _Pid, Reason}, State = #state{reader = Reader, again = Again}) ->
    State1 = State#state{reader = undefined, again = false},
    case Reason of
        {cstate, Result} -> {noreply, found(Result, Again, State1)};
        _ -> {noreply, State1}
    end;
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State = #state{monitor = Monitor}) ->
    %% The ClusterController stopped, or the connection to its node ended.
    %% The worker registers again when the coordinated state has another
    %% term, or at the next poll with the same ClusterController.
    %% The next poll is soon, so that the worker registers with a new
    %% ClusterController shortly after its election.
    {noreply, find(schedule(State#state{monitor = undefined, registered = false, fast_ms = ?FAST_MS}))};
handle_info(_Message, State) ->
    {noreply, State}.

%% Starts the timer of the next poll. A worker with an answer of the
%% ClusterController polls every `poll_ms'. A worker without an answer
%% polls after 10 ms, and doubles the time with each poll up to `poll_ms',
%% so that it registers shortly after the election of a
%% ClusterController.
schedule(State = #state{options = Options, timer = Timer, registered = Registered, fast_ms = Fast}) ->
    _ = is_reference(Timer) andalso erlang:cancel_timer(Timer),
    PollMs = maps:get(poll_ms, Options, 1000),
    case Registered of
        true ->
            State#state{timer = erlang:send_after(PollMs, self(), poll), fast_ms = ?FAST_MS};
        false ->
            State#state{timer = erlang:send_after(min(Fast, PollMs), self(), poll), fast_ms = min(PollMs, Fast * 2)}
    end.

%% Starts a process that reads the coordinated state, if no read is in
%% progress.
find(State = #state{reader = undefined, options = Options = #{coordinators := Coordinators}}) ->
    Timeout = maps:get(cstate_timeout_ms, Options, 1000),
    {_Pid, Reader} = spawn_monitor(fun() -> exit({cstate, deadlift_cstate:read(Coordinators, Timeout)}) end),
    State#state{reader = Reader};
find(State) ->
    State.

%% Handles the coordinated state that a read returned, and registers if
%% the ClusterController has not answered. With `Again' false, the worker
%% does not send a second registration to the ClusterController that it
%% knows. A monitor of a process that has stopped ends at once, so the
%% worker waits for the next poll.
found(Result, Again, State = #state{controller = Known}) ->
    case Result of
        {ok, #{controller := Known}} when Again ->
            register(State);
        {ok, #{controller := Known}} ->
            State;
        {ok, #{controller := Controller}} ->
            register(watch(Controller, State));
        _ ->
            State
    end.

watch(Controller = {_Term, Pid}, State = #state{monitor = Old}) ->
    _ = is_reference(Old) andalso erlang:demonitor(Old, [flush]),
    Monitor =
        case is_pid(Pid) of
            true -> erlang:monitor(process, Pid);
            false -> undefined
        end,
    State#state{controller = Controller, monitor = Monitor, registered = false}.

register(State = #state{controller = {_Term, undefined}}) ->
    State;
register(State = #state{controller = {Term, Pid}, monitor = Monitor0, options = Options}) ->
    %% After the end of a monitor, a new monitor and the registration
    %% connect the nodes again.
    Monitor =
        case Monitor0 of
            undefined -> erlang:monitor(process, Pid);
            _ -> Monitor0
        end,
    ok = gen_server:cast(Pid, {register, self(), #{node => node(), class => maps:get(class, Options, unset)}, Term}),
    State#state{monitor = Monitor}.

start_default(log, Options) -> deadlift_log:start(Options);
start_default(proxy, Options) -> deadlift_epoch:start(Options).
