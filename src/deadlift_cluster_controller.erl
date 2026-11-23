%% @doc The ClusterController.
%%
%% The ClusterController monitors the roles of the transaction system and
%% recruits a new Sequencer when a role fails. See section 2.3.1 of Zhou
%% et al., "FoundationDB: A Distributed Unbundled Transactional Key Value
%% Store", SIGMOD 2021, and `fdbserver/ClusterController.actor.cpp' and
%% `design/recovery-internals.md' of FoundationDB 7.3.
%%
%% A process of this module is a candidate or the leader. Several nodes
%% can each run one candidate. The leader is the ClusterController.
%%
%% == Election ==
%%
%% The coordinated state has the term and the process of the
%% ClusterController ({@link deadlift_cstate}).
%%
%% <ol>
%% <li>A candidate reads the coordinated state every `poll_ms' (default
%%   1000 ms) and monitors the ClusterController of the term.</li>
%% <li>If the term has no ClusterController, or the monitor of the
%%   ClusterController has ended, the candidate proposes itself with the
%%   term that it read (`deadlift_cstate:elect/4').</li>
%% <li>The coordinated state accepts one proposal for a term. The
%%   candidate of that proposal is the leader of the next term.</li>
%% <li>The leader reads the coordinated state every `poll_ms'. If the
%%   coordinated state has another term, the leader is a candidate
%%   again.</li>
%% </ol>
%%
%% A separate process performs each read and each proposal, with a limit
%% of `cstate_timeout_ms' (default 1000 ms). The candidate or the leader
%% answers its other messages in that time. If the answer to a proposal
%% does not arrive, the candidate learns the result from the next
%% read.
%%
%% Two processes can be the leader at the same time, for at most `poll_ms'
%% plus the time of a read. Each of them can recruit a Sequencer. The
%% recovery count of the coordinated state then lets one Sequencer write,
%% as for any two recoveries at the same time. FoundationDB has the same
%% property (`design/recovery-internals.md').
%%
%% == Workers ==
%%
%% A worker ({@link deadlift_worker}) registers with the leader. The
%% leader monitors each registered worker, and removes a worker when the
%% worker stops or the connection to its node ends.
%%
%% == Recruitment ==
%%
%% The leader recruits when it has no epoch and at least `min_workers'
%% workers (default 1):
%%
%% <ol>
%% <li>The leader orders the workers by their nodes, and starts at a
%%   different worker in each attempt. For the LogServers, the workers of
%%   the class `log' come first and the workers of the class `stateless'
%%   last. The first `logs' workers of this order receive one LogServer
%%   each. For the Sequencer, the Resolver and the Proxy, the workers of
%%   the class `stateless' come first and the workers of the class `log'
%%   last. The first three workers of this order receive one of these
%%   roles each. With fewer workers, a worker receives several roles.
%%   With the option `stateless => together', the first worker of this
%%   order receives the three roles. A message between two of them then
%%   stays inside one node. The failure of each of the three roles starts
%%   a recovery in both cases.</li>
%% <li>The leader starts the Sequencer. The Sequencer performs the
%%   recovery, and starts the LogServers and the Resolver on the chosen
%%   workers.</li>
%% <li>After the recovery, the leader starts the Proxy.</li>
%% <li>The leader sends the information about the database to every
%%   worker (see {@link deadlift_worker}).</li>
%% </ol>
%%
%% The leader monitors the Sequencer, the LogServers, the Resolver and the
%% Proxy of the attempt. When one of them stops, or the connection to its
%% node ends, or the recovery fails, the leader abandons the attempt and
%% recruits again after `retry_ms' (default 500 ms).
%%
%% The leader also compares the recovery count of the coordinated state
%% with its epoch at every read. If the count is above the epoch, another
%% Sequencer has started a recovery, and the leader recruits again. In
%% FoundationDB, the roles of an ended epoch stop, and the
%% ClusterController learns it from their failure.
%%
%% == Departures from FoundationDB ==
%%
%% <ul>
%% <li>FoundationDB elects the ClusterController with a register on each
%%   coordinator, which is not a consensus operation, and the leader sends
%%   a heartbeat to the coordinators (`fdbserver/LeaderElection.actor.cpp').
%%   deadlift stores the leader in the coordinated state, and a candidate
%%   detects the failure of the leader with an Erlang monitor. This part
%%   is not in the paper.</li>
%% <li>In FoundationDB 7.3 the recovery runs in the process of the
%%   ClusterController. In deadlift the Sequencer performs the recovery,
%%   as in the paper.</li>
%% <li>In the paper, the Sequencer recruits the Proxies. In deadlift the
%%   ClusterController starts the Proxy after the recovery.</li>
%% </ul>
%%
%% == Limitations ==
%%
%% <ul>
%% <li>A candidate proposes itself when the connection to the node of the
%%   leader ends. If two candidates cannot connect to each other and both
%%   can connect to the coordinators, each election replaces the leader,
%%   and each new leader starts a recovery.</li>
%% <li>The leader does not stop the roles of an abandoned epoch. A
%%   Sequencer and a Proxy of an ended epoch continue to run.</li>
%% <li>The leader learns the Sequencer of an attempt in a message. If the
%%   leader abandons the attempt before that message, the leader stops
%%   the Sequencer when the message arrives. If the network loses the
%%   message, the Sequencer continues, and the recovery of the next
%%   attempt can fail on the recovery count. The leader then recruits
%%   again.</li>
%% <li>The leader keeps the workers and the epoch in memory. A new leader
%%   starts a recovery, also when the roles of the current epoch run.
%%   FoundationDB has the same property.</li>
%% <li>The leader does not start StorageServers.</li>
%% <li>A worker has one of three classes. FoundationDB has more classes,
%%   a fitness of each class for each role, and the locality of a
%%   process.</li>
%% </ul>
-module(deadlift_cluster_controller).

-behaviour(gen_server).

-include("deadlift_eta.hrl").
-eta_observe({state, [role, term, db_info]}).

-export([start/1, start_link/1, stop/1, info/1, force_recovery/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([recruit/4]).

-export_type([options/0, event/0]).

%% `sequencer' holds further options of the Sequencer, and `proxy' further
%% options of the Proxy. `notify' receives the events of the process. A
%% test uses it.
-type options() :: #{
    coordinators := deadlift_cstate:servers(),
    logs => pos_integer(),
    replication => pos_integer(),
    min_workers => pos_integer(),
    %% `spread' places the Sequencer, the Resolver and the Proxy on three
    %% workers. `together' places them on one worker. The default is
    %% `spread'.
    stateless => spread | together,
    sequencer => map(),
    proxy => map(),
    poll_ms => pos_integer(),
    retry_ms => non_neg_integer(),
    recovery_timeout_ms => pos_integer(),
    cstate_timeout_ms => pos_integer(),
    notify => fun((event()) -> term()),
    %% `monitor_roles => false' and `step_down => false' plant defects
    %% for the simulation. With the first, the leader does not recruit
    %% after the failure of a role. With the second, a leader stays the
    %% leader when the coordinated state has another term.
    monitor_roles => boolean(),
    step_down => boolean()
}.

-type event() ::
    {leader, Term :: pos_integer()}
    | {candidate, Term :: non_neg_integer()}
    | {recruiting, Attempt :: pos_integer(), #{atom() => term()}}
    | {recovered, map()}
    | {recovery_failed, term()}
    | {role_failed, deadlift_worker:role(), pid()}
    | {superseded, Epoch :: pos_integer(), Count :: pos_integer()}.
%% `{recovered, Result}' has the result of the recovery
%% (`deadlift_recovery:result()') with the keys `sequencer' and `proxy'.

-record(generation, {
    attempt :: pos_integer(),
    %% The process that waits for the recovery, with its monitor.
    recruiter :: {pid(), reference()} | undefined,
    sequencer :: pid() | undefined,
    %% The monitored roles of the attempt.
    roles = #{} :: #{reference() => {deadlift_worker:role(), pid()}},
    recovered = false :: boolean()
}).

-record(state, {
    options :: options(),
    role = candidate :: candidate | leader,
    %% The term of this process as the leader.
    term = 0 :: non_neg_integer(),
    %% The term and the ClusterController that this process read last.
    known = {0, undefined} :: {non_neg_integer(), pid() | undefined},
    monitor :: reference() | undefined,
    %% Whether the ClusterController of `known' has stopped or is not
    %% connected.
    down = true :: boolean(),
    %% The monitor of the process that reads the coordinated state or
    %% proposes this process.
    job :: reference() | undefined,
    workers = #{} :: #{pid() => {node(), deadlift_worker:class(), reference()}},
    attempt = 0 :: non_neg_integer(),
    %% Whether the leader waits for `retry_ms' after a failure.
    waiting = false :: boolean(),
    generation :: #generation{} | undefined,
    db_info :: deadlift_worker:db_info() | undefined
}).

%% @doc Starts a candidate without a link to the caller.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Controller) ->
    gen_server:stop(Controller).

%% @doc Returns whether the process is the leader, its term, the nodes of
%% the registered workers, and the information about the database.
-spec info(gen_server:server_ref()) ->
    #{
        role := candidate | leader,
        term := non_neg_integer(),
        workers := [node()],
        db_info := deadlift_worker:db_info() | undefined
    }.
info(Controller) ->
    gen_server:call(Controller, info).

%% @doc Asks the process to abandon its epoch and to recruit again. A
%% candidate ignores the request. The ClusterController of FoundationDB
%% has the trigger `forceMasterFailure' for the same purpose.
-spec force_recovery(gen_server:server_ref()) -> ok.
force_recovery(Controller) ->
    gen_server:cast(Controller, force_recovery).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options) ->
    self() ! poll,
    {ok, #state{options = Options}}.

%% @private
handle_call(info, _From, State = #state{role = Role, term = Term, known = {Known, _}, workers = Workers, db_info = Info}) ->
    Reply = #{
        role => Role,
        term =>
            case Role of
                leader -> Term;
                candidate -> Known
            end,
        workers => lists:sort([Node || {Node, _Class, _Monitor} <- maps:values(Workers)]),
        db_info => Info
    },
    {reply, Reply, State}.

%% @private
%% The registration of a worker for the term of this leader.
handle_cast({register, Worker, #{node := Node, class := Class}, Term}, State = #state{role = leader, term = Term, workers = Workers, db_info = Info}) ->
    Workers1 =
        case Workers of
            #{Worker := _} -> Workers;
            _ -> Workers#{Worker => {Node, Class, erlang:monitor(process, Worker)}}
        end,
    ok = gen_server:cast(Worker, {db_info, Info}),
    {noreply, maybe_recruit(State#state{workers = Workers1})};
handle_cast(force_recovery, State = #state{role = leader}) ->
    {noreply, maybe_recruit(abandon(State#state{waiting = false}))};
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info(poll, State = #state{options = Options}) ->
    _ = erlang:send_after(maps:get(poll_ms, Options, 1000), self(), poll),
    {noreply, poll(State)};
handle_info(recruit, State) ->
    {noreply, maybe_recruit(State#state{waiting = false})};
handle_info({recruited, Attempt, Role, Pid}, State = #state{role = leader, generation = Generation = #generation{attempt = Attempt, roles = Roles}}) ->
    Generation1 = Generation#generation{roles = Roles#{erlang:monitor(process, Pid) => {Role, Pid}}},
    Generation2 =
        case Role of
            sequencer -> Generation1#generation{sequencer = Pid};
            _ -> Generation1
        end,
    {noreply, State#state{generation = Generation2}};
handle_info({recruited, _Attempt, sequencer, Pid}, State = #state{role = leader}) ->
    %% The Sequencer of an attempt that the leader has abandoned. The
    %% clause above handles the attempt of the generation.
    exit(Pid, kill),
    {noreply, State};
handle_info({recovered, Attempt, Result, Proxy}, State = #state{role = leader, generation = Generation = #generation{attempt = Attempt}}) ->
    #state{term = Term} = State,
    #generation{recruiter = {_Pid, Monitor}, sequencer = Sequencer, roles = Roles} = Generation,
    true = erlang:demonitor(Monitor, [flush]),
    Generation1 = Generation#generation{
        recruiter = undefined,
        roles = Roles#{erlang:monitor(process, Proxy) => {proxy, Proxy}},
        recovered = true
    },
    Info = #{term => Term, epoch => maps:get(epoch, Result), sequencer => Sequencer, proxies => [Proxy]},
    ok = notify({recovered, Result#{sequencer => Sequencer, proxy => Proxy}}, State),
    {noreply, broadcast(State#state{generation = Generation1, db_info = Info})};
handle_info({recruit_failed, Attempt, Reason}, State = #state{role = leader, generation = #generation{attempt = Attempt}}) ->
    ok = notify({recovery_failed, Reason}, State),
    {noreply, retry(abandon(State))};
handle_info({'DOWN', Job, process, _Pid, Reason}, State0 = #state{job = Job}) ->
    State = State0#state{job = undefined},
    case Reason of
        {cstate, Result} -> {noreply, read(Result, State)};
        {elected, Result} -> {noreply, elected(Result, State)};
        _ -> {noreply, State}
    end;
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State = #state{monitor = Monitor}) ->
    %% The ClusterController that this candidate watches.
    {noreply, elect(State#state{monitor = undefined, down = true})};
handle_info({'DOWN', Monitor, process, Pid, Reason}, State = #state{workers = Workers, generation = Generation}) ->
    case {Workers, Generation} of
        {#{Pid := {_Node, _Class, Monitor}}, _} ->
            {noreply, State#state{workers = maps:remove(Pid, Workers)}};
        {_, #generation{recruiter = {Pid, Monitor}}} ->
            ok = notify({recovery_failed, {recruiter, Reason}}, State),
            {noreply, retry(abandon(State))};
        {_, #generation{roles = Roles = #{Monitor := {sequencer, Pid}}, recovered = false, recruiter = {_, _}}} ->
            %% The Sequencer stopped during its recovery. The recruiter
            %% waits for the Sequencer and reports the reason.
            {noreply, State#state{generation = Generation#generation{roles = maps:remove(Monitor, Roles)}}};
        {_, #generation{roles = #{Monitor := {Role, Pid}}}} ->
            ok = notify({role_failed, Role, Pid}, State),
            case State#state.options of
                #{monitor_roles := false} -> {noreply, State};
                _ -> {noreply, retry(abandon(State))}
            end;
        _ ->
            {noreply, State}
    end;
handle_info(_Message, State) ->
    {noreply, State}.

%% ---------------------------------------------------------------------------
%% Election
%% ---------------------------------------------------------------------------

%% Starts a process that reads the coordinated state, if no read and no
%% proposal is in progress. The ClusterController process does not wait
%% for the coordinated state, so it answers its other messages in that
%% time.
poll(State = #state{job = undefined, options = Options = #{coordinators := Coordinators}}) ->
    Timeout = maps:get(cstate_timeout_ms, Options, 1000),
    {_Pid, Monitor} = spawn_monitor(fun() -> exit({cstate, deadlift_cstate:read(Coordinators, Timeout)}) end),
    State#state{job = Monitor};
poll(State) ->
    State.

%% Handles the coordinated state that a read returned.
read(Result, State = #state{options = Options, role = Role, term = Term, known = Known}) ->
    Self = self(),
    case Result of
        {ok, #{controller := {Term, Self}, count := Count}} when Role =:= leader ->
            superseded(Count, State);
        {ok, #{controller := _Controller}} when Role =:= leader, map_get(step_down, Options) =:= false ->
            State;
        {ok, #{controller := Controller}} when Role =:= leader ->
            %% Another candidate has won an election.
            State1 = watch(Controller, abandon(State)),
            ok = notify({candidate, element(1, Controller)}, State1),
            elect(forget_workers(State1#state{role = candidate, db_info = undefined}));
        {ok, #{controller := {Term1, Self}}} ->
            %% The coordinated state accepted a proposal of this process,
            %% and the answer did not arrive.
            lead(Term1, State);
        {ok, #{controller := Known}} ->
            elect(State);
        {ok, #{controller := Controller}} ->
            elect(watch(Controller, State));
        _ ->
            State
    end.

%% Recruits again if the recovery count of the coordinated state is above
%% the epoch of the leader. Another Sequencer has then raised the count,
%% and the LogServers of the epoch of the leader are stopped or will be
%% stopped. No role of the epoch stops in this case, so the monitors of
%% the leader do not show it. The other Sequencer belongs to another
%% leader of an earlier term, or to an attempt that this leader has
%% abandoned.
superseded(Count, State = #state{generation = #generation{recovered = true}, db_info = #{epoch := Epoch}}) when is_integer(Epoch), Count > Epoch ->
    ok = notify({superseded, Epoch, Count}, State),
    maybe_recruit(abandon(State#state{waiting = false}));
superseded(_Count, State) ->
    State.

%% Monitors the ClusterController of the term.
watch(Controller = {_Term, Pid}, State = #state{monitor = Old}) ->
    _ = is_reference(Old) andalso erlang:demonitor(Old, [flush]),
    case is_pid(Pid) of
        true -> State#state{known = Controller, monitor = erlang:monitor(process, Pid), down = false};
        false -> State#state{known = Controller, monitor = undefined, down = true}
    end.

%% Starts a process that proposes this process, if the ClusterController
%% of the term has stopped or is not connected, and no read and no
%% proposal is in progress.
elect(State = #state{role = candidate, down = true, job = undefined, known = {Term, _Pid}, options = Options = #{coordinators := Coordinators}}) ->
    Self = self(),
    Timeout = maps:get(cstate_timeout_ms, Options, 1000),
    {_Pid1, Monitor} = spawn_monitor(fun() -> exit({elected, deadlift_cstate:elect(Coordinators, Term, Self, Timeout)}) end),
    State#state{job = Monitor};
elect(State) ->
    State.

%% Handles the answer to a proposal.
elected(Result, State = #state{role = candidate}) ->
    case Result of
        {ok, Term1} ->
            lead(Term1, State);
        {error, {term, Actual, Current}} when Current =:= self() ->
            %% The coordinated state accepted an earlier proposal of this
            %% process.
            lead(Actual, State);
        {error, {term, Actual, Current}} ->
            elect(watch({Actual, Current}, State));
        _ ->
            %% The coordinated state did not answer. The next poll reads
            %% it, and the process proposes itself again.
            State
    end;
elected(_Result, State) ->
    State.

lead(Term, State = #state{monitor = Old}) ->
    _ = is_reference(Old) andalso erlang:demonitor(Old, [flush]),
    State1 = State#state{
        role = leader,
        term = Term,
        known = {Term, self()},
        monitor = undefined,
        down = false,
        db_info = #{term => Term, epoch => undefined, sequencer => undefined, proxies => []}
    },
    ok = notify({leader, Term}, State1),
    State1.

forget_workers(State = #state{workers = Workers}) ->
    lists:foreach(fun({_Node, _Class, Monitor}) -> erlang:demonitor(Monitor, [flush]) end, maps:values(Workers)),
    State#state{workers = #{}}.

%% ---------------------------------------------------------------------------
%% Recruitment
%% ---------------------------------------------------------------------------

maybe_recruit(State = #state{role = leader, generation = undefined, waiting = false, workers = Workers, options = Options, attempt = Attempt0}) ->
    Logs = maps:get(logs, Options, 3),
    case map_size(Workers) >= maps:get(min_workers, Options, 1) of
        true ->
            Attempt = Attempt0 + 1,
            Plan = plan(Workers, Logs, Attempt, maps:get(stateless, Options, spread)),
            ok = notify({recruiting, Attempt, Plan}, State),
            Recruiter = spawn_monitor(?MODULE, recruit, [self(), Attempt, Plan, Options]),
            State#state{attempt = Attempt, generation = #generation{attempt = Attempt, recruiter = Recruiter}};
        false ->
            State
    end;
maybe_recruit(State) ->
    State.

%% The workers of the roles of one attempt. The workers are in the order
%% of their nodes, and the attempt selects the first worker.
plan(Workers, Logs, Attempt, Stateless0) ->
    Sorted = lists:sort([{Node, Class, Worker} || {Worker, {Node, Class, _Monitor}} <- maps:to_list(Workers)]),
    Count = length(Sorted),
    %% The workers of one rank in the order of their nodes, with a first
    %% worker that the attempt selects. Two attempts in sequence then have
    %% different workers for a role, if the rank has several workers.
    Rotate = fun
        ([]) ->
            [];
        (Group) ->
            {Front, Back} = lists:split(Attempt rem length(Group), Group),
            Back ++ Front
    end,
    Order = fun(Ranks) ->
        lists:append([
            Rotate([Worker || {_Node, Class, Worker} <- Sorted, maps:get(Class, Ranks) =:= Rank])
         || Rank <- [0, 1, 2]
        ])
    end,
    ForLogs = Order(#{log => 0, unset => 1, stateless => 2}),
    Stateless = Order(#{stateless => 0, unset => 1, log => 2}),
    Nth = fun(N, List) -> lists:nth(N rem Count + 1, List) end,
    %% With `together', the three roles are on the first worker of the
    %% order.
    Step =
        case Stateless0 of
            spread -> 1;
            together -> 0
        end,
    #{
        logs => [Nth(N, ForLogs) || N <- lists:seq(0, Logs - 1)],
        sequencer => Nth(0, Stateless),
        resolver => Nth(Step, Stateless),
        proxy => Nth(2 * Step, Stateless)
    }.

%% @private
%% The process of one attempt. It starts the Sequencer, waits for the end
%% of the recovery, and starts the Proxy. The Sequencer calls `Recruit' and
%% `RecruitResolver' in its own process.
recruit(Controller, Attempt, Plan, Options = #{coordinators := Coordinators}) ->
    #{logs := LogWorkers, sequencer := SequencerWorker, resolver := ResolverWorker, proxy := ProxyWorker} = Plan,
    Report = fun
        (Role, Reply = {ok, Pid}) when is_pid(Pid) ->
            Controller ! {recruited, Attempt, Role, Pid},
            Reply;
        (_Role, Reply) ->
            Reply
    end,
    Recruit = fun(LogOptions = #{id := {_Epoch, Index}}) ->
        Report(log, deadlift_worker:start_role(lists:nth(Index, LogWorkers), log, LogOptions))
    end,
    RecruitResolver = fun(ResolverOptions) ->
        Report(resolver, deadlift_worker:start_role(ResolverWorker, resolver, ResolverOptions))
    end,
    SequencerOptions = (maps:get(sequencer, Options, #{}))#{
        coordinators => Coordinators,
        logs => maps:get(logs, Options, 3),
        replication => maps:get(replication, Options, 2),
        recruit => Recruit,
        recruit_resolver => RecruitResolver
    },
    case Report(sequencer, deadlift_worker:start_role(SequencerWorker, sequencer, SequencerOptions)) of
        {ok, Sequencer} ->
            try deadlift_sequencer:recovered(Sequencer, maps:get(recovery_timeout_ms, Options, 10000)) of
                {ok, Result} ->
                    ProxyOptions = (maps:get(proxy, Options, #{}))#{sequencer => Sequencer},
                    case deadlift_worker:start_role(ProxyWorker, proxy, ProxyOptions) of
                        {ok, Proxy} ->
                            Controller ! {recovered, Attempt, Result, Proxy};
                        {error, Reason} ->
                            Controller ! {recruit_failed, Attempt, {proxy, Reason}}
                    end
            catch
                exit:{{shutdown, {recovery, Reason}}, _Call} ->
                    Controller ! {recruit_failed, Attempt, Reason};
                exit:{Reason, _Call} ->
                    Controller ! {recruit_failed, Attempt, {sequencer, Reason}}
            end;
        {error, Reason} ->
            Controller ! {recruit_failed, Attempt, {sequencer, Reason}}
    end.

%% Ends the attempt. The leader stops the Sequencer of an attempt whose
%% recovery has not ended, so that the Sequencer does not recruit after
%% this point. If the leader does not know the Sequencer yet, it stops the
%% Sequencer when the message of the recruiter arrives. The recruiter and
%% the other roles continue. The leader ignores the later messages of the
%% recruiter.
abandon(State = #state{generation = undefined}) ->
    State;
abandon(State = #state{generation = Generation, db_info = Info}) ->
    #generation{recruiter = Recruiter, sequencer = Sequencer, roles = Roles, recovered = Recovered} = Generation,
    case Recruiter of
        {_Pid, Monitor} -> true = erlang:demonitor(Monitor, [flush]);
        undefined -> true
    end,
    _ = is_pid(Sequencer) andalso not Recovered andalso exit(Sequencer, kill),
    lists:foreach(fun(Monitor) -> erlang:demonitor(Monitor, [flush]) end, maps:keys(Roles)),
    State1 = State#state{generation = undefined},
    case Info of
        #{epoch := Epoch} when Epoch =/= undefined ->
            broadcast(State1#state{db_info = Info#{epoch := undefined, sequencer := undefined, proxies := []}});
        _ ->
            State1
    end.

%% Recruits again after `retry_ms'. A registration of a worker in that
%% time does not start an attempt.
retry(State = #state{waiting = true}) ->
    State;
retry(State = #state{options = Options}) ->
    _ = erlang:send_after(maps:get(retry_ms, Options, 500), self(), recruit),
    State#state{waiting = true}.

%% Sends the information about the database to every registered worker.
broadcast(State = #state{workers = Workers, db_info = Info}) ->
    lists:foreach(fun(Worker) -> gen_server:cast(Worker, {db_info, Info}) end, lists:sort(maps:keys(Workers))),
    State.

notify(Event, #state{options = Options}) ->
    case Options of
        #{notify := Notify} ->
            _ = Notify(Event),
            ok;
        _ ->
            ok
    end.
