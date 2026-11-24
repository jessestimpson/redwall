%% @doc The ClusterController.
%%
%% The ClusterController monitors the roles of the transaction system and
%% recruits a new Sequencer when a role fails. In redwall, the Sequencer,
%% the Resolver and the Proxy of an epoch are one process
%% ({@link redwall_epoch}), which this module calls the process of the
%% epoch. See section 2.3.1 of Zhou
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
%% ClusterController ({@link redwall_cstate}).
%%
%% <ol>
%% <li>A candidate reads the coordinated state every `poll_ms' (default
%%   1000 ms) and monitors the ClusterController of the term.</li>
%% <li>If the term has no ClusterController, or the monitor of the
%%   ClusterController has ended, the candidate proposes itself with the
%%   term that it read (`redwall_cstate:elect/4').</li>
%% <li>The coordinated state accepts one proposal for a term. The
%%   candidate of that proposal is the leader of the next term.</li>
%% <li>The leader reads the coordinated state every `poll_ms'. If the
%%   coordinated state has another term, the leader is a candidate
%%   again.</li>
%% </ol>
%%
%% A candidate polls again after 10 ms when the coordinated state did not
%% answer, and doubles the time up to `poll_ms'. A peace cluster has no
%% leader directly after its start, and does not answer in that time.
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
%% A worker ({@link redwall_worker}) registers with the leader. The
%% leader monitors each registered worker, and removes a worker when the
%% worker stops or the connection to its node ends.
%%
%% == Recruitment ==
%%
%% The leader recruits when it has no epoch and at least `min_workers'
%% workers (default 1). With several workers, each LogServer of an epoch
%% is on a worker of its own: an epoch has `logs' LogServers, or one for
%% each worker if the leader has fewer workers, and `replication' is at
%% most the number of LogServers. With one worker, every LogServer is on
%% that worker. A cluster of several nodes sets `min_workers' to 2 or
%% more, so that the first epoch does not have every LogServer on the
%% first worker that registers.
%%
%% <ol>
%% <li>The leader orders the workers by their nodes, and starts at a
%%   different worker in each attempt. For the LogServers, the workers of
%%   the class `log' come first and the workers of the class `stateless'
%%   last. The first `logs' workers of this order receive one LogServer
%%   each. For the process of the epoch, the workers of the class
%%   `stateless' come first and the workers of the class `log' last. The
%%   first worker of this order receives the process. With fewer workers,
%%   a worker receives several roles.</li>
%% <li>The leader starts the process of the epoch. The process performs
%%   the recovery, and starts the LogServers on the chosen workers.</li>
%% <li>The leader sends the information about the database to every
%%   worker (see {@link redwall_worker}).</li>
%% </ol>
%%
%% The leader monitors the process of the epoch and the LogServers of the
%% attempt. When one of them stops, or the connection to its node ends, or
%% the recovery fails, the leader abandons the attempt and recruits again
%% after `retry_ms' (default 500 ms).
%%
%% The leader also compares the recovery count of the coordinated state
%% with its epoch at every read. If the count is above the epoch, the
%% process of another epoch has started a recovery, and the leader
%% recruits again. In FoundationDB, the roles of an ended epoch stop, and
%% the ClusterController learns it from their failure.
%%
%% == Pop ==
%%
%% The leader lets the LogServers discard the records that every
%% StorageServer of the cluster has stored. Without the pop, a LogServer
%% keeps every record, and its memory grows with each commit.
%%
%% <ol>
%% <li>After each read of the coordinated state (every `poll_ms'), the
%%   leader starts a process for one pop, if the process of the pop
%%   before has ended.</li>
%% <li>The process asks each StorageServer of the list of the coordinated
%%   state for its stored version (`redwall_storage:stored/2'), with a
%%   limit of `cstate_timeout_ms' for each answer.</li>
%% <li>If every StorageServer of the list answers with the generation of
%%   the coordinated state that the leader has read, the pop version is
%%   the lowest stored version.</li>
%% <li>The process sends the pop version to every LogServer of every log
%%   system of the coordinated state (`redwall_log:pop/2'). A LogServer
%%   discards the records at or below that version, up to its Known
%%   Committed Version.</li>
%% </ol>
%%
%% The leader sends no pop in these cases:
%%
%% <ul>
%% <li>The list has fewer than `pop_storages' StorageServers. The default
%%   is the replication degree of the LogServers. A discarded record is
%%   then on as many StorageServers as a record has LogServers with its
%%   body. With `infinity', the leader sends no pop.</li>
%% <li>A StorageServer of the list does not answer, copies from a peer, or
%%   has read another generation of the coordinated state. A StorageServer
%%   of a node that does not start again stays in the list until
%%   `redwall_cstate:forget_storage/3' removes the node. The LogServers
%%   keep every record in that time.</li>
%% </ul>
%%
%% In FoundationDB, each StorageServer sends a pop for its tag to each
%% LogServer, and a LogServer discards a record when every tag of the
%% record is popped. redwall has no tags: every StorageServer reads every
%% record. One process therefore collects the stored versions. This part
%% is not in the paper.
%%
%% Limitations of the pop:
%%
%% <ul>
%% <li>A StorageServer keeps its data in memory. After a pop, the
%%   discarded records are on the StorageServers only. If every
%%   StorageServer with these records stops, the data is lost: a new
%%   StorageServer does not continue, and the database is not reset.
%%   Without a pop, a new StorageServer reads every record from the
%%   LogServers.</li>
%% <li>A StorageServer that is not in the list can need a discarded
%%   record. It then copies from a peer (see "Pop" in
%%   {@link redwall_storage}).</li>
%% <li>A LogServer of an ended epoch keeps the records above its Known
%%   Committed Version, and the coordinated state keeps the log system of
%%   each ended epoch.</li>
%% </ul>
%%
%% == Departures from FoundationDB ==
%%
%% <ul>
%% <li>FoundationDB elects the ClusterController with a register on each
%%   coordinator, which is not a consensus operation, and the leader sends
%%   a heartbeat to the coordinators (`fdbserver/LeaderElection.actor.cpp').
%%   redwall stores the leader in the coordinated state, and a candidate
%%   detects the failure of the leader with an Erlang monitor. This part
%%   is not in the paper.</li>
%% <li>In FoundationDB 7.3 the recovery runs in the process of the
%%   ClusterController. In redwall the process of the epoch performs the
%%   recovery, as the Sequencer of the paper does.</li>
%% <li>The ClusterController recruits one process for the Sequencer, the
%%   Resolver and the Proxy. FoundationDB recruits a process for each
%%   role.</li>
%% </ul>
%%
%% == Limitations ==
%%
%% <ul>
%% <li>A candidate proposes itself when the connection to the node of the
%%   leader ends. If two candidates cannot connect to each other and both
%%   can connect to the coordinators, each election replaces the leader,
%%   and each new leader starts a recovery.</li>
%% <li>The leader does not stop the process of an abandoned epoch whose
%%   recovery has ended. The process continues to run.</li>
%% <li>The leader learns the process of an attempt in a message. If the
%%   leader abandons the attempt before that message, the leader stops
%%   the process when the message arrives. If the network loses the
%%   message, the process continues, and the recovery of the next attempt
%%   can fail on the recovery count. The leader then recruits again.</li>
%% <li>The leader keeps the workers and the epoch in memory. A new leader
%%   starts a recovery, also when the roles of the current epoch run.
%%   FoundationDB has the same property.</li>
%% <li>The leader does not start StorageServers.</li>
%% <li>A worker has one of three classes. FoundationDB has more classes,
%%   a fitness of each class for each role, and the locality of a
%%   process.</li>
%% </ul>
-module(redwall_cluster_controller).

-behaviour(gen_server).

-include("redwall_eta.hrl").
-eta_observe({state, [role, term, db_info]}).

-export([start/1, start_link/1, stop/1, info/1, force_recovery/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([recruit/4, pop/2]).

-export_type([options/0, event/0]).

%% `proxy' holds further options of the process of the epoch. `notify' receives the events of the process. A
%% test uses it.
-type options() :: #{
    coordinators := redwall_cstate:servers(),
    logs => pos_integer(),
    replication => pos_integer(),
    min_workers => pos_integer(),

    proxy => map(),
    poll_ms => pos_integer(),
    retry_ms => non_neg_integer(),
    recovery_timeout_ms => pos_integer(),
    cstate_timeout_ms => pos_integer(),
    notify => fun((event()) -> term()),
    %% The lowest number of StorageServers in the list of the coordinated
    %% state with which the leader sends a pop. The default is
    %% `replication'. See "Pop".
    pop_storages => pos_integer() | infinity,
    %% `pop_limit => false' plants a defect for the simulation: the pop
    %% version is the highest version of a StorageServer, and a LogServer
    %% discards the records without the limit of its Known Committed
    %% Version.
    pop_limit => boolean(),
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
    | {reset, Epoch :: pos_integer()}
    | {recovery_failed, term()}
    | {role_failed, redwall_worker:role(), pid()}
    | {superseded, Epoch :: pos_integer(), Count :: pos_integer()}.
%% `{recovered, Result}' has the result of the recovery
%% (`redwall_recovery:result()') with the key `proxy'. `{reset, Epoch}'
%% follows it when the recovery of the epoch has reset the database
%% (`redwall_recovery', "Reset").

%% The first time until the next poll of a candidate after the coordinated
%% state did not answer. The time doubles with each such poll, up to
%% `poll_ms'.
-define(RETRY_MS, 10).

-record(generation, {
    attempt :: pos_integer(),
    %% The process that waits for the recovery, with its monitor.
    recruiter :: {pid(), reference()} | undefined,
    %% The process of the epoch.
    proxy :: pid() | undefined,
    %% The monitored roles of the attempt.
    roles = #{} :: #{reference() => {redwall_worker:role(), pid()}},
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
    %% The monitor of the process of a pop.
    pop_job :: reference() | undefined,
    %% The timer of the next poll, and the time until the next poll of a
    %% candidate after the coordinated state did not answer.
    timer :: reference() | undefined,
    retry_ms = ?RETRY_MS :: pos_integer(),
    workers = #{} :: #{pid() => {node(), redwall_worker:class(), reference(), pid() | undefined}},
    %% The StorageServer of each node, from the last read of the
    %% coordinated state.
    storages = #{} :: #{node() => pid()},
    attempt = 0 :: non_neg_integer(),
    %% Whether the leader waits for `retry_ms' after a failure.
    waiting = false :: boolean(),
    generation :: #generation{} | undefined,
    db_info :: redwall_worker:db_info() | undefined
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
        %% The StorageServers of the cluster, from the coordinated state.
        storages := [pid()],
        db_info := redwall_worker:db_info() | undefined
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
        workers => lists:sort([Node || {Node, _Class, _Monitor, _Storage} <- maps:values(Workers)]),
        storages => storages(State),
        db_info => Info
    },
    {reply, Reply, State}.

%% @private
%% The registration of a worker for the term of this leader.
handle_cast({register, Worker, Registration = #{node := Node, class := Class}, Term}, State = #state{role = leader, term = Term, workers = Workers, db_info = Info}) ->
    %% The StorageServer of the node of the worker, or `undefined'.
    Storage = maps:get(storage, Registration, undefined),
    Workers1 =
        case Workers of
            #{Worker := {_Node, _Class, Monitor, _Storage}} -> Workers#{Worker := {Node, Class, Monitor, Storage}};
            _ -> Workers#{Worker => {Node, Class, erlang:monitor(process, Worker), Storage}}
        end,
    %% The information about the database has the StorageServers. Every
    %% worker receives it when the StorageServers change.
    %% The leader records a StorageServer that the coordinated state does
    %% not have for the node. A temporary process writes it, and the
    %% leader then reads the coordinated state, which has the list of the
    %% StorageServers of the cluster.
    _ =
        is_pid(Storage) andalso maps:get(Node, State#state.storages, undefined) =/= Storage andalso
            begin
                Self = self(),
                #{coordinators := Coordinators} = State#state.options,
                Timeout = maps:get(cstate_timeout_ms, State#state.options, 1000),
                spawn(fun() ->
                    _ = redwall_cstate:storage(Coordinators, Node, Storage, Timeout),
                    Self ! poll
                end)
            end,
    ok = gen_server:cast(Worker, {db_info, Info}),
    {noreply, maybe_recruit(State#state{workers = Workers1})};
handle_cast(force_recovery, State = #state{role = leader}) ->
    {noreply, maybe_recruit(abandon(State#state{waiting = false}))};
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
handle_info(poll, State = #state{options = Options, timer = Timer}) ->
    _ = is_reference(Timer) andalso erlang:cancel_timer(Timer),
    {noreply, poll(State#state{timer = erlang:send_after(maps:get(poll_ms, Options, 1000), self(), poll)})};
handle_info(recruit, State) ->
    {noreply, maybe_recruit(State#state{waiting = false})};
handle_info({recruited, Attempt, Role, Pid}, State = #state{role = leader, generation = Generation = #generation{attempt = Attempt, roles = Roles}}) ->
    Generation1 = Generation#generation{roles = Roles#{erlang:monitor(process, Pid) => {Role, Pid}}},
    Generation2 =
        case Role of
            proxy -> Generation1#generation{proxy = Pid};
            _ -> Generation1
        end,
    {noreply, State#state{generation = Generation2}};
handle_info({recruited, _Attempt, proxy, Pid}, State = #state{role = leader}) ->
    %% The process of an attempt that the leader has abandoned. The
    %% clause above handles the attempt of the generation.
    exit(Pid, kill),
    {noreply, State};
handle_info({recovered, Attempt, Result, Proxy}, State = #state{role = leader, generation = Generation = #generation{attempt = Attempt}}) ->
    #state{term = Term} = State,
    #generation{recruiter = {_Pid, Monitor}} = Generation,
    true = erlang:demonitor(Monitor, [flush]),
    Generation1 = Generation#generation{recruiter = undefined, proxy = Proxy, recovered = true},
    Info = #{term => Term, epoch => maps:get(epoch, Result), proxies => [Proxy], storages => storages(State)},
    ok = notify({recovered, Result#{proxy => Proxy}}, State),
    %% The recovery has reset the database. See "Reset" in
    %% `redwall_recovery'.
    _ = maps:get(reset, Result, false) andalso notify({reset, maps:get(epoch, Result)}, State),
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
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State = #state{pop_job = Monitor}) ->
    {noreply, State#state{pop_job = undefined}};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, State = #state{monitor = Monitor}) ->
    %% The ClusterController that this candidate watches.
    {noreply, elect(State#state{monitor = undefined, down = true})};
handle_info({'DOWN', Monitor, process, Pid, Reason}, State = #state{workers = Workers, generation = Generation}) ->
    case {Workers, Generation} of
        {#{Pid := {_Node, _Class, Monitor, _Storage}}, _} ->
            %% The StorageServer of the worker stays in the coordinated
            %% state, so the status of the cluster names a StorageServer
            %% that does not run.
            {noreply, State#state{workers = maps:remove(Pid, Workers)}};
        {_, #generation{recruiter = {Pid, Monitor}}} ->
            ok = notify({recovery_failed, {recruiter, Reason}}, State),
            {noreply, retry(abandon(State))};
        {_, #generation{roles = Roles = #{Monitor := {proxy, Pid}}, recovered = false, recruiter = {_, _}}} ->
            %% The process of the epoch stopped during its recovery. The
            %% recruiter waits for the process and reports the reason.
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
    {_Pid, Monitor} = spawn_monitor(fun() -> exit({cstate, redwall_cstate:read(Coordinators, Timeout)}) end),
    State#state{job = Monitor};
poll(State) ->
    State.

%% Handles the coordinated state that a read returned.
read(Result, State0 = #state{options = Options, role = Role, term = Term, known = Known}) ->
    Self = self(),
    State =
        case Result of
            {ok, CState} -> known_storages(CState, State0);
            _ -> State0
        end,
    case Result of
        {ok, CState1 = #{controller := {Term, Self}, count := Count}} when Role =:= leader ->
            superseded(Count, maybe_pop(CState1, State));
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
            elect(State#state{retry_ms = ?RETRY_MS});
        {ok, #{controller := Controller}} ->
            elect(watch(Controller, State#state{retry_ms = ?RETRY_MS}));
        _ ->
            soon(State)
    end.

%% Polls again soon, after the coordinated state did not answer a
%% candidate. A peace cluster without a leader does not answer, for
%% example directly after its start. The time is 10 ms and doubles up to
%% `poll_ms'.
soon(State = #state{role = candidate, options = Options, timer = Timer, retry_ms = Retry}) ->
    _ = is_reference(Timer) andalso erlang:cancel_timer(Timer),
    PollMs = maps:get(poll_ms, Options, 1000),
    State#state{timer = erlang:send_after(min(Retry, PollMs), self(), poll), retry_ms = min(PollMs, Retry * 2)};
soon(State) ->
    State.

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
    {_Pid1, Monitor} = spawn_monitor(fun() -> exit({elected, redwall_cstate:elect(Coordinators, Term, Self, Timeout)}) end),
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
            soon(State)
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
        db_info = #{term => Term, epoch => undefined, proxies => [], storages => storages(State)}
    },
    ok = notify({leader, Term}, State1),
    State1.

forget_workers(State = #state{workers = Workers}) ->
    lists:foreach(fun({_Node, _Class, Monitor, _Storage}) -> erlang:demonitor(Monitor, [flush]) end, maps:values(Workers)),
    State#state{workers = #{}}.

%% ---------------------------------------------------------------------------
%% Recruitment
%% ---------------------------------------------------------------------------

maybe_recruit(State = #state{role = leader, generation = undefined, waiting = false, workers = Workers, options = Options0, attempt = Attempt0}) ->
    %% With several workers, each LogServer of an epoch is on a worker of
    %% its own. With fewer workers than `logs', the epoch has one
    %% LogServer for each worker, and `replication' is at most that
    %% number. Two LogServers on one node stop together when the node
    %% stops, so the two copies of a record could be lost with one node.
    %% With one worker, every LogServer is on that worker: the stop of the
    %% node ends the cluster in any case, and several LogServers let the
    %% cluster continue after the stop of one LogServer process.
    Logs =
        case map_size(Workers) of
            Count when Count > 1 -> min(maps:get(logs, Options0, 3), Count);
            _ -> maps:get(logs, Options0, 3)
        end,
    Options = Options0#{logs => Logs, replication => min(maps:get(replication, Options0, 2), Logs)},
    case map_size(Workers) >= maps:get(min_workers, Options, 1) of
        true ->
            Attempt = Attempt0 + 1,
            Plan = plan(Workers, Logs, Attempt),
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
plan(Workers, Logs, Attempt) ->
    Sorted = lists:sort([{Node, Class, Worker} || {Worker, {Node, Class, _Monitor, _Storage}} <- maps:to_list(Workers)]),
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
    #{
        logs => [Nth(N, ForLogs) || N <- lists:seq(0, Logs - 1)],
        proxy => Nth(0, Stateless)
    }.

%% @private
%% The process of one attempt. It starts the process of the epoch and waits
%% for the end of its recovery. The process of the epoch calls `Recruit'
%% in its own process.
recruit(Controller, Attempt, Plan, Options = #{coordinators := Coordinators}) ->
    #{logs := LogWorkers, proxy := ProxyWorker} = Plan,
    Report = fun
        (Role, Reply = {ok, Pid}) when is_pid(Pid) ->
            Controller ! {recruited, Attempt, Role, Pid},
            Reply;
        (_Role, Reply) ->
            Reply
    end,
    Recruit = fun(LogOptions = #{id := {_Epoch, Index}}) ->
        Report(log, redwall_worker:start_role(lists:nth(Index, LogWorkers), log, LogOptions))
    end,
    ProxyOptions = (maps:get(proxy, Options, #{}))#{
        coordinators => Coordinators,
        logs => maps:get(logs, Options, 3),
        replication => maps:get(replication, Options, 2),
        recruit => Recruit
    },
    case Report(proxy, redwall_worker:start_role(ProxyWorker, proxy, ProxyOptions)) of
        {ok, Proxy} ->
            try redwall_epoch:recovered(Proxy, maps:get(recovery_timeout_ms, Options, 10000)) of
                {ok, Result} ->
                    Controller ! {recovered, Attempt, Result, Proxy}
            catch
                exit:{{shutdown, {recovery, Reason}}, _Call} ->
                    Controller ! {recruit_failed, Attempt, Reason};
                exit:{Reason, _Call} ->
                    Controller ! {recruit_failed, Attempt, {proxy, Reason}}
            end;
        {error, Reason} ->
            Controller ! {recruit_failed, Attempt, {proxy, Reason}}
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
    #generation{recruiter = Recruiter, proxy = Proxy, roles = Roles, recovered = Recovered} = Generation,
    case Recruiter of
        {_Pid, Monitor} -> true = erlang:demonitor(Monitor, [flush]);
        undefined -> true
    end,
    _ = is_pid(Proxy) andalso not Recovered andalso exit(Proxy, kill),
    lists:foreach(fun(Monitor) -> erlang:demonitor(Monitor, [flush]) end, maps:keys(Roles)),
    State1 = State#state{generation = undefined},
    case Info of
        #{epoch := Epoch} when Epoch =/= undefined ->
            broadcast(State1#state{db_info = Info#{epoch := undefined, proxies := []}});
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

%% The StorageServers of the cluster from the coordinated state, in the
%% order of their nodes.
storages(#state{storages = Storages}) ->
    [Storage || {_Node, Storage} <- lists:sort(maps:to_list(Storages))].

%% Takes the StorageServers from a read of the coordinated state. If they
%% have changed, every worker receives the information about the
%% database.
known_storages(#{storages := Storages}, State = #state{role = leader, storages = Known, db_info = Info}) when Storages =/= Known, is_map(Info) ->
    State1 = State#state{storages = Storages},
    broadcast(State1#state{db_info = Info#{storages => storages(State1)}});
known_storages(#{storages := Storages}, State) ->
    %% A candidate keeps the list, so that it has the list when it
    %% becomes the leader.
    State#state{storages = Storages};
known_storages(_CState, State) ->
    State.

%% ---------------------------------------------------------------------------
%% Pop
%% ---------------------------------------------------------------------------

%% Starts the process of one pop, if the process of the pop before has
%% ended, the coordinated state has a log system, and its list has at
%% least `pop_storages' StorageServers. See "Pop" in the module
%% documentation.
maybe_pop(CState = #{storages := Storages, current := Current}, State = #state{pop_job = undefined, options = Options}) when
    map_size(Storages) > 0, Current =/= undefined
->
    case map_size(Storages) >= maps:get(pop_storages, Options, maps:get(replication, Options, 2)) of
        true ->
            PopOptions = #{
                timeout => maps:get(cstate_timeout_ms, Options, 1000),
                limit => maps:get(pop_limit, Options, true)
            },
            {_Pid, Monitor} = spawn_monitor(?MODULE, pop, [CState, PopOptions]),
            State#state{pop_job = Monitor};
        false ->
            State
    end;
maybe_pop(_CState, State) ->
    State.

%% @private
%% The process of one pop. It asks each StorageServer of the coordinated
%% state `CState' for its stored version, and sends the lowest one to
%% every LogServer of `CState'. It sends nothing if a StorageServer does
%% not answer, is not ready, or has read another generation of the
%% coordinated state: the versions start again after a reset of the
%% database, so a stored version of another generation is no version of
%% these LogServers.
-spec pop(redwall_cstate:state(), #{timeout := timeout(), limit := boolean()}) -> ok.
pop(#{generation := Generation, storages := Storages, current := Current, old := Old}, #{timeout := Timeout, limit := Limit}) ->
    Pids = [Pid || {_Node, Pid} <- lists:sort(maps:to_list(Storages))],
    case stored(Pids, Generation, Timeout, []) of
        {ok, Answers} ->
            {Version, LogLimit} =
                case Limit of
                    true -> {lists:min([Stored || #{stored := Stored} <- Answers]), kcv};
                    %% The planted defect `pop_limit => false'.
                    false -> {lists:max([Highest || #{version := Highest} <- Answers]), none}
                end,
            lists:foreach(
                fun(Log) ->
                    case redwall_log:ref(Log) of
                        undefined -> ok;
                        Pid -> redwall_log:pop(Pid, Version, LogLimit)
                    end
                end,
                [Log || Version > 0, #{logs := Logs} <- [Current | Old], Log <- Logs]
            );
        error ->
            ok
    end.

%% The answers of the StorageServers, if each of them answers with the
%% generation `Generation'.
stored([], _Generation, _Timeout, Answers) ->
    {ok, Answers};
stored([Pid | Pids], Generation, Timeout, Answers) ->
    try redwall_storage:stored(Pid, Timeout) of
        {ok, Answer = #{generation := Generation}} -> stored(Pids, Generation, Timeout, [Answer | Answers]);
        _NotReadyOrOtherGeneration -> error
    catch
        exit:_ -> error
    end.

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
