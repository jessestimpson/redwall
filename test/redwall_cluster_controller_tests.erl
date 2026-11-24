-module(redwall_cluster_controller_tests).

-include_lib("eunit/include/eunit.hrl").

-export([fake_worker/3, stub_cstate/1]).

%% ---------------------------------------------------------------------------
%% One real worker
%% ---------------------------------------------------------------------------

%% One node with one worker and two candidates. The coordinated state is a
%% `redwall_cstate_local' process.
controller_test_() ->
    {timeout, 60, fun controller/0}.

controller() ->
    {ok, _} = application:ensure_all_started(redwall),
    {ok, CState} = redwall_cstate_local:start(#{}),
    Coordinators = {local, CState},
    Options = options(Coordinators),
    {ok, Worker} = redwall_worker:start(#{coordinators => Coordinators, poll_ms => 20}),
    {ok, A} = redwall_cluster_controller:start(Options),
    {ok, Storage} = redwall_storage:start(#{coordinators => Coordinators, poll_ms => 5}),
    try
        %% The first candidate wins the election of term 1 and recruits the
        %% epoch 1 on the worker.
        #{term := 1, epoch := 1, proxies := [Proxy1]} = await_epoch(1),
        ?assertMatch(#{role := leader, term := 1, workers := [_]}, redwall_cluster_controller:info(A)),
        ?assertMatch({ok, #{controller := {1, A}}}, redwall_cstate:read(Coordinators)),
        ?assertMatch(#{controller := {1, A}, registered := true}, redwall_worker:info()),
        %% A second candidate watches the leader.
        {ok, B} = redwall_cluster_controller:start(Options),
        ok = await(fun() -> maps:get(term, redwall_cluster_controller:info(B)) =:= 1 end, 100),
        ?assertMatch(#{role := candidate, term := 1}, redwall_cluster_controller:info(B)),
        %% The client finds the Proxy in the information about the database.
        Db = redwall:open(#{proxy => fun() -> hd(maps:get(proxies, redwall_worker:db_info())) end, storage => Storage}),
        ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 1) end),
        ?assertEqual(1, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
        %% A LogServer stops. The leader recruits the epoch 2.
        exit(lists:nth(2, log_pids(Coordinators)), kill),
        #{term := 1, epoch := 2, proxies := [Proxy2]} = await_epoch(2),
        ?assertNotEqual(Proxy1, Proxy2),
        ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 2) end),
        ?assertEqual(2, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
        %% The process of the epoch stops, three times. The leader recruits
        %% the epochs 3, 4 and 5.
        lists:foreach(
            fun(Epoch) ->
                exit(hd(maps:get(proxies, redwall_worker:db_info())), kill),
                #{epoch := Epoch} = await_epoch(Epoch)
            end,
            [3, 4, 5]
        ),
        %% A request for a recovery. A candidate ignores the request.
        ok = redwall_cluster_controller:force_recovery(B),
        ok = redwall_cluster_controller:force_recovery(A),
        #{epoch := 6} = await_epoch(6),
        ?assertMatch(#{role := candidate}, redwall_cluster_controller:info(B)),
        %% The leader stops. The second candidate wins the election of
        %% term 2, the worker registers with it, and it recruits the
        %% epoch 7.
        exit(A, kill),
        #{term := 2, epoch := 7} = await_epoch(7),
        ?assertMatch(#{role := leader, term := 2, workers := [_]}, redwall_cluster_controller:info(B)),
        ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, b, 3) end),
        ?assertEqual({2, 3}, redwall:transactional(Db, fun(Tx) -> {redwall:get(Tx, a), redwall:get(Tx, b)} end)),
        %% A third candidate takes the term 3 from the leader. The leader
        %% reads the coordinated state and is a candidate again. It has
        %% no workers and no information about the database.
        {ok, 3} = redwall_cstate:elect(Coordinators, 2, self()),
        ok = await(fun() -> maps:get(role, redwall_cluster_controller:info(B)) =:= candidate end, 100),
        ?assertMatch(#{term := 3, workers := [], db_info := undefined}, redwall_cluster_controller:info(B)),
        %% The worker sends its registration to the process of term 3, at
        %% every poll, because the process does not answer.
        ok = await(fun() -> maps:get(controller, redwall_worker:info()) =:= {3, self()} end, 100),
        ok = registration(Worker, 3),
        ok = registration(Worker, 3),
        ?assertMatch(#{registered := false}, redwall_worker:info()),
        %% The worker ignores the information of a ClusterController of
        %% another term.
        Before = redwall_worker:db_info(),
        ok = gen_server:cast(Worker, {db_info, #{term => 2, epoch => 99, proxies => []}}),
        ?assertMatch(#{registered := false}, redwall_worker:info()),
        ?assertEqual(Before, redwall_worker:db_info()),
        ok = redwall_cluster_controller:stop(B),
        ok = redwall_worker:stop(),
        ?assertEqual(undefined, redwall_worker:db_info()),
        ?assertMatch({error, {worker, _}}, redwall_worker:start_role(Worker, log, #{}))
    after
        exit(A, kill),
        catch redwall_storage:stop(Storage),
        exit(Worker, kill),
        ok = application:stop(redwall)
    end.

%% Waits for a registration of `Worker' for `Term' in the mailbox of this
%% process.
registration(Worker, Term) ->
    receive
        {'$gen_cast', {register, Worker, #{class := unset}, Term}} -> ok
    after 1000 -> error(no_registration)
    end.

%% The coordinated state is on a peace cluster of three servers.
peace_test_() ->
    {timeout, 60, fun on_peace/0}.

on_peace() ->
    {ok, _} = application:ensure_all_started(redwall),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([redwall_cc, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    {ok, Ids, []} = peace:start_cluster(Ids, redwall_cstate:machine(), #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}}),
    Options = (options(Ids))#{poll_ms => 50},
    process_flag(trap_exit, true),
    {ok, Worker} = redwall_worker:start_link(#{coordinators => Ids, poll_ms => 50}),
    {ok, A} = redwall_cluster_controller:start_link(Options),
    {ok, B} = redwall_cluster_controller:start_link(Options),
    try
        #{epoch := 1} = await_epoch(1),
        {ok, #{controller := {1, Leader}}} = redwall_cstate:read(Ids),
        ?assert(lists:member(Leader, [A, B])),
        [Other] = [A, B] -- [Leader],
        ?assertMatch(#{role := leader}, redwall_cluster_controller:info(Leader)),
        ?assertMatch(#{role := candidate}, redwall_cluster_controller:info(Other)),
        {ok, Storage} = redwall_storage:start(#{coordinators => Ids, poll_ms => 5}),
        Db = redwall:open(#{proxy => fun() -> hd(maps:get(proxies, redwall_worker:db_info())) end, storage => Storage}),
        ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 1) end),
        %% The leader stops. The other candidate recruits the epoch 2.
        exit(Leader, kill),
        #{term := 2, epoch := 2} = await_epoch(2),
        ?assertEqual(1, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
        ok = redwall_storage:stop(Storage)
    after
        [unlink(Pid) || Pid <- [A, B, Worker]],
        exit(A, kill),
        exit(B, kill),
        exit(Worker, kill),
        process_flag(trap_exit, false),
        flush(),
        ok = application:stop(redwall),
        ok = application:stop(peace)
    end.

%% ---------------------------------------------------------------------------
%% Several workers of one node
%% ---------------------------------------------------------------------------

%% A node has one `redwall_worker'. The tests below use processes that
%% register as workers and start roles as a worker does (`fake_worker/3').

%% The leader waits for `min_workers' workers. It places the LogServers on
%% the workers of the class `log', and the other roles on the workers of
%% the class `stateless' and then `unset'.
%% Every LogServer of an epoch stops. The LogServers keep their records
%% in memory, so the recovery resets the database, and the leader sends
%% the event `reset' after the event `recovered' of that epoch.
reset_event_test_() ->
    {timeout, 60, fun reset_event/0}.

reset_event() ->
    with_controller(#{}, fun(_Controller, CState) ->
        Coordinators = {local, CState},
        {ok, Worker} = redwall_worker:start(#{coordinators => Coordinators, poll_ms => 20}),
        try
            #{epoch := 1} = await_epoch(1),
            ?assertMatch({recovered, #{epoch := 1, reset := false}}, next(recovered)),
            ?assertEqual(none, peek(reset)),
            ?assertMatch({ok, #{generation := 0}}, redwall_cstate:read(Coordinators)),
            [exit(Pid, kill) || Pid <- log_pids(Coordinators)],
            {reset, Epoch} = next(reset),
            ?assert(Epoch > 1),
            ?assertMatch({ok, #{generation := 1, old := [], current := #{epoch := Epoch, durability := memory}}}, redwall_cstate:read(Coordinators))
        after
            redwall_worker:stop(Worker)
        end
    end).

placement_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{min_workers => 5}, fun(Controller, CState) ->
            ?assertEqual({leader, 1}, next(leader)),
            [L1, L2, L3, S1] = [fake(Controller, Node, Class) || {Node, Class} <- [{l1, log}, {l2, log}, {l3, log}, {s1, stateless}]],
            ok = await(fun() -> length(maps:get(workers, redwall_cluster_controller:info(Controller))) =:= 4 end, 100),
            ?assertEqual(none, peek(recruiting)),
            _U1 = fake(Controller, u1, unset),
            {recruiting, 1, Plan1} = next(recruiting),
            ?assertEqual([L1, L2, L3], lists:sort(maps:get(logs, Plan1))),
            ?assertEqual(S1, maps:get(proxy, Plan1)),
            {recovered, #{epoch := 1, proxy := Proxy}} = next(recovered),
            ?assertEqual({S1, proxy}, started(Proxy)),
            ?assertEqual(
                lists:sort([{L1, log}, {L2, log}, {L3, log}]),
                lists:sort([started(Pid) || Pid <- log_pids({local, CState})])
            ),
            %% Each fake worker has received the information about the
            %% epoch.
            ok = await(fun() -> db_info(L1) =:= #{term => 1, epoch => 1, proxies => [Proxy], storages => []} end, 100),
            %% The second attempt starts at another worker. The classes
            %% decide in the same way.
            ok = redwall_cluster_controller:force_recovery(Controller),
            {recruiting, 2, Plan2} = next(recruiting),
            ?assertEqual([L1, L2, L3], lists:sort(maps:get(logs, Plan2))),
            ?assertNotEqual(maps:get(logs, Plan1), maps:get(logs, Plan2)),
            ?assertEqual(S1, maps:get(proxy, Plan2)),
            {recovered, #{epoch := 2}} = next(recovered),
            %% A worker stops. The leader removes it. The test stops the
            %% LogServer that the worker started, as a node does when it
            %% stops, and the leader recruits without the worker.
            exit(L1, kill),
            ok = await(fun() -> length(maps:get(workers, redwall_cluster_controller:info(Controller))) =:= 4 end, 100),
            [exit(Pid, kill) || {Pid, log} <- roles(L1)],
            {role_failed, log, _} = next(role_failed),
            %% The leader has 4 workers and waits for a fifth one.
            timer:sleep(100),
            ?assertEqual(none, peek(recruiting)),
            L4 = fake(Controller, l4, log),
            {recruiting, 3, Plan3} = next(recruiting),
            ?assertEqual([L2, L3, L4], lists:sort(maps:get(logs, Plan3))),
            {recovered, #{epoch := 3}} = next(recovered)
        end)
    end}.

%% The process of an epoch is on a worker of the class `stateless'. Two
%% attempts in sequence use two workers of the class.
rotation_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{min_workers => 3}, fun(Controller, _CState) ->
            L1 = fake(Controller, l1, log),
            S1 = fake(Controller, s1, stateless),
            S2 = fake(Controller, s2, stateless),
            {recruiting, 1, Plan1 = #{proxy := First}} = next(recruiting),
            ?assert(lists:member(First, [S1, S2])),
            ?assertEqual(L1, hd(maps:get(logs, Plan1))),
            {recovered, #{epoch := 1}} = next(recovered),
            ok = redwall_cluster_controller:force_recovery(Controller),
            {recruiting, 2, #{proxy := Second}} = next(recruiting),
            ?assertEqual([S1, S2], lists:sort([First, Second])),
            {recovered, #{epoch := 2}} = next(recovered)
        end)
    end}.

%% A role does not start. The leader recruits again.
start_failure_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{}, fun(Controller, _CState) ->
            W = fake(Controller, w1, unset),
            {recovered, #{epoch := 1}} = next(recovered),
            %% The worker refuses the process of the epoch once.
            W ! {fail, proxy},
            ok = redwall_cluster_controller:force_recovery(Controller),
            ?assertEqual({recovery_failed, {proxy, refused}}, next(recovery_failed)),
            {recovered, #{epoch := 2}} = next(recovered),
            %% The worker refuses a LogServer once. The recovery fails
            %% after it has raised the recovery count.
            W ! {fail, log},
            ok = redwall_cluster_controller:force_recovery(Controller),
            ?assertEqual({recovery_failed, {recruit, refused}}, next(recovery_failed)),
            {recovered, #{epoch := 4}} = next(recovered),
            ?assertMatch(#{db_info := #{epoch := 4}}, redwall_cluster_controller:info(Controller))
        end)
    end}.

%% The leader abandons an attempt during the recovery. It stops the
%% Sequencer of the attempt, and the next attempt recovers.
abandon_during_recovery_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{}, fun(Controller, CState) ->
            W = fake(Controller, w1, unset),
            {recovered, #{epoch := 1}} = next(recovered),
            %% The worker starts the next LogServer after 300 ms, so the
            %% recovery of the second attempt has not ended at the time
            %% of the next request.
            W ! {delay, log, 300},
            ok = redwall_cluster_controller:force_recovery(Controller),
            {recruiting, 2, _} = next(recruiting),
            ok = await(fun() -> length([x || {_Pid, proxy} <- roles(W)]) =:= 2 end, 100),
            Second = lists:last([Pid || {Pid, proxy} <- roles(W)]),
            ?assert(is_process_alive(Second)),
            ok = redwall_cluster_controller:force_recovery(Controller),
            {recruiting, 3, _} = next(recruiting),
            ok = await(fun() -> not is_process_alive(Second) end, 100),
            {recovered, #{epoch := Epoch}} = next(recovered),
            %% The second Sequencer raised the recovery count before it
            %% stopped.
            ?assertEqual(3, Epoch),
            ?assertEqual(none, peek(recovery_failed)),
            ?assertMatch({ok, #{current := #{epoch := Epoch}}}, redwall_cstate:read({local, CState}))
        end)
    end}.

%% Another Sequencer raises the recovery count. No role of the epoch of
%% the leader stops. The leader reads the count and recruits again.
superseded_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{}, fun(Controller, CState) ->
            _ = fake(Controller, w1, unset),
            {recovered, #{epoch := 1}} = next(recovered),
            {ok, 2, _} = redwall_cstate:lock({local, CState}, 1, make_ref()),
            ?assertEqual({superseded, 1, 2}, next(superseded)),
            {recovered, #{epoch := 3}} = next(recovered),
            timer:sleep(100),
            ?assertEqual(none, peek(superseded)),
            ?assertMatch(#{db_info := #{epoch := 3}}, redwall_cluster_controller:info(Controller))
        end)
    end}.

%% After the failure of a role, the leader waits for `retry_ms'. The
%% registration of a worker in that time does not start an attempt.
retry_delay_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{retry_ms => 400}, fun(Controller, _CState) ->
            _ = fake(Controller, w1, unset),
            {recovered, #{epoch := 1, proxy := Proxy}} = next(recovered),
            exit(Proxy, kill),
            {role_failed, proxy, Proxy} = next(role_failed),
            _ = fake(Controller, w2, unset),
            ok = await(fun() -> length(maps:get(workers, redwall_cluster_controller:info(Controller))) =:= 2 end, 20),
            timer:sleep(100),
            ?assertEqual(none, peek(recruiting)),
            {recruiting, 2, _} = next(recruiting),
            {recovered, #{epoch := 2}} = next(recovered)
        end)
    end}.

%% With the planted defect `monitor_roles => false', the leader does not
%% recruit after the failure of a role.
no_monitor_roles_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{monitor_roles => false}, fun(Controller, _CState) ->
            _ = fake(Controller, w1, unset),
            {recovered, #{epoch := 1, proxy := Proxy}} = next(recovered),
            exit(Proxy, kill),
            {role_failed, proxy, Proxy} = next(role_failed),
            timer:sleep(100),
            ?assertEqual(none, peek(recruiting)),
            ?assertMatch(#{db_info := #{epoch := 1}}, redwall_cluster_controller:info(Controller))
        end)
    end}.

%% ---------------------------------------------------------------------------
%% Election
%% ---------------------------------------------------------------------------

%% 5 candidates start at the same time. One is the leader of term 1. The
%% leader stops 4 times, and each time one of the others is the leader of
%% the next term.
candidates_test_() ->
    {timeout, 60, fun() ->
        {ok, CState} = redwall_cstate_local:start(#{}),
        Coordinators = {local, CState},
        Options = (options(Coordinators))#{poll_ms => 10},
        Controllers = [element(2, redwall_cluster_controller:start(Options)) || _ <- lists:seq(1, 5)],
        Leaders = fun() ->
            [C || C <- Controllers, is_process_alive(C), maps:get(role, redwall_cluster_controller:info(C)) =:= leader]
        end,
        try
            lists:foreach(
                fun(Term) ->
                    ok = await(
                        fun() ->
                            case redwall_cstate:read(Coordinators) of
                                {ok, #{controller := {Term, Leader}}} -> Leaders() =:= [Leader];
                                _ -> false
                            end
                        end,
                        200
                    ),
                    %% Each other candidate knows the term.
                    ok = await(
                        fun() -> [] =:= [C || C <- Controllers, is_process_alive(C), maps:get(term, redwall_cluster_controller:info(C)) =/= Term] end,
                        200
                    ),
                    [Leader] = Leaders(),
                    exit(Leader, kill)
                end,
                [1, 2, 3, 4, 5]
            ),
            ?assertEqual([], [C || C <- Controllers, is_process_alive(C)])
        after
            [exit(C, kill) || C <- Controllers],
            exit(CState, kill)
        end
    end}.

%% The coordinated state refuses the proposal of the candidate, and names
%% the ClusterController of term 1. The candidate watches that process, and
%% proposes itself for term 2 when the process stops.
lost_election_test_() ->
    {timeout, 60, fun() ->
        Other = spawn(fun() ->
            receive
                stop -> ok
            end
        end),
        Stub = spawn(?MODULE, stub_cstate, [#{test => self(), read => {0, undefined}, elect => {error, {term, 1, Other}}}]),
        {ok, Controller} = redwall_cluster_controller:start((options({local, Stub}))#{poll_ms => 20}),
        try
            ok = proposed({elect, 0, Controller}),
            ok = await(
                fun() -> redwall_cluster_controller:info(Controller) =:= #{role => candidate, term => 1, workers => [], storages => [], db_info => undefined} end,
                100
            ),
            %% The coordinated state has the term 1 from now on, and
            %% accepts the next proposal.
            Stub ! {set, #{read => {1, Other}, elect => {ok, 2}}},
            timer:sleep(60),
            ?assertMatch(#{role := candidate}, redwall_cluster_controller:info(Controller)),
            Other ! stop,
            ok = proposed({elect, 1, Controller}),
            ok = await(fun() -> maps:get(role, redwall_cluster_controller:info(Controller)) =:= leader end, 100),
            ?assertMatch(#{term := 2}, redwall_cluster_controller:info(Controller))
        after
            exit(Controller, kill),
            exit(Stub, kill),
            flush()
        end
    end}.

%% The coordinated state does not answer. The candidate stays a candidate,
%% and is the leader after the coordinated state answers. A leader stays
%% the leader while the coordinated state does not answer.
cstate_timeout_test_() ->
    {timeout, 60, fun() ->
        Stub = spawn(?MODULE, stub_cstate, [#{test => self(), read => silent, elect => silent}]),
        {ok, Controller} = redwall_cluster_controller:start((options({local, Stub}))#{poll_ms => 20, cstate_timeout_ms => 20}),
        try
            timer:sleep(100),
            ?assertMatch(#{role := candidate, term := 0}, redwall_cluster_controller:info(Controller)),
            %% The read answers, and the proposal does not.
            Stub ! {set, #{read => {0, undefined}}},
            ok = proposed({elect, 0, Controller}),
            ?assertMatch(#{role := candidate, term := 0}, redwall_cluster_controller:info(Controller)),
            Stub ! {set, #{elect => {ok, 1}}},
            ok = await(fun() -> maps:get(role, redwall_cluster_controller:info(Controller)) =:= leader end, 100),
            Stub ! {set, #{read => silent}},
            timer:sleep(100),
            ?assertMatch(#{role := leader, term := 1}, redwall_cluster_controller:info(Controller)),
            %% The coordinated state has the term 2 of another process.
            Stub ! {set, #{read => {2, self()}}},
            ok = await(fun() -> maps:get(role, redwall_cluster_controller:info(Controller)) =:= candidate end, 100)
        after
            exit(Controller, kill),
            exit(Stub, kill),
            flush()
        end
    end}.

%% The coordinated state accepts the proposal of the candidate, and the
%% answer does not arrive. The candidate learns from the next read that it
%% is the leader.
lost_answer_test_() ->
    {timeout, 60, fun() ->
        Stub = spawn(?MODULE, stub_cstate, [#{test => self(), read => {0, undefined}, elect => silent}]),
        {ok, Controller} = redwall_cluster_controller:start((options({local, Stub}))#{poll_ms => 20, cstate_timeout_ms => 20}),
        try
            ok = proposed({elect, 0, Controller}),
            ?assertMatch(#{role := candidate}, redwall_cluster_controller:info(Controller)),
            Stub ! {set, #{read => {1, Controller}}},
            ok = await(fun() -> redwall_cluster_controller:info(Controller) =:= #{role => leader, term => 1, workers => [], storages => [], db_info => #{term => 1, epoch => undefined, proxies => [], storages => []}} end, 100)
        after
            exit(Controller, kill),
            exit(Stub, kill),
            flush()
        end
    end}.

%% With the planted defect `step_down => false', a leader stays the leader
%% when the coordinated state has another term.
no_step_down_test_() ->
    {timeout, 60, fun() ->
        {ok, CState} = redwall_cstate_local:start(#{}),
        {ok, Controller} = redwall_cluster_controller:start((options({local, CState}))#{poll_ms => 10, step_down => false}),
        try
            ok = await(fun() -> maps:get(role, redwall_cluster_controller:info(Controller)) =:= leader end, 100),
            {ok, 2} = redwall_cstate:elect({local, CState}, 1, self()),
            timer:sleep(100),
            ?assertMatch(#{role := leader, term := 1}, redwall_cluster_controller:info(Controller))
        after
            exit(Controller, kill),
            exit(CState, kill)
        end
    end}.

proposed(Value) ->
    receive
        {proposed, Value} -> ok
    after 1000 -> error({no_proposal, Value})
    end.

%% ---------------------------------------------------------------------------
%% The worker
%% ---------------------------------------------------------------------------

%% The function of the option `roles' raises an error. The worker answers
%% with the error and continues.
worker_role_error_test() ->
    {ok, CState} = redwall_cstate_local:start(#{}),
    {ok, Worker} = redwall_worker:start(#{
        coordinators => {local, CState},
        roles => fun
            (log, _Options) -> error(broken);
            (_Role, _Options) -> {ok, self()}
        end
    }),
    try
        ?assertEqual({error, {error, broken}}, redwall_worker:start_role(Worker, log, #{})),
        ?assertEqual({ok, Worker}, redwall_worker:start_role(Worker, proxy, #{})),
        ?assertEqual(undefined, redwall_worker:db_info()),
        ?assertEqual(#{controller => {0, undefined}, registered => false, candidate => undefined}, redwall_worker:info())
    after
        ok = redwall_worker:stop(),
        exit(CState, kill)
    end.

%% The coordinated state does not answer. The worker and the candidate
%% answer their requests without a wait for the coordinated state.
responsive_test_() ->
    {timeout, 60, fun() ->
        Stub = spawn(?MODULE, stub_cstate, [#{test => self(), read => silent, elect => silent}]),
        Coordinators = {local, Stub},
        {ok, Worker} = redwall_worker:start(#{
            coordinators => Coordinators,
            poll_ms => 10,
            cstate_timeout_ms => 300,
            roles => fun(_Role, _Options) -> {ok, self()} end
        }),
        {ok, Controller} = redwall_cluster_controller:start((options(Coordinators))#{poll_ms => 10, cstate_timeout_ms => 300}),
        try
            %% Several polls of each process have passed.
            timer:sleep(700),
            {WorkerUs, {ok, Worker}} = timer:tc(fun() -> redwall_worker:start_role(Worker, log, #{}) end),
            {ControllerUs, #{role := candidate}} = timer:tc(fun() -> redwall_cluster_controller:info(Controller) end),
            ?assert(WorkerUs < 100000, WorkerUs),
            ?assert(ControllerUs < 100000, ControllerUs),
            %% Neither process has a queue of poll messages.
            ?assert(element(2, process_info(Worker, message_queue_len)) < 3),
            ?assert(element(2, process_info(Controller, message_queue_len)) < 3)
        after
            exit(Worker, kill),
            exit(Controller, kill),
            exit(Stub, kill),
            flush()
        end
    end}.

%% The worker starts a candidate for the ClusterController. The two
%% processes stop together.
worker_candidate_test_() ->
    {timeout, 60, fun() ->
        {ok, CState} = redwall_cstate_local:start(#{}),
        Coordinators = {local, CState},
        Start = fun() ->
            {ok, Worker} = redwall_worker:start(#{
                coordinators => Coordinators,
                poll_ms => 20,
                candidate => maps:remove(coordinators, options(Coordinators))
            }),
            #{candidate := Candidate} = redwall_worker:info(),
            {Worker, Candidate}
        end,
        Down = fun(Pid) ->
            Monitor = erlang:monitor(process, Pid),
            receive
                {'DOWN', Monitor, process, Pid, _} -> ok
            after 2000 -> error({alive, Pid})
            end
        end,
        try
            %% The candidate of the worker is the leader, and recruits on
            %% its own worker.
            {W1, C1} = Start(),
            #{term := 1, epoch := 1} = await_epoch(1),
            ?assertMatch({ok, #{controller := {1, C1}}}, redwall_cstate:read(Coordinators)),
            %% The stop of the worker stops the candidate.
            ok = redwall_worker:stop(),
            ok = Down(W1),
            ok = Down(C1),
            %% A new worker with a new candidate. The candidate wins the
            %% election of term 2 and recruits the epoch 2.
            {W2, C2} = Start(),
            #{term := 2, epoch := 2} = await_epoch(2),
            %% The stop of the candidate stops the worker.
            exit(C2, kill),
            ok = Down(W2),
            ?assertEqual(undefined, whereis(redwall_worker))
        after
            catch exit(whereis(redwall_worker), kill),
            exit(CState, kill)
        end
    end}.

%% The poll interval of each process is 5 seconds. A node still has an
%% epoch, a commit and a read in less than 1 second after its start,
%% because a process that waits polls after 10 ms at first. After the stop
%% of the process of the epoch, the next epoch, a commit and a read take
%% less than 1 second too.
start_time_test_() ->
    {timeout, 60, fun() ->
        {ok, CState} = redwall_cstate_local:start(#{}),
        Coordinators = {local, CState},
        Start = erlang:monotonic_time(millisecond),
        %% The StorageServer starts first, and the coordinated state has
        %% no epoch.
        {ok, Storage} = redwall_storage:start(#{coordinators => Coordinators, poll_ms => 5000}),
        {ok, Worker} = redwall_worker:start(#{
            coordinators => Coordinators,
            poll_ms => 5000,
            candidate => #{poll_ms => 5000, retry_ms => 10, proxy => #{versions_per_second => 0}}
        }),
        try
            Db = redwall:open(#{
                proxy => fun() ->
                    case redwall_worker:db_info() of
                        #{proxies := [Proxy]} -> Proxy;
                        _ -> undefined
                    end
                end,
                storage => Storage
            }),
            #{epoch := 1, proxies := [First]} = await_epoch(1),
            ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 1) end),
            ?assertEqual(1, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
            Started = erlang:monotonic_time(millisecond) - Start,
            ?assert(Started < 1000, Started),
            Kill = erlang:monotonic_time(millisecond),
            exit(First, kill),
            #{epoch := 2} = await_epoch(2),
            ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 2) end),
            ?assertEqual(2, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
            Recovered = erlang:monotonic_time(millisecond) - Kill,
            ?assert(Recovered < 1000, Recovered)
        after
            exit(Worker, kill),
            exit(Storage, kill),
            exit(CState, kill)
        end
    end}.

%% The application starts the worker, with its candidate, and the
%% StorageServer of the node from its environment. `redwall:open/0' then
%% returns a database without the name of a process.
application_test_() ->
    {timeout, 60, fun() ->
        {ok, CState} = redwall_cstate_local:start(#{}),
        Coordinators = {local, CState},
        _ = application:load(redwall),
        ok = application:set_env(redwall, worker, #{
            coordinators => Coordinators,
            poll_ms => 20,
            candidate => maps:remove(coordinators, options(Coordinators))
        }),
        ok = application:set_env(redwall, storage, #{coordinators => Coordinators, poll_ms => 5}),
        try
            {ok, _} = application:ensure_all_started(redwall),
            #{epoch := 1} = await_epoch(1),
            %% The StorageServer of the application registers with the
            %% worker, and the ClusterController gives the StorageServers
            %% of the cluster to the worker.
            [{redwall_storage, StoragePid, _, _}] = [Child || Child = {redwall_storage, _, _, _} <- supervisor:which_children(redwall_sup)],
            ok = await(fun() -> redwall_worker:storages(redwall_worker) =:= [StoragePid] end, 100),
            #{candidate := Leader} = redwall_worker:info(),
            ?assertMatch(#{storages := [StoragePid]}, redwall_cluster_controller:info(Leader)),
            Db = redwall:open(),
            ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 1) end),
            ?assertEqual(1, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
            %% The process of the epoch stops. The database finds the
            %% process of the next epoch.
            exit(hd(maps:get(proxies, redwall_worker:db_info())), kill),
            #{epoch := 2} = await_epoch(2),
            ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, a, 2) end),
            ?assertEqual(2, redwall:transactional(Db, fun(Tx) -> redwall:get(Tx, a) end)),
            %% An option of `open/1' replaces a default.
            ?assertMatch({redwall_database, #{direct := false, retry_delay_ms := 1}}, redwall:open(#{direct => false, profile => ets})),
            #{candidate := Candidate} = redwall_worker:info(),
            ok = application:stop(redwall),
            ?assertEqual(undefined, whereis(redwall_worker)),
            ?assertNot(is_process_alive(Candidate))
        after
            _ = application:stop(redwall),
            ok = application:unset_env(redwall, worker),
            ok = application:unset_env(redwall, storage),
            exit(CState, kill)
        end,
        %% Without a worker of the application, `open/0' raises an error.
        {ok, _} = application:ensure_all_started(redwall),
        ?assertError(no_worker, redwall:open()),
        ok = application:stop(redwall)
    end}.

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

options(Coordinators) ->
    #{
        coordinators => Coordinators,
        poll_ms => 20,
        retry_ms => 10,
        proxy => #{lock_timeout_ms => 100, versions_per_second => 0}
    }.

%% Runs `Fun' with one candidate and a `redwall_cstate_local' process.
%% The events of the candidate arrive in the mailbox of the test.
with_controller(Extra, Fun) ->
    {ok, _} = application:ensure_all_started(redwall),
    {ok, CState} = redwall_cstate_local:start(#{}),
    Test = self(),
    Options = maps:merge((options({local, CState}))#{notify => fun(Event) -> Test ! {event, Event} end}, Extra),
    {ok, Controller} = redwall_cluster_controller:start(Options),
    ok = await(fun() -> maps:get(role, redwall_cluster_controller:info(Controller)) =:= leader end, 100),
    Fakes = ets:new(fakes, [public, bag]),
    put(fakes, Fakes),
    try
        Fun(Controller, CState)
    after
        exit(Controller, kill),
        [exit(Pid, kill) || {worker, Pid} <- ets:lookup(Fakes, worker)],
        erase(fakes),
        ok = application:stop(redwall),
        exit(CState, kill),
        flush()
    end.

flush() ->
    receive
        _ -> flush()
    after 0 -> ok
    end.

%% The next event of the kind `Kind'. The function removes the events of
%% other kinds before it.
next(Kind) ->
    receive
        {event, Event} when element(1, Event) =:= Kind -> Event;
        {event, _Other} -> next(Kind)
    after 5000 -> error({no_event, Kind})
    end.

%% An event of the kind `Kind' that has arrived, or `none'. The function
%% removes no event.
peek(Kind) ->
    {messages, Messages} = process_info(self(), messages),
    case [Event || {event, Event} <- Messages, element(1, Event) =:= Kind] of
        [Event | _] -> Event;
        [] -> none
    end.

%% Starts a process that registers with `Controller' as the worker of
%% `Node' with the class `Class'.
fake(Controller, Node, Class) ->
    Fakes = get(fakes),
    Pid = spawn(?MODULE, fake_worker, [Controller, #{node => Node, class => Class}, Fakes]),
    true = ets:insert(Fakes, {worker, Pid}),
    Pid.

%% The fake worker that started the role `Pid', and the kind of the role.
started(Pid) ->
    [{{role, Pid}, Worker, Role}] = ets:lookup(get(fakes), {role, Pid}),
    {Worker, Role}.

%% The roles that the fake worker `Worker' started, oldest first.
roles(Worker) ->
    [{Pid, Role} || {_Key, Pid, Role} <- ets:lookup(get(fakes), {started, Worker})].

db_info(Worker) ->
    case ets:lookup(get(fakes), {db_info, Worker}) of
        [{_, Info}] -> Info;
        [] -> undefined
    end.

%% @private
fake_worker(Controller, Registration, Fakes) ->
    #{term := Term} = redwall_cluster_controller:info(Controller),
    ok = gen_server:cast(Controller, {register, self(), Registration, Term}),
    fake_loop(Fakes, #{}).

fake_loop(Fakes, Faults) ->
    receive
        {fail, Role} ->
            fake_loop(Fakes, Faults#{Role => fail});
        {delay, Role, Ms} ->
            fake_loop(Fakes, Faults#{Role => {delay, Ms}});
        {'$gen_cast', {db_info, Info}} ->
            true = ets:match_delete(Fakes, {{db_info, self()}, '_'}),
            true = ets:insert(Fakes, {{db_info, self()}, Info}),
            fake_loop(Fakes, Faults);
        {'$gen_call', From, {start_role, Role, Options}} ->
            case maps:get(Role, Faults, none) of
                fail ->
                    gen_server:reply(From, {error, refused});
                Fault ->
                    _ =
                        case Fault of
                            {delay, Ms} -> timer:sleep(Ms);
                            none -> ok
                        end,
                    Module =
                        case Role of
                            log -> redwall_log;
                            proxy -> redwall_epoch
                        end,
                    {ok, Pid} = Module:start(Options),
                    true = ets:insert(Fakes, [{{role, Pid}, self(), Role}, {{started, self()}, Pid, Role}]),
                    gen_server:reply(From, {ok, Pid})
            end,
            fake_loop(Fakes, maps:remove(Role, Faults))
    end.

%% @private
%% A process in the place of `redwall_cstate_local'. `read' is the term
%% and the process of the ClusterController, and `elect' is the answer to a
%% proposal. With `silent' the process does not answer.
stub_cstate(Config = #{test := Test}) ->
    receive
        {set, New} ->
            stub_cstate(maps:merge(Config, New));
        {'$gen_call', From, read} ->
            case Config of
                #{read := silent} -> ok;
                #{read := Controller} -> gen_server:reply(From, {ok, (redwall_cstate:init([]))#{controller := Controller}})
            end,
            stub_cstate(Config);
        {'$gen_call', From, {propose, Value}} ->
            Test ! {proposed, Value},
            case Config of
                #{elect := silent} -> ok;
                #{elect := Reply} -> gen_server:reply(From, Reply)
            end,
            stub_cstate(Config)
    end.

%% The processes of the LogServers of the current epoch, from the
%% coordinated state.
log_pids(Coordinators) ->
    {ok, #{current := #{logs := Logs}}} = redwall_cstate:read(Coordinators),
    [Pid || {_Id, Pid} <- Logs].

%% Waits until the worker of this node has the information about the
%% epoch `Epoch'.
await_epoch(Epoch) ->
    ok = await(
        fun() ->
            case redwall_worker:db_info() of
                #{epoch := Epoch, proxies := [_]} -> true;
                _ -> false
            end
        end,
        400
    ),
    redwall_worker:db_info().

await(_Fun, 0) ->
    error(timeout);
await(Fun, Tries) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(10),
            await(Fun, Tries - 1)
    end.
