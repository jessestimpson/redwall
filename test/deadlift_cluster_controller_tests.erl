-module(deadlift_cluster_controller_tests).

-include_lib("eunit/include/eunit.hrl").

-export([fake_worker/3, stub_cstate/1]).

%% ---------------------------------------------------------------------------
%% One real worker
%% ---------------------------------------------------------------------------

%% One node with one worker and two candidates. The coordinated state is a
%% `deadlift_cstate_local' process.
controller_test_() ->
    {timeout, 60, fun controller/0}.

controller() ->
    {ok, _} = application:ensure_all_started(deadlift),
    {ok, CState} = deadlift_cstate_local:start(#{}),
    Coordinators = {local, CState},
    Options = options(Coordinators),
    {ok, Worker} = deadlift_worker:start(#{coordinators => Coordinators, poll_ms => 20}),
    {ok, A} = deadlift_cluster_controller:start(Options),
    {ok, Storage} = deadlift_storage:start(#{coordinators => Coordinators, poll_ms => 5}),
    try
        %% The first candidate wins the election of term 1 and recruits the
        %% epoch 1 on the worker.
        #{term := 1, epoch := 1, sequencer := Sequencer1} = await_epoch(1),
        ?assertMatch(#{role := leader, term := 1, workers := [_]}, deadlift_cluster_controller:info(A)),
        ?assertMatch({ok, #{controller := {1, A}}}, deadlift_cstate:read(Coordinators)),
        ?assertMatch(#{controller := {1, A}, registered := true}, deadlift_worker:info()),
        %% A second candidate watches the leader.
        {ok, B} = deadlift_cluster_controller:start(Options),
        ok = await(fun() -> maps:get(term, deadlift_cluster_controller:info(B)) =:= 1 end, 100),
        ?assertMatch(#{role := candidate, term := 1}, deadlift_cluster_controller:info(B)),
        %% The client finds the Proxy in the information about the database.
        Db = deadlift:open(#{proxy => fun() -> hd(maps:get(proxies, deadlift_worker:db_info())) end, storage => Storage}),
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, a, 1) end),
        ?assertEqual(1, deadlift:transactional(Db, fun(Tx) -> deadlift:get(Tx, a) end)),
        %% A LogServer stops. The leader recruits the epoch 2.
        exit(deadlift_log:ref({1, 2}), kill),
        #{term := 1, epoch := 2, sequencer := Sequencer2} = await_epoch(2),
        ?assertNotEqual(Sequencer1, Sequencer2),
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, a, 2) end),
        ?assertEqual(2, deadlift:transactional(Db, fun(Tx) -> deadlift:get(Tx, a) end)),
        %% The Proxy stops. The leader recruits the epoch 3.
        exit(hd(maps:get(proxies, deadlift_worker:db_info())), kill),
        #{epoch := 3} = await_epoch(3),
        %% The Sequencer stops. The leader recruits the epoch 4.
        exit(maps:get(sequencer, deadlift_worker:db_info()), kill),
        #{epoch := 4} = await_epoch(4),
        %% The Resolver stops. The leader recruits the epoch 5.
        exit(deadlift_resolver:ref(4), kill),
        #{epoch := 5} = await_epoch(5),
        %% A request for a recovery. A candidate ignores the request.
        ok = deadlift_cluster_controller:force_recovery(B),
        ok = deadlift_cluster_controller:force_recovery(A),
        #{epoch := 6} = await_epoch(6),
        ?assertMatch(#{role := candidate}, deadlift_cluster_controller:info(B)),
        %% The leader stops. The second candidate wins the election of
        %% term 2, the worker registers with it, and it recruits the
        %% epoch 7.
        exit(A, kill),
        #{term := 2, epoch := 7} = await_epoch(7),
        ?assertMatch(#{role := leader, term := 2, workers := [_]}, deadlift_cluster_controller:info(B)),
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, b, 3) end),
        ?assertEqual({2, 3}, deadlift:transactional(Db, fun(Tx) -> {deadlift:get(Tx, a), deadlift:get(Tx, b)} end)),
        %% A third candidate takes the term 3 from the leader. The leader
        %% reads the coordinated state and is a candidate again. It has
        %% no workers and no information about the database.
        {ok, 3} = deadlift_cstate:elect(Coordinators, 2, self()),
        ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(B)) =:= candidate end, 100),
        ?assertMatch(#{term := 3, workers := [], db_info := undefined}, deadlift_cluster_controller:info(B)),
        %% The worker sends its registration to the process of term 3, at
        %% every poll, because the process does not answer.
        ok = await(fun() -> maps:get(controller, deadlift_worker:info()) =:= {3, self()} end, 100),
        ok = registration(Worker, 3),
        ok = registration(Worker, 3),
        ?assertMatch(#{registered := false}, deadlift_worker:info()),
        %% The worker ignores the information of a ClusterController of
        %% another term.
        Before = deadlift_worker:db_info(),
        ok = gen_server:cast(Worker, {db_info, #{term => 2, epoch => 99, sequencer => undefined, proxies => []}}),
        ?assertMatch(#{registered := false}, deadlift_worker:info()),
        ?assertEqual(Before, deadlift_worker:db_info()),
        ok = deadlift_cluster_controller:stop(B),
        ok = deadlift_worker:stop(),
        ?assertEqual(undefined, deadlift_worker:db_info()),
        ?assertMatch({error, {worker, _}}, deadlift_worker:start_role(Worker, log, #{}))
    after
        exit(A, kill),
        catch deadlift_storage:stop(Storage),
        exit(Worker, kill),
        ok = application:stop(deadlift)
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
    {ok, _} = application:ensure_all_started(deadlift),
    Tag = erlang:unique_integer([positive]),
    Ids = [{list_to_atom(lists:concat([deadlift_cc, Tag, "_", N])), node()} || N <- [1, 2, 3]],
    {ok, Ids, []} = peace:start_cluster(Ids, deadlift_cstate:machine(), #{election_timeout_ms => 100, storage => {peace_storage_mem, #{}}}),
    Options = (options(Ids))#{poll_ms => 50},
    process_flag(trap_exit, true),
    {ok, Worker} = deadlift_worker:start_link(#{coordinators => Ids, poll_ms => 50}),
    {ok, A} = deadlift_cluster_controller:start_link(Options),
    {ok, B} = deadlift_cluster_controller:start_link(Options),
    try
        #{epoch := 1} = await_epoch(1),
        {ok, #{controller := {1, Leader}}} = deadlift_cstate:read(Ids),
        ?assert(lists:member(Leader, [A, B])),
        [Other] = [A, B] -- [Leader],
        ?assertMatch(#{role := leader}, deadlift_cluster_controller:info(Leader)),
        ?assertMatch(#{role := candidate}, deadlift_cluster_controller:info(Other)),
        {ok, Storage} = deadlift_storage:start(#{coordinators => Ids, poll_ms => 5}),
        Db = deadlift:open(#{proxy => fun() -> hd(maps:get(proxies, deadlift_worker:db_info())) end, storage => Storage}),
        ok = deadlift:transactional(Db, fun(Tx) -> deadlift:set(Tx, a, 1) end),
        %% The leader stops. The other candidate recruits the epoch 2.
        exit(Leader, kill),
        #{term := 2, epoch := 2} = await_epoch(2),
        ?assertEqual(1, deadlift:transactional(Db, fun(Tx) -> deadlift:get(Tx, a) end)),
        ok = deadlift_storage:stop(Storage)
    after
        [unlink(Pid) || Pid <- [A, B, Worker]],
        exit(A, kill),
        exit(B, kill),
        exit(Worker, kill),
        process_flag(trap_exit, false),
        flush(),
        ok = application:stop(deadlift),
        ok = application:stop(peace)
    end.

%% ---------------------------------------------------------------------------
%% Several workers of one node
%% ---------------------------------------------------------------------------

%% A node has one `deadlift_worker'. The tests below use processes that
%% register as workers and start roles as a worker does (`fake_worker/3').

%% The leader waits for `min_workers' workers. It places the LogServers on
%% the workers of the class `log', and the other roles on the workers of
%% the class `stateless' and then `unset'.
placement_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{min_workers => 5}, fun(Controller, _CState) ->
            ?assertEqual({leader, 1}, next(leader)),
            [L1, L2, L3, S1] = [fake(Controller, Node, Class) || {Node, Class} <- [{l1, log}, {l2, log}, {l3, log}, {s1, stateless}]],
            ok = await(fun() -> length(maps:get(workers, deadlift_cluster_controller:info(Controller))) =:= 4 end, 100),
            ?assertEqual(none, peek(recruiting)),
            U1 = fake(Controller, u1, unset),
            {recruiting, 1, Plan1} = next(recruiting),
            ?assertEqual([L1, L2, L3], lists:sort(maps:get(logs, Plan1))),
            ?assertEqual({S1, U1}, {maps:get(sequencer, Plan1), maps:get(resolver, Plan1)}),
            ?assert(lists:member(maps:get(proxy, Plan1), [L1, L2, L3])),
            {recovered, #{epoch := 1, sequencer := Sequencer, proxy := Proxy}} = next(recovered),
            ?assertEqual({S1, sequencer}, started(Sequencer)),
            ?assertEqual(proxy, element(2, started(Proxy))),
            ?assertEqual(
                lists:sort([{L1, log}, {L2, log}, {L3, log}]),
                lists:sort([started(deadlift_log:ref({1, Index})) || Index <- [1, 2, 3]])
            ),
            ?assertEqual({U1, resolver}, started(deadlift_resolver:ref(1))),
            %% Each fake worker has received the information about the
            %% epoch.
            ok = await(fun() -> db_info(L1) =:= #{term => 1, epoch => 1, sequencer => Sequencer, proxies => [Proxy]} end, 100),
            %% The second attempt starts at another worker. The classes
            %% decide in the same way.
            ok = deadlift_cluster_controller:force_recovery(Controller),
            {recruiting, 2, Plan2} = next(recruiting),
            ?assertEqual([L1, L2, L3], lists:sort(maps:get(logs, Plan2))),
            ?assertNotEqual(maps:get(logs, Plan1), maps:get(logs, Plan2)),
            ?assertEqual({S1, U1}, {maps:get(sequencer, Plan2), maps:get(resolver, Plan2)}),
            {recovered, #{epoch := 2}} = next(recovered),
            %% A worker stops. The leader removes it. The test stops the
            %% LogServer that the worker started, as a node does when it
            %% stops, and the leader recruits without the worker.
            exit(L1, kill),
            ok = await(fun() -> length(maps:get(workers, deadlift_cluster_controller:info(Controller))) =:= 4 end, 100),
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

%% A role does not start. The leader recruits again.
start_failure_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{}, fun(Controller, _CState) ->
            W = fake(Controller, w1, unset),
            {recovered, #{epoch := 1}} = next(recovered),
            %% The worker refuses the Sequencer once.
            W ! {fail, sequencer},
            ok = deadlift_cluster_controller:force_recovery(Controller),
            ?assertEqual({recovery_failed, {sequencer, refused}}, next(recovery_failed)),
            {recovered, #{epoch := 2}} = next(recovered),
            %% The worker refuses a LogServer once. The recovery of the
            %% Sequencer fails after it has raised the recovery count.
            W ! {fail, log},
            ok = deadlift_cluster_controller:force_recovery(Controller),
            ?assertEqual({recovery_failed, {recruit, refused}}, next(recovery_failed)),
            {recovered, #{epoch := 4}} = next(recovered),
            %% The worker refuses the Resolver once.
            W ! {fail, resolver},
            ok = deadlift_cluster_controller:force_recovery(Controller),
            ?assertEqual({recovery_failed, {recruit_resolver, refused}}, next(recovery_failed)),
            {recovered, #{epoch := 6}} = next(recovered),
            %% The worker refuses the Proxy once. The recovery has ended,
            %% so the next attempt has the next epoch.
            W ! {fail, proxy},
            ok = deadlift_cluster_controller:force_recovery(Controller),
            ?assertEqual({recovery_failed, {proxy, refused}}, next(recovery_failed)),
            {recovered, #{epoch := 8}} = next(recovered),
            ?assertMatch(#{db_info := #{epoch := 8}}, deadlift_cluster_controller:info(Controller))
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
            ok = deadlift_cluster_controller:force_recovery(Controller),
            {recruiting, 2, _} = next(recruiting),
            ok = await(fun() -> length([x || {_Pid, sequencer} <- roles(W)]) =:= 2 end, 100),
            Second = lists:last([Pid || {Pid, sequencer} <- roles(W)]),
            ?assert(is_process_alive(Second)),
            ok = deadlift_cluster_controller:force_recovery(Controller),
            {recruiting, 3, _} = next(recruiting),
            ok = await(fun() -> not is_process_alive(Second) end, 100),
            {recovered, #{epoch := Epoch}} = next(recovered),
            %% The second Sequencer raised the recovery count before it
            %% stopped.
            ?assertEqual(3, Epoch),
            ?assertEqual(none, peek(recovery_failed)),
            ?assertMatch({ok, #{current := #{epoch := Epoch}}}, deadlift_cstate:read({local, CState}))
        end)
    end}.

%% Another Sequencer raises the recovery count. No role of the epoch of
%% the leader stops. The leader reads the count and recruits again.
superseded_test_() ->
    {timeout, 60, fun() ->
        with_controller(#{}, fun(Controller, CState) ->
            _ = fake(Controller, w1, unset),
            {recovered, #{epoch := 1}} = next(recovered),
            {ok, 2, _} = deadlift_cstate:lock({local, CState}, 1, make_ref()),
            ?assertEqual({superseded, 1, 2}, next(superseded)),
            {recovered, #{epoch := 3}} = next(recovered),
            timer:sleep(100),
            ?assertEqual(none, peek(superseded)),
            ?assertMatch(#{db_info := #{epoch := 3}}, deadlift_cluster_controller:info(Controller))
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
            ok = await(fun() -> length(maps:get(workers, deadlift_cluster_controller:info(Controller))) =:= 2 end, 20),
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
            ?assertMatch(#{db_info := #{epoch := 1}}, deadlift_cluster_controller:info(Controller))
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
        {ok, CState} = deadlift_cstate_local:start(#{}),
        Coordinators = {local, CState},
        Options = (options(Coordinators))#{poll_ms => 10},
        Controllers = [element(2, deadlift_cluster_controller:start(Options)) || _ <- lists:seq(1, 5)],
        Leaders = fun() ->
            [C || C <- Controllers, is_process_alive(C), maps:get(role, deadlift_cluster_controller:info(C)) =:= leader]
        end,
        try
            lists:foreach(
                fun(Term) ->
                    ok = await(
                        fun() ->
                            case deadlift_cstate:read(Coordinators) of
                                {ok, #{controller := {Term, Leader}}} -> Leaders() =:= [Leader];
                                _ -> false
                            end
                        end,
                        200
                    ),
                    %% Each other candidate knows the term.
                    ok = await(
                        fun() -> [] =:= [C || C <- Controllers, is_process_alive(C), maps:get(term, deadlift_cluster_controller:info(C)) =/= Term] end,
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
        {ok, Controller} = deadlift_cluster_controller:start((options({local, Stub}))#{poll_ms => 20}),
        try
            ok = proposed({elect, 0, Controller}),
            ok = await(
                fun() -> deadlift_cluster_controller:info(Controller) =:= #{role => candidate, term => 1, workers => [], db_info => undefined} end,
                100
            ),
            %% The coordinated state has the term 1 from now on, and
            %% accepts the next proposal.
            Stub ! {set, #{read => {1, Other}, elect => {ok, 2}}},
            timer:sleep(60),
            ?assertMatch(#{role := candidate}, deadlift_cluster_controller:info(Controller)),
            Other ! stop,
            ok = proposed({elect, 1, Controller}),
            ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(Controller)) =:= leader end, 100),
            ?assertMatch(#{term := 2}, deadlift_cluster_controller:info(Controller))
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
        {ok, Controller} = deadlift_cluster_controller:start((options({local, Stub}))#{poll_ms => 20, cstate_timeout_ms => 20}),
        try
            timer:sleep(100),
            ?assertMatch(#{role := candidate, term := 0}, deadlift_cluster_controller:info(Controller)),
            %% The read answers, and the proposal does not.
            Stub ! {set, #{read => {0, undefined}}},
            ok = proposed({elect, 0, Controller}),
            ?assertMatch(#{role := candidate, term := 0}, deadlift_cluster_controller:info(Controller)),
            Stub ! {set, #{elect => {ok, 1}}},
            ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(Controller)) =:= leader end, 100),
            Stub ! {set, #{read => silent}},
            timer:sleep(100),
            ?assertMatch(#{role := leader, term := 1}, deadlift_cluster_controller:info(Controller)),
            %% The coordinated state has the term 2 of another process.
            Stub ! {set, #{read => {2, self()}}},
            ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(Controller)) =:= candidate end, 100)
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
        {ok, Controller} = deadlift_cluster_controller:start((options({local, Stub}))#{poll_ms => 20, cstate_timeout_ms => 20}),
        try
            ok = proposed({elect, 0, Controller}),
            ?assertMatch(#{role := candidate}, deadlift_cluster_controller:info(Controller)),
            Stub ! {set, #{read => {1, Controller}}},
            ok = await(fun() -> deadlift_cluster_controller:info(Controller) =:= #{role => leader, term => 1, workers => [], db_info => #{term => 1, epoch => undefined, sequencer => undefined, proxies => []}} end, 100)
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
        {ok, CState} = deadlift_cstate_local:start(#{}),
        {ok, Controller} = deadlift_cluster_controller:start((options({local, CState}))#{poll_ms => 10, step_down => false}),
        try
            ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(Controller)) =:= leader end, 100),
            {ok, 2} = deadlift_cstate:elect({local, CState}, 1, self()),
            timer:sleep(100),
            ?assertMatch(#{role := leader, term := 1}, deadlift_cluster_controller:info(Controller))
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
    {ok, CState} = deadlift_cstate_local:start(#{}),
    {ok, Worker} = deadlift_worker:start(#{
        coordinators => {local, CState},
        roles => fun
            (log, _Options) -> error(broken);
            (_Role, _Options) -> {ok, self()}
        end
    }),
    try
        ?assertEqual({error, {error, broken}}, deadlift_worker:start_role(Worker, log, #{})),
        ?assertEqual({ok, Worker}, deadlift_worker:start_role(Worker, proxy, #{})),
        ?assertEqual(undefined, deadlift_worker:db_info()),
        ?assertEqual(#{controller => {0, undefined}, registered => false}, deadlift_worker:info())
    after
        ok = deadlift_worker:stop(),
        exit(CState, kill)
    end.

%% The coordinated state does not answer. The worker and the candidate
%% answer their requests without a wait for the coordinated state.
responsive_test_() ->
    {timeout, 60, fun() ->
        Stub = spawn(?MODULE, stub_cstate, [#{test => self(), read => silent, elect => silent}]),
        Coordinators = {local, Stub},
        {ok, Worker} = deadlift_worker:start(#{
            coordinators => Coordinators,
            poll_ms => 10,
            cstate_timeout_ms => 300,
            roles => fun(_Role, _Options) -> {ok, self()} end
        }),
        {ok, Controller} = deadlift_cluster_controller:start((options(Coordinators))#{poll_ms => 10, cstate_timeout_ms => 300}),
        try
            %% Several polls of each process have passed.
            timer:sleep(700),
            {WorkerUs, {ok, Worker}} = timer:tc(fun() -> deadlift_worker:start_role(Worker, log, #{}) end),
            {ControllerUs, #{role := candidate}} = timer:tc(fun() -> deadlift_cluster_controller:info(Controller) end),
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

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

options(Coordinators) ->
    #{
        coordinators => Coordinators,
        poll_ms => 20,
        retry_ms => 10,
        sequencer => #{lock_timeout_ms => 100, versions_per_second => 0}
    }.

%% Runs `Fun' with one candidate and a `deadlift_cstate_local' process.
%% The events of the candidate arrive in the mailbox of the test.
with_controller(Extra, Fun) ->
    {ok, _} = application:ensure_all_started(deadlift),
    {ok, CState} = deadlift_cstate_local:start(#{}),
    Test = self(),
    Options = maps:merge((options({local, CState}))#{notify => fun(Event) -> Test ! {event, Event} end}, Extra),
    {ok, Controller} = deadlift_cluster_controller:start(Options),
    ok = await(fun() -> maps:get(role, deadlift_cluster_controller:info(Controller)) =:= leader end, 100),
    Fakes = ets:new(fakes, [public, bag]),
    put(fakes, Fakes),
    try
        Fun(Controller, CState)
    after
        exit(Controller, kill),
        [exit(Pid, kill) || {worker, Pid} <- ets:lookup(Fakes, worker)],
        erase(fakes),
        ok = application:stop(deadlift),
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
    #{term := Term} = deadlift_cluster_controller:info(Controller),
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
                    Module = list_to_existing_atom("deadlift_" ++ atom_to_list(Role)),
                    {ok, Pid} = Module:start(Options),
                    true = ets:insert(Fakes, [{{role, Pid}, self(), Role}, {{started, self()}, Pid, Role}]),
                    gen_server:reply(From, {ok, Pid})
            end,
            fake_loop(Fakes, maps:remove(Role, Faults))
    end.

%% @private
%% A process in the place of `deadlift_cstate_local'. `read' is the term
%% and the process of the ClusterController, and `elect' is the answer to a
%% proposal. With `silent' the process does not answer.
stub_cstate(Config = #{test := Test}) ->
    receive
        {set, New} ->
            stub_cstate(maps:merge(Config, New));
        {'$gen_call', From, read} ->
            case Config of
                #{read := silent} -> ok;
                #{read := Controller} -> gen_server:reply(From, {ok, (deadlift_cstate:init([]))#{controller := Controller}})
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

%% Waits until the worker of this node has the information about the
%% epoch `Epoch'.
await_epoch(Epoch) ->
    ok = await(
        fun() ->
            case deadlift_worker:db_info() of
                #{epoch := Epoch, proxies := [_]} -> true;
                _ -> false
            end
        end,
        400
    ),
    deadlift_worker:db_info().

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
