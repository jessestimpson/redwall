%% This module is the eta harness for the transaction system of redwall.
%% The harness starts the system, runs a workload of commits, recoveries and
%% kills against it, and checks invariants after every step.
%%
%% == Simulated system ==
%%
%% <ul>
%% <li>The coordinated state is one `redwall_cstate_local' process, or a
%%   peace cluster (see "Runs with a peace cluster"). The process applies one value at a time,
%%   which is the register with atomic operations of
%%   `formal/Recovery.tla'.</li>
%% <li>The process of an epoch, a recovery and a StorageServer read the
%%   process of each LogServer from the coordinated state. Two simulated
%%   nodes connect with the first message between them.</li>
%% <li>A `recover' operation starts the process of a new epoch
%%   (`redwall_epoch'), which does the work of the Sequencer, of the
%%   Resolver and of the Proxy. This is the work of a ClusterController. In these runs the harness does
%%   this work at times that the seed selects, so two recoveries overlap
%%   more often than with a ClusterController.</li>
%% <li>A recovery recruits 3 LogServers with a replication degree of
%%   2.</li>
%% <li>A run has 1, 2 or 3 StorageServers. Each one sends each request
%%   to a LogServer again every 1 s of the virtual clock.</li>
%% <li>The run has the simulated network of eta. The coordinated state,
%%   the StorageServer, each Sequencer, each Proxy, each Resolver and each
%%   LogServer run on a simulated node of their own. All clients share one node. The network policy of the
%%   run can lose or delay a message between two nodes.</li>
%% <li>A LogServer starts on its own node, through `erpc' from the node
%%   of the process of the epoch.</li>
%% </ul>
%%
%% == Runs with a peace cluster ==
%%
%% With the config `cstate => peace', the coordinated state is a peace
%% cluster in the place of the `redwall_cstate_local' process:
%%
%% <ul>
%% <li>The cluster has 3 servers, each on a simulated node of its own,
%%   with majority quorums, an election time limit of 50 ms and a
%%   heartbeat of 10 ms. peace is compiled with the parse transform of
%%   eta in the `dst' profile.</li>
%% <li>The first `recover' operation starts the servers, and a process
%%   that forms the cluster (`peace:form_cluster/3').</li>
%% <li>The machine of the servers is `redwall_dst_machine', which applies
%%   the values with `redwall_cstate' and records the state of the
%%   highest index that a server has applied. The harness reads that
%%   state for the invariants.</li>
%% <li>Each server has the configuration of `redwall_coordinator': it
%%   keeps its data in the write-ahead log of peace, on the simulated
%%   disk of eta (`disk => true' of the run).</li>
%% <li>`{kill_coordinator, R}' kills the node of one server that runs, or
%%   of every server that runs in 1 operation of 4, after the cluster has
%%   formed. The kill is a crash of the disk of the node. The next
%%   `recover' operation starts each killed server again with its
%%   data.</li>
%% <li>The invariant `coordinators_agree': no two servers have two states
%%   of the coordinated state for one index of the log of peace.</li>
%% <li>The planted defect `unsynced_coordinators': a server never syncs
%%   its files (`redwall_dst_nosync_io').</li>
%% <li>`{partition, R1, R2}' can cut a node of a server off. A cut can
%%   leave the cluster without a majority until the next `heal'.</li>
%% <li>A call to the cluster can end without an answer after peace has
%%   decided its value. The `redwall_cstate_local' process has this case
%%   only for a lost answer.</li>
%% </ul>
%%
%% == Runs with ClusterControllers ==
%%
%% With the config key `controller', ClusterControllers recruit the
%% Sequencers:
%%
%% <ul>
%% <li>The first `recover' operation starts 8 worker nodes: 4 of the class
%%   `stateless', of which 3 have a candidate for the ClusterController,
%%   and 4 of the class `log'. Each worker has `poll_ms' of 200 ms. A
%%   ClusterController recruits with at least 3 workers.</li>
%% <li>Each later `recover' operation starts a new worker node for each
%%   killed worker node, and asks the ClusterController for a recovery
%%   (`redwall_cluster_controller:force_recovery/1').</li>
%% <li>`kill_log', `kill_sequencer', `kill_proxy' and `kill_resolver' kill
%%   the worker node of one role of that kind that runs. The kill stops
%%   every role on the node, and the candidate if the node has one.</li>
%% <li>The harness kills a worker node only if no epoch then has 2 killed
%%   LogServers.</li>
%% <li>The ClusterController recruits again after the failure of a role,
%%   and a candidate wins an election after the failure of the
%%   leader.</li>
%% </ul>
%%
%% <li>After the last operation, the harness removes every cut, replaces
%%   every killed worker node, and stops the loss of messages. The run
%%   continues until the checks of `check_final/2' hold, for at most
%%   20,000 steps.</li>
%% </ul>
%%
%% The invariants of the other runs hold in these runs. These runs have
%% one more invariant, `one_leader': no two ClusterController processes
%% are the leader of one term. `check_final/2' checks at the end of a run:
%%
%% <ul>
%% <li>`leader' - the ClusterController of the coordinated state runs and
%%   is the leader.</li>
%% <li>`stale_leader' - no other process is a leader.</li>
%% <li>`epoch' - the leader has the current epoch of the coordinated
%%   state, and the process of the epoch runs.</li>
%% <li>`worker_db_info' - each worker that runs has the information of
%%   the leader about the database.</li>
%% </ul>
%%
%% == Workload ==
%%
%% <ul>
%% <li>`recover': start the process of a new epoch.</li>
%% <li>`{transaction, R1, R2}': a client runs one transaction. It gets a
%%   read version from one Proxy, and commits through a Proxy that can be
%%   another one. Between the two steps, the client reads its read keys
%%   from the StorageServer at the read version. The StorageServer has an
%%   MVCC window of 100,000 versions in the runs with an odd seed, and of
%%   5,000,000 versions in the other runs. One client in three then
%%   waits, and commits through the Proxy of the highest epoch that
%%   runs. The client uses the Proxy of the highest epoch in 3 of 4
%%   cases, and any Proxy that runs in the others. A Proxy can belong to an
%%   epoch that has ended. The transaction reads and writes some of 4
%%   keys. 2 transactions in 7 also read a key range. 2 transactions in
%%   11 clear a key range before they write their keys.</li>
%% <li>`{kill_log, R}': kill one LogServer of the current epoch. The
%%   harness kills at most 1 LogServer of each epoch, because the design
%%   tolerates the loss of k - 1 LogServers of an epoch, and a LogServer of
%%   redwall keeps its records in memory.</li>
%% <li>`{kill_sequencer, R}': kill the process of one epoch. The process
%%   can be in the middle of its recovery.</li>
%% <li>`{kill_proxy, R}': kill the process of one epoch whose recovery
%%   has ended.</li>
%% <li>`{kill_resolver, R}': kill the process of one epoch whose recovery
%%   has ended, at most once for each epoch. The three operations are
%%   from the time when the Sequencer, the Proxy and the Resolver were
%%   three processes.</li>
%% <li>`{kill_storage, R}': kill one StorageServer, and start a new
%%   StorageServer without data on a new node. The new StorageServer
%%   copies the data from another StorageServer of the run that has data,
%%   and otherwise reads every record from the LogServers of every epoch
%%   (see "Copy from another StorageServer" in `redwall_storage'). In a run with
%%   several StorageServers, the new StorageServer starts at once after 1
%%   kill in 2, and with the next `recover' operation after the other
%%   kills. A run has 1, 2 or 3
%%   StorageServers, each on a node of its own. The number depends on the
%%   seed. A client reads from the StorageServers of the run through the
%%   client library, which continues with the next StorageServer when one
%%   does not answer. 1 client in 2 runs on the node of a StorageServer
%%   and reads the ETS table of that StorageServer. A kill of the node
%%   also stops these clients. The other clients run on the node
%%   `clients' and read by message. 1 client in 5 reads by message in
%%   every case.</li>
%% <li>`{partition, R1, R2}': cut the connection between two nodes. Each
%%   side receives `noconnection' for its monitors on the other side.</li>
%% <li>`heal': remove every cut.</li>
%% </ul>
%%
%% A kill stops every process on the node of the target
%% (`eta_net:kill_node/1').
%%
%% `R' is a random number that selects the target when the operation runs.
%% An operation contains no pid, so that a trace can be replayed.
%%
%% == Invariants ==
%%
%% The harness checks these invariants after every step. The first three
%% are invariants of the model.
%%
%% <ul>
%% <li>`durable' - when an epoch has ended, every LSN that a client
%%   received as committed in that epoch is at or below the Recovery
%%   Version that ended the epoch. This is `Durable' of the model.</li>
%% <li>`unique_versions' - no LSN is acknowledged in two epochs, and no
%%   pair of an LSN and an index in a batch is acknowledged to two
%%   clients. This is `UniqueVersions' of the model.</li>
%% <li>`no_loss' - no recovery ends with `missing_body'. This is `NoLoss'
%%   of the model.</li>
%% <li>`body_available' - the body of every acknowledged record is on a
%%   LogServer of its epoch that runs.</li>
%% <li>`copied' - when a recovery ends an epoch, every LogServer of the new
%%   epoch that runs has the body of every acknowledged record of the
%%   ended epoch above the previous epoch's end version.</li>
%% <li>`no_conflict' - no acknowledged transaction read a key, alone or in
%%   a key range, that another
%%   acknowledged transaction wrote at a commit version above the read
%%   version of the first transaction and below its commit version, or
%%   at its commit version with a lower index in the batch. This is the
%%   guarantee of the Resolver.</li>
%% <li>`read_version_order' - a read version is at or above the commit
%%   version of every transaction that was acknowledged before the client
%%   asked for the read version, in any epoch. The harness checks every
%%   read version that a client received, also when the commit failed. A Proxy of an ended epoch
%%   must not give a read version.</li>
%% <li>`read_value' - a read of a key at a read version returns the write
%%   of the acknowledged transaction with the highest commit version at or
%%   below the read version, and with the highest index among the
%%   transactions of that version, or a write with a higher commit version
%%   at or below the read version whose client received no answer. No
%%   acknowledged transaction wrote or cleared the key between the
%%   returned write and the read version. The read returns `not_found'
%%   only in three cases: no acknowledged transaction wrote the key at or
%%   below the read version, the last acknowledged transaction that
%%   changed the key cleared it, or a transaction without a known result
%%   cleared the key.</li>
%% <li>`storage_rollback' - when the StorageServer has a version of an
%%   epoch, it has no write between the end version of an earlier epoch
%%   and the first version of the epoch after that one. A recovery
%%   discarded every record in that range.</li>
%% <li>`replicas_agree' - two StorageServers have the same writes for each
%%   key, for the versions that both have applied and that are above the
%%   oldest version of both. The writes that a recovery discarded are not
%%   compared, because `storage_rollback' covers them.</li>
%% <li>`select_agrees' - a `redwall:select/4' of a client returned the
%%   result of `ets:match_spec_run/2' on the result of `get_range/3' at
%%   the same read version. A client with a read range evaluates two
%%   match specs on the range.</li>
%% <li>`mapped_range_agrees' - a `redwall:get_mapped_range/4' of a client
%%   returned the rows of `get_range/3' at the same read version, each
%%   with the result of its second read. A client with a read range reads
%%   the range with two mappers: one with a second read of a key, and one
%%   with a second read of a prefix.</li>
%%   1 read range in 4 of a transaction is read with
%%   `get_mapped_range/4' only, so that `no_conflict' checks the read
%%   conflict ranges of a mapped range read. Each read with a limit
%%   returns the first rows of the read without a limit.</li>
%% <li>`own_writes' - before the commit, the reads of a transaction
%%   return its sets and its clears: `get/2', `get_range/3' and
%%   `select/4' of a snapshot. `get_mapped_range/4' raises 2039. After a
%%   failed commit, `on_error/2' leaves a transaction without a read
%%   version and without a mutation.</li>
%% <li>`kcv_below_dv' - in each epoch, the Known Committed Version of
%%   every LogServer that runs is at or below the Durable Version of every
%%   LogServer that runs.</li>
%% <li>`chain' - in each LogServer, the previous LSN of each record is the
%%   LSN of the record before it.</li>
%% </ul>
%%
%% == Planted defects ==
%%
%% The config key `mode' selects a defect:
%%
%% <ul>
%% <li>`ack_replicas' - a Proxy answers a client after the 2 LogServers
%%   with the body have answered. See `RecoveryAckReplicasOnly' in
%%   `formal/README.md'.</li>
%% <li>`no_cas' - the coordinated state accepts a write from a Sequencer
%%   whose epoch is not the recovery count. See `RecoveryNoCas'.</li>
%% <li>`no_copy' - a recovery copies no record to the new LogServers.</li>
%% <li>`no_wait' - a StorageServer answers a read at a version that it has
%%   not applied.</li>
%% <li>`one_no_clear' - the second StorageServer of a run does not apply a
%%   clear.</li>
%% <li>`no_elect_cas' - the coordinated state accepts the proposal of a
%%   candidate with any term. This defect violates `one_leader'.</li>
%% <li>`no_monitor_roles' - a ClusterController does not recruit after
%%   the failure of a role. `check_final/2' finds a run that ends without
%%   an epoch.</li>
%% <li>`no_step_down' - a ClusterController stays the leader when the
%%   coordinated state has another term. `check_final/2' finds a run that
%%   ends with two leaders.</li>
%% <li>`no_check' - the Resolver admits every transaction.</li>
%% <li>`no_range_check' - the Resolver does not check the read ranges of a
%%   transaction.</li>
%% <li>`no_batch_check' - the Resolver does not check a transaction
%%   against the earlier transactions of its batch.</li>
%% <li>`no_gap' - the first commit version of an epoch is the Recovery
%%   Version plus 1. The Resolver of the new epoch then admits a
%%   transaction with a read version of the previous epoch.</li>
%% <li>`no_report' - a Proxy does not report a committed LSN to the
%%   Sequencer.</li>
%% <li>`no_confirm' - a Proxy gives a read version without the
%%   confirmation that its epoch is live.</li>
%% <li>`no_rollback' - the StorageServer keeps the writes above the end
%%   version of an epoch.</li>
%% <li>`no_write_range' - the Resolver does not record the write ranges of
%%   a transaction, so a clear of a key range gives no conflict.</li>
%% <li>`no_clear' - the StorageServer does not apply a clear.</li>
%% <li>`no_window_check' - the StorageServer answers a read below its
%%   oldest version, after it has removed older versions.</li>
%% </ul>
%%
%% == Limitations ==
%%
%% <ul>
%% <li>Without the config `cstate => peace', the coordinated state is one
%%   process that is never killed.</li>
%% <li>The peace cluster has no change of its members in a run, and no
%%   server is replaced.</li>
%% <li>A killed process does not start again. A LogServer has no disk.</li>
%% <li>The application start and `redwall:open/0' are not in the run. A
%%   client names its Proxy and its StorageServers.</li>
%% <li>A client does not run a transaction again after an error. It calls
%%   `redwall:on_error/2' and ends.</li>
%% <li>redwall does not send a message again. A lost message to a
%%   LogServer stops the commits of the epoch until the next recovery.
%%   FoundationDB runs on TCP, and treats a broken connection as a failure
%%   of the process.</li>
%% <li>The virtual clock advances only when no process can run. The
%%   time-outs of calls and the time limit of the lock fire only then.</li>
%% </ul>
-module(redwall_dst).

-behaviour(eta_harness).

-export([init/2, processes/1, generate/2, execute/2, check/1, check_final/2, settle_done/1, terminate/1]).
-export([stats/0, reset_stats/0]).

-define(STATS, redwall_dst_stats).

%% The gap between the Recovery Version and the first commit version of an
%% epoch: the default of `max_versions_in_flight'.
-define(EPOCH_GAP, 100000000).

%% ---------------------------------------------------------------------------
%% eta_harness
%% ---------------------------------------------------------------------------

init(Seed, Config) ->
    Mode = maps:get(mode, Config, correct),
    %% The MVCC window of the StorageServer is 100,000 versions in the runs
    %% with an odd seed: the versions of 100 ms of the virtual clock. In
    %% the other runs it is the default of 5,000,000 versions.
    Window =
        case Seed rem 2 of
            1 -> 100000;
            0 -> 5000000
        end,
    %% The modules in dst/ are not part of the redwall application, so
    %% `preload => [redwall]' does not load them.
    {module, _} = code:ensure_loaded(redwall_dst_client),
    Tab = ets:new(redwall_dst, [public, ordered_set]),
    Cas = #{cas => Mode =/= no_cas, elect_cas => Mode =/= no_elect_cas},
    %% The topology is declared before any process creates a monitor.
    CState =
        case maps:get(cstate, Config, local) of
            local ->
                {ok, Local} = redwall_cstate_local:start(Cas),
                ok = eta_net:place(coordinators, [Local]),
                true = ets:insert(Tab, {{node, coordinators}, true}),
                {local, Local};
            peace ->
                %% A peace cluster of 3 servers, each on a node of its
                %% own. The first `recover' operation starts the servers,
                %% because a process on a simulated node starts only in a
                %% step of the scheduler.
                {module, _} = code:ensure_loaded(redwall_dst_machine),
                {module, _} = code:ensure_loaded(redwall_dst_nosync_io),
                ok = clear_dir(disk_dir()),
                ok = redwall_dst_machine:configure(Cas#{tab => Tab}),
                Ids = redwall_dst_client:coordinator_ids(),
                Nodes = [Node || {_Name, Node} <- Ids],
                [ok = eta_net:place(Node, []) || Node <- Nodes],
                [true = eta_net:with_node(A, fun() -> eta_net:connect_node(B) end) || A <- Nodes, B <- Nodes, A < B],
                true = ets:insert(Tab, [{{node, Node}, true} || Node <- Nodes]),
                {peace, Tab, Ids}
        end,
    ok = eta_net:place(clients, []),
    true = ets:insert(Tab, {{node, clients}, true}),
    %% 1, 2 or 3 StorageServers, each on a node of its own. The first
    %% `recover' operation starts them, because a process on a simulated
    %% node starts only in a step of the scheduler.
    Count = Seed div 2 rem 3 + 1,
    true = ets:insert(Tab, [{{storage_down, Slot}, -1} || Slot <- lists:seq(1, Count)]),
    {ok, #{
        tab => Tab,
        cstate => CState,
        storage_count => Count,
        mode => Mode,
        window => Window,
        faults => maps:get(faults, Config, true),
        %% Whether ClusterControllers recruit the Sequencers of the run.
        controller => maps:get(controller, Config, false),
        %% Whether the workload cuts connections. With `controllers', it
        %% cuts connections between nodes with a ClusterController
        %% process only.
        partitions => maps:get(partitions, Config, true),
        %% The number of the last operation of the run, or `undefined'.
        last_op => maps:get(last_op, Config, undefined),
        coordinators_started => false,
        %% Every process that an operation started, oldest first.
        ops => [],
        %% The number of operations that the run has executed.
        n => 0
    }}.

%% The coordinated state, and the operation processes in the order of
%% their start. The scheduler adopts the Sequencers, the
%% Proxies, the LogServers and the StorageServers as children of the
%% processes that start them.
processes(#{cstate := {local, CState}, ops := Ops}) ->
    [CState | [Pid || Pid <- Ops, is_process_alive(Pid)]];
processes(#{ops := Ops}) ->
    [Pid || Pid <- Ops, is_process_alive(Pid)].

%% Starts the StorageServer of the place `Slot'. `N' is the number of the
%% operation, and `Generation' is the number of StorageServers of the
%% place before this one. A new StorageServer has no data. It reads every
%% record from the LogServers of every epoch. The StorageServer runs on
%% the node `storage_Slot_N'. After a kill, an operation process on that
%% node calls this function, and the StorageServer starts on the node of
%% that process.
%%
%% With the planted defect `one_no_clear', the StorageServer of the place
%% 2 does not apply a clear. A run with one StorageServer has no defect.
start_storage(Tab, CState, {Mode, Window}, {Slot, N}, Generation) ->
    ok = redwall_dst_client:join(Tab, Mode),
    {ok, Storage} = redwall_storage:start(#{
        coordinators => redwall_dst_client:coordinators(CState),
        %% The other StorageServers of the run that run. A StorageServer
        %% that starts after a kill copies the data from one of them, if
        %% one of them has data.
        peers => fun() -> [Peer || Peer <- storages(Tab), Peer =/= self(), is_process_alive(Peer)] end,
        poll_ms => 100,
        mvcc_window => Window,
        rollback => Mode =/= no_rollback,
        check_window => Mode =/= no_window_check,
        clear => Mode =/= no_clear andalso not (Mode =:= one_no_clear andalso Slot =:= 2),
        wait => Mode =/= no_wait
    }),
    Node = redwall_dst_client:storage_node({Slot, N}),
    true = ets:insert(Tab, [{{storage_started, {Slot, N}}, Storage}, {{storage, Slot}, Storage, Generation, Node}]),
    Storage.

%% The StorageServers that the table of the run has, in the order of
%% their places.
storages(Tab) ->
    [Storage || [_Slot, Storage] <- lists:sort(ets:match(Tab, {{storage, '$1'}, '$2', '_', '_'}))].

%% The first operation of a run is a recovery, which creates epoch 1.
generate(#{n := 0}, Rand) ->
    {recover, Rand};
generate(#{faults := Faults, cstate := CState}, Rand0) ->
    {Roll, Rand1} = rand:uniform_s(Rand0),
    {R, Rand2} = rand:uniform_s(1000, Rand1),
    Op =
        if
            not Faults, Roll < 0.08 -> recover;
            not Faults -> {transaction, R};
            %% Faults are rare. A LogServer that is lost or cut off stops the
            %% commits of its epoch until the next recovery, and only an
            %% acknowledged commit can violate most of the invariants.
            Roll < 0.12 -> recover;
            Roll < 0.93 -> {transaction, R};
            Roll < 0.945 -> {kill_log, R};
            Roll < 0.96 -> {kill_sequencer, R};
            Roll < 0.97 -> {kill_proxy, R};
            Roll < 0.975 -> {kill_resolver, R};
            Roll < 0.98 -> {kill_storage, R};
            Roll < 0.988 -> partition;
            %% Only a run with a peace cluster has this operation, so the
            %% operations of the other runs are the same as before.
            Roll < 0.996, element(1, CState) =:= peace -> {kill_coordinator, R};
            true -> heal
        end,
    case Op of
        partition ->
            {R2, Rand3} = rand:uniform_s(1000, Rand2),
            {{partition, R, R2}, Rand3};
        {transaction, R} ->
            {R2, Rand3} = rand:uniform_s(1000, Rand2),
            {{transaction, R, R2}, Rand3};
        _ ->
            {Op, Rand2}
    end.

execute(Op, S0 = #{n := N0}) ->
    N = N0 + 1,
    S = execute(Op, N, S0#{n := N}),
    case S of
        #{last_op := N, controller := true} -> repair(N, S);
        _ -> S
    end.

%% After the last operation of a run with ClusterControllers, the harness
%% removes every cut, and starts a new worker node for each killed worker
%% node and a new StorageServer for each place without one. From this
%% point the network loses no message, because redwall does not send a
%% message again and a lost message costs the time limit of a call. The
%% system then has the nodes for an epoch, and `check_final/2' checks that
%% it has one.
repair(N, S00 = #{tab := Tab, cstate := CState, mode := Mode, window := Window}) ->
    ok = eta_net:heal_all(),
    S0 = restart_coordinators(S00),
    ok = eta_net:set_policy((eta_net:policy())#{drop_p => 0.0}),
    S1 = lists:foldl(
        fun([Slot, Generation], Acc) ->
            true = ets:delete(Tab, {storage_down, Slot}),
            spawn_op(redwall_dst_client:storage_node({Slot, N * 100}), fun() -> start_storage(Tab, CState, {Mode, Window}, {Slot, N * 100}, Generation + 1) end, Acc)
        end,
        S0,
        lists:sort(ets:match(Tab, {{storage_down, '$1'}, '$2'}))
    ),
    replace_workers(N * 100, S1).

%% Starts a new worker node for each killed worker node, with the class of
%% the killed node, and with a candidate if the killed node had one.
replace_workers(Base, S = #{tab := Tab, cstate := CState, mode := Mode}) ->
    lists:foldl(
        fun({K, [Node, {Class, Candidate}]}, Acc) ->
            true = ets:delete(Tab, {worker_down, Node}),
            spawn_op(redwall_dst_client:worker_node(Base + K), fun() -> redwall_dst_client:worker(Tab, CState, Mode, Class, Candidate) end, Acc)
        end,
        S,
        lists:enumerate(lists:sort(ets:match(Tab, {{worker_down, '$1'}, '$2'})))
    ).

execute(recover, 1, S0 = #{cstate := {peace, Tab, Ids}, coordinators_started := false}) ->
    %% The servers of the peace cluster, and a process that forms the
    %% cluster.
    S1 = lists:foldl(fun(Id, Acc) -> start_coordinator(Id, Acc) end, S0, Ids),
    S2 = spawn_op(clients, fun() -> redwall_dst_client:form_cluster(Tab, Ids) end, S1),
    execute(recover, 1, S2#{coordinators_started := true});
execute({kill_coordinator, R}, _N, S = #{tab := Tab, cstate := {peace, Tab, Ids}}) ->
    %% A kill of the node of a coordinator that runs. The kill is also a
    %% crash of the simulated disk of the node: the node loses the data
    %% that the coordinator has not synced. Any number of coordinators can
    %% be killed, because each one starts again with its data at the next
    %% `recover' operation. The harness kills no coordinator before the
    %% cluster has formed.
    %% 1 operation in 4 kills every coordinator that runs, as a loss of
    %% power of every computer does.
    Running = [Id || Id <- Ids, not ets:member(Tab, {coordinator_down, Id})],
    Targets =
        case R rem 4 of
            0 -> Running;
            _ -> [Id || Id <- [pick(R div 4, Running)], Id =/= none]
        end,
    case ets:member(Tab, cluster_formed) of
        true ->
            lists:foreach(
                fun(Id = {_Name, Node}) ->
                    ok = eta_net:kill_node(Node),
                    true = ets:insert(Tab, {{coordinator_down, Id}, true}),
                    bump(kills_coordinator)
                end,
                Targets
            ),
            _ = Targets =/= [] andalso Targets =:= Running andalso bump(kills_of_every_coordinator),
            S;
        false ->
            S
    end;
execute(recover, N, S00 = #{tab := Tab, cstate := CState, mode := Mode, window := Window, controller := Controller}) ->
    %% The operation starts each killed coordinator again, on its node.
    S0 = restart_coordinators(S00),
    %% The operation also starts a new StorageServer for each place whose
    %% StorageServer was killed without a new one.
    S = lists:foldl(
        fun([Slot, Generation], Acc) ->
            true = ets:delete(Tab, {storage_down, Slot}),
            spawn_op(redwall_dst_client:storage_node({Slot, N}), fun() -> start_storage(Tab, CState, {Mode, Window}, {Slot, N}, Generation + 1) end, Acc)
        end,
        S0,
        lists:sort(ets:match(Tab, {{storage_down, '$1'}, '$2'}))
    ),
    case Controller of
        false ->
            spawn_op(redwall_dst_client:sequencer_node(N), fun() -> redwall_dst_client:recover(Tab, N, CState, Mode) end, S);
        true when N =:= 1 ->
            %% 4 worker nodes of the class `stateless', of which 3 have a
            %% candidate for the ClusterController, and 4 worker nodes of
            %% the class `log'.
            lists:foldl(
                fun(K, Acc) ->
                    Class =
                        case K =< 4 of
                            true -> stateless;
                            false -> log
                        end,
                    spawn_op(redwall_dst_client:worker_node(K), fun() -> redwall_dst_client:worker(Tab, CState, Mode, Class, K =< 3) end, Acc)
                end,
                S,
                lists:seq(1, 8)
            );
        true ->
            %% A new worker node for each killed worker node. Then the
            %% operation asks the ClusterController for a recovery.
            S1 = replace_workers(N * 10, S),
            spawn_op(clients, fun() -> redwall_dst_client:force_recovery(Tab) end, S1)
    end;
execute({transaction, R1, R2}, N, S = #{tab := Tab}) ->
    Proxies = lists:keysort(2, [{Proxy, Epoch} || [Proxy, Epoch] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'}), is_process_alive(Proxy)]),
    %% The Proxy of the highest epoch in 3 of 4 cases, and any Proxy that
    %% runs in the others.
    Choose = fun
        (_R) when Proxies =:= [] -> none;
        (R) when R rem 4 =:= 0 -> pick(R div 4, Proxies);
        (_R) -> lists:last(Proxies)
    end,
    case {Choose(R1), Choose(R2)} of
        {none, none} -> S;
        {Reader, Proxy} ->
            %% 1 client in 2 runs on the node of a StorageServer that is
            %% alive, and reads the ETS table of that StorageServer. The
            %% other clients run on the node `clients', and read by
            %% message.
            Local = [Node || [Storage, Node] <- lists:sort(ets:match(Tab, {{storage, '_'}, '$1', '_', '$2'})), is_process_alive(Storage)],
            Node =
                case {R1 rem 2, Local} of
                    {0, [_ | _]} -> pick(R2, Local);
                    _ -> clients
                end,
            spawn_op(Node, fun() -> redwall_dst_client:transaction(Tab, N, Reader, Proxy, R1 + R2) end, S)
    end;
execute({Kill, R}, _N, S = #{tab := Tab, controller := true}) when Kill =:= kill_log; Kill =:= kill_sequencer; Kill =:= kill_proxy; Kill =:= kill_resolver ->
    %% With ClusterControllers, a kill stops the worker node of a role
    %% that runs, with every role on the node. The process of an epoch
    %% does the work of the Sequencer, of the Resolver and of the Proxy,
    %% so the three kills stop the node of such a process.
    {Role, Name} =
        case Kill of
            kill_log -> {log, log};
            kill_sequencer -> {proxy, sequencer};
            kill_proxy -> {proxy, proxy};
            kill_resolver -> {proxy, resolver}
        end,
    Nodes = lists:usort([Node || [Pid, Node] <- ets:match(Tab, {{at, '$1'}, '$2', Role}), is_process_alive(Pid)]),
    case pick(R, Nodes) of
        none -> S;
        Node -> kill_worker(Node, Name, S)
    end;
execute({kill_log, R}, _N, S = #{tab := Tab, cstate := CState}) ->
    case current(CState) of
        #{epoch := Epoch, logs := Logs} ->
            case ets:member(Tab, {killed_log, Epoch}) of
                true ->
                    S;
                false ->
                    {Id, _Pid} = pick(R, Logs),
                    case lists:keymember(Id, 1, log_pids(Tab)) of
                        false ->
                            S;
                        true ->
                            true = ets:insert(Tab, {{killed_log, Epoch}, true}),
                            kill(redwall_dst_client:log_node(Id), kills_log, S)
                    end
            end;
        undefined ->
            S
    end;
execute({kill_sequencer, R}, _N, S = #{tab := Tab}) ->
    case pick(R, [N || [N, Pid] <- ets:match(Tab, {{sequencer, '$1'}, '$2'}), is_process_alive(Pid)]) of
        none -> S;
        N -> kill(redwall_dst_client:sequencer_node(N), kills_sequencer, S)
    end;
execute({kill_proxy, R}, _N, S = #{tab := Tab}) ->
    case pick(R, [N || [N, Pid] <- ets:match(Tab, {{proxy, '$1'}, '$2', '_'}), is_process_alive(Pid)]) of
        none -> S;
        N -> kill(redwall_dst_client:sequencer_node(N), kills_proxy, S)
    end;
execute({kill_resolver, R}, _N, S = #{tab := Tab}) ->
    Running = [N || [N] <- ets:match(Tab, {{recovered, '$1'}, '_', '_', '_'}), not ets:member(Tab, {killed_resolver, N})],
    case pick(R, lists:sort(Running)) of
        none ->
            S;
        N ->
            true = ets:insert(Tab, {{killed_resolver, N}, true}),
            kill(redwall_dst_client:sequencer_node(N), kills_resolver, S)
    end;
execute({kill_storage, R}, N, S = #{tab := Tab, cstate := CState, mode := Mode, window := Window, storage_count := Count}) ->
    Slot = R rem Count + 1,
    case [Row || Row = {_, Storage, _, _} <- ets:lookup(Tab, {storage, Slot}), is_process_alive(Storage)] of
        [{_, Storage, Generation, Node}] ->
            %% The rollbacks of the StorageServer that the harness kills.
            case eta_observe:read(Storage) of
                #{rollbacks := Rollbacks, copies := Copies} ->
                    bump(rollbacks, Rollbacks),
                    bump(storage_copies, Copies);
                _ ->
                    ok
            end,
            S1 = kill(Node, kills_storage, S),
            case Count > 1 andalso R div Count rem 2 =:= 1 of
                true ->
                    %% In a run with several StorageServers, 1 kill in 2
                    %% leaves the place without a StorageServer until the
                    %% next `recover' operation.
                    true = ets:insert(Tab, {{storage_down, Slot}, Generation}),
                    S1;
                false ->
                    %% An operation process starts the new StorageServer,
                    %% because a process starts only in a step of the
                    %% scheduler.
                    spawn_op(redwall_dst_client:storage_node({Slot, N}), fun() -> start_storage(Tab, CState, {Mode, Window}, {Slot, N}, Generation + 1) end, S1)
            end;
        [] ->
            %% The StorageServer of the place has not started.
            S
    end;
execute({partition, _R1, _R2}, _N, S = #{partitions := false}) ->
    S;
execute({partition, R1, R2}, _N, S = #{tab := Tab, partitions := Partitions}) ->
    Nodes =
        case Partitions of
            %% A cut between two nodes with a ClusterController process
            %% that runs. A candidate then proposes itself while the
            %% leader runs.
            controllers -> lists:sort([Node || [Node, Pid] <- ets:match(Tab, {{controller, '$1'}, '$2'}), is_process_alive(Pid)]);
            _ -> [Node || [Node] <- ets:match(Tab, {{node, '$1'}, '_'})]
        end,
    case {pick(R1, Nodes), pick(R2, Nodes)} of
        {A, A} ->
            S;
        {A, B} ->
            ok = eta_net:partition(A, B),
            bump(partitions),
            S
    end;
execute(heal, _N, S) ->
    ok = eta_net:heal_all(),
    S.

check(#{tab := Tab, cstate := CState}) ->
    Storages = storages(Tab),
    Acks = [{C, Epoch, LSN} || [C, Epoch, LSN] <- ets:match(Tab, {{ack, '$1'}, '$2', '$3'})],
    Logs = logs(Tab),
    first_violation([
        fun() -> no_loss(Tab) end,
        fun() -> unique_versions(Tab, Acks) end,
        fun() -> durable(Acks, CState) end,
        fun() -> body_available(Acks, Logs) end,
        fun() -> copied(Tab, Acks, CState, Logs) end,
        fun() -> transactions(Tab, Acks, Logs) end,
        fun() -> storage_rollback(Tab, Storages, CState) end,
        fun() -> kcv_below_dv(Logs) end,
        fun() -> chain(Logs) end,
        fun() -> one_leader(Tab) end,
        fun() -> select_agrees(Tab) end,
        fun() -> mapped_range_agrees(Tab) end,
        fun() -> own_writes(Tab) end,
        fun() -> coordinators_agree(Tab) end
    ]).

%% A `select/4' of a client returned the result of the match spec on the
%% result of `get_range/3' at the same read version.
select_agrees(Tab) ->
    case ets:match(Tab, {{select_differs, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(select_agrees, Detail)
    end.

%% No two servers of the peace cluster have two states of the coordinated
%% state for one index of the log of peace. See `redwall_dst_machine'.
coordinators_agree(Tab) ->
    case ets:match(Tab, {{cstate_differs, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(coordinators_agree, Detail)
    end.

%% The reads of a transaction returned its own sets and clears, and
%% `on_error/2' left a transaction without a read version and without a
%% mutation.
own_writes(Tab) ->
    case ets:match(Tab, {{own_writes_differ, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(own_writes, Detail)
    end.

%% A `get_mapped_range/4' of a client returned the rows of `get_range/3'
%% at the same read version, each with the result of its second read.
mapped_range_agrees(Tab) ->
    case ets:match(Tab, {{mapped_differs, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(mapped_range_agrees, Detail)
    end.

%% The ClusterController processes that run and are the leader, each with
%% its term and its information about the database.
leaders(Tab) ->
    [
        {Term, Pid, Info}
     || [Pid] <- lists:sort(ets:match(Tab, {{controller, '_'}, '$1'})),
        is_process_alive(Pid),
        #{role := leader, term := Term, db_info := Info} <- [eta_observe:read(Pid)]
    ].

%% No two ClusterController processes are the leader of one term. Two
%% leaders of two terms can run at the same time. The harness counts the
%% runs with that state.
one_leader(Tab) ->
    Terms = [Term || {Term, _Pid, _Info} <- leaders(Tab)],
    _ = length(Terms) > 1 andalso ets:insert(Tab, {two_leaders, true}),
    case {Terms -- lists:usort(Terms), ets:match(Tab, {{second_leader, '$1'}, '_'})} of
        {[], []} -> ok;
        {[Term | _], _} -> violation(one_leader, #{term => Term});
        {_, [[Term] | _]} -> violation(one_leader, #{term => Term})
    end.

%% The checks at the end of a run with ClusterControllers. Before the end,
%% the harness has removed every cut and has replaced every killed worker
%% node (`repair/2').
%%
%% <ul>
%% <li>`leader' - the ClusterController of the coordinated state runs and
%%   is the leader of the term of the coordinated state.</li>
%% <li>`stale_leader' - no other process is a leader.</li>
%% <li>`epoch' - the leader has an epoch, which is the current epoch of
%%   the coordinated state, and the process of the epoch runs.</li>
%% <li>`worker_db_info' - each worker that runs has the information of
%%   the leader about the database.</li>
%% </ul>
check_final(_Settled, #{tab := Tab, cstate := CState, controller := true, last_op := Last}) when is_integer(Last) ->
    {Term, Controller} = maps:get(controller, cstate(CState), {0, undefined}),
    Leaders = leaders(Tab),
    Workers = [
        {Node, eta_observe:read(Pid)}
     || [Node, Pid] <- lists:sort(ets:match(Tab, {{worker, '$1'}, '$2', '_'})), is_process_alive(Pid)
    ],
    case lists:keyfind(Controller, 2, Leaders) of
        {Term, Controller, Info} ->
            Epoch =
                case current(CState) of
                    #{epoch := E} -> E;
                    undefined -> 0
                end,
            Alive = fun(Pid) -> is_pid(Pid) andalso is_process_alive(Pid) end,
            first_violation([
                fun() ->
                    case [T || {T, Pid, _} <- Leaders, Pid =/= Controller] of
                        [] -> ok;
                        Stale -> violation(stale_leader, #{terms => Stale, term => Term})
                    end
                end,
                fun() ->
                    case Info of
                        #{epoch := Epoch, proxies := [Proxy]} ->
                            case Alive(Proxy) of
                                true -> ok;
                                false -> violation(epoch, #{epoch => Epoch, reason => role_down})
                            end;
                        #{epoch := Other} ->
                            violation(epoch, #{epoch => Other, current => Epoch})
                    end
                end,
                fun() ->
                    case [Node || {Node, Observed} <- Workers, not is_map(Observed) orelse maps:get(db_info, Observed, undefined) =/= Info] of
                        [] -> ok;
                        Nodes -> violation(worker_db_info, #{nodes => Nodes, workers => length(Workers)})
                    end
                end
            ]);
        _ ->
            violation(leader, #{term => Term, leaders => [T || {T, _, _} <- Leaders]})
    end;
check_final(_Settled, _State) ->
    ok.

%% Ends a run with ClusterControllers when the checks of `check_final/2'
%% hold. Such a run has a large number of settle steps, because a call
%% that was in progress at the last operation can wait for its time
%% limit.
settle_done(State = #{controller := true, last_op := Last}) when is_integer(Last) ->
    check_final(settle_done, State) =:= ok;
settle_done(_State) ->
    false.

terminate(#{tab := Tab, cstate := CState, ops := Ops}) ->
    ok = record_stats(Tab),
    lists:foreach(
        fun(Storage) ->
            case eta_observe:read(Storage) of
                #{rollbacks := Rollbacks, copies := Copies} ->
                    bump(rollbacks, Rollbacks),
                    bump(storage_copies, Copies);
                _ ->
                    ok
            end
        end,
        storages(Tab)
    ),
    %% The ClusterControllers and the workers are killed first, so that
    %% no ClusterController recruits after this point.
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{controller, '_'}, '$1'})]),
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{worker, '_'}, '$1', '_'})]),
    Started =
        [Pid || [Pid] <- ets:match(Tab, {{sequencer, '_'}, '$1'})] ++
            [Pid || [Pid] <- ets:match(Tab, {{resolver, '_'}, '$1'})] ++
            [Pid || [Pid, _] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'})],
    %% The Sequencers are killed first, so that no recovery recruits a
    %% LogServer after the harness has read the table of the run.
    ok = kill_all(Started ++ Ops),
    ok = kill_all([Pid || {_Id, Pid} <- log_pids(Tab)]),
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{storage_started, '_'}, '$1'})] ++ [Pid || {local, Pid} <- [CState]]),
    ok = redwall_dst_machine:clear(),
    _ = [clear_dir(disk_dir()) || {peace, _Tab, _Ids} <- [CState]],
    catch ets:delete(Tab),
    ok.

%% ---------------------------------------------------------------------------
%% Statistics
%% ---------------------------------------------------------------------------

%% The sums over the runs since `reset_stats/0'. A test uses the sums to
%% show that the runs reached the states that the test is about.
stats() ->
    case ets:whereis(?STATS) of
        undefined -> #{};
        _ -> maps:from_list(ets:tab2list(?STATS))
    end.

reset_stats() ->
    case ets:whereis(?STATS) of
        undefined ->
            Owner = spawn(fun() ->
                receive
                    stop -> ok
                end
            end),
            ?STATS = ets:new(?STATS, [named_table, public, {heir, Owner, none}]),
            ok;
        _ ->
            true = ets:delete_all_objects(?STATS),
            ok
    end.

bump(Key) ->
    bump(Key, 1).

bump(Key, N) ->
    case ets:whereis(?STATS) of
        undefined -> ok;
        _ -> _ = ets:update_counter(?STATS, Key, N, {Key, 0})
    end,
    ok.

record_stats(Tab) ->
    Epochs = [Epoch || [Epoch] <- ets:match(Tab, {{recovered, '_'}, '$1', '_', '_'})],
    bump(runs),
    bump(selects, case ets:lookup(Tab, selects) of [{selects, Selects}] -> Selects; [] -> 0 end),
    bump(mapped_reads, case ets:lookup(Tab, mapped_reads) of [{mapped_reads, Mapped}] -> Mapped; [] -> 0 end),
    [
        bump(Counter, case ets:lookup(Tab, Counter) of [{Counter, Count}] -> Count; [] -> 0 end)
     || Counter <- [own_mapped_reads, own_write_checks, on_errors]
    ],
    bump(runs_with_two_leaders, length(ets:lookup(Tab, two_leaders))),
    bump(leaders, length(ets:match(Tab, {{leader, '_'}, '_'}))),
    bump(role_failures, length(ets:match(Tab, {{role_failed, '_'}, '_'}))),
    bump(superseded, length(ets:match(Tab, {{superseded, '_'}, '_'}))),
    bump(acks, length(ets:match(Tab, {{ack, '_'}, '_', '_'}))),
    bump(unknown, length(ets:match(Tab, {{unknown, '_'}, '_'}))),
    bump(not_committed, length(ets:match(Tab, {{rejected, '_'}, not_committed}))),
    bump(too_old, length(ets:match(Tab, {{rejected, '_'}, transaction_too_old}))),
    bump(no_read_version, length(ets:match(Tab, {{no_read_version, '_'}, '_'}))),
    bump(no_read, length(ets:match(Tab, {{no_read, '_'}, '_'}))),
    %% The reads below the MVCC window of the StorageServer.
    bump(read_too_old, length(ets:match(Tab, {{read_too_old, '_'}, '_'}))),
    %% The acknowledged transactions that have their LSN together with
    %% another acknowledged transaction.
    LSNs = [LSN || [LSN] <- ets:match(Tab, {{ack, '_'}, '_', '$1'})],
    Repeated = LSNs -- lists:usort(LSNs),
    bump(batched, length([LSN || LSN <- LSNs, lists:member(LSN, Repeated)])),
    bump(reads, length([x || [#{values := [_ | _]}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    %% The transactions with reads from a StorageServer that the harness
    %% started after a kill.
    bump(reads_after_restart, length([x || [#{values := [_ | _], storage := G}] <- ets:match(Tab, {{read, '_'}, '$1'}), G > 0])),
    %% The transactions with reads at a time when a StorageServer of the
    %% run was not alive, in a run with several StorageServers.
    bump(reads_with_one_down, length([x || [#{values := [_ | _], down := {Down, Count}}] <- ets:match(Tab, {{read, '_'}, '$1'}), Down > 0, Count > 1])),
    bump(reads_by_message, length([x || [#{values := [_ | _], direct := false}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    %% The transactions with reads of a client on the node of a
    %% StorageServer, with direct reads.
    bump(reads_on_storage_node, length([x || [#{values := [_ | _], direct := true, local := true}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    bump(storages, length(ets:match(Tab, {{storage, '_'}, '_', '_', '_'}))),
    %% The acknowledged transactions that cleared a key.
    bump(clears, length([x || [#{clears := [_ | _]}] <- ets:match(Tab, {{txn, '_'}, '$1'})])),
    bump(range_reads, length([x || [#{ranges := [_ | _]}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    %% The acknowledged transactions with a read range.
    bump(range_acks, length([x || [#{ranges := [_ | _]}] <- ets:match(Tab, {{txn, '_'}, '$1'})])),
    bump(recoveries, length(Epochs)),
    bump(recoveries_failed, length(ets:match(Tab, {{recovery_failed, '_'}, '_'}))),
    %% The number of failed recoveries for each kind of reason.
    lists:foreach(
        fun
            ([{exit, Reason}]) when is_tuple(Reason) -> bump({recovery_failed, exit, element(1, Reason)});
            ([{exit, Reason}]) -> bump({recovery_failed, exit, Reason});
            ([Reason]) when is_tuple(Reason) -> bump({recovery_failed, element(1, Reason)});
            ([Reason]) -> bump({recovery_failed, Reason})
        end,
        ets:match(Tab, {{recovery_failed, '_'}, '$1'})
    ),
    bump(runs_with_three_epochs, length([x || lists:max([0 | Epochs]) >= 3])),
    ok.

%% ---------------------------------------------------------------------------
%% Invariants
%% ---------------------------------------------------------------------------

no_loss(Tab) ->
    case [Reason || [Reason = {missing_body, _}] <- ets:match(Tab, {{recovery_failed, '_'}, '$1'})] of
        [] -> ok;
        [Reason | _] -> violation(no_loss, #{reason => Reason})
    end.

unique_versions(Tab, Acks) ->
    ByLSN = lists:foldl(
        fun({C, Epoch, LSN}, Acc) ->
            [{_, #{index := Index}}] = ets:lookup(Tab, {txn, C}),
            maps:update_with(LSN, fun(L) -> [{Epoch, Index, C} | L] end, [{Epoch, Index, C}], Acc)
        end,
        #{},
        Acks
    ),
    %% The acknowledgements of one LSN have one epoch, and each has its own
    %% index.
    Same = fun(Who) ->
        length(lists:usort([Epoch || {Epoch, _Index, _C} <- Who])) > 1 orelse
            length(lists:usort([Index || {_Epoch, Index, _C} <- Who])) < length(Who)
    end,
    case [{LSN, Who} || {LSN, Who = [_, _ | _]} <- maps:to_list(ByLSN), Same(Who)] of
        [] -> ok;
        [{LSN, Who} | _] -> violation(unique_versions, #{lsn => LSN, acknowledged => lists:sort(Who)})
    end.

durable(Acks, CState) ->
    Ended = maps:from_list([{Epoch, End} || #{epoch := Epoch, end_version := End} <- maps:get(old, cstate(CState), [])]),
    case [{C, Epoch, LSN, maps:get(Epoch, Ended)} || {C, Epoch, LSN} <- Acks, is_map_key(Epoch, Ended), LSN > maps:get(Epoch, Ended)] of
        [] ->
            ok;
        [{C, Epoch, LSN, End} | _] ->
            violation(durable, #{client => C, epoch => Epoch, lsn => LSN, end_version => End})
    end.

body_available(Acks, Logs) ->
    Missing = [
        {C, Epoch, LSN}
     || {C, Epoch, LSN} <- Acks,
        not lists:any(
            fun
                ({{E, _Index}, #{records := Records}}) when E =:= Epoch ->
                    has_body(LSN, C, Records);
                (_) ->
                    false
            end,
            Logs
        )
    ],
    case Missing of
        [] -> ok;
        [{C, Epoch, LSN} | _] -> violation(body_available, #{client => C, epoch => Epoch, lsn => LSN})
    end.

%% `Successors' maps an ended epoch to the epoch that ended it, from the
%% order of the log systems in the coordinated state. The table of the run
%% has the PEV and the RV of each recovery that ended.
copied(Tab, Acks, CState, Logs) ->
    #{current := Current, old := Old} = maps:merge(#{current => undefined, old => []}, cstate(CState)),
    Chain = [Epoch || #{epoch := Epoch} <- [Current | Old], Current =/= undefined],
    Successors = maps:from_list(lists:zip(tl(Chain ++ [none]), Chain)),
    Ranges = maps:from_list([{Epoch, {PEV, RV}} || [Epoch, PEV, RV] <- ets:match(Tab, {{recovered, '_'}, '$1', '$2', '$3'})]),
    Missing = [
        {C, Epoch, LSN, Id}
     || {C, Epoch, LSN} <- Acks,
        is_map_key(Epoch, Successors),
        New <- [maps:get(Epoch, Successors)],
        is_map_key(New, Ranges),
        {PEV, RV} <- [maps:get(New, Ranges)],
        LSN > PEV,
        LSN =< RV,
        {Id = {E, _Index}, #{records := Records}} <- Logs,
        E =:= New,
        not has_body(LSN, C, Records)
    ],
    case Missing of
        [] -> ok;
        [{C, Epoch, LSN, Id} | _] -> violation(copied, #{client => C, epoch => Epoch, lsn => LSN, log => Id})
    end.

%% The record `LSN' has a body with an entry of the client `C'.
has_body(LSN, C, Records) ->
    case gb_trees:lookup(LSN, Records) of
        {value, {_Prev, {body, {batch, Entries}}}} -> 
            lists:any(
                fun
                    (Entry) when is_list(Entry) -> lists:any(fun(Mutation) -> match_client(Mutation, C) end, Entry);
                    (_Other) -> false
                end,
                Entries
            );
        _ -> false
    end.

match_client({set, _Key, {client, Writer}}, C) -> Writer =:= C;
match_client(_Mutation, _C) -> false.

%% `no_conflict' and `read_version_order'. `no_conflict' compares every
%% pair of acknowledged transactions. `read_version_order' compares every
%% read version with every acknowledged transaction. The harness checks
%% them only when the number of acknowledged transactions or the number of
%% read versions has changed.
transactions(Tab, Acks, Logs) ->
    Reads = [Read#{client => C} || [C, Read] <- ets:match(Tab, {{read, '$1'}, '$2'})],
    Rejected = [C || [C] <- ets:match(Tab, {{rejected, '$1'}, '_'})],
    Count = {length(Acks), length(Reads), length([x || #{values := _} <- Reads]), length(Rejected)},
    case ets:lookup(Tab, transactions_checked) of
        [{_, Count}] ->
            ok;
        _ ->
            Txns = [
                Txn#{client => C, epoch => Epoch, version => LSN}
             || {C, Epoch, LSN} <- Acks, [{_, Txn}] <- [ets:lookup(Tab, {txn, C})]
            ],
            case first_violation([fun() -> no_conflict(Txns) end, fun() -> read_version_order(Reads, Txns) end, fun() -> read_value(Reads, Txns, open_clears(Tab, Acks, Rejected), written(Logs)) end]) of
                ok ->
                    true = ets:insert(Tab, {transactions_checked, Count}),
                    ok;
                Violation ->
                    Violation
            end
    end.

no_conflict(Txns) ->
    Conflicts = [
        {T, W}
     || T = #{read_version := ReadVersion, version := Version, index := Index, reads := Reads} <- Txns,
        W = #{version := Written, index := WriterIndex, writes := Writes, clears := Clears} <- Txns,
        Written > ReadVersion,
        %% An earlier version, or an earlier transaction of the batch.
        {Written, WriterIndex} < {Version, Index},
        %% A key that the reader read and the writer wrote or cleared.
        [Key || Key <- Writes ++ Clears, lists:member(Key, Reads)] =/= []
    ],
    case Conflicts of
        [] ->
            ok;
        [{T, W} | _] ->
            violation(no_conflict, #{
                reader => maps:with([client, epoch, read_version, version, index, reads], T),
                writer => maps:with([client, epoch, version, index, writes, clears], W)
            })
    end.

%% `Reads' are the read versions that the clients received, also of the
%% transactions that did not commit.
read_version_order(Reads, Txns) ->
    Stale = [
        {T, A}
     || T = #{read_version := ReadVersion, start := Start} <- Reads,
        A = #{version := Version, 'end' := End} <- Txns,
        End < Start,
        ReadVersion < Version
    ],
    case Stale of
        [] ->
            ok;
        [{T, A} | _] ->
            violation(read_version_order, #{
                reader => maps:with([client, reader_epoch, read_version], T),
                acknowledged => maps:with([client, epoch, version], A)
            })
    end.

%% The keys that a transaction without a known result cleared: the cleared
%% keys of every client that started a commit and received neither an
%% acknowledgement nor a rejection. Such a clear can be applied.
open_clears(Tab, Acks, Rejected) ->
    Done = [C || {C, _Epoch, _LSN} <- Acks] ++ Rejected,
    lists:usort(lists:append([Clears || [C, Clears] <- ets:match(Tab, {{attempt, '$1'}, '$2'}), not lists:member(C, Done)])).

%% The LSN of the record of each client, from the records of the
%% LogServers that run. A client commits one transaction, and every write
%% of the transaction has the value `{client, C}'.
written(Logs) ->
    maps:from_list([
        {C, LSN}
     || {_Id, #{records := Records}} <- Logs,
        {LSN, {_Prev, {body, {batch, Entries}}}} <- gb_trees:to_list(Records),
        Entry <- Entries,
        is_list(Entry),
        {set, _Key, {client, C}} <- Entry
    ]).

%% `Reads' have the values that the clients read through the client
%% library: `{Key, not_found}' or `{Key, {Value, unknown}}'. The value of a
%% write is `{client, C}'. The LSN of the record that wrote the value is
%% the commit version of the client `C' if `C' received an
%% acknowledgement, and else the LSN of `Written'. `OpenClears' are the
%% keys of `open_clears/3'.
read_value(Reads, Txns, OpenClears, Written) ->
    ByClient0 = maps:from_list([{C, T} || T = #{client := C} <- Txns]),
    ByClient = {ByClient0, Written},
    Wrong = [
        {R, Key, Found, Reason}
     || R = #{read_version := ReadVersion, values := Values} <- Reads,
        {Key, Found} <- Values,
        Reason <- wrong_value(Key, Found, ReadVersion, ByClient, Txns, OpenClears)
    ],
    case Wrong of
        [] ->
            ok;
        [{R, Key, Found, Reason} | _] ->
            violation(read_value, #{
                reader => maps:with([client, reader_epoch, read_version], R),
                key => Key,
                found => Found,
                reason => Reason
            })
    end.

%% The reasons why the value `Found' is wrong. The empty list means that
%% the value is correct.
wrong_value(Key, not_found, ReadVersion, _ByClient, Txns, OpenClears) ->
    %% The key has no value if the last acknowledged transaction that
    %% changed the key cleared it, or if a transaction without a known
    %% result cleared the key.
    case lists:sort([{{Version, Index}, T} || T = #{version := Version, index := Index} <- changers(Key, 0, ReadVersion, Txns)]) of
        [] ->
            [];
        Changers ->
            {_Order, Last = #{clears := Clears}} = lists:last(Changers),
            [{missed, maps:with([client, epoch, version], Last)} || not lists:member(Key, Clears), not lists:member(Key, OpenClears)]
    end;
wrong_value(Key, {{client, Writer}, unknown}, ReadVersion, {ByClient, Logged}, Txns, OpenClears) ->
    case {ByClient, Logged} of
        {#{Writer := #{version := Written}}, _} ->
            wrong_value(Key, {{client, Writer}, Written}, ReadVersion, ByClient, Txns, OpenClears);
        {_, #{Writer := Written}} ->
            wrong_value(Key, {{client, Writer}, Written}, ReadVersion, ByClient, Txns, OpenClears);
        _NoRecordOnALogServerThatRuns ->
            []
    end;
wrong_value(Key, {{client, Writer}, Written}, ReadVersion, ByClient, Txns, _OpenClears) ->
    [above_read_version || Written > ReadVersion] ++
        case ByClient of
            #{Writer := #{version := Version, writes := Writes}} ->
                [{writer_committed_at, Version} || Version =/= Written] ++ [writer_did_not_write_key || not lists:member(Key, Writes)];
            _ ->
                []
        end ++
        [{missed, maps:with([client, epoch, version], T)} || T <- changers(Key, Written, ReadVersion, Txns)] ++
        %% An acknowledged transaction of the same batch, after the writer.
        [
            {missed_in_batch, maps:with([client, epoch, version, index], T)}
         || #{Writer := #{version := Version, index := Index}} <- [ByClient],
            Version =:= Written,
            T = #{index := Later} <- changers(Key, Written - 1, Written, Txns),
            Later > Index
        ].

%% The acknowledged transactions that wrote or cleared `Key' at a commit
%% version above `After' and at or below `UpTo'.
changers(Key, After, UpTo, Txns) ->
    [
        T
     || T = #{version := Version, writes := Writes, clears := Clears} <- Txns,
        Version > After,
        Version =< UpTo,
        lists:member(Key, Writes) orelse lists:member(Key, Clears)
    ].

%% The coordinated state has the end version of each ended epoch. The
%% first version of the next epoch is the end version plus the gap.
storage_rollback(Tab, Storages, CState) ->
    %% The checks read every write of each StorageServer, so the harness
    %% performs them only when a StorageServer or the coordinated state
    %% has changed.
    Observed = [observe(Storage) || Storage <- Storages],
    Old = maps:get(old, cstate(CState), []),
    Key = erlang:phash2({Observed, Old}),
    case ets:lookup(Tab, storage_checked) of
        [{_, Key}] ->
            ok;
        _ ->
            Checks =
                [fun() -> storage_rollback(One, Old) end || One <- Observed] ++
                    [fun() -> replicas_agree(A, B, Old) end || {I, A} <- lists:enumerate(Observed), {J, B} <- lists:enumerate(Observed), I < J],
            case first_violation(Checks) of
                ok ->
                    true = ets:insert(Tab, {storage_checked, Key}),
                    ok;
                Violation ->
                    Violation
            end
    end.

%% The published state of a StorageServer, with the rows of the table of
%% the data. The table does not exist after the kill of the
%% StorageServer.
observe(Storage) ->
    case eta_observe:read(Storage) of
        Published = #{data := _Table, history := _History} ->
            try
                Published#{data := redwall_storage:versions(Published)}
            catch
                error:badarg -> no_table
            end;
        Other ->
            Other
    end.

%% Two StorageServers have the same writes for each key, for the versions
%% above the oldest version of both, up to the lower one of the two
%% versions. A write that a recovery discarded is not compared.
replicas_agree(#{version := VersionA, oldest := OldestA, data := DataA}, #{version := VersionB, oldest := OldestB, data := DataB}, Old) ->
    Low = max(OldestA, OldestB),
    High = min(VersionA, VersionB),
    Ends = [End || #{end_version := End} <- Old, End > 0],
    Compared = fun(Data) ->
        maps:filtermap(
            fun(_Key, Versions) ->
                case [Write || Write = {Written, _Change} <- Versions, Written > Low, Written =< High, not lists:any(fun(End) -> Written > End andalso Written < End + ?EPOCH_GAP end, Ends)] of
                    [] -> false;
                    Writes -> {true, Writes}
                end
            end,
            Data
        )
    end,
    {A, B} = {Compared(DataA), Compared(DataB)},
    case A =:= B of
        true ->
            ok;
        false ->
            Keys = [Key || Key <- lists:usort(maps:keys(A) ++ maps:keys(B)), maps:get(Key, A, []) =/= maps:get(Key, B, [])],
            Key = hd(Keys),
            violation(replicas_agree, #{key => Key, first => maps:get(Key, A, []), second => maps:get(Key, B, []), above => Low, up_to => High})
    end;
replicas_agree(_NoEventYetOrNoTable, _Other, _Old) ->
    ok.

storage_rollback(Observed, Old) ->
    case Observed of
        #{version := Version, data := Data} ->
            Ends = [End || #{end_version := End} <- Old, End > 0, Version >= End + ?EPOCH_GAP],
            Kept = [
                {Key, Written, End}
             || End <- Ends,
                {Key, Versions} <- maps:to_list(Data),
                {Written, _Value} <- Versions,
                Written > End,
                Written < End + ?EPOCH_GAP
            ],
            case Kept of
                [] -> ok;
                [{Key, Written, End} | _] -> violation(storage_rollback, #{key => Key, written => Written, end_version => End, storage_version => Version})
            end;
        _NoEventYet ->
            ok
    end.

kcv_below_dv(Logs) ->
    ByEpoch = lists:foldl(
        fun({{Epoch, _Index}, #{dv := DV, kcv := KCV}}, Acc) ->
            maps:update_with(Epoch, fun({MaxKCV, MinDV}) -> {max(MaxKCV, KCV), min(MinDV, DV)} end, {KCV, DV}, Acc)
        end,
        #{},
        Logs
    ),
    case [{Epoch, KCV, DV} || {Epoch, {KCV, DV}} <- maps:to_list(ByEpoch), KCV > DV] of
        [] -> ok;
        [{Epoch, KCV, DV} | _] -> violation(kcv_below_dv, #{epoch => Epoch, kcv => KCV, dv => DV})
    end.

chain(Logs) ->
    case [Id || {Id, #{records := Records}} <- Logs, not chained(gb_trees:to_list(Records))] of
        [] -> ok;
        [Id | _] -> violation(chain, #{log => Id})
    end.

%% The first record of a LogServer has any previous LSN.
chained([{LSN, _} | Rest = [{_Next, {Prev, _Body}} | _]]) ->
    Prev =:= LSN andalso chained(Rest);
chained(_) ->
    true.

first_violation([]) ->
    ok;
first_violation([Check | Rest]) ->
    case Check() of
        ok -> first_violation(Rest);
        Violation -> Violation
    end.

%% Every violation has a `property' key. `eta_shrink' compares two
%% failures by this key.
violation(Property, Detail) ->
    {violation, Detail#{property => Property}}.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

spawn_op(Node, Fun, S = #{ops := Ops}) ->
    Pid = eta_run:spawn_op(Fun),
    ok = eta_net:place(Node, [Pid]),
    S#{ops := Ops ++ [Pid]}.

%% Element `R' of the list, counted in a circle, or `none'.
pick(_R, []) -> none;
pick(R, List) -> lists:nth(R rem length(List) + 1, List).

%% Starts the coordinator `Id' on its node.
start_coordinator(Id = {_Name, Node}, S = #{mode := Mode}) ->
    Dir = disk_dir(),
    spawn_op(Node, fun() -> redwall_dst_client:coordinator(Id, Dir, Mode) end, S).

%% Starts each killed coordinator again.
restart_coordinators(S = #{tab := Tab, cstate := {peace, Tab, Ids}}) ->
    lists:foldl(
        fun(Id, Acc) ->
            case ets:take(Tab, {coordinator_down, Id}) of
                [_Row] ->
                    bump(restarts_coordinator),
                    start_coordinator(Id, Acc);
                [] ->
                    Acc
            end
        end,
        S,
        Ids
    );
restart_coordinators(S) ->
    S.

%% The directory of the data of the coordinators. Every run uses this one
%% directory, so that the name of the directory is the same in every run.
%% The harness empties the directory before and after each run. The
%% environment variable `REDWALL_DST_DISK_DIR' sets the directory.
%% `dst/run.sh' gives each test a directory of its own, because the tests
%% run at the same time.
disk_dir() ->
    case os:getenv("REDWALL_DST_DISK_DIR") of
        false -> filename:join(filename:basedir(user_cache, "redwall_dst"), "disk");
        Dir -> Dir
    end.

%% Deletes the files and the directories of a directory. The function
%% uses `prim_file', because the file server is a process that no schedule
%% controls.
clear_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Names} ->
            lists:foreach(
                fun(Name) ->
                    Path = filename:join(Dir, Name),
                    case prim_file:delete(Path) of
                        ok ->
                            ok;
                        {error, _} ->
                            ok = clear_dir(Path),
                            ok = prim_file:del_dir(Path)
                    end
                end,
                Names
            );
        {error, enoent} ->
            ok
    end.

%% Reads the coordinated state that the process publishes. The read sends
%% no message to the process.
cstate({local, CState}) ->
    case eta_observe:read(CState) of
        {State, _Cas} when is_map(State) -> State;
        _ -> #{}
    end;
%% The state with the highest index that a server of the peace cluster has
%% applied. See `redwall_dst_machine'.
cstate({peace, Tab, _Ids}) ->
    case ets:lookup(Tab, cstate) of
        [{cstate, _Index, State}] -> State;
        [] -> #{}
    end.

current(CState) ->
    maps:get(current, cstate(CState), undefined).

%% The LogServers that run, from the table of the run.
log_pids(Tab) ->
    lists:sort([{Id, Pid} || [Id, Pid] <- ets:match(Tab, {{log, '$1'}, '$2'}), is_process_alive(Pid)]).

%% The published state of each LogServer that runs and has handled at least
%% one event.
logs(Tab) ->
    [{Id, State#{records := Tree}} || {Id, Pid} <- log_pids(Tab), State <- [eta_observe:read(Pid)], is_map(State), is_map_key(records, State), Tree <- records(State)].

%% The records of a LogServer as a tree from the LSN to the previous LSN
%% and the body. The LogServer keeps the records in an ETS table. The
%% result is the empty list if the table does not exist.
records(#{records := Table}) ->
    try ets:tab2list(Table) of
        Rows -> [gb_trees:from_orddict([{LSN, {Prev, Body}} || {LSN, Prev, Body} <- Rows])]
    catch
        error:badarg -> []
    end.

%% Kills a worker node, if the kill leaves every epoch with at most one
%% killed LogServer. An epoch keeps a record on 2 of its 3 LogServers, so
%% a second kill can remove an acknowledged record.
kill_worker(Node, Role, S = #{tab := Tab}) ->
    Epochs = lists:usort([Epoch || [{Epoch, _Index}, Pid] <- ets:match(Tab, {{log, '$1'}, '$2'}), [] =/= ets:match(Tab, {{at, Pid}, Node, log})]),
    case lists:any(fun(Epoch) -> ets:member(Tab, {killed_log, Epoch}) end, Epochs) of
        true ->
            S;
        false ->
            true = ets:insert(Tab, [{{killed_log, Epoch}, true} || Epoch <- Epochs]),
            Candidate =
                case ets:lookup(Tab, {controller, Node}) of
                    [{_, Controller}] -> is_process_alive(Controller);
                    [] -> false
                end,
            _ = Candidate andalso bump(kills_controller),
            [{_, _Worker, Class}] = ets:lookup(Tab, {worker, Node}),
            true = ets:insert(Tab, {{worker_down, Node}, {Class, Candidate}}),
            bump(kills_worker),
            kill(Node, list_to_atom("kills_" ++ atom_to_list(Role)), S)
    end.

%% Kills every process on a simulated node, between steps.
kill(Node, Stat, S) ->
    ok = eta_net:kill_node(Node),
    bump(Stat),
    S.

%% Kills the processes at the end of a run and waits until each process is
%% dead. The scheduler has released every process by then, so a watcher
%% handles its `DOWN' messages at once.
kill_all(Pids) ->
    Refs = [{Pid, erlang:monitor(process, Pid)} || Pid <- Pids],
    lists:foreach(fun({Pid, _Ref}) -> exit(Pid, kill) end, Refs),
    lists:foreach(
        fun({Pid, Ref}) ->
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            end
        end,
        Refs
    ).
