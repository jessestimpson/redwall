%% This module is the eta harness for the transaction system of deadlift.
%% The harness starts the system, runs a workload of commits, recoveries and
%% kills against it, and checks invariants after every step.
%%
%% == Simulated system ==
%%
%% <ul>
%% <li>The coordinated state is one `deadlift_cstate_local' process. The
%%   run has no peace cluster. The process applies one value at a time,
%%   which is the register with atomic operations of
%%   `formal/Recovery.tla'.</li>
%% <li>The registry is one `deadlift_registry' process.</li>
%% <li>A `recover' operation starts a Sequencer. When the recovery of the
%%   Sequencer ends, the operation starts a Proxy for the new epoch. This
%%   is the work of a ClusterController, which deadlift does not have.</li>
%% <li>A recovery recruits 3 LogServers with a replication degree of
%%   2.</li>
%% <li>One StorageServer runs for the whole run. It sends each request to
%%   a LogServer again every 1 s of the virtual clock.</li>
%% <li>The run has the simulated network of eta. The coordinated state,
%%   the StorageServer, each Sequencer, each Proxy, each Resolver and each
%%   LogServer run on a simulated node of their own. All clients share one node. The network policy of the
%%   run can lose or delay a message between two nodes.</li>
%% <li>The registry is on no node. Every process reads it as a local
%%   table, and no message to the registry is lost.</li>
%% </ul>
%%
%% == Workload ==
%%
%% <ul>
%% <li>`recover': start a Sequencer and, after its recovery, a Proxy.</li>
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
%%   deadlift keeps its records in memory.</li>
%% <li>`{kill_sequencer, R}': kill one Sequencer. The Sequencer can be in
%%   the middle of a recovery.</li>
%% <li>`{kill_proxy, R}': kill one Proxy.</li>
%% <li>`{kill_resolver, R}': kill the Resolver of one epoch.</li>
%% <li>`kill_storage': kill the StorageServer, and start a new
%%   StorageServer without data on a new node. The new StorageServer reads
%%   every record from the LogServers of every epoch. A client that starts
%%   after the kill reads from the new StorageServer.</li>
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
%% <li>The coordinated state is one process that is never killed. The run
%%   has no peace cluster.</li>
%% <li>A killed process does not start again. A LogServer has no disk.</li>
%% <li>The run has one StorageServer at a time.</li>
%% <li>deadlift does not send a message again. A lost message to a
%%   LogServer stops the commits of the epoch until the next recovery.
%%   FoundationDB runs on TCP, and treats a broken connection as a failure
%%   of the process.</li>
%% <li>The virtual clock advances only when no process can run. The
%%   time-outs of calls and the time limit of the lock fire only then.</li>
%% </ul>
-module(deadlift_dst).

-behaviour(eta_harness).

-export([init/2, processes/1, generate/2, execute/2, check/1, terminate/1]).
-export([stats/0, reset_stats/0]).

-define(STATS, deadlift_dst_stats).

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
    %% The modules in dst/ are not part of the deadlift application, so
    %% `preload => [deadlift]' does not load them.
    {module, _} = code:ensure_loaded(deadlift_dst_client),
    Tab = ets:new(deadlift_dst, [public, ordered_set]),
    {ok, Registry} = deadlift_registry:start(),
    {ok, CState} = deadlift_cstate_local:start(#{cas => Mode =/= no_cas}),
    %% The topology is declared before any process creates a monitor.
    ok = eta_net:place(coordinators, [CState]),
    ok = eta_net:place(clients, []),
    true = ets:insert(Tab, [{{node, coordinators}, true}, {{node, clients}, true}]),
    Storage = start_storage(Tab, CState, {Mode, Window}, 0, 0),
    ok = eta_net:place(deadlift_dst_client:storage_node(0), [Storage]),
    {ok, #{
        tab => Tab,
        registry => Registry,
        cstate => CState,
        %% The first StorageServer. The table of the run has the
        %% StorageServer that runs.
        storage => Storage,
        mode => Mode,
        window => Window,
        faults => maps:get(faults, Config, true),
        %% Every process that an operation started, oldest first.
        ops => [],
        %% The number of operations that the run has executed.
        n => 0
    }}.

%% The registry, the coordinated state, and the operation processes in the
%% order of their start. The scheduler adopts the Sequencers, the Proxies
%% and the LogServers as children of the processes that start them.
processes(#{registry := Registry, cstate := CState, storage := Storage, ops := Ops}) ->
    [Registry, CState | [Pid || Pid <- [Storage | Ops], is_process_alive(Pid)]].

%% Starts a StorageServer. `N' is the number of the operation, and
%% `Generation' is the number of StorageServers before this one. A new
%% StorageServer has no data. It reads every record from the LogServers of
%% every epoch. The StorageServer runs on the node `storage_N'. After a
%% kill, an operation process on that node calls this function, and the
%% StorageServer starts on the node of that process.
start_storage(Tab, CState, {Mode, Window}, N, Generation) ->
    {ok, Storage} = deadlift_storage:start(#{
        coordinators => {local, CState},
        poll_ms => 100,
        mvcc_window => Window,
        rollback => Mode =/= no_rollback,
        check_window => Mode =/= no_window_check,
        clear => Mode =/= no_clear
    }),
    Node = deadlift_dst_client:storage_node(N),
    true = ets:insert(Tab, [{{node, Node}, true}, {{storage_started, N}, Storage}, {storage, Storage, Generation, Node}]),
    Storage.

%% The StorageServer that the table of the run has.
storage(Tab) ->
    [{storage, Storage, _Generation, _Node}] = ets:lookup(Tab, storage),
    Storage.

%% The first operation of a run is a recovery, which creates epoch 1.
generate(#{n := 0}, Rand) ->
    {recover, Rand};
generate(#{faults := Faults}, Rand0) ->
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
            Roll < 0.98 -> kill_storage;
            Roll < 0.988 -> partition;
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

execute(Op, S = #{n := N0}) ->
    execute(Op, N0 + 1, S#{n := N0 + 1}).

execute(recover, N, S = #{tab := Tab, cstate := CState, mode := Mode}) ->
    true = ets:insert(Tab, {{node, deadlift_dst_client:sequencer_node(N)}, true}),
    spawn_op(deadlift_dst_client:sequencer_node(N), fun() -> deadlift_dst_client:recover(Tab, N, CState, Mode) end, S);
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
        {Reader, Proxy} -> spawn_op(clients, fun() -> deadlift_dst_client:transaction(Tab, N, Reader, Proxy, R1 + R2) end, S)
    end;
execute({kill_log, R}, _N, S = #{tab := Tab, cstate := CState}) ->
    case current(CState) of
        #{epoch := Epoch, logs := Logs} ->
            case ets:member(Tab, {killed_log, Epoch}) of
                true ->
                    S;
                false ->
                    Id = pick(R, Logs),
                    case deadlift_log:ref(Id) of
                        undefined ->
                            S;
                        _Pid ->
                            true = ets:insert(Tab, {{killed_log, Epoch}, true}),
                            kill(deadlift_dst_client:log_node(Id), kills_log, S)
                    end
            end;
        undefined ->
            S
    end;
execute({kill_sequencer, R}, _N, S = #{tab := Tab}) ->
    case pick(R, [N || [N, Pid] <- ets:match(Tab, {{sequencer, '$1'}, '$2'}), is_process_alive(Pid)]) of
        none -> S;
        N -> kill(deadlift_dst_client:sequencer_node(N), kills_sequencer, S)
    end;
execute({kill_proxy, R}, _N, S = #{tab := Tab}) ->
    case pick(R, [N || [N, Pid] <- ets:match(Tab, {{proxy, '$1'}, '$2', '_'}), is_process_alive(Pid)]) of
        none -> S;
        N -> kill(deadlift_dst_client:proxy_node(N), kills_proxy, S)
    end;
execute({kill_resolver, R}, _N, S = #{tab := Tab}) ->
    Running = [N || [N] <- ets:match(Tab, {{recovered, '$1'}, '_', '_', '_'}), not ets:member(Tab, {killed_resolver, N})],
    case pick(R, lists:sort(Running)) of
        none ->
            S;
        N ->
            true = ets:insert(Tab, {{killed_resolver, N}, true}),
            kill(deadlift_dst_client:resolver_node(N), kills_resolver, S)
    end;
execute(kill_storage, N, S = #{tab := Tab, cstate := CState, mode := Mode, window := Window}) ->
    [{storage, Storage, Generation, Node}] = ets:lookup(Tab, storage),
    case is_process_alive(Storage) of
        true ->
            %% The rollbacks of the StorageServer that the harness kills.
            case eta_observe:read(Storage) of
                #{rollbacks := Rollbacks} -> bump(rollbacks, Rollbacks);
                _ -> ok
            end,
            S1 = kill(Node, kills_storage, S),
            %% An operation process starts the new StorageServer, because a
            %% process starts only in a step of the scheduler.
            spawn_op(deadlift_dst_client:storage_node(N), fun() -> start_storage(Tab, CState, {Mode, Window}, N, Generation + 1) end, S1);
        false ->
            %% The StorageServer after the last kill has not started.
            S
    end;
execute({partition, R1, R2}, _N, S = #{tab := Tab}) ->
    Nodes = [Node || [Node] <- ets:match(Tab, {{node, '$1'}, '_'})],
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
    Storage = storage(Tab),
    Acks = [{C, Epoch, LSN} || [C, Epoch, LSN] <- ets:match(Tab, {{ack, '$1'}, '$2', '$3'})],
    Logs = logs(),
    first_violation([
        fun() -> no_loss(Tab) end,
        fun() -> unique_versions(Tab, Acks) end,
        fun() -> durable(Acks, CState) end,
        fun() -> body_available(Acks, Logs) end,
        fun() -> copied(Tab, Acks, CState, Logs) end,
        fun() -> transactions(Tab, Acks) end,
        fun() -> storage_rollback(Tab, Storage, CState) end,
        fun() -> kcv_below_dv(Logs) end,
        fun() -> chain(Logs) end
    ]).

terminate(#{tab := Tab, registry := Registry, cstate := CState, ops := Ops}) ->
    Storage = storage(Tab),
    ok = record_stats(Tab),
    case eta_observe:read(Storage) of
        #{rollbacks := Rollbacks} -> bump(rollbacks, Rollbacks);
        _ -> ok
    end,
    Started =
        [Pid || [Pid] <- ets:match(Tab, {{sequencer, '_'}, '$1'})] ++
            [Pid || [Pid, _] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'})],
    %% The Sequencers are killed first, so that no recovery recruits a
    %% LogServer after the harness has read the registry.
    ok = kill_all(Started ++ Ops),
    ok = kill_all([Pid || {_Id, Pid} <- log_pids()]),
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{storage_started, '_'}, '$1'})] ++ [CState, Registry]),
    ok = await_unregistered(1000),
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
transactions(Tab, Acks) ->
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
            case first_violation([fun() -> no_conflict(Txns) end, fun() -> read_version_order(Reads, Txns) end, fun() -> read_value(Reads, Txns, open_clears(Tab, Acks, Rejected)) end]) of
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

%% `Reads' have the values that the clients read from the StorageServer:
%% `{Key, not_found}' or `{Key, {Value, Written}}', where `Written' is the
%% LSN of the record that wrote the value. The value of a write is
%% `{client, C}'. `OpenClears' are the keys of `open_clears/3'.
read_value(Reads, Txns, OpenClears) ->
    ByClient = maps:from_list([{C, T} || T = #{client := C} <- Txns]),
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
storage_rollback(Tab, Storage, CState) ->
    %% The check reads every write of the StorageServer, so the harness
    %% performs it only when the StorageServer or the coordinated state has
    %% changed.
    %% The published state has the ETS table of the data. The harness
    %% reads the rows of the table. The table does not exist after the
    %% kill of the StorageServer.
    Observed =
        case eta_observe:read(Storage) of
            Published = #{data := Table} ->
                try
                    Published#{data := maps:from_list(ets:tab2list(Table))}
                catch
                    error:badarg -> no_table
                end;
            Other ->
                Other
        end,
    Old = maps:get(old, cstate(CState), []),
    Key = erlang:phash2({Observed, Old}),
    case ets:lookup(Tab, storage_checked) of
        [{_, Key}] ->
            ok;
        _ ->
            case storage_rollback(Observed, Old) of
                ok ->
                    true = ets:insert(Tab, {storage_checked, Key}),
                    ok;
                Violation ->
                    Violation
            end
    end.

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

%% Reads the coordinated state that the process publishes. The read sends
%% no message to the process.
cstate(CState) ->
    case eta_observe:read(CState) of
        {State, _Cas} when is_map(State) -> State;
        _ -> #{}
    end.

current(CState) ->
    maps:get(current, cstate(CState), undefined).

%% The LogServers that run, from the table of the registry.
log_pids() ->
    case ets:whereis(deadlift_registry) of
        undefined -> [];
        _ -> lists:sort([{Id, Pid} || {{log, Id}, Pid, _Monitor} <- ets:tab2list(deadlift_registry), is_process_alive(Pid)])
    end.

%% The published state of each LogServer that runs and has handled at least
%% one event.
logs() ->
    [{Id, State} || {Id, Pid} <- log_pids(), State <- [eta_observe:read(Pid)], is_map(State), is_map_key(records, State)].

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

await_unregistered(0) ->
    {error, registry_still_registered};
await_unregistered(N) ->
    case whereis(deadlift_registry) of
        undefined ->
            ok;
        _ ->
            timer:sleep(1),
            await_unregistered(N - 1)
    end.
