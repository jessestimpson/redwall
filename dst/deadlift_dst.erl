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
%% <li>The run has the simulated network of eta. The coordinated state,
%%   each Sequencer, each Proxy and each LogServer run on a simulated node
%%   of their own. All clients share one node. The network policy of the
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
%%   another one. One client in three waits between the two steps, and
%%   then commits through the Proxy of the highest epoch that runs. The client uses the Proxy of the highest epoch in 3 of 4
%%   cases, and any Proxy that runs in the others. A Proxy can belong to an
%%   epoch that has ended. The transaction reads and writes some of 4
%%   keys.</li>
%% <li>`{kill_log, R}': kill one LogServer of the current epoch. The
%%   harness kills at most 1 LogServer of each epoch, because the design
%%   tolerates the loss of k - 1 LogServers of an epoch, and a LogServer of
%%   deadlift keeps its records in memory.</li>
%% <li>`{kill_sequencer, R}': kill one Sequencer. The Sequencer can be in
%%   the middle of a recovery.</li>
%% <li>`{kill_proxy, R}': kill one Proxy.</li>
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
%% <li>`unique_versions' - no LSN is acknowledged in two epochs or to two
%%   clients. This is `UniqueVersions' of the model.</li>
%% <li>`no_loss' - no recovery ends with `missing_body'. This is `NoLoss'
%%   of the model.</li>
%% <li>`body_available' - the body of every acknowledged record is on a
%%   LogServer of its epoch that runs.</li>
%% <li>`copied' - when a recovery ends an epoch, every LogServer of the new
%%   epoch that runs has the body of every acknowledged record of the
%%   ended epoch above the previous epoch's end version.</li>
%% <li>`no_conflict' - no acknowledged transaction read a key that another
%%   acknowledged transaction wrote at a commit version above the read
%%   version of the first transaction and below its commit version. This is
%%   the guarantee of the Resolver.</li>
%% <li>`read_version_order' - a read version is at or above the commit
%%   version of every transaction that was acknowledged before the client
%%   asked for the read version, in any epoch. The harness checks every
%%   read version that a client received, also when the commit failed. A Proxy of an ended epoch
%%   must not give a read version.</li>
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
%% <li>`no_gap' - the first commit version of an epoch is the Recovery
%%   Version plus 1. The Resolver of the new epoch then admits a
%%   transaction with a read version of the previous epoch.</li>
%% <li>`no_report' - a Proxy does not report a committed LSN to the
%%   Sequencer.</li>
%% <li>`no_confirm' - a Proxy gives a read version without the
%%   confirmation that its epoch is live.</li>
%% </ul>
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The coordinated state is one process that is never killed. The run
%%   has no peace cluster.</li>
%% <li>A killed process does not start again. A LogServer has no disk.</li>
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

%% ---------------------------------------------------------------------------
%% eta_harness
%% ---------------------------------------------------------------------------

init(_Seed, Config) ->
    Mode = maps:get(mode, Config, correct),
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
    {ok, #{
        tab => Tab,
        registry => Registry,
        cstate => CState,
        mode => Mode,
        faults => maps:get(faults, Config, true),
        %% Every process that an operation started, oldest first.
        ops => [],
        %% The number of operations that the run has executed.
        n => 0
    }}.

%% The registry, the coordinated state, and the operation processes in the
%% order of their start. The scheduler adopts the Sequencers, the Proxies
%% and the LogServers as children of the processes that start them.
processes(#{registry := Registry, cstate := CState, ops := Ops}) ->
    [Registry, CState | [Pid || Pid <- Ops, is_process_alive(Pid)]].

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
            Roll < 0.98 -> partition;
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
    Acks = [{C, Epoch, LSN} || [C, Epoch, LSN] <- ets:match(Tab, {{ack, '$1'}, '$2', '$3'})],
    Logs = logs(),
    first_violation([
        fun() -> no_loss(Tab) end,
        fun() -> unique_versions(Acks) end,
        fun() -> durable(Acks, CState) end,
        fun() -> body_available(Acks, Logs) end,
        fun() -> copied(Tab, Acks, CState, Logs) end,
        fun() -> transactions(Tab, Acks) end,
        fun() -> kcv_below_dv(Logs) end,
        fun() -> chain(Logs) end
    ]).

terminate(#{tab := Tab, registry := Registry, cstate := CState, ops := Ops}) ->
    ok = record_stats(Tab),
    Started =
        [Pid || [Pid] <- ets:match(Tab, {{sequencer, '_'}, '$1'})] ++
            [Pid || [Pid, _] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'})],
    %% The Sequencers are killed first, so that no recovery recruits a
    %% LogServer after the harness has read the registry.
    ok = kill_all(Started ++ Ops),
    ok = kill_all([Pid || {_Id, Pid} <- log_pids()]),
    ok = kill_all([CState, Registry]),
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

unique_versions(Acks) ->
    ByLSN = lists:foldl(
        fun({C, Epoch, LSN}, Acc) -> maps:update_with(LSN, fun(L) -> [{Epoch, C} | L] end, [{Epoch, C}], Acc) end,
        #{},
        Acks
    ),
    case [{LSN, Who} || {LSN, Who = [_, _ | _]} <- maps:to_list(ByLSN)] of
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
                    case gb_trees:lookup(LSN, Records) of
                        {value, {_Prev, {body, [{_Key, {client, C}} | _]}}} -> true;
                        _ -> false
                    end;
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

has_body(LSN, C, Records) ->
    case gb_trees:lookup(LSN, Records) of
        {value, {_Prev, {body, [{_Key, {client, C}} | _]}}} -> true;
        _ -> false
    end.

%% `no_conflict' and `read_version_order'. `no_conflict' compares every
%% pair of acknowledged transactions. `read_version_order' compares every
%% read version with every acknowledged transaction. The harness checks
%% them only when the number of acknowledged transactions or the number of
%% read versions has changed.
transactions(Tab, Acks) ->
    Reads = [Read#{client => C} || [C, Read] <- ets:match(Tab, {{read, '$1'}, '$2'})],
    Count = {length(Acks), length(Reads)},
    case ets:lookup(Tab, transactions_checked) of
        [{_, Count}] ->
            ok;
        _ ->
            Txns = [
                Txn#{client => C, epoch => Epoch, version => LSN}
             || {C, Epoch, LSN} <- Acks, [{_, Txn}] <- [ets:lookup(Tab, {txn, C})]
            ],
            case first_violation([fun() -> no_conflict(Txns) end, fun() -> read_version_order(Reads, Txns) end]) of
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
     || T = #{read_version := ReadVersion, version := Version, reads := Reads} <- Txns,
        W = #{version := Written, writes := Writes} <- Txns,
        Written > ReadVersion,
        Written < Version,
        Writes -- (Writes -- Reads) =/= []
    ],
    case Conflicts of
        [] ->
            ok;
        [{T, W} | _] ->
            violation(no_conflict, #{
                reader => maps:with([client, epoch, read_version, version, reads], T),
                writer => maps:with([client, epoch, version, writes], W)
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
