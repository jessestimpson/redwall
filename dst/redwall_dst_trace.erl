%% This module implements trace validation. The module converts a simulated
%% run into a behaviour of `formal/Recovery.tla'. TLC then accepts or
%% rejects the behaviour.
%%
%% TLC checks the model exhaustively, but that check covers only the model.
%% The simulation checks the code, but only against invariants. Trace
%% validation connects the model and the code: every change that the
%% coordinated state, a LogServer or the process of an epoch makes must be
%% a step that the model allows in the state that the model has reached at
%% that point.
%%
%% == Method ==
%%
%% With the config `trace => Table', the harness calls `record/3' after
%% every step of the scheduler. `record/3' reads the published state of the
%% coordinated state, of each LogServer and of the process of each epoch,
%% and keeps the state when it differs from the state before
%% (`redwall_dst:check/1').
%%
%% `actions/1' derives the actions of the model from the changes between
%% two states, in the order of the run. It does not use a statement of the
%% code about the action that the code took.
%%
%% | Change | Actions |
%% |---|---|
%% | The recovery count of the coordinated state rises to `n' | `LockCState(n)' |
%% | The current log system of the coordinated state becomes the one of the epoch `n' | `LockLog(n, l)' for LogServers of the previous epoch that the epoch `n' stopped, then `Recruit(n)' and `WriteCState(n)' |
%% | A LogServer has a new record whose previous LSN is the Recovery Version of its epoch | `RecoveryTxn(n, R)', then `Persist' |
%% | A LogServer has another new record | `Persist(e, l, v)' |
%% | The process of an epoch has an epoch for the first time | `Accept(e)' |
%% | The last LSN of the process of an epoch rises | `Assign(e, R)' for each new LSN |
%% | The Known Committed Version of the process of an epoch rises | `Ack(e, v)' for each assigned LSN up to the new value |
%%
%% Any other change is `Invalid', which the model never allows. Examples
%% are a new record of a LogServer that no Sequencer assigned, and a new
%% record of a LogServer after a Sequencer has stopped the LogServer.
%%
%% The last action of a change carries the state that the code has after
%% the change. The trace spec (`formal/TraceRecovery.tla') requires the
%% variables of the model to equal that state:
%%
%% <ul>
%% <li>the recovery count, the current epoch and the end version of each
%%   ended epoch of the coordinated state;</li>
%% <li>the Durable Version and the Known Committed Version of the
%%   LogServer, and whether the LogServer has the body of each new
%%   record;</li>
%% <li>the last LSN, the read version and the Known Committed Version of
%%   the process of an epoch;</li>
%% <li>the Recovery Version and the previous epoch's end version of a
%%   recovery.</li>
%% </ul>
%%
%% TLC has one behaviour to find. Each action must be enabled at its
%% position. For example, `Ack' requires that every LogServer of the epoch
%% has persisted the LSN, `Persist' requires that the previous LSN of the
%% record is the Durable Version of the LogServer, and `WriteCState'
%% requires that the recovery count is the epoch of the Sequencer.
%%
%% == Epochs and LSNs ==
%%
%% The model starts with the epoch 1 in the coordinated state. The trace
%% starts at the first write of the coordinated state of the run. The
%% epoch of that write is the epoch 1 of the model.
%%
%% In the model, an LSN is the previous LSN plus 1, and the first LSN of an
%% epoch is the Recovery Version plus the constant `Gap'. In the code, an
%% LSN follows the clock. `actions/1' therefore gives each LSN of the code
%% a number of the model: the number of the previous LSN plus 1, or the
%% number of the Recovery Version plus `Gap' for a recovery transaction.
%% `Gap' is 1 plus the highest number of LSNs that an epoch of the run
%% assigned above the Recovery Version that ended it. The numbers then
%% have the order of the LSNs.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The values of the LSNs are not validated, only their order. The
%%   limit on the versions in flight is not validated: the trace spec has
%%   `LimitInFlight = FALSE'.</li>
%% <li>A `LockLog' action is placed directly before `Recruit', and not at
%%   the step in which the LogServer was stopped. The LogServers of the
%%   action are a set of LogServers that the Sequencer stopped whose
%%   Durable Versions and Known Committed Versions give the Recovery
%%   Version and the previous epoch's end version of the code. The trace
%%   does not show which answers the Sequencer had received.</li>
%% <li>The model copies each kept record to every new LogServer. The
%%   bodies of the copied records on the new LogServers are not
%%   compared.</li>
%% <li>The position of an `Ack' among the `Assign' actions of one step is
%%   chosen from the Known Committed Version that the LogServers have
%%   after the records. See `step_epoch/4'.</li>
%% <li>The requests for a read version and the StorageServers are not in
%%   the trace: the trace spec has `Reads = {}' and `Storage = FALSE'.
%%   The Resolver, the pop and the ClusterController are not in the
%%   model.</li>
%% <li>The runs have 3 LogServers for each epoch and a replication degree
%%   of 2.</li>
%% </ul>
-module(redwall_dst_trace).

-export([record/3]).
-export([states/1, actions/1, write/2, counts/1]).

%% ---------------------------------------------------------------------------
%% Recording. The harness calls `record/3' after every step.
%% ---------------------------------------------------------------------------

%% Keeps the state of the run in `Table' if it differs from the last kept
%% state. `Tab' is the table of the run.
record(Table, Tab, CState) ->
    View = view(Tab, CState),
    Last = ets:last(Table),
    case Last =/= '$end_of_table' andalso ets:lookup_element(Table, Last, 2) of
        View ->
            ok;
        _ ->
            Next =
                case Last of
                    '$end_of_table' -> 1;
                    _ -> Last + 1
                end,
            true = ets:insert(Table, {Next, View}),
            ok
    end.

view(Tab, CState) ->
    #{
        cs => view_cstate(redwall_dst:cstate(CState)),
        logs => maps:from_list([{Id, view_log(Log)} || {Id, Log} <- redwall_dst:logs(Tab)]),
        epochs => maps:from_list(
            lists:append([
                view_epoch(Pid)
             || Pid <- epoch_pids(Tab), is_process_alive(Pid)
            ])
        )
    }.

%% The processes of the epochs. The row `{sequencer, _}' has a process from
%% its start. In a run with ClusterControllers, two workers can use one
%% key for that row, so the row `{proxy, _}' of the end of the recovery is
%% read too.
epoch_pids(Tab) ->
    lists:usort([Pid || [Pid] <- ets:match(Tab, {{sequencer, '_'}, '$1'})] ++ [Pid || [Pid] <- ets:match(Tab, {{proxy, '_'}, '$1', '_'})]).

view_cstate(#{count := Count, current := Current, old := Old}) ->
    #{
        count => Count,
        current =>
            case Current of
                #{epoch := Epoch} -> Epoch;
                undefined -> 0
            end,
        ends => maps:from_list([{Epoch, End} || #{epoch := Epoch, end_version := End} <- Old])
    };
view_cstate(_NoState) ->
    #{count => 0, current => 0, ends => #{}}.

view_log(#{dv := DV, kcv := KCV, stopped_by := Stopped, records := Tree}) ->
    #{
        dv => DV,
        kcv => KCV,
        stopped_by =>
            case Stopped of
                undefined -> 0;
                _ -> Stopped
            end,
        records => #{LSN => {Prev, Body =/= empty} || {LSN, {Prev, Body}} <- gb_trees:to_list(Tree)}
    }.

%% The state of the process of an epoch, after its recovery has ended.
view_epoch(Pid) ->
    case eta_observe:read(Pid) of
        #{epoch := Epoch, last := Last, live := Live, kcv := KCV, commits := Commits} when is_integer(Epoch) ->
            [{Epoch, #{last => Last, live => Live, kcv => KCV, commits => #{LSN => element(3, Commit) || LSN := Commit <- Commits, LSN =/= 0}}}];
        _ ->
            []
    end.

%% Returns the kept states in the order of the run.
states(Table) ->
    [View || {_Seq, View} <- lists:sort(ets:tab2list(Table))].

%% ---------------------------------------------------------------------------
%% Derivation of the actions.
%% ---------------------------------------------------------------------------

-define(LOGS, 3).
-define(K, 2).
-define(MIN_REPLIES, 2).

%% Returns `{Constants, Actions}' for the states of a run, or `empty' for
%% a run without a write of the coordinated state.
%%
%% An action is a map with `act', the arguments of the action, and `chk':
%% a list of `{Variable, Arguments, Value}' that the model must have after
%% the action.
actions(States) ->
    case lists:dropwhile(fun(#{cs := #{current := Current}}) -> Current =:= 0 end, States) of
        [] ->
            empty;
        [First = #{cs := #{current := E1}} | Rest] ->
            All = [First | Rest],
            S0 = #{
                %% The epoch of the code that is the epoch 1 of the model.
                e1 => E1,
                gap => gap(All, E1),
                %% The number of the model for each LSN of the code.
                m => #{0 => 0},
                %% The LSN of the recovery transaction of each epoch.
                first => #{},
                %% The Recovery Version of each epoch, as an LSN of the code.
                rv => #{E1 => 0},
                %% For each epoch: the LogServers of the previous epoch
                %% that the epoch stopped, with the Durable Version and
                %% the Known Committed Version of each.
                locked => #{},
                %% The LSNs that the process of each epoch has assigned.
                assigned => #{},
                %% The highest LSN of each epoch with an `Ack' action, and
                %% the epochs with an `Accept' action.
                acked => #{},
                accepted => #{},
                %% The Known Committed Version of the LogServers of each
                %% epoch after each LSN. See `step_epoch/4'.
                kcv_after => kcv_after(All),
                %% The Known Committed Version of the new LogServers of
                %% each epoch: the previous epoch's end version.
                pev => pev(All),
                acts => []
            },
            Empty = #{cs => maps:get(cs, First), logs => #{}, epochs => #{}},
            S1 = step(Empty#{cs := (maps:get(cs, First))#{count := E1}}, First, S0),
            #{acts := Acts, m := M, e1 := E1, gap := Gap} = walk(First, Rest, S1),
            Actions = lists:reverse(Acts),
            Epochs = lists:max([1 | [maps:get(e, A) || A <- Actions, is_map_key(e, A)]]),
            {#{max_epoch => Epochs + 1, max_version => lists:max(maps:values(M)) + Gap + 1, gap => Gap, k => ?K, min_replies => ?MIN_REPLIES}, Actions}
    end.

walk(_Prev, [], S) ->
    S;
walk(Prev, [Next | Rest], S) ->
    walk(Next, Rest, step(Prev, Next, S)).

%% The changes between two kept states. One step of the scheduler changes
%% the state of one process.
step(Prev, Next, S0) ->
    S1 = step_cstate(maps:get(cs, Prev), maps:get(cs, Next), Next, S0),
    S2 = maps:fold(
        fun(Epoch, View, S) -> step_epoch(Epoch, maps:get(Epoch, maps:get(epochs, Prev), undefined), View, S) end,
        S1,
        maps:get(epochs, Next)
    ),
    maps:fold(
        fun(Id, View, S) -> step_log(Id, maps:get(Id, maps:get(logs, Prev), undefined), View, S) end,
        S2,
        maps:get(logs, Next)
    ).

%% The coordinated state. The epochs of one change are handled in their
%% order: the lock of an epoch, then its write.
step_cstate(Same, Same, _Next, S) ->
    S;
step_cstate(#{count := C0, current := U0, ends := Ends0}, CS = #{count := C1, current := U1, ends := Ends1}, _Next, S0) ->
    %% The epochs that wrote the coordinated state in this change.
    Ended = lists:sort(maps:keys(Ends1) -- maps:keys(Ends0)),
    Writers =
        case {U1 =:= U0, Ended} of
            {true, []} -> [];
            {false, [U0 | Others]} -> Others ++ [U1];
            _ -> invalid
        end,
    case Writers of
        invalid ->
            add(#{act => "Invalid", what => "coordinated state"}, S0);
        _ ->
            S1 = lists:foldl(
                fun(N, S) ->
                    SL =
                        case N > C0 of
                            true -> add(#{act => "LockCState", e => me(N, S), chk => [{"csCount", [], me(N, S)}]}, S);
                            false -> S
                        end,
                    case lists:member(N, Writers) of
                        true -> write_cstate(N, CS, SL);
                        false -> SL
                    end
                end,
                S0,
                lists:seq(C0, C1)
            ),
            case [N || N <- Writers, N < C0 orelse N > C1] of
                [] -> S1;
                _ -> add(#{act => "Invalid", what => "write of the coordinated state outside the recovery count"}, S1)
            end
    end.

%% The write of the epoch `N': the `LockLog' actions, `Recruit' and
%% `WriteCState'.
write_cstate(N, #{ends := Ends}, S0 = #{locked := Locked, m := M, rv := RVs, pev := PEVs}) ->
    %% The epoch that this write ended, and its end version.
    case [{O, End} || {O, End} <- maps:to_list(Ends), O < N, not lists:any(fun(Other) -> Other > O andalso Other < N end, maps:keys(Ends))] of
        [{Old, RV}] when is_map_key(RV, M) ->
            PEV = maps:get(N, PEVs, unknown),
            case replied(maps:get(N, Locked, #{}), Old, RV, PEV) of
                none ->
                    add(#{act => "Invalid", what => "no set of stopped LogServers gives the Recovery Version"}, S0);
                Replied ->
                    E = me(N, S0),
                    S1 = lists:foldl(fun(L, S) -> add(#{act => "LockLog", e => E, l => L}, S) end, S0, Replied),
                    Recruit = [{"rv", [E], maps:get(RV, M)}] ++ [{"pev", [E], maps:get(PEV, M)} || is_map_key(PEV, M)],
                    S2 = add(#{act => "Recruit", e => E, chk => Recruit}, S1),
                    S3 = add(#{act => "WriteCState", e => E, chk => [{"csCur", [], E}, {"csEnd", [me(Old, S0)], maps:get(RV, M)}]}, S2),
                    S3#{rv := RVs#{N => RV}}
            end;
        _ ->
            add(#{act => "Invalid", what => "end of the previous epoch"}, S0)
    end.

%% The largest set of the LogServers of the epoch `Old' that the Sequencer
%% stopped whose Durable Versions have the minimum `RV' and whose Known
%% Committed Versions have the maximum `PEV'. `none' without such a set of
%% at least `?MIN_REPLIES' LogServers.
replied(Locked, Old, RV, PEV) ->
    Stopped = [{L, DV, KCV} || {{Epoch, L}, {DV, KCV}} <- maps:to_list(Locked), Epoch =:= Old],
    Sets = [Set || Set <- subsets(Stopped), length(Set) >= ?MIN_REPLIES, lists:min([DV || {_, DV, _} <- Set]) =:= RV, PEV =:= unknown orelse lists:max([KCV || {_, _, KCV} <- Set]) =:= PEV],
    case lists:sort(fun(A, B) -> length(A) >= length(B) end, Sets) of
        [Set | _] -> lists:sort([L || {L, _, _} <- Set]);
        [] -> none
    end.

subsets([]) -> [[]];
subsets([X | Rest]) -> [[X | Set] || Set <- subsets(Rest)] ++ subsets(Rest).

%% The process of an epoch.
step_epoch(_Epoch, Same, Same, S) ->
    S;
step_epoch(Epoch, undefined, View, S0 = #{first := Firsts, pev := PEVs}) ->
    %% The recovery has ended. The process starts with the recovery
    %% transaction as its last LSN and with the previous epoch's end
    %% version as its Known Committed Version. It can have assigned
    %% further LSNs in the same step.
    case Firsts of
        #{Epoch := First} ->
            S1 = accept(Epoch, S0),
            Start = #{last => First, live => First, kcv => maps:get(Epoch, PEVs, 0), commits => #{}},
            S2 = S1#{assigned := maps:merge(#{Epoch => []}, maps:get(assigned, S1))},
            case View of
                #{last := First} -> check_epoch(Epoch, View, S2);
                _ -> step_epoch(Epoch, Start, View, S2)
            end;
        _ ->
            add(#{act => "Invalid", what => "an epoch accepts commits and no LogServer has its recovery transaction"}, S0)
    end;
step_epoch(Epoch, #{last := L0, kcv := K0}, View = #{last := L1, kcv := K1, commits := Commits}, S0 = #{assigned := Assigned, acked := Acked, kcv_after := After}) ->
    Had = maps:get(Epoch, Assigned, []),
    %% The new LSNs, oldest first, from the previous LSN of each one.
    New = chain(L1, L0, Commits, []),
    Acks = [V || V <- lists:sort(Had), V > K0, V > maps:get(Epoch, Acked, 0), V =< K1],
    case New of
        invalid ->
            add(#{act => "Invalid", what => "the last LSN of an epoch has no chain to the LSN before"}, S0);
        _ ->
            %% The Known Committed Version that the record of an LSN
            %% carries is the Known Committed Version of the process at
            %% the time of the `Assign'. The LogServers have it after the
            %% record. An `Ack' of this change is before the `Assign' of
            %% an LSN if the LogServers have a Known Committed Version at
            %% or above the acknowledged LSN after the next LSN for which
            %% a LogServer was observed.
            {Left, S1} = lists:foldl(
                fun({Prev, V}, {Pending, S}) ->
                    Target = target(Epoch, V, After),
                    {Before, Later} = lists:partition(fun(A) -> Target =:= unknown orelse A =< Target end, Pending),
                    SA = lists:foldl(fun(A, Acc) -> ack(Epoch, A, Acc) end, S, Before),
                    {Later, assign(Epoch, Prev, V, SA)}
                end,
                {Acks, S0},
                New
            ),
            S2 = lists:foldl(fun(A, Acc) -> ack(Epoch, A, Acc) end, S1, Left),
            check_epoch(Epoch, View, S2#{assigned := Assigned#{Epoch => Had ++ [V || {_Prev, V} <- New]}})
    end.

accept(Epoch, S = #{accepted := Accepted}) ->
    add(#{act => "Accept", e => me(Epoch, S)}, S#{accepted := Accepted#{Epoch => true}}).

chain(Last, Last, _Commits, Acc) ->
    Acc;
chain(V, Last, Commits, Acc) when V > Last ->
    case Commits of
        #{V := Prev} -> chain(Prev, Last, Commits, [{Prev, V} | Acc]);
        _ -> invalid
    end;
chain(_V, _Last, _Commits, _Acc) ->
    invalid.

target(Epoch, V, After) ->
    case [KCV || {LSN, KCV} <- maps:get(Epoch, After, []), LSN >= V] of
        [KCV | _] -> KCV;
        [] -> unknown
    end.

ack(Epoch, V, S = #{m := M, acked := Acked}) ->
    case M of
        #{V := Number} -> add(#{act => "Ack", e => me(Epoch, S), v => Number}, S#{acked := Acked#{Epoch => V}});
        _ -> add(#{act => "Invalid", what => "an acknowledged LSN is unknown"}, S)
    end.

assign(Epoch, Prev, V, S = #{m := M}) ->
    case M of
        #{Prev := Number} ->
            add(#{act => "Assign", e => me(Epoch, S), r => replicas(V), chk => [{"last", [me(Epoch, S)], Number + 1}]}, S#{m := M#{V => Number + 1}});
        _ ->
            add(#{act => "Invalid", what => "the previous LSN of an assigned LSN is unknown"}, S)
    end.

%% The state of the process of an epoch after a change.
check_epoch(Epoch, #{last := Last, live := Live, kcv := KCV}, S = #{m := M}) ->
    E = me(Epoch, S),
    case M of
        #{Last := ML, Live := MV, KCV := MK} ->
            check([{"last", [E], ML}, {"live", [E], MV}, {"pkcv", [E], MK}], S);
        _ ->
            add(#{act => "Invalid", what => "the process of an epoch has an unknown LSN"}, S)
    end.

%% A LogServer.
step_log(_Id, Same, Same, S) ->
    S;
step_log({Epoch, _L}, _Prev, _View, S = #{e1 := E1}) when Epoch < E1 ->
    S;
step_log({Epoch, _L}, Prev0, #{dv := DV}, S = #{rv := RVs}) when not is_map_key(Epoch, RVs) ->
    %% A LogServer of an epoch that has not written the coordinated state.
    %% It must not store a record.
    case Prev0 of
        #{dv := DV0} when DV0 =/= DV -> add(#{act => "Invalid", what => "a LogServer of an epoch that has not written the coordinated state has a new record"}, S);
        _ -> S
    end;
step_log(Id = {Epoch, L}, Prev0, #{dv := DV, kcv := KCV, stopped_by := Stopped, records := Records}, S0 = #{rv := RVs, locked := Locked}) ->
    case RVs of
        #{Epoch := RV} ->
            %% A LogServer that the trace sees for the first time has the
            %% Recovery Version as its Durable Version before its first
            %% record.
            #{dv := DV0, stopped_by := Stopped0} =
                case Prev0 of
                    undefined -> #{dv => RV, stopped_by => 0};
                    _ -> Prev0
                end,
            New = lists:sort([{LSN, Prev, Body} || LSN := {Prev, Body} <- Records, LSN > DV0, LSN > RV]),
            S1 =
                case New =/= [] andalso Stopped0 =/= 0 of
                    true -> add(#{act => "Invalid", what => "a stopped LogServer has a new record"}, S0);
                    false -> lists:foldl(fun(Record, S) -> persist(Epoch, L, RV, Record, S) end, S0, New)
                end,
            #{m := M} = S1,
            S2 =
                case M of
                    #{DV := MD, KCV := MK} -> check([{"dv", [me(Epoch, S1), L], MD}, {"kcv", [me(Epoch, S1), L], MK}], S1);
                    _ -> add(#{act => "Invalid", what => "a LogServer has an unknown LSN"}, S1)
                end,
            %% A Sequencer has stopped the LogServer. The `LockLog' action
            %% follows at the write of that Sequencer.
            case Stopped =/= Stopped0 of
                true -> S2#{locked := Locked#{Stopped => (maps:get(Stopped, Locked, #{}))#{Id => {DV, KCV}}}};
                false -> S2
            end;
        _ ->
            add(#{act => "Invalid", what => "a LogServer of an epoch that has not written the coordinated state"}, S0)
    end.

persist(Epoch, L, RV, {LSN, Prev, Body}, S0 = #{m := M0, first := Firsts, gap := Gap}) ->
    E = me(Epoch, S0),
    %% The recovery transaction is the record whose previous LSN is the
    %% Recovery Version. The first LogServer that has it gives the
    %% `RecoveryTxn' action.
    S1 =
        case Prev =:= RV andalso not is_map_key(Epoch, Firsts) of
            true ->
                case M0 of
                    #{RV := Before} ->
                        Number =
                            case RV of
                                0 -> 1;
                                _ -> Before + Gap
                            end,
                        add(#{act => "RecoveryTxn", e => E, r => replicas(LSN), chk => [{"first", [E], Number}]}, S0#{m := M0#{LSN => Number}, first := Firsts#{Epoch => LSN}});
                    _ ->
                        add(#{act => "Invalid", what => "the Recovery Version of a recovery transaction is unknown"}, S0)
                end;
            false ->
                S0
        end,
    S2 = unseen_assign(Epoch, Prev, LSN, S1),
    case S2 of
        #{m := #{LSN := V}} ->
            add(#{act => "Persist", e => E, l => L, v => V, chk => [{"data", [E, L, V], Body}]}, S2);
        _ ->
            add(#{act => "Invalid", what => "a LogServer has a record that no Sequencer assigned"}, S2)
    end.

%% A LogServer has a record whose `Assign' the trace has not seen. The
%% process of an epoch can assign an LSN and stop before the harness reads
%% its state: the harness kills a node between two steps, and the process
%% stops itself after an error. If the previous LSN of the record is the
%% last LSN of the epoch in the trace, the record gives the `Assign'
%% action. The actions that the process took before it in the same step
%% are added: `Accept', and the `Ack' of each LSN at or below the Known
%% Committed Version that the LogServers have after the record. The model
%% checks each of them.
unseen_assign(_Epoch, _Prev, LSN, S = #{m := M}) when is_map_key(LSN, M) ->
    S;
unseen_assign(Epoch, Prev, LSN, S0 = #{m := M, first := Firsts, assigned := Assigned, acked := Acked, accepted := Accepted, kcv_after := After}) ->
    Had = maps:get(Epoch, Assigned, []),
    Last = lists:last([maps:get(Epoch, Firsts, none) | Had]),
    case is_map_key(Prev, M) andalso Prev =:= Last of
        true ->
            S1 =
                case Accepted of
                    #{Epoch := true} -> S0;
                    _ -> accept(Epoch, S0)
                end,
            Target = target(Epoch, LSN, After),
            Acks = [V || V <- Had, V > maps:get(Epoch, Acked, 0), Target =/= unknown, V =< Target],
            S2 = lists:foldl(fun(V, S) -> ack(Epoch, V, S) end, S1, Acks),
            S3 = assign(Epoch, Prev, LSN, S2),
            S3#{assigned := Assigned#{Epoch => Had ++ [LSN]}};
        false ->
            S0
    end.

%% The indexes of the LogServers that receive the body of an LSN. See
%% `redwall_log:replicas/3'.
replicas(LSN) ->
    lists:sort(redwall_log:replicas(lists:seq(1, ?LOGS), ?K, LSN)).

%% The epoch of the model for an epoch of the code.
me(Epoch, #{e1 := E1}) ->
    Epoch - E1 + 1.

add(Action, S = #{acts := Acts}) ->
    S#{acts := [maps:merge(#{chk => []}, Action) | Acts]}.

%% Adds `Checks' to the last action.
check(Checks, S = #{acts := [Action = #{chk := Chk} | Acts]}) ->
    S#{acts := [Action#{chk := Chk ++ Checks} | Acts]};
check(_Checks, S = #{acts := []}) ->
    S.

%% For each epoch: `{LSN, KCV}' for each time that a LogServer of the
%% epoch had the Durable Version `LSN' after a change, with its Known
%% Committed Version, in the order of the LSNs.
kcv_after(States) ->
    Pairs = lists:usort([{Epoch, DV, KCV} || #{logs := Logs} <- States, {Epoch, _L} := #{dv := DV, kcv := KCV} <- Logs]),
    maps:groups_from_list(fun({Epoch, _, _}) -> Epoch end, fun({_, DV, KCV}) -> {DV, KCV} end, Pairs).

%% For each epoch: the Known Committed Version of a LogServer of the epoch
%% in the first state that has such a LogServer.
pev(States) ->
    lists:foldl(
        fun(#{logs := Logs}, Acc) ->
            maps:fold(fun({Epoch, _L}, #{kcv := KCV}, A) -> maps:merge(#{Epoch => KCV}, A) end, Acc, Logs)
        end,
        #{},
        States
    ).

%% 1 plus the highest number of LSNs that an epoch assigned above the
%% Recovery Version that ended it.
gap(States, E1) ->
    #{cs := #{ends := Ends}} = lists:last(States),
    LSNs = lists:usort(
        [{Epoch, LSN} || #{logs := Logs} <- States, {Epoch, _L} := #{records := Records} <- Logs, LSN <- maps:keys(Records)] ++
            [{Epoch, LSN} || #{epochs := Epochs} <- States, Epoch := #{last := Last, commits := Commits} <- Epochs, LSN <- [Last | maps:keys(Commits)]]
    ),
    1 + lists:max([0 | [length([LSN || {Epoch, LSN} <- LSNs, Epoch =:= Ended, LSN > End]) || Ended := End <- Ends, Ended >= E1]]).

%% ---------------------------------------------------------------------------
%% Output.
%% ---------------------------------------------------------------------------

%% Writes `TraceData.tla', `TraceData.len' and `events.txt' to `Dir'.
write(Dir, {#{max_epoch := MaxEpoch, max_version := MaxVersion, gap := Gap, k := K, min_replies := MinReplies}, Actions}) ->
    ok = filelib:ensure_path(Dir),
    Data = [
        "---- MODULE TraceData ----\n",
        "EXTENDS Naturals, Sequences\n",
        io_lib:format("TraceLogs == {1, 2, 3}~nTraceK == ~b~nTraceMinReplies == ~b~n", [K, MinReplies]),
        io_lib:format("TraceMaxEpoch == ~b~nTraceMaxVersion == ~b~nTraceGap == ~b~n", [MaxEpoch, MaxVersion, Gap]),
        "Trace == <<\n",
        lists:join(",\n", [tla(Action) || Action <- Actions]),
        "\n>>\n====\n"
    ],
    ok = file:write_file(filename:join(Dir, "TraceData.tla"), Data),
    ok = file:write_file(filename:join(Dir, "TraceData.len"), integer_to_list(length(Actions))),
    ok = file:write_file(filename:join(Dir, "events.txt"), [io_lib:format("~0p~n", [Action]) || Action <- Actions]).

tla(Action = #{act := Act, chk := Chk}) ->
    io_lib:format(
        "[act |-> \"~s\", e |-> ~b, l |-> ~b, v |-> ~b, r |-> {~s}, chk |-> <<~s>>]",
        [
            Act,
            maps:get(e, Action, 0),
            maps:get(l, Action, 0),
            maps:get(v, Action, 0),
            lists:join(", ", [integer_to_list(L) || L <- maps:get(r, Action, [])]),
            lists:join(", ", [tla_check(Check) || Check <- Chk])
        ]
    ).

%% A check has 3 arguments in the trace spec. A missing argument is 0.
tla_check({Variable, Arguments, Value}) ->
    [A, B, C] = Arguments ++ lists:duplicate(3 - length(Arguments), 0),
    io_lib:format("<<\"~s\", ~b, ~b, ~b, ~s>>", [Variable, A, B, C, tla_value(Value)]).

tla_value(true) -> "TRUE";
tla_value(false) -> "FALSE";
tla_value(Value) when is_integer(Value) -> integer_to_list(Value).

%% The number of actions of each kind.
counts({_Constants, Actions}) ->
    lists:foldl(fun(#{act := Act}, Acc) -> maps:update_with(Act, fun(N) -> N + 1 end, 1, Acc) end, #{}, Actions).
