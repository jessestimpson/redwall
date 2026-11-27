%% This module implements trace validation. The module converts a simulated
%% run into a behaviour of `formal/Recovery.tla'. TLC then accepts or
%% rejects the behaviour.
%%
%% TLC checks the model exhaustively, but that check covers only the model.
%% The simulation checks the code, but only against invariants. Trace
%% validation connects the model and the code: every change that the
%% coordinated state, a LogServer, the process of an epoch or a
%% StorageServer makes must be a step that the model allows in the state
%% that the model has reached at that point.
%%
%% == Method ==
%%
%% With the config `trace => Table', the harness calls `record/3' after
%% every step of the scheduler. `record/3' reads the published state of the
%% coordinated state, of each LogServer, of the process of each epoch and
%% of each StorageServer, and keeps the state when it differs from the
%% state before (`redwall_dst:check/1'). A process publishes its state at
%% the end of a callback.
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
%% | The process of an epoch has a new confirmation | `ReadStart(r, e)', then `ReadConfirm(r, l)' for each answer that the process counts later, then `ReadSeq(r)' |
%% | The process of an epoch has removed a confirmation | `ReadDone(r)' |
%% | The version of a StorageServer rises | `SSApply(l, v)' for each record between the two versions |
%% | The version of a StorageServer falls, or the StorageServer reads a later epoch | `SSFollow' |
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
%%   recovery;</li>
%% <li>the read version that the process of an epoch takes for a
%%   confirmation, and the read version of its answer;</li>
%% <li>the epoch and the version of a StorageServer, and for each write
%%   of its store between the version before and the version after,
%%   that the StorageServer of the model has applied the record of the
%%   write and has not removed it.</li>
%% </ul>
%%
%% TLC has one behaviour to find. Each action must be enabled at its
%% position. For example, `Ack' requires that every LogServer of the epoch
%% has persisted the LSN, `Persist' requires that the previous LSN of the
%% record is the Durable Version of the LogServer, and `WriteCState'
%% requires that the recovery count is the epoch of the Sequencer,
%% `ReadDone' requires 2 `ReadConfirm' actions, and `SSApply' requires
%% that the previous LSN of the record is the version of the
%% StorageServer.
%%
%% == StorageServers ==
%%
%% The model has one StorageServer. A run has 1 to 3, and more after a
%% kill. The trace spec keeps the variables of each StorageServer, and a
%% `Switch' element of the trace exchanges them. See
%% `formal/TraceRecovery.tla'. The invariants `StorageSafe' and
%% `StorageComplete' of the model are checked for each StorageServer.
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
%%   trace compares, for each new LogServer, which of these records the
%%   LogServer has with a body. It does not compare the content of a
%%   body.</li>
%% <li>The position of an `Ack' among the `Assign' actions of one step is
%%   chosen from the Known Committed Version that the LogServers have
%%   after the records. See `step_epoch/4'.</li>
%% <li>One confirmation of the process of an epoch is one request of the
%%   model. The trace does not have the time at which a client sent its
%%   request. The requests that share a confirmation arrived before its
%%   start.</li>
%% <li>The process of an epoch counts the answers of the LogServers to a
%%   confirmation and does not keep their names. A `ReadConfirm' action is
%%   placed at the start of the confirmation, for a LogServer that no
%%   `LockLog' action had stopped at that point. The trace therefore does
%%   not validate that the LogServer that answered was not stopped.</li>
%% <li>The trace follows a StorageServer from its start without data. It
%%   does not follow a StorageServer after a copy from a peer or after a
%%   reset of the database. The model has neither.</li>
%% <li>The trace compares the LSNs of the writes of a store with the
%%   records that the StorageServer of the model has applied. It does not
%%   compare keys and values. The model has none.</li>
%% <li>The Resolver, the pop and the ClusterController are not in the
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
        ),
        storages => maps:from_list(
            lists:append([
                view_storage(Tab, Pid)
             || [Pid] <- ets:match(Tab, {{storage_started, '_'}, '$1'}), is_process_alive(Pid)
            ])
        )
    }.

%% The state of a StorageServer. `rows' has the LSNs of the writes that
%% the store of the StorageServer has. `copied' is `true' when the
%% StorageServer has copied from a peer or has removed its data after a
%% reset of the database.
view_storage(Tab, Pid) ->
    case eta_observe:read(Pid) of
        #{version := Version, oldest := Oldest, rollbacks := Rollbacks, copies := Copies, resets := Resets, engine := Engine, view := View, system := System} ->
            Epoch =
                case System of
                    #{epoch := E} -> E;
                    _ -> 0
                end,
            Rows = rows(Tab, Pid, {Version, Rollbacks, Oldest}, Engine, View),
            [{Pid, #{epoch => Epoch, version => Version, copied => Copies + Resets > 0, rows => Rows}}];
        _ ->
            []
    end.

%% The LSNs of the writes of a store, in their order. The table of the run
%% keeps the result for the version, the number of rollbacks and the
%% oldest version of the StorageServer, because a read of the store reads
%% every row.
rows(Tab, Pid, Key, Engine, View) ->
    case ets:lookup(Tab, {trace_rows, Pid}) of
        [{_, Key, Rows}] ->
            Rows;
        _ ->
            Rows =
                try
                    lists:usort([Written || Versions <- maps:values(redwall_storage:versions(#{engine => Engine, store => View})), {Written, _Change} <- Versions])
                catch
                    error:badarg -> []
                end,
            true = ets:insert(Tab, {{trace_rows, Pid}, Key, Rows}),
            Rows
    end.

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
        #{epoch := Epoch, last := Last, live := Live, kcv := KCV, commits := Commits, reads := Reads, next_read := NextRead} when is_integer(Epoch) ->
            [
                {Epoch, #{
                    last => Last,
                    live => Live,
                    kcv => KCV,
                    commits => #{LSN => element(3, Commit) || LSN := Commit <- Commits, LSN =/= 0},
                    %% For each confirmation in progress: the read version
                    %% and the number of confirmations that the Proxy
                    %% still needs (the fields `version' and `needed').
                    reads => #{Id => {element(4, Read), element(5, Read)} || Id := Read <- Reads},
                    next_read => NextRead
                }}
            ];
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
                %% The LogServers of each epoch that a `LockLog' action
                %% has stopped.
                stopped => #{},
                %% The records of each epoch that a LogServer has stored:
                %% the previous LSN of each LSN, and the LogServers with
                %% the body of each LSN.
                recs => #{},
                bodies => #{},
                %% The number of the model for each confirmation of the
                %% process of an epoch. For each number: the LogServers
                %% of the epoch that no `LockLog' action had stopped at
                %% the start, and the number of confirmations that the
                %% Proxy has counted.
                rid => #{},
                cands => #{},
                filled => #{},
                %% The number of the trace spec for each StorageServer,
                %% the StorageServers that the trace does not follow, the
                %% epoch and the version of each StorageServer, and the
                %% StorageServer whose variables the model has.
                sid => #{},
                skipped => #{},
                ss => #{},
                cur => 1,
                %% For each epoch that has written the coordinated state:
                %% the epoch that it ended, with the previous epoch's end
                %% version and the Recovery Version. The new LogServers
                %% whose copied records the trace has compared.
                ended => #{},
                copies => #{},
                %% The confirmations that the process of an epoch started
                %% in the change that `step_epoch/4' handles.
                pending => none,
                acts => []
            },
            Empty = #{cs => maps:get(cs, First), logs => #{}, epochs => #{}, storages => #{}},
            S1 = step(Empty#{cs := (maps:get(cs, First))#{count := E1}}, First, S0),
            S2 = #{acts := Acts, m := M, e1 := E1, gap := Gap, rid := Rids, sid := Sids} = walk(First, Rest, S1),
            Actions = confirmations(lists:reverse(Acts), S2),
            Epochs = lists:max([1 | [maps:get(e, A) || A <- Actions, is_map_key(e, A)]]),
            {
                #{
                    max_epoch => Epochs + 1,
                    max_version => lists:max(maps:values(M)) + Gap + 1,
                    gap => Gap,
                    k => ?K,
                    min_replies => ?MIN_REPLIES,
                    reads => map_size(Rids),
                    storages => max(1, map_size(Sids)),
                    %% The StorageServers that the trace follows from
                    %% their start, and the StorageServers that it does
                    %% not follow or stops to follow.
                    followed => map_size(Sids),
                    skipped => map_size(maps:get(skipped, S2))
                },
                Actions
            }
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
    S3 = maps:fold(
        fun(Id, View, S) -> step_log(Id, maps:get(Id, maps:get(logs, Prev), undefined), View, S) end,
        S2,
        maps:get(logs, Next)
    ),
    #{cs := #{ends := Ends}} = Next,
    lists:foldl(
        fun({Pid, View}, S) -> step_storage(Pid, maps:get(Pid, maps:get(storages, Prev), undefined), View, Ends, S) end,
        S3,
        lists:sort(maps:to_list(maps:get(storages, Next)))
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
                    Stopped = maps:get(stopped, S3),
                    S3#{rv := RVs#{N => RV}, stopped := maps:merge(Stopped, #{{Old, L} => true || L <- Replied}), ended := (maps:get(ended, S3))#{N => {Old, PEV, RV}}}
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
            Start = #{last => First, live => First, kcv => maps:get(Epoch, PEVs, 0), commits => #{}, reads => #{}, next_read => 1},
            S2 = S1#{assigned := maps:merge(#{Epoch => []}, maps:get(assigned, S1))},
            case View of
                #{last := First} -> step_reads(Epoch, Start, View, check_epoch(Epoch, View, S2));
                _ -> step_epoch(Epoch, Start, View, S2)
            end;
        _ ->
            add(#{act => "Invalid", what => "an epoch accepts commits and no LogServer has its recovery transaction"}, S0)
    end;
step_epoch(Epoch, Was = #{last := L0, kcv := K0}, View = #{last := L1, kcv := K1, commits := Commits}, S00 = #{assigned := Assigned, acked := Acked, kcv_after := After}) ->
    Had = maps:get(Epoch, Assigned, []),
    %% The new confirmations. A change can contain the start of a
    %% confirmation and a later `Ack'. `ack/3' places the start of a
    %% confirmation before the `Ack' of an LSN above its read version.
    S0 = S00#{pending := {Epoch, [{Id, maps:get(Id, maps:get(reads, View), answered)} || Id <- lists:seq(maps:get(next_read, Was), maps:get(next_read, View) - 1)]}},
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
            S3 = check_epoch(Epoch, View, S2#{assigned := Assigned#{Epoch => Had ++ [V || {_Prev, V} <- New]}}),
            (step_reads(Epoch, Was, View, S3))#{pending := none}
    end.

%% The confirmations of the process of an epoch. One confirmation answers
%% the requests for a read version that wait at its start, so one
%% confirmation is one request of the model.
%%
%% <ul>
%% <li>A new confirmation gives `ReadStart' and `ReadSeq'. The process
%%   takes its read version in the step in which it starts the
%%   confirmation.</li>
%% <li>The process counts the answers of the LogServers and does not keep
%%   their names. Each counted answer gives one `ReadConfirm' action,
%%   which is placed at the start of the confirmation, for a LogServer
%%   that no `LockLog' action had stopped at that point. A LogServer that
%%   is not stopped when it answers was not stopped at the start. See
%%   `confirmations/2'.</li>
%% <li>A confirmation that the process has removed gives `ReadDone'. The
%%   process removes a confirmation only when it answers the clients.</li>
%% </ul>
step_reads(Epoch, #{reads := Had, next_read := Next0}, #{reads := Reads, next_read := Next1}, S0) ->
    S1 = lists:foldl(
        fun(Id, S = #{rid := Rids}) ->
            {Version, Needed0} = maps:get(Id, Had),
            case {Rids, Reads} of
                {#{{Epoch, Id} := R}, #{Id := {_Version, Needed1}}} -> confirmed(R, Needed0 - Needed1, S);
                {#{{Epoch, Id} := R}, _Answered} -> read_done(R, Version, confirmed(R, Needed0, S));
                _ -> add(#{act => "Invalid", what => "a confirmation without a start"}, S)
            end
        end,
        S0,
        lists:sort(maps:keys(Had))
    ),
    lists:foldl(fun(Id, S) -> read_start(Epoch, Id, maps:get(Id, Reads, answered), S) end, S1, lists:seq(Next0, Next1 - 1)).

read_start(Epoch, Id, _Now, S = #{rid := Rids}) when is_map_key({Epoch, Id}, Rids) ->
    %% `ack/3' has placed the start.
    S;
read_start(Epoch, Id, Now, S0 = #{rid := Rids, cands := Cands, filled := Filled, stopped := Stopped}) ->
    R = map_size(Rids) + 1,
    Running = [L || L <- lists:seq(1, ?LOGS), not is_map_key({Epoch, L}, Stopped)],
    S1 = add(#{act => "ReadStart", q => R, e => me(Epoch, S0)}, S0#{rid := Rids#{{Epoch, Id} => R}, cands := Cands#{R => Running}, filled := Filled#{R => 0}}),
    %% The places of the `ReadConfirm' actions of this confirmation.
    S2 = lists:foldl(fun(J, S) -> add(#{act => slot, q => R, j => J}, S) end, S1, lists:seq(1, ?K)),
    %% A confirmation that the process started and removed in one change
    %% is not in the state. The trace does not have its read version.
    #{m := M} = S2,
    case Now of
        answered -> add(#{act => "ReadDone", q => R}, add(#{act => "ReadSeq", q => R}, S2));
        {Version, _Needed} when is_map_key(Version, M) -> add(#{act => "ReadSeq", q => R, chk => [{"rSeq", [R], maps:get(Version, M)}]}, S2);
        _ -> add(#{act => "Invalid", what => "the read version of a confirmation is unknown"}, S2)
    end.

confirmed(_R, 0, S) ->
    S;
confirmed(R, Count, S = #{filled := Filled}) ->
    S#{filled := Filled#{R := maps:get(R, Filled) + Count}}.

read_done(R, Version, S = #{m := M}) ->
    case M of
        #{Version := Number} -> add(#{act => "ReadDone", q => R, chk => [{"rVer", [R], Number}]}, S);
        _ -> add(#{act => "Invalid", what => "the read version of an answer is unknown"}, S)
    end.

%% Replaces the places of the `ReadConfirm' actions. The confirmation `R'
%% has one action for each answer that the process counted.
confirmations(Actions, #{filled := Filled, cands := Cands}) ->
    lists:filtermap(
        fun
            (#{act := slot, q := R, j := J}) ->
                case {J =< maps:get(R, Filled), lists:sublist(maps:get(R, Cands), J, 1)} of
                    {false, _} -> false;
                    {true, [L]} -> {true, #{act => "ReadConfirm", q => R, l => L, chk => []}};
                    {true, []} -> {true, #{act => "Invalid", what => "more confirmations than LogServers that were not stopped at the start", chk => []}}
                end;
            (_Action) ->
                true
        end,
        Actions
    ).

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

ack(Epoch, V, S0) ->
    %% The confirmations of this change with a read version below `V'
    %% started before this `Ack'.
    S = #{m := M, acked := Acked} =
        case S0 of
            #{pending := {Epoch, Starts}} ->
                lists:foldl(fun({Id, Now = {Version, _Needed}}, Acc) when Version < V -> read_start(Epoch, Id, Now, Acc); (_Later, Acc) -> Acc end, S0, Starts);
            _ ->
                S0
        end,
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
                    #{DV := MD, KCV := MK} -> copied(Id, Records, check([{"dv", [me(Epoch, S1), L], MD}, {"kcv", [me(Epoch, S1), L], MK}], S1));
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

%% The records that the recovery copied to a new LogServer. The model
%% copies each record of the previous epoch above the previous epoch's
%% end version, up to the Recovery Version, whose body a LogServer that
%% answered the recovery has. The new LogServer of the code must have the
%% body of the same records. The check is made once for each LogServer,
%% when the trace has the first state of the LogServer after the write of
%% the coordinated state.
copied(Id, _Records, S = #{copies := Copies}) when is_map_key(Id, Copies) ->
    S;
copied(Id = {Epoch, L}, Records, S = #{copies := Copies, ended := Ended, recs := Recs, m := M}) ->
    Checks =
        case Ended of
            #{Epoch := {Old, PEV, RV}} when is_integer(PEV) ->
                [
                    {"data", [me(Epoch, S), L, maps:get(LSN, M)], case Records of #{LSN := {_Prev, Body}} -> Body; _ -> false end}
                 || LSN <- lists:sort(maps:keys(maps:get(Old, Recs, #{}))), LSN > PEV, LSN =< RV, is_map_key(LSN, M)
                ];
            _ ->
                []
        end,
    check(Checks, S#{copies := Copies#{Id => true}}).

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
        #{m := #{LSN := V}, recs := Recs, bodies := Bodies} ->
            Has = [L || Body] ++ maps:get({Epoch, LSN}, Bodies, []),
            S3 = S2#{recs := Recs#{Epoch => (maps:get(Epoch, Recs, #{}))#{LSN => Prev}}, bodies := Bodies#{{Epoch, LSN} => lists:usort(Has)}},
            add(#{act => "Persist", e => E, l => L, v => V, chk => [{"data", [E, L, V], Body}]}, S3);
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

%% A StorageServer. The trace follows a StorageServer from its start. The
%% model has one StorageServer, which starts without data in the epoch 1.
%% The trace does not follow a StorageServer after it has copied from a
%% peer or has removed its data after a reset of the database.
%%
%% The actions are derived from the epoch and the version before and after
%% the change:
%%
%% <ul>
%% <li>A higher version in the same epoch gives `SSApply' for each record
%%   of the chain of the epoch between the two versions, with a LogServer
%%   of the epoch that has the body.</li>
%% <li>A lower version gives `SSFollow': the rollback.</li>
%% <li>A later epoch gives, for each epoch between: `SSApply' up to the
%%   end version of the epoch or `SSFollow' down to it, and then `SSFollow'
%%   to the next epoch.</li>
%% </ul>
%%
%% The last action carries the epoch and the version of the StorageServer,
%% and a check that the model has applied the record of each write of the
%% store between the two versions. After a rollback the check has every
%% write of the store at or below the version.
step_storage(_Pid, Same, Same, _Ends, S) ->
    S;
step_storage(Pid, Prev, View = #{copied := Copied}, Ends, S0 = #{sid := Sids, skipped := Skipped, ss := SS, e1 := E1}) ->
    case {Skipped, Sids} of
        {#{Pid := true}, _} -> S0;
        _ when Copied -> S0#{skipped := Skipped#{Pid => true}};
        {_, #{Pid := _}} -> storage_path(Pid, Prev, View, Ends, S0);
        _ -> storage_path(Pid, Prev, View, Ends, S0#{sid := Sids#{Pid => map_size(Sids) + 1}, ss := SS#{Pid => {E1, 0}}})
    end.

storage_path(Pid, Prev, #{epoch := Epoch, version := Version, rows := Rows}, Ends, S0 = #{ss := SS, sid := Sids}) ->
    {E0, V0} = maps:get(Pid, SS),
    %% A StorageServer that has not read the coordinated state has no
    %% epoch.
    Target =
        case Epoch of
            0 -> E0;
            _ -> Epoch
        end,
    Count = length(maps:get(acts, S0)),
    S1 = path(maps:get(Pid, Sids), E0, V0, Target, Version, Ends, S0),
    S2 = S1#{ss := SS#{Pid => {Target, Version}}},
    case length(maps:get(acts, S2)) of
        Count ->
            S2;
        _ ->
            Old =
                case Prev of
                    undefined -> [];
                    #{rows := Had} -> Had
                end,
            %% The StorageServer publishes its state at the end of a
            %% callback, and writes to its store during the callback. The
            %% store can therefore have writes above the published
            %% version. A write is checked when the version reaches it.
            Checked = [LSN || LSN <- Rows, LSN =< Version, Version < V0 orelse LSN > V0 orelse not lists:member(LSN, Old)],
            #{m := M} = S2,
            case {M, [LSN || LSN <- Checked, not is_map_key(LSN, M)]} of
                {#{Version := Number}, []} ->
                    check([{"ssEpoch", [], me(Target, S2)}, {"ssVersion", [], Number}] ++ [{"ssHas", [maps:get(LSN, M)], true} || LSN <- Checked], S2);
                _ ->
                    add(#{act => "Invalid", what => "a StorageServer has a version or a write of an unknown LSN"}, S2)
            end
    end.

%% The actions from the version `V' of the epoch `E' to the version `Vt'
%% of the epoch `Et'.
path(_Id, E, V, E, V, _Ends, S) ->
    S;
path(Id, E, V, E, Vt, _Ends, S) when Vt > V ->
    applies(Id, E, V, Vt, S);
path(Id, E, _V, E, _Vt, _Ends, S) ->
    storage(Id, #{act => "SSFollow"}, S);
path(Id, E, V, Et, Vt, Ends, S0 = #{rv := RVs}) when E < Et ->
    %% The epochs that have written the coordinated state after `E'.
    case {Ends, [N || N <- maps:keys(RVs), N > E]} of
        {#{E := End}, Later = [_ | _]} ->
            S1 =
                if
                    V < End -> applies(Id, E, V, End, S0);
                    V > End -> storage(Id, #{act => "SSFollow"}, S0);
                    true -> S0
                end,
            path(Id, lists:min(Later), End, Et, Vt, Ends, storage(Id, #{act => "SSFollow"}, S1));
        _ ->
            add(#{act => "Invalid", what => "a StorageServer leaves an epoch that has not ended"}, S0)
    end;
path(_Id, _E, _V, _Et, _Vt, _Ends, S) ->
    add(#{act => "Invalid", what => "a StorageServer reads an earlier epoch"}, S).

applies(Id, Epoch, V, Vt, S0 = #{recs := Recs}) ->
    case chain(Vt, V, maps:get(Epoch, Recs, #{}), []) of
        invalid ->
            add(#{act => "Invalid", what => "a StorageServer has a version without a chain of records of its epoch"}, S0);
        Chain ->
            lists:foldl(
                fun({_Prev, LSN}, S = #{m := M, bodies := Bodies}) ->
                    case {M, maps:get({Epoch, LSN}, Bodies, [])} of
                        {#{LSN := Number}, [L | _]} -> storage(Id, #{act => "SSApply", l => L, v => Number}, S);
                        _ -> add(#{act => "Invalid", what => "a StorageServer applied a record whose body no LogServer has"}, S)
                    end
                end,
                S0,
                Chain
            )
    end.

%% Adds an action of the StorageServer `Id'. The model has the variables
%% of one StorageServer. A `Switch' step of the trace spec exchanges them.
storage(Id, Action, S = #{cur := Id}) ->
    add(Action, S);
storage(Id, Action, S) ->
    add(Action, add(#{act => "Switch", q => Id}, S#{cur := Id})).

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
write(Dir, {#{max_epoch := MaxEpoch, max_version := MaxVersion, gap := Gap, k := K, min_replies := MinReplies, reads := Reads, storages := Storages}, Actions}) ->
    ok = filelib:ensure_path(Dir),
    Data = [
        "---- MODULE TraceData ----\n",
        "EXTENDS Naturals, Sequences\n",
        io_lib:format("TraceLogs == {1, 2, 3}~nTraceK == ~b~nTraceMinReplies == ~b~n", [K, MinReplies]),
        io_lib:format("TraceMaxEpoch == ~b~nTraceMaxVersion == ~b~nTraceGap == ~b~n", [MaxEpoch, MaxVersion, Gap]),
        io_lib:format("TraceReads == 1..~b~nTraceStorages == ~b~n", [Reads, Storages]),
        "Trace == <<\n",
        lists:join(",\n", [tla(Action) || Action <- Actions]),
        "\n>>\n====\n"
    ],
    ok = file:write_file(filename:join(Dir, "TraceData.tla"), Data),
    ok = file:write_file(filename:join(Dir, "TraceData.len"), integer_to_list(length(Actions))),
    ok = file:write_file(filename:join(Dir, "events.txt"), [io_lib:format("~0p~n", [Action]) || Action <- Actions]).

tla(Action = #{act := Act, chk := Chk}) ->
    io_lib:format(
        "[act |-> \"~s\", e |-> ~b, q |-> ~b, l |-> ~b, v |-> ~b, r |-> {~s}, chk |-> <<~s>>]",
        [
            Act,
            maps:get(e, Action, 0),
            maps:get(q, Action, 0),
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
counts({#{followed := Followed, skipped := Skipped}, Actions}) ->
    lists:foldl(
        fun(#{act := Act}, Acc) -> maps:update_with(Act, fun(N) -> N + 1 end, 1, Acc) end,
        #{"StorageServers followed" => Followed, "StorageServers not followed" => Skipped},
        Actions
    ).
