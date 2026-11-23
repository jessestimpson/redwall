%% The processes that perform the operations of the simulation. See
%% `deadlift_dst'.
%%
%% Each function runs in a process that the scheduler of eta steps. A
%% function records its result in the table of the run. The harness reads
%% the table in its checks.
-module(deadlift_dst_client).

-include("deadlift_eta.hrl").

-export([recover/4, transaction/5, join/2, start_log/3]).
-export([worker/5, force_recovery/1, worker_node/1]).
-export([sequencer_node/1, log_node/1, storage_node/1, keys/0]).

%% Starts the process of a new epoch and waits for the end of its
%% recovery. This is the work of a ClusterController. The runs without the
%% config key `controller' use this function.
%%
%% `N' is the number of the operation. `Mode' selects a planted defect.
recover(Tab, N, CState, Mode) ->
    ok = join(Tab, Mode),
    %% Each LogServer runs on a simulated node of its own. The process of
    %% the epoch starts the LogServer on that node through `erpc'. The call
    %% has a limit of time, because the network can lose the request.
    Recruit = fun(Log = #{id := Id}) ->
        Log1 =
            case Mode of
                %% The planted defect: a new LogServer starts without the
                %% records that the recovery read from the previous epoch.
                no_copy -> Log#{records := []};
                _ -> Log
            end,
        ok = eta_net:place(log_node(Id), []),
        try
            erpc:call(log_node(Id), ?MODULE, start_log, [Tab, Log1, Mode], 1000)
        catch
            error:{erpc, Reason} -> {error, {erpc, Reason}};
            Class:Reason -> {error, {Class, Reason}}
        end
    end,
    Options = (proxy_options(N, Mode))#{
        coordinators => {local, CState},
        logs => 3,
        replication => 2,
        lock_timeout_ms => 200,
        recruit => Recruit
    },
    %% The process starts on the node of this process.
    {ok, Proxy} = deadlift_epoch:start(Options),
    true = ets:insert(Tab, {{sequencer, N}, Proxy}),
    try deadlift_epoch:recovered(Proxy) of
        {ok, #{epoch := Epoch, recovery_version := RV, previous_epoch_end_version := PEV}} ->
            true = ets:insert(Tab, [{{proxy, N}, Proxy, Epoch}, {{recovered, N}, Epoch, PEV, RV}])
    catch
        exit:{{shutdown, {recovery, Reason}}, _Call} ->
            true = ets:insert(Tab, {{recovery_failed, N}, Reason});
        exit:Reason ->
            %% The process was killed, or did not answer in time.
            true = ets:insert(Tab, {{recovery_failed, N}, {exit, reason(Reason)}})
    end.

%% Starts the worker of the node of the caller. With `Candidate', the
%% function also starts a candidate for the ClusterController on the node.
%% The worker starts each role with `start_role/5', which records the role
%% in the table of the run and applies the planted defect of `Mode'.
worker(Tab, CState, Mode, Class, Candidate) ->
    ok = join(Tab, Mode),
    Node = node(),
    Roles = fun(Role, Options) -> start_role(Tab, Node, Mode, Role, Options) end,
    Options0 = #{coordinators => {local, CState}, class => Class, roles => Roles, poll_ms => 200, cstate_timeout_ms => 500},
    Options =
        case Candidate of
            true ->
                %% The worker starts the candidate, and the two processes
                %% are linked.
                Options0#{
                    candidate => #{
                        logs => 3,
                        replication => 2,
                        %% Each LogServer of an epoch is on a node of its
                        %% own.
                        min_workers => 3,
                        proxy => #{lock_timeout_ms => 200},
                        poll_ms => 200,
                        retry_ms => 50,
                        recovery_timeout_ms => 2000,
                        cstate_timeout_ms => 500,
                        notify => fun(Event) -> event(Tab, Node, Event) end,
                        %% The planted defects of the ClusterController.
                        monitor_roles => Mode =/= no_monitor_roles,
                        step_down => Mode =/= no_step_down
                    }
                };
            false ->
                Options0
        end,
    {ok, Worker} = deadlift_worker:start(Options),
    true = ets:insert(Tab, {{worker, Node}, Worker, Class}),
    case deadlift_worker:info() of
        #{candidate := Controller} when is_pid(Controller) -> true = ets:insert(Tab, {{controller, Node}, Controller});
        _ -> true
    end.

%% Starts a role on the node `Node', for the worker of that node.
start_role(Tab, Node, Mode, Role, Options0) ->
    Count = ets:update_counter(Tab, roles, 1, {roles, 0}),
    Options =
        case {Role, Mode} of
            {log, no_copy} -> Options0#{records := []};
            {proxy, _} -> maps:merge(Options0, proxy_options(Count, Mode));
            _ -> Options0
        end,
    Start =
        case Role of
            log -> fun deadlift_log:start/1;
            proxy -> fun deadlift_epoch:start/1
        end,
    case Start(Options) of
        {ok, Pid} ->
            Key =
                case Options of
                    #{id := Id} when Role =:= log -> {log, Id};
                    _ -> {Role, Count}
                end,
            %% The row `{proxy, _}' of the process of an epoch has the
            %% epoch, so the function `event/3' inserts it after the
            %% recovery. The row `{sequencer, _}' has each such process
            %% from its start.
            _ =
                case Role of
                    proxy -> ets:insert(Tab, {{sequencer, {controller, Count}}, Pid});
                    log -> ets:insert(Tab, {Key, Pid})
                end,
            true = ets:insert(Tab, {{at, Pid}, Node, Role}),
            {ok, Pid};
        Error ->
            Error
    end.

%% Records an event of a ClusterController in the table of the run.
event(Tab, Node, {leader, Term}) ->
    %% A second leader of one term is a violation of `one_leader'.
    ets:insert_new(Tab, {{leader, Term}, Node}) orelse ets:insert(Tab, {{second_leader, Term}, Node});
event(Tab, _Node, {recovered, #{epoch := Epoch, recovery_version := RV, previous_epoch_end_version := PEV, proxy := Proxy}}) ->
    ets:insert(Tab, [{{proxy, {controller, Epoch}}, Proxy, Epoch}, {{recovered, {controller, Epoch}}, Epoch, PEV, RV}]);
event(Tab, _Node, {recovery_failed, Reason}) ->
    Count = ets:update_counter(Tab, events, 1, {events, 0}),
    Short =
        case Reason of
            _ when is_tuple(Reason) -> element(1, Reason);
            _ -> Reason
        end,
    ets:insert(Tab, {{recovery_failed, {controller, Count}}, {controller, Short}});
event(Tab, _Node, {superseded, Epoch, _Count}) ->
    ets:insert(Tab, {{superseded, Epoch}, true});
event(Tab, _Node, {role_failed, Role, _Pid}) ->
    Count = ets:update_counter(Tab, events, 1, {events, 0}),
    ets:insert(Tab, {{role_failed, Count}, Role});
event(_Tab, _Node, _Event) ->
    true.

%% Asks each ClusterController process of the run for a recovery. The
%% leader recruits, and a candidate ignores the request.
force_recovery(Tab) ->
    lists:foreach(
        fun([Controller]) -> deadlift_cluster_controller:force_recovery(Controller) end,
        lists:sort(ets:match(Tab, {{controller, '_'}, '$1'}))
    ).

%% The options of the process of an epoch for a planted defect. `Count'
%% selects the batch time of 0 ms, 5 ms or 50 ms of the virtual clock.
%% 1 process in 2 closes a batch at once when no batch is in progress,
%% 1 process in 2 shares the confirmations for read versions, and 1
%% process in 2 has 3 batches in progress at the same time (`batch_depth').
proxy_options(Count, Mode) ->
    Options = #{
        ack =>
            case Mode of
                ack_replicas -> replicas;
                _ -> all
            end,
        report => Mode =/= no_report,
        confirm => Mode =/= no_confirm,
        check => Mode =/= no_check,
        check_ranges => Mode =/= no_range_check,
        check_batch => Mode =/= no_batch_check,
        check_write_ranges => Mode =/= no_write_range,
        batch_ms => lists:nth(Count rem 3 + 1, [0, 5, 50]),
        batch_idle => Count rem 2 =:= 0,
        read_version_share => Count div 2 rem 2 =:= 0,
        batch_depth => lists:nth(Count div 4 rem 2 + 1, [1, 3])
    },
    case Mode of
        %% The planted defect: the first commit version of an epoch is the
        %% Recovery Version plus 1, inside the MVCC window.
        no_gap -> Options#{epoch_gap => 1};
        _ -> Options
    end.

%% Records the node of the caller in the table of the run. The workload
%% cuts connections between the recorded nodes. Two nodes connect with the
%% first message between them.
join(Tab, _Mode) ->
    true = ets:insert(Tab, {{node, node()}, true}),
    ok.

%% Starts a LogServer on the node of the caller. `erpc' calls this
%% function on the node of the LogServer.
start_log(Tab, Log = #{id := Id}, Mode) ->
    ok = join(Tab, Mode),
    case deadlift_log:start(Log) of
        {ok, Pid} ->
            true = ets:insert(Tab, {{log, Id}, Pid}),
            {ok, Pid};
        Error ->
            Error
    end.

%% Runs one transaction. `C' is the number of the client.
%%
%% The client gets a read version from the Proxy `Reader', and commits
%% through the Proxy `Proxy' of `Epoch'. The two Proxies can belong to
%% different epochs. `R' selects the keys that the transaction reads and
%% writes. The transaction writes at least one key, and the value of each
%% key is `{client, C}'.
%%
%% 2 transactions in 7 also read one key range. The range starts at one of
%% the 4 keys and ends at a later key, or after the last key. The client
%% reads the range from the StorageServer, and sends the range to the Proxy
%% as a read range. The client records the keys of the range as read keys,
%% with `not_found' for a key that the StorageServer did not return.
%%
%% 2 transactions in 11 clear one key range before they write their keys.
%% The cleared keys of the transaction are the keys of the range that the
%% transaction does not write. The client records them before the commit,
%% in the row `{attempt, C}', because a clear can be applied when the
%% client receives no answer.
%%
%% The client records every read version that it receives, also when the
%% commit fails. With the read version, the client reads its read keys from
%% the StorageServer, and records the values. The reads and the commit go
%% through the client library, `deadlift'.
%%
%% One client in three waits between the read version and the commit, for
%% 20 ms to 100 ms of the virtual clock. A recovery can end the epoch in
%% that time. After the wait the client commits through the Proxy of the
%% highest epoch that runs. These clients bring a read version of an ended
%% epoch to the Resolver of a later epoch.
%%
%% A Proxy collects the commits of a short time into one batch, so two
%% clients can receive one LSN. The client records the index of its
%% transaction in the batch.
%%
%% The table of the run has a counter, `stamp'. The client takes a stamp
%% before it asks for the read version, and with the answer to the commit.
%% The harness compares the stamps of two clients in `read_version_order'.
transaction(Tab, C, {Reader, ReaderEpoch}, Chosen, R) ->
    Keys = keys(),
    Reads = [Key || {I, Key} <- lists:enumerate(0, Keys), (R bsr I) band 1 =:= 1],
    Writes0 = [Key || {I, Key} <- lists:enumerate(4, Keys), (R bsr I) band 1 =:= 1],
    Writes =
        case Writes0 of
            [] -> [lists:nth(R rem length(Keys) + 1, Keys)];
            _ -> Writes0
        end,
    Ranges =
        case R rem 7 < 2 of
            true -> [range(R div 7)];
            false -> []
        end,
    %% The transaction of the client library. Its Proxy is the Proxy that
    %% the row `{commit_proxy, C}' names at the time of the commit. Its
    %% StorageServers are the StorageServers that the table of the run has
    %% at the time of a read. One of them can be killed, with its new
    %% StorageServer not yet started. The client library reads the ETS
    %% table of a StorageServer on the node of this process. 1 client in
    %% 5 reads by message in every case.
    Direct = R rem 5 =/= 0,
    Db = deadlift:open(#{
        proxy => fun() -> ets:lookup_element(Tab, {commit_proxy, C}, 2) end,
        storage => fun() -> storages(Tab) end,
        direct => Direct
    }),
    Tx = deadlift:create_transaction(Db),
    Start = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
    %% The client asks the Proxy `Reader' for the read version itself,
    %% because the Proxy of the commit can be another one.
    case deadlift_epoch:read_version(Reader) of
        {ok, ReadVersion} ->
            ok = deadlift:set_read_version(Tx, ReadVersion),
            Read = #{read_version => ReadVersion, reader_epoch => ReaderEpoch, start => Start},
            true = ets:insert(Tab, {{read, C}, Read}),
            case read_keys(Tab, Db, Tx, Reads, Ranges) of
                {ok, Values, Generation, Down} ->
                    true = ets:insert(Tab, {{read, C}, Read#{values => Values, ranges => Ranges, storage => Generation, down => Down, direct => Direct, local => node() =/= clients}}),
                    commit(Tab, C, Tx, Read, {Reads, Ranges}, Writes, Chosen, R);
                {error, transaction_too_old} ->
                    %% The read version is below the MVCC window of the
                    %% StorageServer.
                    true = ets:insert(Tab, {{read_too_old, C}, ReaderEpoch});
                {error, _Code} ->
                    true = ets:insert(Tab, {{no_read, C}, ReaderEpoch})
            end;
        {error, _Reason} ->
            true = ets:insert(Tab, {{no_read_version, C}, ReaderEpoch})
    end.

%% Reads the keys and the ranges through the client library, which reads
%% the ETS table of the StorageServer in this process. The result has
%% `{Key, not_found}' or `{Key, {Value, unknown}}' for each key, and for
%% each key of the workload in each range. The client library does not
%% return the LSN of the record that wrote a value. The harness finds that
%% LSN in the records of the LogServers.
read_keys(Tab, Db, Tx, Keys, Ranges) ->
    %% `Generation' is the number of StorageServers that the harness
    %% started after a kill. `Down' is the number of StorageServers that
    %% are not alive, and the number of StorageServers.
    Generation = lists:sum([G || [G] <- ets:match(Tab, {{storage, '_'}, '_', '$1', '_'})]),
    Down = {length([x || Storage <- storages(Tab), not is_process_alive(Storage)]), length(storages(Tab))},
    try
        Single = [{Key, found(deadlift:wait(deadlift:get(Tx, Key)))} || Key <- Keys],
        InRanges = lists:append([
            begin
                %% 1 range in 4 is read with `get_mapped_range/4' only,
                %% with a mapper that maps each key to itself. The
                %% transaction then has the read conflict ranges of the
                %% mapped range read and no other for the range, and the
                %% invariant `no_conflict' checks them.
                Found =
                    case ets:update_counter(Tab, range_number, 1, {range_number, 0}) rem 4 of
                        0 ->
                            _ = ets:update_counter(Tab, own_mapped_reads, 1, {own_mapped_reads, 0}),
                            Mapped = deadlift:get_mapped_range(Tx, Begin, End, {<<"k">>, <<"{K[1]}">>}),
                            Rows = [Row || {Row, _Second, [Same]} <- Mapped, Same =:= Row],
                            _ = length(Rows) =:= length(Mapped) orelse ets:insert(Tab, {{mapped_differs, {own, Begin, End}}, #{range => Range, mapped => Mapped}}),
                            Rows;
                        _ ->
                            Rows = deadlift:get_range(Tx, Begin, End),
                            ok = select(Tab, Tx, Range, Rows),
                            Rows
                    end,
                ok = mapped_range(Tab, Db, deadlift:get_read_version(Tx), Range, Found),
                %% The read returns no key outside the range, and the keys
                %% are in their order.
                [] = [Key || {Key, _Value} <- Found, not lists:member(Key, keys_in(Range))],
                Found = lists:ukeysort(1, Found),
                [{Key, found(proplists:get_value(Key, Found, not_found))} || Key <- keys_in(Range)]
            end
         || Range = {Begin, End} <- Ranges
        ]),
        {ok, Single ++ InRanges, Generation, Down}
    catch
        error:{erlfdb_error, 1007} -> {error, transaction_too_old};
        error:{erlfdb_error, Code} -> {error, Code}
    end.

%% Evaluates two match specs on the range with `deadlift:select/4', at the
%% read version of the transaction. The first one returns every row, and
%% the second one the keys of the rows of a client with an even number.
%% The results must be the results of `ets:match_spec_run/2' on `Found',
%% the result of `get_range/3'. The function records a difference in the
%% table of the run, for the invariant `select_agrees'.
select(Tab, Tx, {Begin, End}, Found) ->
    Specs = [
        [{{'$1', '$2'}, [], [{{'$1', '$2'}}]}],
        [{{'$1', {client, '$2'}}, [{'=:=', {'rem', '$2', 2}, 0}], ['$1']}]
    ],
    lists:foreach(
        fun(Spec) ->
            Expected = ets:match_spec_run(Found, ets:match_spec_compile(Spec)),
            N = ets:update_counter(Tab, selects, 1, {selects, 0}),
            Limit = N rem 3 + 1,
            First = lists:sublist(Expected, Limit),
            case {deadlift:select(Tx, Begin, End, Spec), deadlift:select(Tx, Begin, End, Spec, [{limit, Limit}])} of
                {Expected, First} -> ok;
                Other -> true = ets:insert(Tab, {{select_differs, N}, #{range => {Begin, End}, expected => Expected, limit => Limit, selected => Other}})
            end
        end,
        Specs
    ).

%% Reads the range with `deadlift:get_mapped_range/4' and two mappers, at
%% the read version of the transaction:
%%
%% <ul>
%% <li>The mapper `{<<"k">>, <<"{K[1]}">>}' maps each key to itself, so the
%%   second read of a row must return the row.</li>
%% <li>The mapper `{<<"k">>, <<"{...}">>}' maps each key to every key of
%%   the workload, so the second read of a row must return the result of
%%   `get_range/3' on every key.</li>
%% </ul>
%%
%% `Found' is the result of `get_range/3' on the range. The reads use a
%% second transaction with the same read version, so that the transaction
%% of the client gets no read conflict range from them. The function
%% records a difference in the table of the run, for the invariant
%% `mapped_range_agrees'.
mapped_range(Tab, Db, ReadVersion, {Begin, End}, Found) ->
    Tx = {deadlift_transaction, Table} = deadlift:create_transaction(Db),
    try
        ok = deadlift:set_read_version(Tx, ReadVersion),
        Every = {From, To} = deadlift_mapper:range(range, deadlift_tuple:pack({<<"k">>})),
        All = deadlift:get_range(deadlift:snapshot(Tx), From, To),
        Cases = [
            {{<<"k">>, <<"{K[1]}">>}, [{Row, deadlift_mapper:range(point, Key), [Row]} || Row = {Key, _Value} <- Found]},
            {{<<"k">>, <<"{...}">>}, [{Row, Every, All} || Row <- Found]}
        ],
        lists:foreach(
            fun({Mapper, Expected}) ->
                N = ets:update_counter(Tab, mapped_reads, 1, {mapped_reads, 0}),
                Limit = N rem 3 + 1,
                First = lists:sublist(Expected, Limit),
                case {deadlift:get_mapped_range(Tx, Begin, End, Mapper), deadlift:get_mapped_range(Tx, Begin, End, Mapper, [{limit, Limit}])} of
                    {Expected, First} -> ok;
                    Other -> true = ets:insert(Tab, {{mapped_differs, N}, #{range => {Begin, End}, mapper => Mapper, expected => Expected, limit => Limit, mapped => Other}})
                end
            end,
            Cases
        )
    after
        ets:delete(Table)
    end.

%% The StorageServers that the table of the run has.
storages(Tab) ->
    [Storage || [_Slot, Storage] <- lists:sort(ets:match(Tab, {{storage, '$1'}, '$2', '_', '_'}))].

found(not_found) -> not_found;
found(Value) -> {Value, unknown}.

%% The keys of the workload in a range.
keys_in({Begin, End}) ->
    [Key || Key <- keys(), Key >= Begin, Key < End].

%% One of the 10 ranges whose bounds are the 4 keys and the key 5.
range(R) ->
    Bounds = keys() ++ [key(5)],
    Pairs = [{Begin, End} || Begin <- Bounds, End <- Bounds, Begin < End],
    lists:nth(R rem length(Pairs) + 1, Pairs).

commit(Tab, C, Tx, #{read_version := ReadVersion, reader_epoch := ReaderEpoch, start := Start}, {Reads, Ranges}, Writes, Chosen, R) ->
    {Proxy, Epoch} =
        case R rem 3 of
            0 ->
                ok = timer:sleep(20 * (R rem 5 + 1)),
                latest_proxy(Tab, Chosen);
            _ ->
                Chosen
        end,
    true = ets:insert(Tab, [{{invoked, C}, Epoch}, {{commit_proxy, C}, Proxy}]),
    Cleared =
        case R rem 11 < 2 of
            true -> [range(R div 11)];
            false -> []
        end,
    Clears = lists:append([keys_in(Range) || Range <- Cleared]) -- Writes,
    true = ets:insert(Tab, {{attempt, C}, Clears}),
    lists:foreach(fun({Begin, End}) -> ok = deadlift:clear_range(Tx, Begin, End) end, Cleared),
    lists:foreach(fun(Key) -> ok = deadlift:set(Tx, Key, {client, C}) end, Writes),
    ok = own_writes(Tab, C, Tx, ReadVersion, Writes, Clears),
    try deadlift:wait(deadlift:commit(Tx)) of
        ok ->
            LSN = deadlift:get_committed_version(Tx),
            <<LSN:64, Index:16>> = deadlift:get_versionstamp(Tx),
            End = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
            true = ets:insert(Tab, {{txn, C}, #{
                read_version => ReadVersion,
                reader_epoch => ReaderEpoch,
                %% The read keys and the keys of the read ranges.
                reads => lists:usort(Reads ++ lists:append([keys_in(Range) || Range <- Ranges])),
                writes => Writes,
                %% The keys that the transaction cleared and did not write.
                clears => Clears,
                cleared => Cleared,
                ranges => Ranges,
                %% The position of the transaction in its batch.
                index => Index,
                start => Start,
                'end' => End
            }}),
            true = ets:insert(Tab, {{ack, C}, Epoch, LSN})
    catch
        %% The error codes of FoundationDB. See `deadlift'.
        error:{erlfdb_error, 1021} ->
            true = ets:insert(Tab, {{unknown, C}, Epoch}),
            after_error(Tab, Tx, 1021);
        error:{erlfdb_error, 1020} ->
            true = ets:insert(Tab, {{rejected, C}, not_committed}),
            after_error(Tab, Tx, 1020);
        error:{erlfdb_error, 1007} ->
            true = ets:insert(Tab, {{rejected, C}, transaction_too_old}),
            after_error(Tab, Tx, 1007)
    end.

%% Calls `deadlift:on_error/2' after a failed commit, as
%% `deadlift:transactional/2' does. The call waits for the backoff on the
%% virtual clock, and leaves a transaction without a read version and
%% without a mutation. The client does not run the transaction again.
after_error(Tab, Tx = {deadlift_transaction, Table}, Code) ->
    ok = deadlift:on_error(Tx, Code),
    _ = ets:update_counter(Tab, on_errors, 1, {on_errors, 0}),
    case deadlift:is_read_only(Tx) andalso ets:lookup(Table, read_version) =:= [] of
        true -> true;
        false -> ets:insert(Tab, {{own_writes_differ, {on_error, Code}}, #{code => Code}})
    end.

%% Checks the reads of a transaction that has written and cleared keys,
%% before the commit. `Writes' are the keys that the transaction has set,
%% and `Clears' are the keys that it has cleared and not set.
%%
%% <ul>
%% <li>`get/2' returns the value of the transaction for a key that it
%%   set, and `not_found' for a key that it cleared.</li>
%% <li>`get_range/3' and `select/4' on every key return the stored rows
%%   at the read version with the sets and the clears of the transaction.
%%   The stored rows are read through a second transaction with the same
%%   read version.</li>
%% <li>`get_mapped_range/4' on every key raises 2039, because the
%%   transaction has written a key of the range.</li>
%% </ul>
%%
%% The reads are reads of a snapshot, so the transaction gets no read
%% conflict range from them. The function records a difference in the
%% table of the run, for the invariant `own_writes'. It checks nothing
%% when a read fails with an error code.
own_writes(Tab, C, Tx, ReadVersion, Writes, Clears) ->
    [{db, Servers}] = ets:lookup(element(2, Tx), db),
    Plain = {deadlift_transaction, Table} = deadlift:create_transaction({deadlift_database, Servers}),
    Snapshot = deadlift:snapshot(Tx),
    {Begin, End} = {key(1), key(5)},
    try
        ok = deadlift:set_read_version(Plain, ReadVersion),
        Stored = deadlift:get_range(deadlift:snapshot(Plain), Begin, End),
        Expected = lists:ukeymerge(1, [{Key, {client, C}} || Key <- lists:sort(Writes)], [Row || Row = {Key, _} <- Stored, not lists:member(Key, Clears)]),
        All = [{{'$1', '$2'}, [], [{{'$1', '$2'}}]}],
        Mapped =
            try deadlift:get_mapped_range(Tx, Begin, End, {<<"k">>, <<"{K[1]}">>}) of
                Rows -> {ok, Rows}
            catch
                error:{erlfdb_error, 2039} -> reads_your_writes
            end,
        Got = #{
            sets => [deadlift:get(Snapshot, Key) || Key <- Writes],
            clears => [deadlift:get(Snapshot, Key) || Key <- Clears],
            range => deadlift:get_range(Snapshot, Begin, End),
            select => deadlift:select(Snapshot, Begin, End, All),
            limit => deadlift:select(Snapshot, Begin, End, All, [{limit, 2}]),
            mapped => Mapped
        },
        Want = #{
            sets => [{client, C} || _ <- Writes],
            clears => [not_found || _ <- Clears],
            range => Expected,
            select => Expected,
            limit => lists:sublist(Expected, 2),
            mapped => reads_your_writes
        },
        _ = ets:update_counter(Tab, own_write_checks, 1, {own_write_checks, 0}),
        _ = Got =:= Want orelse ets:insert(Tab, {{own_writes_differ, C}, #{got => Got, expected => Want, writes => Writes, clears => Clears}}),
        ok
    catch
        error:{erlfdb_error, _Code} -> ok
    after
        ets:delete(Table)
    end.

%% The Proxy of the highest epoch that runs, or `Default'.
latest_proxy(Tab, Default) ->
    Running = [{Epoch, Proxy} || [Proxy, Epoch] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'}), is_process_alive(Proxy)],
    case lists:sort(Running) of
        [] ->
            Default;
        Sorted ->
            {Epoch, Proxy} = lists:last(Sorted),
            {Proxy, Epoch}
    end.

%% The keys of the workload. A small number of keys gives many conflicts.
%% The keys are packed tuples, so that the mapper of
%% `deadlift:get_mapped_range/4' can read them.
keys() -> [key(I) || I <- [1, 2, 3, 4]].

key(I) -> deadlift_tuple:pack({<<"k">>, I}).

%% The simulated nodes. `N' is the number of the `recover' operation.
sequencer_node(N) -> list_to_atom("sequencer_" ++ integer_to_list(N)).

worker_node(N) -> list_to_atom("worker_" ++ integer_to_list(N)).



%% The node of the StorageServer of the place `Slot' that the operation
%% `N' started. The first StorageServer of a place has the number 0.
storage_node({Slot, N}) -> list_to_atom(lists:concat(["storage_", Slot, "_", N])).

log_node({Epoch, Index}) ->
    list_to_atom("log_" ++ integer_to_list(Epoch) ++ "_" ++ integer_to_list(Index)).

%% An exit reason without the pid of the call, so that the table holds the
%% same term in every run of a seed.
reason({Reason, {gen_server, call, _}}) -> Reason;
reason({Reason, {eta_net, call, _}}) -> Reason;
reason(Reason) -> Reason.
