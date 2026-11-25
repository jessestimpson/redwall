%% The processes that perform the operations of the simulation. See
%% `redwall_dst'.
%%
%% Each function runs in a process that the scheduler of eta steps. A
%% function records its result in the table of the run. The harness reads
%% the table in its checks.
-module(redwall_dst_client).

-include("redwall_eta.hrl").

-export([recover/4, transaction/5, watch/3, join/2, start_log/3]).
-export([worker/5, force_recovery/1, worker_node/1, register_storage/4]).
-export([sequencer_node/1, log_node/1, storage_node/1, keys/0]).
-export([coordinators/1, coordinator_ids/0, coordinator_node/1, coordinator/3, form_cluster/2]).

%% Starts the process of a new epoch and waits for the end of its
%% recovery. This is the work of a ClusterController. The runs without the
%% config key `controller' use this function.
%%
%% `N' is the number of the operation. `Mode' selects a planted defect.
recover(Tab, N, CState, Mode) ->
    ok = join(Tab, Mode),
    %% A recovery waits for the peace cluster only until the first read
    %% of the run has succeeded. A later recovery can fail because the
    %% cluster has no leader.
    _ = ets:member(Tab, cluster_ready) orelse await_cluster(Tab, CState, 200),
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
        coordinators => coordinators(CState),
        logs => 3,
        replication => 2,
        lock_timeout_ms => 200,
        recruit => Recruit
    },
    %% The process starts on the node of this process.
    {ok, Proxy} = redwall_epoch:start(Options),
    true = ets:insert(Tab, {{sequencer, N}, Proxy}),
    try redwall_epoch:recovered(Proxy) of
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
    Options0 = #{coordinators => coordinators(CState), class => Class, roles => Roles, poll_ms => 200, cstate_timeout_ms => 500},
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
                        step_down => Mode =/= no_step_down,
                        pop_limit => Mode =/= pop_no_limit
                    }
                };
            false ->
                Options0
        end,
    {ok, Worker} = redwall_worker:start(Options),
    true = ets:insert(Tab, {{worker, Node}, Worker, Class}),
    case redwall_worker:info() of
        #{candidate := Controller} when is_pid(Controller) -> true = ets:insert(Tab, {{controller, Node}, Controller});
        _ -> true
    end.

%% Records the StorageServer `Storage' of the place `Slot' in the list of
%% the coordinated state, under the name of the place. A run with the pop
%% uses this function. A ClusterController sends a pop when every
%% StorageServer of that list has answered. The function tries `Tries'
%% times, because the coordinated state can give no answer.
register_storage(_CState, _Slot, _Storage, 0) ->
    ok;
register_storage(CState, Slot, Storage, Tries) ->
    Name = list_to_atom(lists:concat(["storage_", Slot])),
    try redwall_cstate:storage(coordinators(CState), Name, Storage, 500) of
        ok -> ok;
        _ -> register_storage(CState, Slot, Storage, Tries - 1)
    catch
        _:_ -> register_storage(CState, Slot, Storage, Tries - 1)
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
            log -> fun redwall_log:start/1;
            proxy -> fun redwall_epoch:start/1
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
        fun([Controller]) -> redwall_cluster_controller:force_recovery(Controller) end,
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
    case redwall_log:start(Log) of
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
%% through the client library, `redwall'.
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
    Db = redwall:open(#{
        proxy => fun() -> ets:lookup_element(Tab, {commit_proxy, C}, 2) end,
        storage => fun() -> storages(Tab) end,
        direct => Direct
    }),
    Tx = redwall:create_transaction(Db),
    Start = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
    %% The client asks the Proxy `Reader' for the read version itself,
    %% because the Proxy of the commit can be another one.
    case redwall_epoch:read_version(Reader) of
        {ok, ReadVersion} ->
            ok = redwall:set_read_version(Tx, ReadVersion),
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

%% Creates one watch, and waits for its future. `C' is the number of the
%% client, and `R' selects the key and the kind of the transaction. See
%% "Runs with watches" in `redwall_dst'.
%%
%% <ul>
%% <li>2 clients in 3 set the key to `{watch, C}' and create the watch in
%%   the same transaction. The other clients read the key and create the
%%   watch, in a transaction without a mutation.</li>
%% <li>The Proxy of the database is the Proxy of the highest epoch that
%%   runs, at the time of each request. The StorageServers are the
%%   StorageServers of the table of the run at that time.</li>
%% <li>After a commit, the client records the row `{watch, C}' with the
%%   key, the value and the version of the watch. When the future is
%%   ready, it records the row `{watch_fired, C}', and reads the key in
%%   a new transaction. The value of the watch is then a violation of
%%   `watch_change'.</li>
%% <li>After a failed commit, the future must have the error of the
%%   commit (`watch_error').</li>
%% </ul>
watch(Tab, C, R) ->
    [{watches, Kind}] = ets:lookup(Tab, watches),
    [{mode, Mode}] = ets:lookup(Tab, mode),
    Key = watch_key(R rem 2 + 1),
    Db = redwall:open(#{
        proxy => fun() ->
            case latest_proxy(Tab, none) of
                {Proxy, _Epoch} -> Proxy;
                none -> undefined
            end
        end,
        storage => fun() -> storages(Tab) end,
        direct => R rem 5 =/= 0,
        watch_call_timeout_ms =>
            case Kind of
                live -> infinity;
                true -> 1000
            end,
        watch_verify => Mode =/= watch_no_verify
    }),
    Tx = redwall:create_transaction(Db),
    Sets = R rem 3 =/= 0,
    try
        Seen =
            case Sets of
                true ->
                    ok = redwall:set(Tx, Key, {watch, C}),
                    {watch, C};
                false ->
                    redwall:get(Tx, Key)
            end,
        {Seen, redwall:watch(Tx, Key)}
    of
        {Value, Future} ->
            try redwall:commit(Tx) of
                ok ->
                    Version =
                        case redwall:get_committed_version(Tx) of
                            -1 -> redwall:get_read_version(Tx);
                            LSN -> LSN
                        end,
                    true = ets:insert(Tab, {{watch, C}, #{key => Key, value => Value, version => Version, set => Sets}}),
                    watch_wait(Tab, C, Db, Key, Value, Future, Kind)
            catch
                error:{erlfdb_error, Code} ->
                    Got =
                        try
                            redwall:wait(Future, [{timeout, 1000}])
                        catch
                            error:{erlfdb_error, FutureCode} -> FutureCode;
                            error:{timeout, _} -> timeout
                        end,
                    case Got of
                        Code -> ets:insert(Tab, {{watch_error, C}, Code});
                        _ -> ets:insert(Tab, {{watch_violation, C}, #{property => watch_error, client => C, commit => Code, future => Got}})
                    end
            end
    catch
        error:{erlfdb_error, Code} ->
            %% The read of the key failed.
            ets:insert(Tab, {{watch_no_read, C}, Code})
    end.

watch_wait(Tab, C, Db, Key, Value, Future, Kind) ->
    Timeout =
        case Kind of
            live -> infinity;
            true -> 3000
        end,
    try redwall:wait(Future, [{timeout, Timeout}]) of
        ok ->
            true = ets:insert(Tab, {{watch_fired, C}, true}),
            case watch_reread(Db, Key, 10) of
                {ok, Value} -> ets:insert(Tab, {{watch_violation, C}, #{property => watch_change, client => C, key => Key, value => Value}});
                {ok, _Other} -> ets:insert(Tab, {{watch_checked, C}, true});
                error -> true
            end
    catch
        error:{timeout, _} ->
            ok = redwall:cancel(Future),
            ets:insert(Tab, {{watch_pending, C}, true});
        error:{erlfdb_error, Code} ->
            ets:insert(Tab, {{watch_violation, C}, #{property => watch_error, client => C, commit => ok, future => Code}})
    end.

%% Reads the key in a new transaction, for at most `Tries' attempts.
watch_reread(_Db, _Key, 0) ->
    error;
watch_reread(Db, Key, Tries) ->
    Tx = {redwall_transaction, Table} = redwall:create_transaction(Db),
    try
        {ok, redwall:get(redwall:snapshot(Tx), Key)}
    catch
        error:{erlfdb_error, _Code} ->
            ok = timer:sleep(50),
            watch_reread(Db, Key, Tries - 1)
    after
        ets:delete(Table)
    end.

%% The keys of the watches. They are not keys of the transactions of the
%% workload, and no read range of the workload contains them.
watch_key(I) -> redwall_tuple:pack({<<"w">>, I}).

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
        Single = [{Key, found(redwall:wait(redwall:get(Tx, Key)))} || Key <- Keys],
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
                            Mapped = redwall:get_mapped_range(Tx, Begin, End, {<<"k">>, <<"{K[1]}">>}),
                            Rows = [Row || {Row, _Second, [Same]} <- Mapped, Same =:= Row],
                            _ = length(Rows) =:= length(Mapped) orelse ets:insert(Tab, {{mapped_differs, {own, Begin, End}}, #{range => Range, mapped => Mapped}}),
                            Rows;
                        _ ->
                            Rows = redwall:get_range(Tx, Begin, End),
                            ok = select(Tab, Tx, Range, Rows),
                            Rows
                    end,
                ok = mapped_range(Tab, Db, redwall:get_read_version(Tx), Range, Found),
                ok = range_options(Tab, Db, redwall:get_read_version(Tx), Range, Found),
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

%% Evaluates two match specs on the range with `redwall:select/4', at the
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
            case {redwall:select(Tx, Begin, End, Spec), redwall:select(Tx, Begin, End, Spec, [{limit, Limit}])} of
                {Expected, First} -> ok;
                Other -> true = ets:insert(Tab, {{select_differs, N}, #{range => {Begin, End}, expected => Expected, limit => Limit, selected => Other}})
            end
        end,
        Specs
    ).

%% Reads the range with `redwall:get_mapped_range/4' and two mappers, at
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
    Tx = {redwall_transaction, Table} = redwall:create_transaction(Db),
    try
        ok = redwall:set_read_version(Tx, ReadVersion),
        Every = {From, To} = redwall_mapper:range(range, redwall_tuple:pack({<<"k">>})),
        All = redwall:get_range(redwall:snapshot(Tx), From, To),
        Cases = [
            {{<<"k">>, <<"{K[1]}">>}, [{Row, redwall_mapper:range(point, Key), [Row]} || Row = {Key, _Value} <- Found]},
            {{<<"k">>, <<"{...}">>}, [{Row, Every, All} || Row <- Found]}
        ],
        lists:foreach(
            fun({Mapper, Expected}) ->
                N = ets:update_counter(Tab, mapped_reads, 1, {mapped_reads, 0}),
                Limit = N rem 3 + 1,
                First = lists:sublist(Expected, Limit),
                case {redwall:get_mapped_range(Tx, Begin, End, Mapper), redwall:get_mapped_range(Tx, Begin, End, Mapper, [{limit, Limit}])} of
                    {Expected, First} -> ok;
                    Other -> true = ets:insert(Tab, {{mapped_differs, N}, #{range => {Begin, End}, mapper => Mapper, expected => Expected, limit => Limit, mapped => Other}})
                end
            end,
            Cases
        )
    after
        ets:delete(Table)
    end.

%% Reads the range with the options `reverse' and `limit' of
%% `redwall:get_range/4', and resolves two key selectors with
%% `redwall:get_key/2', at the read version of the transaction. `Found'
%% is the result of `get_range/3' on the range. The reads use a second
%% transaction with the same read version, in a database with the kind
%% `binary', which has key selectors. The function records a difference
%% in the table of the run, for the invariant `range_options_agree'.
range_options(Tab, {redwall_database, Servers}, ReadVersion, {Begin, End}, Found) ->
    Tx = {redwall_transaction, Table} = redwall:create_transaction({redwall_database, Servers#{keys => binary}}),
    try
        ok = redwall:set_read_version(Tx, ReadVersion),
        N = ets:update_counter(Tab, range_option_reads, 1, {range_option_reads, 0}),
        Limit = N rem 3 + 1,
        Backward = lists:reverse(Found),
        %% Every key of the database, for the keys of the selectors. A
        %% run with watches also has the keys of the watches.
        Keys = [Key || {Key, _Value} <- redwall:get_range(redwall:snapshot(Tx), <<>>, <<255>>)],
        Expected = {
            Backward,
            lists:sublist(Found, Limit),
            lists:sublist(Backward, Limit),
            %% The first key at or above `Begin', and the last key below
            %% `End', or a bound of the keys of the kind `binary'.
            hd([Key || Key <- Keys, Key >= Begin] ++ [<<255>>]),
            lists:last([<<>> | [Key || Key <- Keys, Key < End]])
        },
        Read = {
            redwall:get_range(Tx, Begin, End, [{reverse, true}]),
            redwall:get_range(Tx, Begin, End, [{limit, Limit}]),
            redwall:get_range(Tx, Begin, End, [{limit, Limit}, {reverse, true}]),
            redwall:get_key(Tx, redwall_key:first_greater_or_equal(Begin)),
            redwall:get_key(Tx, redwall_key:last_less_than(End))
        },
        _ = Read =:= Expected orelse ets:insert(Tab, {{range_options_differ, N}, #{range => {Begin, End}, limit => Limit, expected => Expected, read => Read}}),
        ok
    after
        ets:delete(Table)
    end.

%% The coordinators for the options of a process. `CState' is the
%% coordinated state of the run: `{local, Pid}' for a
%% `redwall_cstate_local' process, or `{peace, Tab, Ids}' for a peace
%% cluster with the servers `Ids'.
coordinators({local, Pid}) -> {local, Pid};
coordinators({peace, _Tab, Ids}) -> Ids.

%% The servers of the peace cluster of a run, each on a node of its own.
coordinator_ids() -> [{list_to_atom("redwall_coordinator_" ++ integer_to_list(I)), coordinator_node(I)} || I <- [1, 2, 3]].

coordinator_node(I) -> list_to_atom("coordinator_" ++ integer_to_list(I)).

%% Starts the coordinator `Id' on the node of the caller, and waits. The
%% server is linked to this process, so a kill of this process stops the
%% server. The configuration is the configuration of
%% `redwall_coordinator', with the machine of the simulation in the
%% place of `redwall_cstate'. The coordinator keeps its data in the
%% directory `Dir', on the simulated disk of eta, so a killed coordinator
%% starts again with its data. The snapshot interval of 8 values and the
%% chunk of 64 bytes make a coordinator write a checkpoint in several
%% steps in each run.
coordinator(Id, Dir, Mode) ->
    Wal =
        case Mode of
            %% The planted defect: the coordinator never syncs its files.
            unsynced_coordinators -> #{checkpoint_chunk_bytes => 64, io => redwall_dst_nosync_io};
            _ -> #{checkpoint_chunk_bytes => 64}
        end,
    {ok, Config0} = redwall_coordinator:config(#{
        id => Id,
        dir => Dir,
        wal => Wal,
        election_timeout_ms => 50,
        heartbeat_ms => 10,
        snapshot_interval => 8
    }),
    Config = Config0#{machine := {module, redwall_dst_machine, []}},
    case peace_server:start_link(Config) of
        {ok, _Pid} ->
            receive
                stop -> ok
            end;
        {error, _Reason} ->
            ok
    end.

%% Forms the peace cluster. The servers start in other processes at the
%% same time, and a message can be lost, so the function tries again
%% until every server has the configuration. It then records the row
%% `cluster_formed' in the table of the run. The harness kills no server
%% of the cluster before that, because `peace:form_cluster/3' needs an
%% answer from every server.
form_cluster(Tab, Ids) ->
    case peace:form_cluster(Ids, majority, 500) of
        {ok, _Formed, []} ->
            true = ets:insert(Tab, {cluster_formed, true}),
            ok;
        _NotRunningOrNotFormed ->
            ok = timer:sleep(20),
            form_cluster(Tab, Ids)
    end.

%% Waits until a read of the peace cluster succeeds, for at most
%% `Attempts' reads, and then records the row `cluster_ready' in the table
%% of the run. The first `recover' operation starts the servers of the
%% cluster and the first recovery at the same time. Without the wait, the
%% first recovery fails in 99 runs of 100 with `no_leader' or `noproc',
%% and the run has no epoch until the next `recover' operation. A
%% ClusterController recruits again after a failed recovery, so a run
%% with ClusterControllers does not need the wait.
await_cluster(_Tab, {local, _Pid}, _Attempts) ->
    ok;
await_cluster(_Tab, _CState, 0) ->
    ok;
await_cluster(Tab, CState = {peace, _Tab, Ids}, Attempts) ->
    case redwall_cstate:read(Ids, 200) of
        {ok, _State} ->
            true = ets:insert(Tab, {cluster_ready, true}),
            ok;
        {error, _Reason} ->
            ok = timer:sleep(20),
            await_cluster(Tab, CState, Attempts - 1)
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
    lists:foreach(fun({Begin, End}) -> ok = redwall:clear_range(Tx, Begin, End) end, Cleared),
    lists:foreach(fun(Key) -> ok = redwall:set(Tx, Key, {client, C}) end, Writes),
    ok = own_writes(Tab, C, Tx, ReadVersion, Writes, Clears),
    try redwall:wait(redwall:commit(Tx)) of
        ok ->
            LSN = redwall:get_committed_version(Tx),
            <<LSN:64, Index:16>> = redwall:get_versionstamp(Tx),
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
        %% The error codes of FoundationDB. See `redwall'.
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

%% Calls `redwall:on_error/2' after a failed commit, as
%% `redwall:transactional/2' does. The call waits for the backoff on the
%% virtual clock, and leaves a transaction without a read version and
%% without a mutation. The client does not run the transaction again.
after_error(Tab, Tx = {redwall_transaction, Table}, Code) ->
    ok = redwall:on_error(Tx, Code),
    _ = ets:update_counter(Tab, on_errors, 1, {on_errors, 0}),
    case redwall:is_read_only(Tx) andalso ets:lookup(Table, read_version) =:= [] of
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
    Plain = {redwall_transaction, Table} = redwall:create_transaction({redwall_database, Servers}),
    Snapshot = redwall:snapshot(Tx),
    {Begin, End} = {key(1), key(5)},
    try
        ok = redwall:set_read_version(Plain, ReadVersion),
        Stored = redwall:get_range(redwall:snapshot(Plain), Begin, End),
        Expected = lists:ukeymerge(1, [{Key, {client, C}} || Key <- lists:sort(Writes)], [Row || Row = {Key, _} <- Stored, not lists:member(Key, Clears)]),
        All = [{{'$1', '$2'}, [], [{{'$1', '$2'}}]}],
        Mapped =
            try redwall:get_mapped_range(Tx, Begin, End, {<<"k">>, <<"{K[1]}">>}) of
                Rows -> {ok, Rows}
            catch
                error:{erlfdb_error, 2039} -> reads_your_writes
            end,
        Got = #{
            sets => [redwall:get(Snapshot, Key) || Key <- Writes],
            clears => [redwall:get(Snapshot, Key) || Key <- Clears],
            range => redwall:get_range(Snapshot, Begin, End),
            select => redwall:select(Snapshot, Begin, End, All),
            limit => redwall:select(Snapshot, Begin, End, All, [{limit, 2}]),
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
%% `redwall:get_mapped_range/4' can read them.
keys() -> [key(I) || I <- [1, 2, 3, 4]].

key(I) -> redwall_tuple:pack({<<"k">>, I}).

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
