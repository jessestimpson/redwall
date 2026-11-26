%% The processes that perform the operations of the simulation. See
%% `redwall_dst'.
%%
%% Each function runs in a process that the scheduler of eta steps. A
%% function records its result in the table of the run. The harness reads
%% the table in its checks.
-module(redwall_dst_client).

-include("redwall_eta.hrl").

-export([recover/4, transaction/5, watch/3, late_watch/3, late_change/4, watch_key/1, join/2, start_log/3]).
-export([worker/5, force_recovery/1, worker_node/1, register_storage/4]).
-export([sequencer_node/1, log_node/1, storage_node/1, keys/0, canon/1]).
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
            {log, no_push_order} -> Options0#{order => false};
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
        equal_keys => Mode =/= no_equal_keys,
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
    case redwall_log:start(Log#{order => Mode =/= no_push_order}) of
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
%%
%% In a run with the config `keys => mixed', the 4 keys are terms of 4
%% types, and `R' selects the form of the number key and the form of the
%% tuple key of the client. See `keys/2' and "Runs with mixed keys" in
%% `redwall_dst'. The client calls the client library with its forms of
%% the keys. Every key that the client records in the table of the run is
%% the canonical form of the key (`canon/1'), so the harness compares two
%% keys that are equal and not identical as one key.
transaction(Tab, C, {Reader, ReaderEpoch}, Chosen, R) ->
    Space = {_Kind, Keys, _Bound} = key_space(Tab, R),
    Reads = [Key || {I, Key} <- lists:enumerate(0, Keys), (R bsr I) band 1 =:= 1],
    Writes0 = [Key || {I, Key} <- lists:enumerate(4, Keys), (R bsr I) band 1 =:= 1],
    Writes =
        case Writes0 of
            [] -> [lists:nth(R rem length(Keys) + 1, Keys)];
            _ -> Writes0
        end,
    Ranges =
        case R rem 7 < 2 of
            true -> [range(R div 7, Space)];
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
            case read_keys(Tab, Db, Tx, Reads, Ranges, Space) of
                {ok, Values, Generation, Down} ->
                    true = ets:insert(Tab, {{read, C}, Read#{values => Values, ranges => Ranges, storage => Generation, down => Down, direct => Direct, local => node() =/= clients}}),
                    ok = count_forms(Tab, read, Reads, Space),
                    %% What the client observed, for the external checker.
                    %% See `redwall_dst_history'.
                    ReadEnd = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
                    true = ets:insert(Tab, {{history, C}, #{start => Start, read_end => ReadEnd, reads => Values, outcome => read}}),
                    commit(Tab, C, Tx, Read, {Reads, Ranges}, Writes, Chosen, R, Space);
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
%% <li>In a run without the config `watch_clears', 2 clients in 3 set the
%%   key to `{watch, C}' and create the watch in the same transaction. The
%%   other clients read the key and create the watch, in a transaction
%%   without a mutation.</li>
%% <li>In a run with the config `watch_clears', 1 client in 3 sets the
%%   key to `{watch, C}', 1 client in 3 reads the key, and 1 client in 3
%%   clears the key. Each client creates the watch in the same
%%   transaction. The value of the watch of a client that clears the key
%%   is `not_found'.</li>
%% <li>The Proxy of the database is the Proxy of the highest epoch that
%%   runs, at the time of each request. The StorageServers are the
%%   StorageServers of the table of the run at that time.</li>
%% <li>After a commit, the client records the row `{watch, C}' with the
%%   key, the value, the version and the kind of the watch. When the
%%   future is ready, the client records the row `{watch_fired, C}', and
%%   reads the key in a new transaction.</li>
%% <li>In a run without the config `watch_clears', the value of the watch
%%   as the result of that read is a violation of `watch_change'. The
%%   client records the violation.</li>
%% <li>In a run with the config `watch_clears', the client records the
%%   value and the read version of that read in the row
%%   `{watch_reread, C}'. The harness decides `watch_change' from the row
%%   and from the records of the LogServers.</li>
%% <li>After a failed commit, the future must have the error of the
%%   commit (`watch_error').</li>
%% </ul>
watch(Tab, C, R) ->
    [{watches, Kind}] = ets:lookup(Tab, watches),
    Key = watch_key(R rem 2 + 1),
    Db = watch_db(Tab, R rem 5 =/= 0, Kind),
    Tx = redwall:create_transaction(Db),
    Act = watch_kind(Tab, R),
    try
        watch_create(Tx, Key, C, Act)
    of
        {Value, Future} ->
            true = watcher(Tab, C, Future),
            try redwall:commit(Tx) of
                ok ->
                    Version =
                        case redwall:get_committed_version(Tx) of
                            -1 -> redwall:get_read_version(Tx);
                            LSN -> LSN
                        end,
                    true = ets:insert(Tab, {{watch, C}, watch_row(Tab, Key, Value, Version, Act)}),
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

%% The database of a client with a watch. `Kind' is `true' or `live'.
%% With `live', a request of a watch has no time limit. With the planted
%% defect `watch_no_verify', the client does not read the key after the
%% answer of a StorageServer.
watch_db(Tab, Direct, Kind) ->
    [{mode, Mode}] = ets:lookup(Tab, mode),
    redwall:open(#{
        proxy => fun() ->
            case latest_proxy(Tab, none) of
                {Proxy, _Epoch} -> Proxy;
                none -> undefined
            end
        end,
        storage => fun() -> storages(Tab) end,
        direct => Direct,
        watch_call_timeout_ms =>
            case Kind of
                live -> infinity;
                true -> 1000
            end,
        watch_verify => Mode =/= watch_no_verify
    }).

%% Records the process of the watch of a future in the table of the run.
%% The process is not linked to the client. The harness stops it at the
%% end of the run.
watcher(Tab, C, {redwall_future, _Reference, {Pid, _Status, _Count}}) ->
    Count = ets:update_counter(Tab, watchers, 1, {watchers, 0}),
    ets:insert(Tab, {{watcher, C, Count}, Pid}).

%% The row `{watch, C}' of a committed watch: the key, the value and the
%% version of the watch, and whether the transaction set the key. In a
%% run with the config `watch_clears', the row also has the kind of the
%% transaction.
watch_row(Tab, Key, Value, Version, Act) ->
    Row = #{key => Key, value => Value, version => Version, set => Act =:= set},
    case ets:member(Tab, watch_clears) of
        true -> Row#{kind => Act};
        false -> Row
    end.

%% The kind of the transaction of a watch: `set', `read' or `clear'. A
%% run without the config `watch_clears' has no kind `clear'.
watch_kind(Tab, R) ->
    case {ets:member(Tab, watch_clears), R rem 3} of
        {_, 0} -> read;
        {true, 2} -> clear;
        {_, _} -> set
    end.

%% Sets, reads or clears the key, and creates the watch in the same
%% transaction. The result has the value of the watch and the future. The
%% value is `not_found' for a key without a value.
watch_create(Tx, Key, C, Act) ->
    Seen =
        case Act of
            set ->
                ok = redwall:set(Tx, Key, {watch, C}),
                {watch, C};
            read ->
                redwall:get(Tx, Key);
            clear ->
                ok = redwall:clear(Tx, Key),
                not_found
        end,
    {Seen, redwall:watch(Tx, Key)}.

watch_wait(Tab, C, Db, Key, Value, Future, Kind) ->
    Timeout =
        case Kind of
            live -> infinity;
            true -> 3000
        end,
    try redwall:wait(Future, [{timeout, Timeout}]) of
        ok ->
            true = ets:insert(Tab, {{watch_fired, C}, true}),
            case {watch_reread(Db, Key, 10), ets:member(Tab, watch_clears)} of
                {{ok, Seen, ReadVersion}, true} ->
                    %% The harness decides `watch_change'.
                    ets:insert(Tab, {{watch_reread, C}, #{value => Seen, read_version => ReadVersion}});
                {{ok, Value, _ReadVersion}, false} ->
                    ets:insert(Tab, {{watch_violation, C}, #{property => watch_change, client => C, key => Key, value => Value}});
                {{ok, _Other, _ReadVersion}, false} ->
                    ets:insert(Tab, {{watch_checked, C}, true});
                {error, _Clears} ->
                    true
            end
    catch
        error:{timeout, _} ->
            ok = redwall:cancel(Future),
            ets:insert(Tab, {{watch_pending, C}, true});
        error:{erlfdb_error, Code} ->
            ets:insert(Tab, {{watch_violation, C}, #{property => watch_error, client => C, commit => ok, future => Code}})
    end.

%% Reads the key in a new transaction, for at most `Tries' attempts. The
%% result has the value, or `not_found', and the read version of the
%% read.
watch_reread(_Db, _Key, 0) ->
    error;
watch_reread(Db, Key, Tries) ->
    Tx = {redwall_transaction, Table} = redwall:create_transaction(Db),
    try
        Seen = redwall:get(redwall:snapshot(Tx), Key),
        {ok, Seen, redwall:get_read_version(Tx)}
    catch
        error:{erlfdb_error, _Code} ->
            ok = timer:sleep(50),
            watch_reread(Db, Key, Tries - 1)
    after
        ets:delete(Table)
    end.

%% Creates one late watch, and waits for its future. The harness starts
%% this client after the last operation of a run with the config
%% `late_watches', when it has stopped the faults. See "Runs with late
%% watches" in `redwall_dst'. `C' is the number of the client, and `R'
%% selects the key and the kind of the transaction, as for `watch/3'.
%%
%% <ul>
%% <li>The client has no time limit: a request of a watch has no time
%%   limit, and the client waits for its future without a limit.</li>
%% <li>The client runs its transaction again after an error, until the
%%   commit succeeds. Before each new attempt it waits for 50 ms. After
%%   every third failed attempt in a row with another error than a
%%   conflict, the client asks the ClusterControllers for a recovery,
%%   because an epoch with a lost message to a LogServer commits no
%%   transaction.</li>
%% <li>After the commit, the client records the row `{watch, C}' with
%%   `late => true'. When the future is ready, it records the rows
%%   `{watch_fired, C}' and `{watch_reread, C}', as `watch/3' does.</li>
%% </ul>
late_watch(Tab, C, R) ->
    Key = watch_key(R rem 2 + 1),
    Db = watch_db(Tab, R rem 5 =/= 0, live),
    Act = watch_kind(Tab, R),
    Create = fun(Tx) ->
        Created = {_Value, Future} = watch_create(Tx, Key, C, Act),
        true = watcher(Tab, C, Future),
        Created
    end,
    {Value, Version, Future} = late_commit(Tab, C, Db, 0, Create),
    true = ets:insert(Tab, {{watch, C}, (watch_row(Tab, Key, Value, Version, Act))#{late => true}}),
    watch_wait(Tab, C, Db, Key, Value, Future, live).

%% Changes the key of the late watches with the number `I': sets the key
%% to `{watch, C}'. The client first waits until `Watchers' late watches
%% have a committed transaction. It runs its transaction again after an
%% error, as `late_watch/3' does. After the commit it records the row
%% `{late_change, C}' with the key and the commit version.
late_change(Tab, C, I, Watchers) ->
    ok = late_await(Tab, Watchers),
    Key = watch_key(I),
    Db = watch_db(Tab, true, live),
    {ok, Version, none} = late_commit(Tab, C, Db, 0, fun(Tx) -> {redwall:set(Tx, Key, {watch, C}), none} end),
    true = ets:insert(Tab, {{late_change, C}, #{key => Key, version => Version}}),
    ok.

%% Waits until the table of the run has `Watchers' late watches.
late_await(Tab, Watchers) ->
    case length([C || [C, #{late := true}] <- ets:match(Tab, {{watch, '$1'}, '$2'})]) >= Watchers of
        true ->
            ok;
        false ->
            ok = timer:sleep(20),
            late_await(Tab, Watchers)
    end.

%% Runs `Fun' in a new transaction and commits, until the commit
%% succeeds. `Fun' returns a result and the future of a watch, or `none'.
%% The result of this function has the result of `Fun', the version of
%% the watch and the future. The version is the commit version, or the
%% read version for a transaction without a mutation. `Failed' is the
%% number of failed attempts in a row with another error than a conflict.
%% `C' is the number of the client.
late_commit(Tab, C, Db, Failed, Fun) ->
    Tx = {redwall_transaction, Table} = redwall:create_transaction(Db),
    try
        {Result, Future} = Fun(Tx),
        try
            ok = redwall:commit(Tx),
            Version =
                case redwall:get_committed_version(Tx) of
                    -1 -> redwall:get_read_version(Tx);
                    LSN -> LSN
                end,
            {done, {Result, Version, Future}}
        catch
            error:{erlfdb_error, Code} ->
                %% The future of the watch has the error of the commit.
                _ = Future =/= none andalso redwall:cancel(Future, [{flush, true}]),
                {error, Code}
        end
    of
        {done, Done} ->
            Done;
        {error, CommitCode} ->
            late_again(Tab, C, Db, Failed, CommitCode, Fun)
    catch
        error:{erlfdb_error, ReadCode} ->
            late_again(Tab, C, Db, Failed, ReadCode, Fun)
    after
        ets:delete(Table)
    end.

%% Records the failed attempt of the client `C' in the row
%% `{late_failed, C}', with the number of failed attempts and the last
%% error code, and starts the next attempt.
late_again(Tab, C, Db, Failed0, Code, Fun) ->
    _ = ets:update_counter(Tab, late_retries, 1, {late_retries, 0}),
    Attempts =
        case ets:lookup(Tab, {late_failed, C}) of
            [{_, #{attempts := Before}}] -> Before;
            [] -> 0
        end,
    true = ets:insert(Tab, {{late_failed, C}, #{attempts => Attempts + 1, code => Code}}),
    Failed =
        case Code of
            %% A conflict with another transaction.
            1020 -> 0;
            _ -> Failed0 + 1
        end,
    _ = Failed rem 3 =:= 0 andalso Failed > 0 andalso begin
        _ = ets:update_counter(Tab, late_recoveries, 1, {late_recoveries, 0}),
        force_recovery(Tab)
    end,
    ok = timer:sleep(50),
    late_commit(Tab, C, Db, Failed, Fun).

%% The 2 keys of the watches. They are not keys of the transactions of
%% the workload, and no read range and no cleared range of the workload
%% contains them.
watch_key(I) -> redwall_tuple:pack({<<"w">>, I}).

%% Reads the keys and the ranges through the client library, which reads
%% the ETS table of the StorageServer in this process. The result has
%% `{Key, not_found}' or `{Key, {Value, unknown}}' for each key, and for
%% each key of the workload in each range. The client library does not
%% return the LSN of the record that wrote a value. The harness finds that
%% LSN in the records of the LogServers.
%%
%% Each key of the result is the canonical form of the key. In a run with
%% mixed keys, a read of a range can return a key in another form than
%% the form of the client, because a StorageServer returns the key of the
%% last write that it applied. The function counts these rows in the
%% counter `other_form_rows' of the table of the run.
%%
%% In a run with mixed keys, the function reads every range with
%% `get_range/3', and reads no range with `get_mapped_range/4', because a
%% mapper needs packed keys.
read_keys(Tab, Db, Tx, Keys, Ranges, Space = {Kind, _Keys, _Bound}) ->
    %% `Generation' is the number of StorageServers that the harness
    %% started after a kill. `Down' is the number of StorageServers that
    %% are not alive, and the number of StorageServers.
    Generation = lists:sum([G || [G] <- ets:match(Tab, {{storage, '_'}, '_', '$1', '_'})]),
    Down = {length([x || Storage <- storages(Tab), not is_process_alive(Storage)]), length(storages(Tab))},
    try
        Single = [{canon(Key), found(redwall:wait(redwall:get(Tx, Key)))} || Key <- Keys],
        InRanges = lists:append([
            begin
                %% 1 range in 4 is read with `get_mapped_range/4' only,
                %% with a mapper that maps each key to itself. The
                %% transaction then has the read conflict ranges of the
                %% mapped range read and no other for the range, and the
                %% invariant `no_conflict' checks them.
                Found =
                    case Kind =:= packed andalso ets:update_counter(Tab, range_number, 1, {range_number, 0}) rem 4 of
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
                ok = mapped_range(Tab, Db, redwall:get_read_version(Tx), Range, Found, Kind),
                ok = range_options(Tab, Db, redwall:get_read_version(Tx), Range, Found, Kind),
                %% The read returns no key outside the range, and the keys
                %% are in their order.
                InRange = keys_in(Range, Space),
                Canonical = canon(Found),
                [] = [Key || {Key, _Value} <- Canonical, not lists:member(Key, canon(InRange))],
                Found = lists:ukeysort(1, Found),
                Other = length([Key || {Key, _Value} <- Found, not lists:member(Key, InRange)]),
                _ = Other > 0 andalso ets:update_counter(Tab, other_form_rows, Other, {other_form_rows, 0}),
                [{canon(Key), found(proplists:get_value(canon(Key), Canonical, not_found))} || Key <- InRange]
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
%% table of the run, for the invariant `select_agrees'. The function
%% compares the canonical forms of the results, because two reads at one
%% read version can return one key in two forms.
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
            Selected = {redwall:select(Tx, Begin, End, Spec), redwall:select(Tx, Begin, End, Spec, [{limit, Limit}])},
            case canon(Selected) =:= canon({Expected, First}) of
                true -> ok;
                false -> true = ets:insert(Tab, {{select_differs, N}, #{range => {Begin, End}, expected => Expected, limit => Limit, selected => Selected}})
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
%%
%% In a run with mixed keys, the function reads nothing, because a mapper
%% needs packed keys.
mapped_range(_Tab, _Db, _ReadVersion, _Range, _Found, mixed) ->
    ok;
mapped_range(Tab, Db, ReadVersion, {Begin, End}, Found, packed) ->
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
%%
%% In a run with mixed keys, the database of the second transaction has
%% the kind `any', because the kind `binary' accepts binary keys only.
%% The function then reads the range with the options, and resolves no
%% key selector, because a database of the kind `any' has no key
%% selectors. The function compares the canonical forms of the results.
range_options(Tab, {redwall_database, Servers}, ReadVersion, {Begin, End}, Found, mixed) ->
    Tx = {redwall_transaction, Table} = redwall:create_transaction({redwall_database, Servers}),
    try
        ok = redwall:set_read_version(Tx, ReadVersion),
        N = ets:update_counter(Tab, range_option_reads, 1, {range_option_reads, 0}),
        Limit = N rem 3 + 1,
        Backward = lists:reverse(Found),
        Expected = {Backward, lists:sublist(Found, Limit), lists:sublist(Backward, Limit)},
        Read = {
            redwall:get_range(Tx, Begin, End, [{reverse, true}]),
            redwall:get_range(Tx, Begin, End, [{limit, Limit}]),
            redwall:get_range(Tx, Begin, End, [{limit, Limit}, {reverse, true}])
        },
        _ = canon(Read) =:= canon(Expected) orelse ets:insert(Tab, {{range_options_differ, N}, #{range => {Begin, End}, limit => Limit, expected => Expected, read => Read}}),
        ok
    after
        ets:delete(Table)
    end;
range_options(Tab, {redwall_database, Servers}, ReadVersion, {Begin, End}, Found, packed) ->
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

%% The keys of the client in a range, in the forms of the client.
keys_in({Begin, End}, {_Kind, Keys, _Bound}) ->
    [Key || Key <- Keys, Key >= Begin, Key < End].

%% One of the 10 ranges whose bounds are the 4 keys of the client and the
%% upper bound of the keys.
range(R, {_Kind, Keys, Bound}) ->
    Bounds = Keys ++ [Bound],
    Pairs = [{Begin, End} || Begin <- Bounds, End <- Bounds, Begin < End],
    lists:nth(R rem length(Pairs) + 1, Pairs).

commit(Tab, C, Tx, #{read_version := ReadVersion, reader_epoch := ReaderEpoch, start := Start}, {Reads, Ranges}, Writes, Chosen, R, Space) ->
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
            true -> [range(R div 11, Space)];
            false -> []
        end,
    Clears = lists:append([keys_in(Range, Space) || Range <- Cleared]) -- Writes,
    true = ets:insert(Tab, {{attempt, C}, canon(Clears)}),
    lists:foreach(fun({Begin, End}) -> ok = redwall:clear_range(Tx, Begin, End) end, Cleared),
    lists:foreach(fun(Key) -> ok = redwall:set(Tx, Key, {client, C}) end, Writes),
    ok = own_writes(Tab, C, Tx, ReadVersion, Writes, Clears, Space),
    %% From here, the client does not know whether the transaction is
    %% committed, until the commit returns.
    ok = history(Tab, C, #{writes => canon(Writes), clears => canon(Clears), outcome => unknown}),
    try redwall:wait(redwall:commit(Tx)) of
        ok ->
            LSN = redwall:get_committed_version(Tx),
            <<LSN:64, Index:16>> = redwall:get_versionstamp(Tx),
            End = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
            ok = history(Tab, C, #{outcome => committed, 'end' => End}),
            true = ets:insert(Tab, {{txn, C}, #{
                read_version => ReadVersion,
                reader_epoch => ReaderEpoch,
                %% The read keys and the keys of the read ranges.
                reads => lists:usort(canon(Reads ++ lists:append([keys_in(Range, Space) || Range <- Ranges]))),
                writes => canon(Writes),
                %% The keys that the transaction cleared and did not write.
                clears => canon(Clears),
                cleared => Cleared,
                ranges => Ranges,
                %% The position of the transaction in its batch.
                index => Index,
                start => Start,
                'end' => End
            }}),
            ok = count_forms(Tab, write, Writes, Space),
            true = ets:insert(Tab, {{ack, C}, Epoch, LSN})
    catch
        %% The error codes of FoundationDB. See `redwall'.
        error:{erlfdb_error, 1021} ->
            true = ets:insert(Tab, {{unknown, C}, Epoch}),
            after_error(Tab, Tx, 1021);
        error:{erlfdb_error, 1020} ->
            ok = history(Tab, C, #{outcome => not_committed}),
            true = ets:insert(Tab, {{rejected, C}, not_committed}),
            after_error(Tab, Tx, 1020);
        error:{erlfdb_error, 1007} ->
            ok = history(Tab, C, #{outcome => not_committed}),
            true = ets:insert(Tab, {{rejected, C}, transaction_too_old}),
            after_error(Tab, Tx, 1007)
    end.

%% Adds `Fields' to the row of the client `C' for the external checker.
%% See `redwall_dst_history'.
history(Tab, C, Fields) ->
    [{_, Row}] = ets:lookup(Tab, {history, C}),
    true = ets:insert(Tab, {{history, C}, maps:merge(Row, Fields)}),
    ok.

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
%%
%% The function compares the canonical forms of the results. In a run
%% with mixed keys:
%%
%% <ul>
%% <li>The function does not call `get_mapped_range/4', because a mapper
%%   needs packed keys.</li>
%% <li>`get/2' also returns the value of the transaction for the other
%%   form of a key that the transaction set, and `not_found' for the
%%   other form of a key that it cleared (`other_form/1').</li>
%% </ul>
own_writes(Tab, C, Tx, ReadVersion, Writes, Clears, {Kind, Keys, Bound}) ->
    [{db, Servers}] = ets:lookup(element(2, Tx), db),
    Plain = {redwall_transaction, Table} = redwall:create_transaction({redwall_database, Servers}),
    Snapshot = redwall:snapshot(Tx),
    {Begin, End} = {hd(Keys), Bound},
    try
        ok = redwall:set_read_version(Plain, ReadVersion),
        Stored = redwall:get_range(redwall:snapshot(Plain), Begin, End),
        Expected = lists:ukeymerge(1, [{Key, {client, C}} || Key <- lists:sort(Writes)], [Row || Row = {Key, _} <- Stored, not lists:member(canon(Key), canon(Clears))]),
        All = [{{'$1', '$2'}, [], [{{'$1', '$2'}}]}],
        Mapped =
            case Kind of
                packed ->
                    try redwall:get_mapped_range(Tx, Begin, End, {<<"k">>, <<"{K[1]}">>}) of
                        Rows -> {ok, Rows}
                    catch
                        error:{erlfdb_error, 2039} -> reads_your_writes
                    end;
                mixed ->
                    not_read
            end,
        Got0 = #{
            sets => [redwall:get(Snapshot, Key) || Key <- Writes],
            clears => [redwall:get(Snapshot, Key) || Key <- Clears],
            range => redwall:get_range(Snapshot, Begin, End),
            select => redwall:select(Snapshot, Begin, End, All),
            limit => redwall:select(Snapshot, Begin, End, All, [{limit, 2}])
        },
        Want0 = #{
            sets => [{client, C} || _ <- Writes],
            clears => [not_found || _ <- Clears],
            range => Expected,
            select => Expected,
            limit => lists:sublist(Expected, 2)
        },
        {Got, Want} =
            case Kind of
                packed ->
                    {Got0#{mapped => Mapped}, Want0#{mapped => reads_your_writes}};
                mixed ->
                    {
                        Got0#{
                            other_sets => [redwall:get(Snapshot, other_form(Key)) || Key <- Writes],
                            other_clears => [redwall:get(Snapshot, other_form(Key)) || Key <- Clears]
                        },
                        Want0#{other_sets => maps:get(sets, Want0), other_clears => maps:get(clears, Want0)}
                    }
            end,
        _ = ets:update_counter(Tab, own_write_checks, 1, {own_write_checks, 0}),
        _ = canon(Got) =:= canon(Want) orelse ets:insert(Tab, {{own_writes_differ, C}, #{got => Got, expected => Want, writes => Writes, clears => Clears}}),
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

%% The keys of the workload in a run without the config `keys => mixed'.
%% A small number of keys gives many conflicts. The keys are packed
%% tuples, so that the mapper of `redwall:get_mapped_range/4' can read
%% them.
keys() -> [key(I) || I <- [1, 2, 3, 4]].

key(I) -> redwall_tuple:pack({<<"k">>, I}).

%% The keys of one client: `{Kind, Keys, Bound}'. `Kind' is `packed', or
%% `mixed' in a run with the config `keys => mixed', which has the row
%% `{keys, mixed}' in the table of the run. `Keys' are the 4 keys in the
%% order of Erlang terms, and `Bound' is a key above the 4 keys, for the
%% end of a range. `R' is the random number of the client.
key_space(Tab, R) ->
    case ets:lookup(Tab, keys) of
        [] -> {packed, keys(), key(5)};
        [{keys, mixed}] -> {mixed, keys(mixed, R div 256 rem 4), <<"k5">>}
    end.

%% The 4 keys of a client in a run with mixed keys: a number, an atom, a
%% tuple and a binary. The order of Erlang terms has a number before an
%% atom, an atom before a tuple, and a tuple before a binary. `Forms' is
%% a number from 0 to 3:
%%
%% <ul>
%% <li>With an odd number, the number key is the float `1.0'. Otherwise
%%   it is the integer `1'.</li>
%% <li>With 2 or 3, the tuple key is `{k, 3.0}'. Otherwise it is
%%   `{k, 3}'.</li>
%% </ul>
%%
%% The two forms of a key are equal (`==') and not identical (`=:=').
%% redwall treats them as one key.
keys(mixed, Forms) ->
    Number =
        case Forms band 1 of
            0 -> 1;
            1 -> 1.0
        end,
    Tuple =
        case Forms band 2 of
            0 -> {k, 3};
            2 -> {k, 3.0}
        end,
    [Number, k, Tuple, <<"k4">>].

%% The other form of a key with two forms, and the key itself for a key
%% with one form.
other_form(1) -> 1.0;
other_form(1.0) -> 1;
other_form({k, 3}) -> {k, 3.0};
other_form({k, 3.0}) -> {k, 3};
other_form(Key) -> Key.

%% The canonical form of a term: each float with the value of an integer
%% is replaced by that integer, also inside a tuple, a list and a map.
%% Two keys of the workload that are equal have one canonical form. The
%% harness and the client compare the canonical forms of keys, and use
%% them as the keys of maps and of rows. The canonical form of a packed
%% key is the key itself.
canon(Float) when is_float(Float), trunc(Float) == Float -> trunc(Float);
canon(Tuple) when is_tuple(Tuple) -> list_to_tuple(canon(tuple_to_list(Tuple)));
canon([Head | Tail]) -> [canon(Head) | canon(Tail)];
canon(Map) when is_map(Map) -> maps:from_list(canon(maps:to_list(Map)));
canon(Term) -> Term.

%% Counts the forms of the number key in a run with mixed keys, in the
%% table of the run. `Kind' is `read' or `write', and `Used' are the keys
%% that the client read with `get/2', or the keys that its acknowledged
%% transaction set. The counters are `integer_key_reads',
%% `float_key_reads', `integer_key_writes' and `float_key_writes'.
count_forms(_Tab, _Kind, _Used, {packed, _Keys, _Bound}) ->
    ok;
count_forms(Tab, Kind, Used, {mixed, _Keys, _Bound}) ->
    lists:foreach(
        fun
            (Key) when Key == 1 ->
                Counter =
                    case {is_float(Key), Kind} of
                        {false, read} -> integer_key_reads;
                        {true, read} -> float_key_reads;
                        {false, write} -> integer_key_writes;
                        {true, write} -> float_key_writes
                    end,
                _ = ets:update_counter(Tab, Counter, 1, {Counter, 0}),
                ok;
            (_Key) ->
                ok
        end,
        Used
    ).

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
