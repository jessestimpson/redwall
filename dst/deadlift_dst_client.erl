%% The processes that perform the operations of the simulation. See
%% `deadlift_dst'.
%%
%% Each function runs in a process that the scheduler of eta steps. A
%% function records its result in the table of the run. The harness reads
%% the table in its checks.
-module(deadlift_dst_client).

-include("deadlift_eta.hrl").

-export([recover/4, transaction/5]).
-export([sequencer_node/1, proxy_node/1, log_node/1, resolver_node/1, storage_node/1, keys/0]).

%% Starts a Sequencer, waits for the end of its recovery, and starts a
%% Proxy for the new epoch. This is the work of a ClusterController, which
%% deadlift does not have.
%%
%% `N' is the number of the operation. `Mode' selects a planted defect.
recover(Tab, N, CState, Mode) ->
    %% Each LogServer runs on a simulated node of its own. The Sequencer
    %% starts the LogServer, so the LogServer starts on the node of the
    %% Sequencer, and is placed on its node at once.
    Recruit = fun(Log = #{id := Id}) ->
        Log1 =
            case Mode of
                %% The planted defect: a new LogServer starts without the
                %% records that the recovery read from the previous epoch.
                no_copy -> Log#{records := []};
                _ -> Log
            end,
        case deadlift_log:start(Log1) of
            {ok, Pid} ->
                ok = eta_net:place(log_node(Id), [Pid]),
                true = ets:insert(Tab, {{node, log_node(Id)}, true}),
                {ok, Pid};
            Error ->
                Error
        end
    end,
    %% The Resolver of the epoch runs on a node of its own.
    RecruitResolver = fun(Resolver) ->
        case deadlift_resolver:start(Resolver#{check => Mode =/= no_check, check_ranges => Mode =/= no_range_check, check_batch => Mode =/= no_batch_check, check_write_ranges => Mode =/= no_write_range}) of
            {ok, Pid} ->
                ok = eta_net:place(resolver_node(N), [Pid]),
                true = ets:insert(Tab, {{node, resolver_node(N)}, true}),
                {ok, Pid};
            Error ->
                Error
        end
    end,
    Options0 = #{
        coordinators => {local, CState},
        logs => 3,
        replication => 2,
        lock_timeout_ms => 200,
        recruit => Recruit,
        recruit_resolver => RecruitResolver
    },
    Options =
        case Mode of
            %% The planted defect: the first commit version of an epoch is
            %% the Recovery Version plus 1, inside the MVCC window.
            no_gap -> Options0#{epoch_gap => 1};
            _ -> Options0
        end,
    %% The Sequencer starts on the node of this process.
    {ok, Sequencer} = deadlift_sequencer:start(Options),
    true = ets:insert(Tab, {{sequencer, N}, Sequencer}),
    try deadlift_sequencer:recovered(Sequencer) of
        {ok, #{epoch := Epoch, recovery_version := RV, previous_epoch_end_version := PEV}} ->
            Ack =
                case Mode of
                    ack_replicas -> replicas;
                    _ -> all
                end,
            %% The batch time of the Proxy is 0 ms, 5 ms or 50 ms of the
            %% virtual clock.
            {ok, Proxy} = deadlift_proxy:start(#{
                sequencer => Sequencer,
                ack => Ack,
                report => Mode =/= no_report,
                confirm => Mode =/= no_confirm,
                batch_ms => lists:nth(N rem 3 + 1, [0, 5, 50])
            }),
            ok = eta_net:place(proxy_node(N), [Proxy]),
            true = ets:insert(Tab, {{node, proxy_node(N)}, true}),
            true = ets:insert(Tab, {{proxy, N}, Proxy, Epoch}),
            true = ets:insert(Tab, {{recovered, N}, Epoch, PEV, RV})
    catch
        exit:{{shutdown, {recovery, Reason}}, _Call} ->
            true = ets:insert(Tab, {{recovery_failed, N}, Reason});
        exit:Reason ->
            %% The Sequencer was killed, or did not answer in time.
            true = ets:insert(Tab, {{recovery_failed, N}, {exit, reason(Reason)}})
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
    %% StorageServer is the StorageServer that runs at the time of a read.
    Db = deadlift:open(#{
        proxy => fun() -> ets:lookup_element(Tab, {commit_proxy, C}, 2) end,
        storage => fun() -> element(2, hd(ets:lookup(Tab, storage))) end
    }),
    Tx = deadlift:create_transaction(Db),
    Start = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
    %% The client asks the Proxy `Reader' for the read version itself,
    %% because the Proxy of the commit can be another one.
    case deadlift_proxy:read_version(Reader) of
        {ok, ReadVersion} ->
            ok = deadlift:set_read_version(Tx, ReadVersion),
            Read = #{read_version => ReadVersion, reader_epoch => ReaderEpoch, start => Start},
            true = ets:insert(Tab, {{read, C}, Read}),
            case read_keys(Tab, Tx, Reads, Ranges) of
                {ok, Values, Generation} ->
                    true = ets:insert(Tab, {{read, C}, Read#{values => Values, ranges => Ranges, storage => Generation}}),
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
read_keys(Tab, Tx, Keys, Ranges) ->
    %% `Generation' is the number of StorageServers before the one that
    %% runs. The harness starts a new StorageServer after a kill.
    [{storage, _Storage, Generation, _Node}] = ets:lookup(Tab, storage),
    try
        Single = [{Key, found(deadlift:wait(deadlift:get(Tx, Key)))} || Key <- Keys],
        InRanges = lists:append([
            begin
                Found = deadlift:get_range(Tx, Begin, End),
                %% The read returns no key outside the range, and the keys
                %% are in their order.
                [] = [Key || {Key, _Value} <- Found, not lists:member(Key, keys_in(Range))],
                Found = lists:ukeysort(1, Found),
                [{Key, found(proplists:get_value(Key, Found, not_found))} || Key <- keys_in(Range)]
            end
         || Range = {Begin, End} <- Ranges
        ]),
        {ok, Single ++ InRanges, Generation}
    catch
        error:{erlfdb_error, 1007} -> {error, transaction_too_old};
        error:{erlfdb_error, Code} -> {error, Code}
    end.

found(not_found) -> not_found;
found(Value) -> {Value, unknown}.

%% The keys of the workload in a range.
keys_in({Begin, End}) ->
    [Key || Key <- keys(), Key >= Begin, Key < End].

%% One of the 10 ranges whose bounds are the 4 keys and `k5'.
range(R) ->
    Bounds = keys() ++ [k5],
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
        error:{erlfdb_error, 1021} -> true = ets:insert(Tab, {{unknown, C}, Epoch});
        error:{erlfdb_error, 1020} -> true = ets:insert(Tab, {{rejected, C}, not_committed});
        error:{erlfdb_error, 1007} -> true = ets:insert(Tab, {{rejected, C}, transaction_too_old})
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
keys() -> [k1, k2, k3, k4].

%% The simulated nodes. `N' is the number of the `recover' operation.
sequencer_node(N) -> list_to_atom("sequencer_" ++ integer_to_list(N)).

proxy_node(N) -> list_to_atom("proxy_" ++ integer_to_list(N)).

resolver_node(N) -> list_to_atom("resolver_" ++ integer_to_list(N)).

%% The node of the StorageServer that the operation `N' started. The first
%% StorageServer has the number 0.
storage_node(N) -> list_to_atom("storage_" ++ integer_to_list(N)).

log_node({Epoch, Index}) ->
    list_to_atom("log_" ++ integer_to_list(Epoch) ++ "_" ++ integer_to_list(Index)).

%% An exit reason without the pid of the call, so that the table holds the
%% same term in every run of a seed.
reason({Reason, {gen_server, call, _}}) -> Reason;
reason({Reason, {eta_net, call, _}}) -> Reason;
reason(Reason) -> Reason.
