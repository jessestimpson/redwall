%% The processes that perform the operations of the simulation. See
%% `deadlift_dst'.
%%
%% Each function runs in a process that the scheduler of eta steps. A
%% function records its result in the table of the run. The harness reads
%% the table in its checks.
-module(deadlift_dst_client).

-include("deadlift_eta.hrl").

-export([recover/4, transaction/5]).
-export([sequencer_node/1, proxy_node/1, log_node/1, resolver_node/1, keys/0]).

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
        case deadlift_resolver:start(Resolver#{check => Mode =/= no_check}) of
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
            no_gap -> Options0#{max_versions_in_flight => 1};
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
            {ok, Proxy} = deadlift_proxy:start(#{sequencer => Sequencer, ack => Ack, report => Mode =/= no_report}),
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
%% The table of the run has a counter, `stamp'. The client takes a stamp
%% before it asks for the read version, and with the answer to the commit.
%% The harness compares the stamps of two clients in `read_version_order'.
transaction(Tab, C, {Reader, ReaderEpoch}, {Proxy, Epoch}, R) ->
    Keys = keys(),
    Reads = [Key || {I, Key} <- lists:enumerate(0, Keys), (R bsr I) band 1 =:= 1],
    Writes0 = [Key || {I, Key} <- lists:enumerate(4, Keys), (R bsr I) band 1 =:= 1],
    Writes =
        case Writes0 of
            [] -> [lists:nth(R rem length(Keys) + 1, Keys)];
            _ -> Writes0
        end,
    Start = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
    case deadlift_proxy:read_version(Reader) of
        {ok, ReadVersion} ->
            true = ets:insert(Tab, {{invoked, C}, Epoch}),
            Transaction = #{
                read_version => ReadVersion,
                reads => Reads,
                writes => [{Key, {client, C}} || Key <- Writes]
            },
            case deadlift_proxy:commit(Proxy, Transaction) of
                {ok, LSN} ->
                    End = ets:update_counter(Tab, stamp, 1, {stamp, 0}),
                    true = ets:insert(Tab, {{txn, C}, #{
                        read_version => ReadVersion,
                        reader_epoch => ReaderEpoch,
                        reads => Reads,
                        writes => Writes,
                        start => Start,
                        'end' => End
                    }}),
                    true = ets:insert(Tab, {{ack, C}, Epoch, LSN});
                {error, commit_unknown_result} ->
                    true = ets:insert(Tab, {{unknown, C}, Epoch});
                {error, Rejected} ->
                    true = ets:insert(Tab, {{rejected, C}, Rejected})
            end;
        {error, _Reason} ->
            true = ets:insert(Tab, {{no_read_version, C}, ReaderEpoch})
    end.

%% The keys of the workload. A small number of keys gives many conflicts.
keys() -> [k1, k2, k3, k4].

%% The simulated nodes. `N' is the number of the `recover' operation.
sequencer_node(N) -> list_to_atom("sequencer_" ++ integer_to_list(N)).

proxy_node(N) -> list_to_atom("proxy_" ++ integer_to_list(N)).

resolver_node(N) -> list_to_atom("resolver_" ++ integer_to_list(N)).

log_node({Epoch, Index}) ->
    list_to_atom("log_" ++ integer_to_list(Epoch) ++ "_" ++ integer_to_list(Index)).

%% An exit reason without the pid of the call, so that the table holds the
%% same term in every run of a seed.
reason({Reason, {gen_server, call, _}}) -> Reason;
reason({Reason, {eta_net, call, _}}) -> Reason;
reason(Reason) -> Reason.
