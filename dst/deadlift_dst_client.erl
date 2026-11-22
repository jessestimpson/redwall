%% The processes that perform the operations of the simulation. See
%% `deadlift_dst'.
%%
%% Each function runs in a process that the scheduler of eta steps. A
%% function records its result in the table of the run. The harness reads
%% the table in its checks.
-module(deadlift_dst_client).

-include("deadlift_eta.hrl").

-export([recover/4, commit/3]).
-export([sequencer_node/1, proxy_node/1, log_node/1]).

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
    Options = #{
        coordinators => {local, CState},
        logs => 3,
        replication => 2,
        lock_timeout_ms => 200,
        recruit => Recruit
    },
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
            {ok, Proxy} = deadlift_proxy:start(#{sequencer => Sequencer, ack => Ack}),
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

%% Commits one record through `Proxy', a Proxy of `Epoch'. `C' is the
%% number of the client, and the body of the record.
commit(Tab, C, {Proxy, Epoch}) ->
    true = ets:insert(Tab, {{invoked, C}, Epoch}),
    case deadlift_proxy:commit(Proxy, {client, C}) of
        {ok, LSN} ->
            true = ets:insert(Tab, {{ack, C}, Epoch, LSN});
        {error, commit_unknown_result} ->
            true = ets:insert(Tab, {{unknown, C}, Epoch})
    end.

%% The simulated nodes. `N' is the number of the `recover' operation.
sequencer_node(N) -> list_to_atom("sequencer_" ++ integer_to_list(N)).

proxy_node(N) -> list_to_atom("proxy_" ++ integer_to_list(N)).

log_node({Epoch, Index}) ->
    list_to_atom("log_" ++ integer_to_list(Epoch) ++ "_" ++ integer_to_list(Index)).

%% An exit reason without the pid of the call, so that the table holds the
%% same term in every run of a seed.
reason({Reason, {gen_server, call, _}}) -> Reason;
reason({Reason, {eta_net, call, _}}) -> Reason;
reason(Reason) -> Reason.
