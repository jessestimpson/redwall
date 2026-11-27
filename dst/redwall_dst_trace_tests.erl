%% This suite validates simulated runs against `formal/Recovery.tla' with
%% TLC. `make trace' runs the suite. The suite needs Java. `make dst' does
%% not need Java.
%%
%% See `redwall_dst_trace' for the method and its limitations, and
%% `formal/TraceRecovery.tla' for the trace spec.
-module(redwall_dst_trace_tests).

-include_lib("eunit/include/eunit.hrl").

-export([check/2]).

-define(TIMEOUT, 1800).

%% Runs the simulation with the options `Opts', writes the trace of the
%% run to `_build/traces/Name' and runs TLC on the trace. Returns
%% `{accepted | rejected | empty | error, Outcome, Counts, Output}'.
%% `Outcome' is the verdict of the invariants of the simulation. `Counts'
%% has the number of actions of each kind.
check(Name, Opts = #{config := Config}) ->
    Table = ets:new(redwall_dst_trace, [public, ordered_set]),
    Result = redwall_dst_tests:run(Opts#{config := Config#{trace => Table}}),
    States = redwall_dst_trace:states(Table),
    true = ets:delete(Table),
    Outcome = maps:get(outcome, Result),
    case redwall_dst_trace:actions(States) of
        empty ->
            {empty, Outcome, #{}, ""};
        Trace ->
            Dir = filename:join(["_build", "traces", Name]),
            ok = redwall_dst_trace:write(Dir, Trace),
            Output = os:cmd("formal/check_trace.sh " ++ Dir ++ " 2>&1"),
            Verdict =
                case Output of
                    "accepted" ++ _ -> accepted;
                    "REJECTED" ++ _ -> rejected;
                    _ -> error
                end,
            {Verdict, Outcome, redwall_dst_trace:counts(Trace), Output}
    end.

%% TLC must accept the trace of every run, and the invariants must hold.
%% Returns the sum of the counts of the actions.
sweep(Prefix, Seeds, OptsFun) ->
    lists:foldl(
        fun(Seed, Sum) ->
            {Verdict, Outcome, Counts, Output} = check(Prefix ++ "-" ++ integer_to_list(Seed), OptsFun(Seed)),
            ?assertEqual({accepted, ok}, {Verdict, Outcome}, {seed, Seed, Output}),
            maps:merge_with(fun(_Action, A, B) -> A + B end, Sum, Counts)
        end,
        #{},
        Seeds
    ).

%% Returns the first seed whose trace TLC rejects, with the verdict of the
%% invariants for that seed and the output of the check.
first_rejected(_Prefix, [], _OptsFun) ->
    error(no_seed_produced_a_trace_that_tlc_rejects);
first_rejected(Prefix, [Seed | Seeds], OptsFun) ->
    case check(Prefix ++ "-" ++ integer_to_list(Seed), OptsFun(Seed)) of
        {rejected, Outcome, _Counts, Output} -> {Seed, Outcome, Output};
        {error, _Outcome, _Counts, Output} -> error({tlc_failed, Seed, Output});
        _ -> first_rejected(Prefix, Seeds, OptsFun)
    end.

opts(Seed, Config) -> redwall_dst_tests:opts(Seed, Config).

%% Runs without kills, partitions and lost messages.
calm_test_() ->
    {timeout, ?TIMEOUT, fun() -> exercised(sweep("calm", lists:seq(1, 40), fun redwall_dst_tests:calm/1)) end}.

%% Kills, partitions, and lost and delayed messages.
faults_test_() ->
    {timeout, ?TIMEOUT, fun() -> exercised(sweep("faults", lists:seq(1, 100), fun(Seed) -> opts(Seed, #{}) end)) end}.

%% ClusterControllers recruit the Sequencers, with faults.
controller_faults_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        Opts = fun(Seed) -> (opts(Seed, #{controller => true, last_op => 80}))#{settle_steps => 20000} end,
        exercised(sweep("controller-faults", lists:seq(1, 40), Opts))
    end}.

%% A peace cluster holds the coordinated state, with faults and kills of
%% coordinators.
peace_faults_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        exercised(sweep("peace-faults", lists:seq(1, 40), fun(Seed) -> redwall_dst_tests:peace(opts(Seed, #{})) end))
    end}.

%% The following tests show that TLC can reject a trace. Each test plants
%% a defect and searches for a seed whose trace TLC rejects.

%% A Proxy answers a client when the LogServers with the body have stored
%% the record. The model requires every LogServer of the epoch.
ack_replicas_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("ack-replicas", ack_replicas, "\"Ack\"") end}.

%% A Sequencer writes the coordinated state without the comparison of the
%% recovery count.
no_cas_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-cas", no_cas, "") end}.

%% The read version of a Sequencer does not follow the commits.
no_report_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-report", no_report, "\"Ack\"") end}.

%% A LogServer stores a record whose previous record it does not have.
no_push_order_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-push-order", no_push_order, "") end}.

%% A Proxy gives a read version without the confirmation that the epoch is
%% live. The model requires the confirmation of `Confirms' LogServers.
no_confirm_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-confirm", no_confirm, "\"ReadDone\"") end}.

%% A StorageServer keeps the writes above the end version of an epoch. The
%% StorageServer of the model removes them.
no_rollback_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-rollback", no_rollback, "\"ssHas\"") end}.

%% TLC rejects a trace with the planted defect `Mode', at an action whose
%% description contains `Action'. TLC accepts the trace of the same seed
%% without the defect.
rejected(Prefix, Mode, Action) ->
    {Seed, Outcome, Output} = first_rejected(Prefix, lists:seq(1, 300), fun(S) -> opts(S, #{mode => Mode}) end),
    ?debugFmt("~s: seed ~b, invariants ~0p, ~s", [Prefix, Seed, verdict(Outcome), string:trim(Output)]),
    ?assertNotEqual(nomatch, string:find(Output, Action), Output),
    ?assertMatch({accepted, ok, _, _}, check(Prefix ++ "-fixed", opts(Seed, #{}))).

%% Asserts that the sweep contains every action of the trace spec.
exercised(Counts) ->
    ?debugFmt("actions: ~0p", [Counts]),
    Actions = [
        "LockCState",
        "LockLog",
        "Recruit",
        "WriteCState",
        "RecoveryTxn",
        "Accept",
        "Assign",
        "Persist",
        "Ack",
        "ReadStart",
        "ReadConfirm",
        "ReadSeq",
        "ReadDone",
        "SSApply",
        "SSFollow"
    ],
    ?assertEqual([], [Action || Action <- Actions, maps:get(Action, Counts, 0) < 10], Counts).

verdict(ok) -> ok;
verdict(Outcome) -> element(1, Outcome).
