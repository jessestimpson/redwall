%% This module is the simulation suite. `make dst' runs it. The node must
%% have one scheduler thread (`ERL_FLAGS="+S 1"'), as the suite of peace
%% does.
-module(deadlift_dst_tests).

-include_lib("eunit/include/eunit.hrl").

-export([opts/2, calm/1, run/1]).

%% The limit for one test, in seconds.
-define(TIMEOUT, 600).

opts(Seed, Config) ->
    #{
        seed => Seed,
        config => Config,
        max_ops => 150,
        max_steps => 100000,
        settle_steps => 2000,
        %% `op_p' is the probability that a step injects an operation. A
        %% commit takes several steps and, with delayed messages, several
        %% advances of the clock. With a higher value, a new recovery ends
        %% an epoch before its commits are acknowledged.
        op_p => 0.03,
        %% When no process is runnable, the run lets time pass more often
        %% than not, so that delayed messages arrive and time-outs fire.
        quiet_p => 0.8,
        preload => [deadlift],
        net => #{policy => #{drop_p => 0.001, delay_p => 0.2, max_delay => 5}}
    }.

%% The options for a run without kills, partitions and lost messages.
calm(Seed) ->
    (opts(Seed, #{faults => false}))#{net => true}.

run(Opts) ->
    eta_run:run(deadlift_dst, Opts).

%% A seed determines a run. The same seed produces the same trace every
%% time, and the recorded trace can be replayed.
determinism_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        lists:foreach(
            fun(Seed) ->
                Opts = opts(Seed, #{}),
                [First | _] = Results = [run(Opts) || _ <- lists:seq(1, 3)],
                ?assertEqual(
                    1,
                    length(lists:usort([maps:get(trace, R) || R <- Results])),
                    {seed, Seed, [eta_run:summary(R) || R <- Results]}
                ),
                ?assertEqual(ok, eta_run:audit(First), {seed, Seed, eta_run:summary(First)}),
                Replayed = eta_run:replay(deadlift_dst, maps:get(trace, First), Opts),
                ?assertEqual(
                    {maps:get(outcome, First), 0},
                    {maps:get(outcome, Replayed), maps:get(skipped, Replayed)},
                    {seed, Seed}
                )
            end,
            lists:seq(1, 20)
        )
    end}.

%% Runs without kills. Clients receive answers.
calm_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        ok = deadlift_dst:reset_stats(),
        sweep(lists:seq(1, 200), fun calm/1),
        Stats = deadlift_dst:stats(),
        ?debugFmt("calm: ~p", [Stats]),
        ?assert(maps:get(acks, Stats, 0) > 500, Stats),
        ?assert(maps:get(recoveries, Stats, 0) > 100, Stats)
    end}.

%% Runs with kills of LogServers, Sequencers and Proxies.
faults_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        ok = deadlift_dst:reset_stats(),
        sweep(lists:seq(1, 500), fun(Seed) -> opts(Seed, #{}) end),
        Stats = deadlift_dst:stats(),
        ?debugFmt("faults: ~p", [Stats]),
        ?assert(maps:get(acks, Stats, 0) > 5000, Stats),
        ?assert(maps:get(kills_log, Stats, 0) > 100, Stats),
        ?assert(maps:get(kills_sequencer, Stats, 0) > 100, Stats),
        ?assert(maps:get(kills_proxy, Stats, 0) > 100, Stats),
        ?assert(maps:get(partitions, Stats, 0) > 100, Stats),
        %% The Resolver rejected transactions for a conflict, and for a
        %% read version of an earlier epoch.
        ?assert(maps:get(not_committed, Stats, 0) > 1000, Stats),
        ?assert(maps:get(too_old, Stats, 0) > 10, Stats),
        ?assert(maps:get(runs_with_three_epochs, Stats, 0) > 100, Stats)
    end}.

%% The planted defect of `RecoveryAckReplicasOnly': a Proxy answers after
%% the 2 LogServers with the body.
ack_replicas_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => ack_replicas}) end),
        ?debugFmt("ack_replicas: seed ~p, ~p", [Seed, Violation]),
        %% Delayed messages let a LogServer answer a lock before it
        %% receives a record that the Proxy sent earlier. The run then
        %% reaches the violation of `durable' that TLC finds.
        ?assert(lists:member(maps:get(property, Violation), [durable, kcv_below_dv, body_available]), Violation),
        Found = lists:usort([
            maps:get(property, V)
         || Seed1 <- lists:seq(1, 40),
            {violation, V} <- [maps:get(outcome, run(opts(Seed1, #{mode => ack_replicas})))]
        ]),
        ?debugFmt("ack_replicas: invariants violated in 40 seeds: ~p", [Found]),
        ?assert(lists:member(durable, Found), Found)
    end}.

%% A planted defect of the harness: a recovery copies no record to the new
%% LogServers.
no_copy_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_copy}) end),
        ?debugFmt("no_copy: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := copied}, Violation)
    end}.

%% A planted defect: the Resolver admits every transaction.
no_check_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_check}) end),
        ?debugFmt("no_check: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := no_conflict}, Violation)
    end}.

%% A planted defect: the first commit version of an epoch is the Recovery
%% Version plus 1. The Resolver of the new epoch has no history, and admits
%% a transaction that read a key below a write of the previous epoch.
no_gap_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_gap}) end),
        ?debugFmt("no_gap: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := no_conflict}, Violation),
        #{reader := #{epoch := ReaderEpoch}, writer := #{epoch := WriterEpoch}} = Violation,
        ?assert(ReaderEpoch > WriterEpoch, Violation)
    end}.

%% A planted defect: a Proxy does not report a committed LSN to the
%% Sequencer, so a read version does not follow the commits.
no_report_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_report}) end),
        ?debugFmt("no_report: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := read_version_order}, Violation)
    end}.

%% The planted defect of `RecoveryNoCas': the coordinated state accepts the
%% write of a Sequencer whose epoch is not the recovery count.
no_cas_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_cas}) end),
        ?debugFmt("no_cas: seed ~p, ~p", [Seed, Violation]),
        ?assert(lists:member(maps:get(property, Violation), [durable, unique_versions]), Violation)
    end}.

sweep(Seeds, OptsFun) ->
    lists:foreach(
        fun(Seed) ->
            Result = run(OptsFun(Seed)),
            ?assertEqual(ok, maps:get(outcome, Result), {seed, Seed, eta_run:summary(Result)}),
            ?assertEqual(ok, eta_run:audit(Result), {seed, Seed, eta_run:summary(Result)})
        end,
        Seeds
    ).

first_violation([], _OptsFun) ->
    error(no_seed_found_the_planted_defect);
first_violation([Seed | Seeds], OptsFun) ->
    case run(OptsFun(Seed)) of
        #{outcome := {violation, Violation}} -> {Seed, Violation};
        #{outcome := ok} -> first_violation(Seeds, OptsFun);
        Result -> error({seed, Seed, eta_run:summary(Result)})
    end.
