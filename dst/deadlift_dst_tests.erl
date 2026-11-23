%% This module is the simulation suite. `make dst' runs each test in a node
%% of its own (`dst/run.sh'), and `make dst-serial' runs the tests in one
%% node. A node must have one scheduler thread (`+S 1'), as the suite of
%% peace does.
-module(deadlift_dst_tests).

-include_lib("eunit/include/eunit.hrl").

-export([opts/2, calm/1, run/1]).

%% The limit for one test, in seconds.
-define(TIMEOUT, 600).

opts(Seed, Config) ->
    #{
        seed => Seed,
        config => Config,
        max_ops => 80,
        max_steps => 100000,
        settle_steps => 2000,
        %% `op_p' is the probability that a step injects an operation. A
        %% commit takes several steps and, with delayed messages, several
        %% advances of the clock. With a higher value, a new recovery ends
        %% an epoch before its commits are acknowledged. The start of a
        %% node takes steps for the messages between the registries.
        op_p => 0.015,
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

%% The runs without faults and the runs with faults are divided into
%% shards, so that `dst/run.sh' runs the shards in several nodes at the
%% same time. A shard has its own seeds and its own statistics. Each limit
%% of a shard is below the share of the shard in the sum over all shards.

%% Runs without kills. Clients receive answers. 200 seeds in 4 shards.
calm_1_test_() -> calm_shard(1).
calm_2_test_() -> calm_shard(2).
calm_3_test_() -> calm_shard(3).
calm_4_test_() -> calm_shard(4).

calm_shard(Shard) ->
    {timeout, ?TIMEOUT, fun() ->
        ok = deadlift_dst:reset_stats(),
        sweep(shard_seeds(Shard, 50), fun calm/1),
        Stats = deadlift_dst:stats(),
        ?debugFmt("calm ~p: ~p", [Shard, Stats]),
        ok = write_stats({calm, Shard}, Stats),
        ?assert(maps:get(acks, Stats, 0) > 1000, Stats),
        ?assert(maps:get(recoveries, Stats, 0) > 25, Stats),
        ?assert(maps:get(batched, Stats, 0) > 100, Stats),
        ?assert(maps:get(range_acks, Stats, 0) > 100, Stats)
    end}.

%% Runs with kills of LogServers, Sequencers, Proxies, Resolvers and the
%% StorageServer. 500 seeds in 5 shards.
faults_1_test_() -> faults_shard(1).
faults_2_test_() -> faults_shard(2).
faults_3_test_() -> faults_shard(3).
faults_4_test_() -> faults_shard(4).
faults_5_test_() -> faults_shard(5).

faults_shard(Shard) ->
    {timeout, ?TIMEOUT, fun() ->
        ok = deadlift_dst:reset_stats(),
        sweep(shard_seeds(Shard, 100), fun(Seed) -> opts(Seed, #{}) end),
        Stats = deadlift_dst:stats(),
        ?debugFmt("faults ~p: ~p", [Shard, Stats]),
        ok = write_stats({faults, Shard}, Stats),
        %% A transaction has 3 steps with messages: the read version, the
        %% reads and the commit. With faults, about 1 transaction in 12 is
        %% acknowledged.
        ?assert(maps:get(acks, Stats, 0) > 150, Stats),
        ?assert(maps:get(kills_log, Stats, 0) > 15, Stats),
        ?assert(maps:get(kills_sequencer, Stats, 0) > 15, Stats),
        ?assert(maps:get(kills_proxy, Stats, 0) > 15, Stats),
        ?assert(maps:get(kills_resolver, Stats, 0) > 15, Stats),
        %% The harness killed StorageServers, and clients read from a
        %% StorageServer that started after a kill.
        ?assert(maps:get(kills_storage, Stats, 0) > 15, Stats),
        ?assert(maps:get(reads_after_restart, Stats, 0) > 100, Stats),
        %% Runs had several StorageServers, clients read while one of
        %% them was not alive, and clients read by message only.
        ?assert(maps:get(storages, Stats, 0) > 150, Stats),
        ?assert(maps:get(reads_with_one_down, Stats, 0) > 3, Stats),
        ?assert(maps:get(reads_by_message, Stats, 0) > 100, Stats),
        %% Clients read values, and the StorageServer removed writes that a
        %% recovery discarded.
        ?assert(maps:get(reads, Stats, 0) > 750, Stats),
        %% Clients read key ranges, and transactions with a read range were
        %% acknowledged.
        ?assert(maps:get(range_reads, Stats, 0) > 300, Stats),
        ?assert(maps:get(range_acks, Stats, 0) > 20, Stats),
        %% Acknowledged transactions cleared keys.
        ?assert(maps:get(clears, Stats, 0) > 15, Stats),
        %% Acknowledged transactions had their LSN together with another
        %% acknowledged transaction.
        ?assert(maps:get(batched, Stats, 0) > 25, Stats),
        ?assert(maps:get(rollbacks, Stats, 0) > 2, Stats),
        %% Reads were below the MVCC window of the StorageServer. With the
        %% direct reads of the client library, a shard has 2 to 15 of
        %% them.
        ?assert(maps:get(read_too_old, Stats, 0) > 0, Stats),
        ?assert(maps:get(partitions, Stats, 0) > 15, Stats),
        %% The Resolver rejected transactions for a conflict, and for a
        %% read version of an earlier epoch.
        ?assert(maps:get(not_committed, Stats, 0) > 150, Stats),
        ?assert(maps:get(too_old, Stats, 0) > 2, Stats),
        %% A Proxy of an ended epoch refused a read version.
        ?assert(maps:get(no_read_version, Stats, 0) > 15, Stats),
        ?assert(maps:get(runs_with_three_epochs, Stats, 0) > 15, Stats)
    end}.

%% The seeds of the shard `Shard', with `Size' seeds in each shard.
shard_seeds(Shard, Size) ->
    lists:seq((Shard - 1) * Size + 1, Shard * Size).

%% Writes the statistics of a shard to the directory that the environment
%% variable DEADLIFT_DST_STATS_DIR names. `dst/run.sh' sets the variable and
%% prints the sums.
write_stats({Kind, Shard}, Stats) ->
    case os:getenv("DEADLIFT_DST_STATS_DIR") of
        false ->
            ok;
        Dir ->
            File = filename:join(Dir, lists:concat([Kind, "_", Shard, ".stats"])),
            file:write_file(File, io_lib:format("~p.~n", [{Kind, Stats}]))
    end.

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
         || Seed1 <- lists:seq(1, 120),
            {violation, V} <- [maps:get(outcome, run(opts(Seed1, #{mode => ack_replicas})))]
        ]),
        ?debugFmt("ack_replicas: invariants violated in 120 seeds: ~p", [Found]),
        ?assert(lists:member(durable, Found), Found)
    end}.

%% A planted defect of the harness: a recovery copies no record to the new
%% LogServers.
no_copy_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_copy}) end),
        ?debugFmt("no_copy: seed ~p, ~p", [Seed, Violation]),
        %% `copied' is the direct violation. `no_loss' is the violation of a
        %% later recovery, which needs a record that the new LogServers did
        %% not receive.
        ?assert(lists:member(maps:get(property, Violation), [copied, no_loss]), Violation)
    end}.

%% A planted defect: the Resolver admits every transaction.
no_check_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_check}) end),
        ?debugFmt("no_check: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := no_conflict}, Violation)
    end}.

%% A planted defect: the Resolver does not check the read ranges of a
%% transaction. The reader of the violation read the key in a range.
no_range_check_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_range_check}) end),
        ?debugFmt("no_range_check: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := no_conflict}, Violation)
    end}.

%% A planted defect: the Resolver does not check a transaction against the
%% earlier transactions of its batch. The two transactions of the violation
%% have one commit version.
no_batch_check_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_batch_check}) end),
        ?debugFmt("no_batch_check: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := no_conflict}, Violation),
        #{reader := #{version := Version, index := Index}, writer := #{version := Written, index := WriterIndex}} = Violation,
        ?assertEqual(Version, Written),
        ?assert(WriterIndex < Index, Violation)
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

%% A planted defect: a Proxy gives a read version without the confirmation
%% that its epoch is live. The Proxy of an ended epoch then gives a read
%% version below a commit of a later epoch.
no_confirm_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_confirm}) end),
        ?debugFmt("no_confirm: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := read_version_order}, Violation),
        #{reader := #{reader_epoch := ReaderEpoch}, acknowledged := #{epoch := Epoch}} = Violation,
        ?assert(ReaderEpoch < Epoch, Violation)
    end}.

%% A planted defect: the StorageServer keeps the writes above the end
%% version of an epoch.
no_rollback_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_rollback}) end),
        ?debugFmt("no_rollback: seed ~p, ~p", [Seed, Violation]),
        ?assert(lists:member(maps:get(property, Violation), [storage_rollback, read_value]), Violation)
    end}.

%% A planted defect: the Resolver does not record the write ranges of a
%% transaction. The writer of the violation cleared the key.
no_write_range_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_write_range}) end),
        ?debugFmt("no_write_range: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := no_conflict, writer := #{clears := [_ | _]}}, Violation)
    end}.

%% A planted defect: the StorageServer does not apply a clear. A read
%% returns a value for a key that a later acknowledged transaction cleared.
no_clear_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_clear}) end),
        ?debugFmt("no_clear: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := read_value, reason := {missed, _}}, Violation)
    end}.

%% A planted defect: the StorageServer answers a read below its oldest
%% version, after it has removed older versions.
no_window_check_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_window_check}) end),
        ?debugFmt("no_window_check: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := read_value}, Violation)
    end}.

%% A planted defect: the StorageServer answers a read at a version that it
%% has not applied. The read does not return a write at or below the read
%% version.
no_wait_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_wait}) end),
        ?debugFmt("no_wait: seed ~p, ~p", [Seed, Violation]),
        ?assertMatch(#{property := read_value}, Violation)
    end}.

%% A planted defect: the second StorageServer of a run does not apply a
%% clear. Two StorageServers then have different writes for a key. A read
%% from the second StorageServer can find the defect first, as a violation
%% of `read_value'. The test continues with the next seed until the
%% comparison of the StorageServers finds the defect.
one_no_clear_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        OptsFun = fun(Seed) -> (calm(Seed))#{config => #{mode => one_no_clear, faults => false}} end,
        Find = fun Find(Seeds) ->
            {Seed, Violation} = first_violation(Seeds, OptsFun),
            case Violation of
                #{property := replicas_agree} ->
                    {Seed, Violation};
                #{property := read_value} ->
                    Find([Later || Later <- Seeds, Later > Seed])
            end
        end,
        {Seed, Violation} = Find(lists:seq(1, 300)),
        ?debugFmt("one_no_clear: seed ~p, ~p", [Seed, Violation]),
        #{first := First, second := Second} = Violation,
        %% The second StorageServer lacks a clear that the first one has.
        ?assert(lists:any(fun({_Version, Change}) -> Change =:= clear end, First -- Second), Violation)
    end}.

%% A planted defect: a registry sends no name to the registry of another
%% node. This defect violates no invariant. A Proxy does not find the
%% LogServers and the Resolver of its epoch, and a second Sequencer does not
%% find the LogServers of the first epoch. The test shows that the runs
%% depend on the exchange of names: no transaction is acknowledged, and
%% each run has at most one recovery.
no_exchange_stops_commits_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        ok = deadlift_dst:reset_stats(),
        Seeds = lists:seq(1, 10),
        sweep(Seeds, fun(Seed) -> (calm(Seed))#{config => #{mode => no_exchange, faults => false}} end),
        Stats = deadlift_dst:stats(),
        ?debugFmt("no_exchange: ~p", [Stats]),
        ?assertEqual(0, maps:get(acks, Stats, 0), Stats),
        ?assert(maps:get(recoveries, Stats, 0) =< length(Seeds), Stats),
        ?assert(maps:get(recoveries, Stats, 0) > 0, Stats)
    end}.

%% The planted defect of `RecoveryNoCas': the coordinated state accepts the
%% write of a Sequencer whose epoch is not the recovery count.
no_cas_is_found_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        {Seed, Violation} = first_violation(lists:seq(1, 300), fun(Seed) -> opts(Seed, #{mode => no_cas}) end),
        ?debugFmt("no_cas: seed ~p, ~p", [Seed, Violation]),
        %% Two Sequencers of one epoch each give read versions. `read_version_order'
        %% is a third consequence of the defect.
        ?assert(lists:member(maps:get(property, Violation), [durable, unique_versions, read_version_order]), Violation)
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
