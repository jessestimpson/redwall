%% This suite checks the client histories of simulated runs for strict
%% serializability with Porcupine, a checker that shares no code with
%% redwall. `make porcupine' runs the suite. The suite needs Go. `make dst'
%% does not need Go.
%%
%% `redwall_dst' checks the transactions with its own invariants, which
%% use the commit versions and the records of the LogServers. Porcupine
%% uses only what the clients observed. See `redwall_dst_history' for the
%% history and `dst/porcupine/main.go' for the model.
-module(redwall_dst_porcupine_tests).

-include_lib("eunit/include/eunit.hrl").

-export([check/2]).

-define(TIMEOUT, 1800).

%% Runs the simulation with the options `Opts', writes the history of the
%% run to `_build/histories/Name.json' and runs Porcupine on the history.
%% Returns `{ok | illegal | error, Outcome, Operations, Output}'. `Outcome'
%% is the verdict of the invariants of the simulation.
check(Name, Opts = #{config := Config}) ->
    Table = ets:new(redwall_dst_history, [public, ordered_set]),
    Result = redwall_dst_tests:run(Opts#{config := Config#{history => Table}}),
    Rows = redwall_dst_history:rows(Table),
    true = ets:delete(Table),
    File = filename:join(["_build", "histories", Name ++ ".json"]),
    ok = redwall_dst_history:write(File, Rows),
    {Verdict, Operations, Output} = porcupine(File, "60s"),
    {Verdict, maps:get(outcome, Result), Operations, Output}.

%% Porcupine prints "unknown" when a check does not finish within the time
%% limit. The check is then repeated with a limit of 600 seconds.
porcupine(File, Limit) ->
    Output = os:cmd("dst/check_history.sh " ++ File ++ " " ++ Limit ++ " 2>&1"),
    case string:lexemes(Output, " \n") of
        ["ok", N, "operations"] ->
            {ok, list_to_integer(N), Output};
        ["ILLEGAL", N, "operations"] ->
            {illegal, list_to_integer(N), Output};
        ["unknown", _N, "operations"] when Limit =:= "60s" ->
            ?debugFmt("~s: no verdict in 60 seconds, repeating with 600 seconds", [File]),
            porcupine(File, "600s");
        _ ->
            {error, 0, Output}
    end.

%% Every run must be strictly serializable according to Porcupine and
%% correct according to the invariants. Returns the number of operations
%% checked.
sweep(Prefix, Seeds, OptsFun) ->
    lists:sum([
        begin
            {Verdict, Outcome, Operations, Output} = check(Prefix ++ "-" ++ integer_to_list(Seed), OptsFun(Seed)),
            ?assertEqual({ok, ok}, {Verdict, Outcome}, {seed, Seed, Output}),
            Operations
        end
     || Seed <- Seeds
    ]).

%% Returns the first seed whose history Porcupine rejects, with the verdict
%% of the invariants for that seed.
first_illegal(_Prefix, [], _OptsFun) ->
    error(no_seed_produced_a_history_that_porcupine_rejects);
first_illegal(Prefix, [Seed | Seeds], OptsFun) ->
    case check(Prefix ++ "-" ++ integer_to_list(Seed), OptsFun(Seed)) of
        {illegal, Outcome, _Operations, _Output} -> {Seed, Outcome};
        {ok, _Outcome, _Operations, _Output} -> first_illegal(Prefix, Seeds, OptsFun);
        {error, _Outcome, _Operations, Output} -> error({porcupine_failed, Seed, Output})
    end.

opts(Seed, Config) -> redwall_dst_tests:opts(Seed, Config).

controller(Opts) -> Opts#{settle_steps => 20000}.

%% Runs without kills, partitions and lost messages.
calm_test_() ->
    {timeout, ?TIMEOUT, fun() -> exercised(sweep("calm", lists:seq(1, 40), fun redwall_dst_tests:calm/1)) end}.

%% Kills, partitions, and lost and delayed messages.
faults_test_() ->
    {timeout, ?TIMEOUT, fun() -> exercised(sweep("faults", lists:seq(1, 100), fun(Seed) -> opts(Seed, #{}) end)) end}.

%% ClusterControllers recruit the Sequencers, with faults.
controller_faults_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        exercised(sweep("controller-faults", lists:seq(1, 40), fun(Seed) -> controller(opts(Seed, #{controller => true, last_op => 80})) end))
    end}.

%% The LogServers discard records, and the harness kills StorageServers.
pop_storage_kills_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        Opts = fun(Seed) -> controller((opts(Seed, #{faults => storage, controller => true, pop => true, last_op => 80}))#{net => true}) end,
        exercised(sweep("pop-storage-kills", lists:seq(1, 30), Opts))
    end}.

%% Keys of several types, with faults. See "Runs with mixed keys" in
%% `redwall_dst'.
mixed_faults_test_() ->
    {timeout, ?TIMEOUT, fun() -> exercised(sweep("mixed-faults", lists:seq(1, 40), fun(Seed) -> opts(Seed, #{keys => mixed}) end)) end}.

%% A peace cluster holds the coordinated state, with faults and kills of
%% coordinators.
peace_faults_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        exercised(sweep("peace-faults", lists:seq(1, 40), fun(Seed) -> redwall_dst_tests:peace(opts(Seed, #{})) end))
    end}.

%% The workload has watches, with faults. The history has the
%% transactions on the 4 keys of the workload, and not the transactions
%% of the watches.
watch_faults_test_() ->
    {timeout, ?TIMEOUT, fun() ->
        exercised(sweep("watch-faults", lists:seq(1, 40), fun(Seed) -> opts(Seed, #{watches => true}) end))
    end}.

%% Mixed keys with the pop and kills of StorageServers. A StorageServer
%% copies rows with mixed keys from a peer.
mixed_pop_test_() ->
    {timeout, ?TIMEOUT, fun() -> exercised(sweep("mixed-pop", lists:seq(1, 30), fun redwall_dst_tests:mixed_pop/1)) end}.

%% The following tests show that Porcupine can reject a history. Each test
%% plants a defect and searches for a seed whose history Porcupine rejects.

%%
%% Porcupine rejected no history in 300 seeds with the planted defect
%% `no_rollback', and no history in 120 seeds with `no_copy' and with
%% `ack_replicas'. The invariants of the simulation find these three
%% defects. See "Limitations" in `dst/porcupine/main.go'.

%% The Resolver does not check the read keys of a transaction.
no_check_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-check", no_check, #{}) end}.

%% The Resolver does not check the read ranges of a transaction.
no_range_check_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-range-check", no_range_check, #{}) end}.

%% The Resolver does not check a transaction against the earlier
%% transactions of its batch.
no_batch_check_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-batch-check", no_batch_check, #{}) end}.

%% The Resolver does not record the keys of a cleared range.
no_write_range_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-write-range", no_write_range, #{}) end}.

%% The Resolver compares keys with `=:='. The run has mixed keys.
no_equal_keys_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-equal-keys", no_equal_keys, #{keys => mixed}) end}.

%% A Proxy gives a read version without a check that its epoch is live.
no_confirm_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-confirm", no_confirm, #{}) end}.

%% A Proxy does not report a committed version to the Sequencer.
no_report_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-report", no_report, #{}) end}.

%% A StorageServer does not apply a clear of a key range.
no_clear_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-clear", no_clear, #{}) end}.

%% A StorageServer stores no row of a copy from another StorageServer.
no_copy_rows_is_rejected_test_() ->
    {timeout, ?TIMEOUT, fun() -> rejected("no-copy-rows", no_copy_rows, #{}) end}.

rejected(Prefix, Mode, Config) ->
    {Seed, Outcome} = first_illegal(Prefix, lists:seq(1, 300), fun(S) -> opts(S, Config#{mode => Mode}) end),
    ?debugFmt("~s: seed ~b, invariants ~0p", [Prefix, Seed, verdict(Outcome)]),
    %% Without the planted defect, the same seed is strictly serializable.
    ?assertMatch({ok, ok, _, _}, check(Prefix ++ "-fixed", opts(Seed, Config))).

%% Asserts that the sweep checked a useful number of operations.
exercised(Operations) ->
    ?debugFmt("operations checked: ~b", [Operations]),
    ?assert(Operations > 500, Operations).

%% The result of the invariants of the simulation, for a log line: `ok' or
%% `violation'.
verdict(ok) -> ok;
verdict(Outcome) -> element(1, Outcome).
