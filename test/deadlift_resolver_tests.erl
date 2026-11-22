-module(deadlift_resolver_tests).

-include_lib("eunit/include/eunit.hrl").

txn(ReadVersion, Reads, Writes) ->
    #{read_version => ReadVersion, reads => Reads, writes => Writes}.

%% Algorithm 1 of the paper.
algorithm_test() ->
    H0 = deadlift_resolver:new(infinity),
    {committed, H1} = deadlift_resolver:check(txn(0, [a], [a, b]), 10, H0),
    %% A read of `a' at version 5 is below the write at version 10.
    {conflict, H2} = deadlift_resolver:check(txn(5, [a], [c]), 11, H1),
    %% A transaction with a conflict records no write.
    {committed, H3} = deadlift_resolver:check(txn(5, [c], []), 12, H2),
    %% A read at the version of the write, or above it, has no conflict.
    {committed, H4} = deadlift_resolver:check(txn(10, [a, b], [a]), 13, H3),
    {conflict, H5} = deadlift_resolver:check(txn(12, [x, a], []), 14, H4),
    %% A transaction without read keys has no conflict.
    {committed, _} = deadlift_resolver:check(txn(0, [], [a]), 15, H5).

%% The MVCC window, with the rule of FoundationDB.
window_test() ->
    H0 = deadlift_resolver:new(100),
    {committed, H1} = deadlift_resolver:check(txn(0, [], [a]), 50, H0),
    %% The oldest version is 150 - 100 = 50. A read version of 49 is too
    %% old. A read version of 50 is not.
    {too_old, H2} = deadlift_resolver:check(txn(49, [b], []), 150, H1),
    {committed, H3} = deadlift_resolver:check(txn(50, [a], []), 150, H2),
    %% A transaction without read keys is never too old.
    {committed, H4} = deadlift_resolver:check(txn(0, [], [b]), 152, H3),
    %% The write of `a' at version 50 leaves the window at version 152. A
    %% read version below the window is too old, so the lost entry cannot
    %% hide a conflict.
    {too_old, H5} = deadlift_resolver:check(txn(40, [a], []), 153, H4),
    {committed, _} = deadlift_resolver:check(txn(60, [a], []), 154, H5).

%% The Resolver of a new epoch has no history. The first commit version of
%% the epoch is more than the window above the last version of the previous
%% epoch, so a read version of the previous epoch is too old.
new_epoch_test() ->
    Window = 5000000,
    LastEpochEnd = 123,
    First = LastEpochEnd + 100000000,
    H0 = deadlift_resolver:new(Window),
    {too_old, H1} = deadlift_resolver:check(txn(LastEpochEnd, [a], [a]), First, H0),
    {committed, _} = deadlift_resolver:check(txn(First, [a], [a]), First + 1, H1).

%% The process handles the versions in their order.
order_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(deadlift),
            ok
        end,
        fun(_) -> ok = application:stop(deadlift) end,
        fun() ->
            {ok, Resolver} = deadlift_resolver:start_link(#{epoch => 7, version => 100}),
            ?assertEqual(Resolver, deadlift_resolver:ref(7)),
            ?assertEqual({error, {epoch, 7}}, gen_server:call(Resolver, {resolve, 6, 100, 101, txn(0, [], [])})),
            %% The request for version 102 arrives before the request for
            %% version 101, and waits.
            R2 = gen_server:send_request(Resolver, {resolve, 7, 101, 102, txn(100, [a], [])}),
            ?assertEqual(timeout, gen_server:wait_response(R2, 50)),
            ?assertEqual(committed, gen_server:call(Resolver, {resolve, 7, 100, 101, txn(100, [], [a])})),
            %% The read of `a' at version 100 is below the write at 101.
            ?assertEqual({reply, conflict}, gen_server:receive_response(R2, 1000)),
            ?assertEqual({error, {handled, 101}}, gen_server:call(Resolver, {resolve, 7, 100, 101, txn(100, [], [a])})),
            ok = deadlift_resolver:stop(Resolver)
        end}.
