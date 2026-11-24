-module(redwall_log_tests).

-include_lib("eunit/include/eunit.hrl").

start(Options) ->
    {ok, Log} = redwall_log:start_link(Options),
    Log.

%% Sends a push and returns the request id.
send(Log, Prev, LSN, KCV, Body) ->
    redwall_log:send_push(Log, 1, Prev, LSN, KCV, Body).

answer(Request) ->
    {reply, Reply} = gen_server:receive_response(Request, 1000),
    Reply.

%% `wait_response/2' keeps the request after a timeout.
no_answer(Request) ->
    ?assertEqual(timeout, gen_server:wait_response(Request, 50)).

push_in_order_test() ->
    Log = start(#{epoch => 1}),
    ?assertEqual({ok, 1}, redwall_log:push(Log, 1, 0, 1, 0, {body, a})),
    ?assertEqual({ok, 2}, redwall_log:push(Log, 1, 1, 2, 1, empty)),
    ?assertEqual({ok, 5}, redwall_log:push(Log, 1, 2, 5, 1, {body, c})),
    ?assertMatch(#{dv := 5, kcv := 1, stopped_by := undefined, waiting := 0}, redwall_log:info(Log)),
    ?assertEqual({ok, [{1, 0, {body, a}}, {2, 1, empty}, {5, 2, {body, c}}]}, redwall_log:peek(Log, 0, infinity)),
    ?assertEqual({ok, [{2, 1, empty}]}, redwall_log:peek(Log, 1, 4)),
    ?assertEqual({ok, []}, redwall_log:peek(Log, 5, infinity)),
    redwall_log:stop(Log).

%% A record that arrives before the record with the previous LSN waits.
push_out_of_order_test() ->
    Log = start(#{epoch => 1}),
    R3 = send(Log, 2, 3, 0, {body, c}),
    R2 = send(Log, 1, 2, 0, {body, b}),
    no_answer(R3),
    no_answer(R2),
    ?assertMatch(#{dv := 0, waiting := 2}, redwall_log:info(Log)),
    ?assertEqual({ok, 1}, redwall_log:push(Log, 1, 0, 1, 0, {body, a})),
    ?assertEqual({ok, 2}, answer(R2)),
    ?assertEqual({ok, 3}, answer(R3)),
    ?assertMatch(#{dv := 3, waiting := 0}, redwall_log:info(Log)),
    redwall_log:stop(Log).

%% The KCV of the LogServer is the maximum of the KCVs of the stored
%% records. A waiting record does not raise the KCV.
kcv_test() ->
    Log = start(#{epoch => 1}),
    R2 = send(Log, 1, 2, 1, empty),
    no_answer(R2),
    ?assertMatch(#{kcv := 0}, redwall_log:info(Log)),
    {ok, 1} = redwall_log:push(Log, 1, 0, 1, 0, empty),
    {ok, 2} = answer(R2),
    ?assertMatch(#{kcv := 1}, redwall_log:info(Log)),
    {ok, 3} = redwall_log:push(Log, 1, 2, 3, 0, empty),
    ?assertMatch(#{kcv := 1}, redwall_log:info(Log)),
    redwall_log:stop(Log).

%% A record that is sent a second time has no second effect.
push_again_test() ->
    Log = start(#{epoch => 1}),
    {ok, 1} = redwall_log:push(Log, 1, 0, 1, 0, {body, a}),
    {ok, 2} = redwall_log:push(Log, 1, 1, 2, 0, {body, b}),
    ?assertEqual({ok, 2}, redwall_log:push(Log, 1, 0, 1, 0, {body, a})),
    ?assertEqual({error, {conflict, 1}}, redwall_log:push(Log, 1, 0, 1, 0, {body, x})),
    ?assertEqual({error, {conflict, 3}}, redwall_log:push(Log, 1, 1, 3, 0, {body, c})),
    %% Two callers wait for the same record.
    R4a = send(Log, 3, 4, 0, {body, d}),
    R4b = send(Log, 3, 4, 0, {body, d}),
    ?assertEqual({error, {conflict, 5}}, redwall_log:push(Log, 1, 3, 5, 0, {body, e})),
    {ok, 3} = redwall_log:push(Log, 1, 2, 3, 0, {body, c}),
    ?assertEqual({ok, 4}, answer(R4a)),
    ?assertEqual({ok, 4}, answer(R4b)),
    redwall_log:stop(Log).

wrong_epoch_test() ->
    Log = start(#{epoch => 2}),
    ?assertEqual({error, {epoch, 2}}, redwall_log:push(Log, 1, 0, 1, 0, empty)),
    ?assertEqual({error, {epoch, 2}}, redwall_log:lock(Log, 2)),
    ?assertEqual({error, {epoch, 2}}, redwall_log:lock(Log, 1)),
    redwall_log:stop(Log).

%% A lock stops the LogServer and answers with the DV and the KCV.
lock_test() ->
    Log = start(#{epoch => 1}),
    {ok, 1} = redwall_log:push(Log, 1, 0, 1, 0, {body, a}),
    {ok, 2} = redwall_log:push(Log, 1, 1, 2, 1, {body, b}),
    R4 = send(Log, 3, 4, 2, {body, d}),
    no_answer(R4),
    %% The LogServer confirms its epoch until the lock.
    ?assertEqual(ok, redwall_log:confirm(Log, 1)),
    ?assertEqual({error, {epoch, 1}}, redwall_log:confirm(Log, 2)),
    ?assertEqual({ok, #{dv => 2, kcv => 1}}, redwall_log:lock(Log, 3)),
    ?assertEqual({error, stopped}, redwall_log:confirm(Log, 1)),
    ?assertEqual({error, stopped}, answer(R4)),
    ?assertEqual({error, stopped}, redwall_log:push(Log, 1, 2, 3, 2, {body, c})),
    ?assertMatch(#{dv := 2, kcv := 1, stopped_by := 3, waiting := 0}, redwall_log:info(Log)),
    %% The same lock a second time gives the same answer.
    ?assertEqual({ok, #{dv => 2, kcv => 1}}, redwall_log:lock(Log, 3)),
    %% A lower epoch gets no answer with the DV. A higher epoch does.
    ?assertEqual({error, {locked, 3}}, redwall_log:lock(Log, 2)),
    ?assertEqual({ok, #{dv => 2, kcv => 1}}, redwall_log:lock(Log, 4)),
    ?assertEqual({error, {locked, 4}}, redwall_log:lock(Log, 3)),
    %% A stopped LogServer answers a peek.
    ?assertEqual({ok, [{1, 0, {body, a}}, {2, 1, {body, b}}]}, redwall_log:peek(Log, 0, infinity)),
    redwall_log:stop(Log).

%% A LogServer of a later epoch starts at the Recovery Version, with the
%% records that the recovery copied.
recruited_test() ->
    Log = start(#{epoch => 2, start => 5, kcv => 3, records => [{5, {body, e}}, {4, empty}]}),
    ?assertMatch(#{epoch := 2, dv := 5, kcv := 3}, redwall_log:info(Log)),
    ?assertEqual({ok, [{4, 0, empty}, {5, 4, {body, e}}]}, redwall_log:peek(Log, 3, infinity)),
    ?assertEqual({ok, 6}, redwall_log:push(Log, 2, 5, 6, 3, {body, f})),
    redwall_log:stop(Log).

records_above_start_test() ->
    process_flag(trap_exit, true),
    ?assertEqual(
        {error, {records_above_start, [6]}},
        redwall_log:start_link(#{epoch => 2, start => 5, records => [{6, empty}]})
    ),
    process_flag(trap_exit, false).

%% A tail request is answered when the LogServer has a record above
%% `After', or when the LogServer is locked.
tail_test() ->
    Log = start(#{epoch => 1}),
    {ok, 1} = redwall_log:push(Log, 1, 0, 1, 0, {body, a}),
    %% The last element is the Known Committed Version of the LogServer.
    ?assertEqual({ok, [{1, 0, {body, a}}], false, 0}, redwall_log:tail(Log, 0)),
    %% No record above 1: the request waits.
    R1 = gen_server:send_request(Log, {tail, 1}),
    no_answer(R1),
    {ok, 2} = redwall_log:push(Log, 1, 1, 2, 1, empty),
    ?assertEqual({ok, [{2, 1, empty}], false, 1}, answer(R1)),
    %% A request above the DV waits for a record above `After'.
    R2 = gen_server:send_request(Log, {tail, 3}),
    {ok, 3} = redwall_log:push(Log, 1, 2, 3, 1, {body, c}),
    no_answer(R2),
    %% A second request of the same process replaces the first one.
    R3 = gen_server:send_request(Log, {tail, 3}),
    {ok, 4} = redwall_log:push(Log, 1, 3, 4, 3, {body, d}),
    ?assertEqual({ok, [{4, 3, {body, d}}], false, 3}, answer(R3)),
    no_answer(R2),
    %% A lock answers a waiting request. A stopped LogServer answers at
    %% once.
    R4 = gen_server:send_request(Log, {tail, 4}),
    no_answer(R4),
    {ok, _} = redwall_log:lock(Log, 2),
    ?assertEqual({ok, [], true, 3}, answer(R4)),
    ?assertEqual({ok, [{4, 3, {body, d}}], true, 3}, redwall_log:tail(Log, 3)),
    redwall_log:stop(Log).
