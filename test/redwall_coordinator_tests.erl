-module(redwall_coordinator_tests).

-include_lib("eunit/include/eunit.hrl").

%% A directory for the data of the coordinators of one test.
dir() ->
    Dir = filename:join([filename:basedir(user_cache, "redwall_test"), "coordinator", integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "file")),
    Dir.

ids() ->
    Tag = erlang:unique_integer([positive]),
    [{list_to_atom(lists:concat([redwall_coordinator_test_, Tag, "_", N])), node()} || N <- [1, 2, 3]].

%% Starts a coordinator without a link to the test process.
start(Id, Dir) ->
    Parent = self(),
    Starter = spawn(fun() ->
        Parent ! {self(), redwall_coordinator:start_link(#{id => Id, dir => Dir, election_timeout_ms => 100})},
        receive
            stop -> ok
        end
    end),
    receive
        {Starter, {ok, Pid}} -> {Starter, Pid};
        {Starter, Error} -> error(Error)
    after 5000 -> error(timeout)
    end.

%% Stops a coordinator as a kill of its node does: without a call to the
%% server.
kill({Starter, Pid}) ->
    Ref = monitor(process, Pid),
    exit(Starter, kill),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 5000 -> error(timeout)
    end.

await(_Fun, 0) ->
    error(timeout);
await(Fun, Attempts) ->
    case Fun() of
        {ok, Result} ->
            Result;
        _ ->
            timer:sleep(50),
            await(Fun, Attempts - 1)
    end.

%% The option `dir' is required, and the options `storage' and `machine'
%% are not allowed.
config_test() ->
    Id = {c1, node()},
    ?assertEqual({error, {missing_option, dir}}, redwall_coordinator:config(#{id => Id})),
    ?assertEqual({error, {missing_option, dir}}, redwall_coordinator:config(#{id => Id, dir => ""})),
    ?assertEqual({error, {missing_option, dir}}, redwall_coordinator:config(#{id => Id, dir => undefined})),
    ?assertEqual({error, {missing_option, id}}, redwall_coordinator:config(#{dir => "/tmp/x"})),
    ?assertEqual({error, {option_not_allowed, storage}}, redwall_coordinator:config(#{id => Id, dir => "/tmp/x", storage => {peace_storage_mem, #{}}})),
    ?assertEqual({error, {option_not_allowed, machine}}, redwall_coordinator:config(#{id => Id, dir => "/tmp/x", machine => {module, m, []}})),
    ?assertEqual({error, {missing_option, dir}}, redwall_coordinator:start_link(#{id => Id})),
    ?assertEqual(
        {ok, #{
            id => Id,
            election_timeout_ms => 100,
            machine => redwall_cstate:machine(),
            storage => {peace_storage_wal, #{dir => "/tmp/x", checkpoint_chunk_bytes => 64}}
        }},
        redwall_coordinator:config(#{id => Id, dir => "/tmp/x", election_timeout_ms => 100, wal => #{checkpoint_chunk_bytes => 64}})
    ).

%% The coordinated state is on the disk: it is the same after the stop of
%% one coordinator and after the stop of every coordinator.
restart_test_() ->
    {timeout, 60, fun() ->
        {ok, _} = application:ensure_all_started(peace),
        Dir = dir(),
        Ids = [Id1, Id2, Id3] = ids(),
        [C1, C2, C3] = [start(Id, Dir) || Id <- Ids],
        ok = redwall_coordinator:form(Ids),
        %% A second call stores nothing new.
        ok = redwall_coordinator:form(Ids),
        {ok, 1, _} = await(fun() ->
            case redwall_cstate:lock(Ids, 0, self(), 1000) of
                {ok, 1, State} -> {ok, {ok, 1, State}};
                Other -> Other
            end
        end, 100),
        %% One coordinator stops and starts again. The other two decide a
        %% value in between.
        ok = kill(C1),
        %% The stopped coordinator can be the leader. The call is then
        %% repeated until the other two have a leader.
        {ok, 2, _} = await(fun() ->
            case redwall_cstate:lock(Ids, 1, self(), 1000) of
                {ok, 2, State1} -> {ok, {ok, 2, State1}};
                Other1 -> Other1
            end
        end, 100),
        C1b = start(Id1, Dir),
        %% The other two stop. The coordinator that started again and one
        %% of them are a majority, so the first one has learned the value.
        ok = kill(C2),
        ok = kill(C3),
        C2b = start(Id2, Dir),
        #{count := 2} = await(fun() -> redwall_cstate:read([Id1, Id2], 1000) end, 100),
        %% Every coordinator stops and starts again.
        ok = kill(C1b),
        ok = kill(C2b),
        Again = [start(Id, Dir) || Id <- Ids],
        State = await(fun() -> redwall_cstate:read(Ids, 1000) end, 100),
        ?assertMatch(#{count := 2, owner := Owner} when Owner =:= self(), State),
        {ok, 3, _} = await(fun() ->
            case redwall_cstate:lock(Ids, 2, self(), 1000) of
                {ok, 3, State3} -> {ok, {ok, 3, State3}};
                Other3 -> Other3
            end
        end, 100),
        %% A coordinator with an empty directory is not a member of the
        %% cluster.
        [ok = kill(C) || C <- Again],
        Lost = start(Id3, dir()),
        ?assertMatch({error, _}, redwall_cstate:read([Id3], 500)),
        ok = kill(Lost)
    end}.

%% The application starts the coordinator of the node from its
%% environment, and starts it again after a failure.
application_test_() ->
    {timeout, 60, fun() ->
        Dir = dir(),
        Id = {redwall_coordinator_app_test, node()},
        _ = application:load(redwall),
        ok = application:set_env(redwall, coordinator, #{id => Id, dir => Dir, election_timeout_ms => 100}),
        try
            {ok, _} = application:ensure_all_started(redwall),
            ok = redwall_coordinator:form([Id]),
            {ok, 1, _} = await(fun() ->
                case redwall_cstate:lock([Id], 0, self(), 1000) of
                    {ok, 1, State} -> {ok, {ok, 1, State}};
                    Other -> Other
                end
            end, 100),
            Pid = whereis(redwall_coordinator_app_test),
            ?assert(lists:keymember(redwall_coordinator, 1, supervisor:which_children(redwall_sup))),
            exit(Pid, kill),
            #{count := 1} = await(fun() -> redwall_cstate:read([Id], 1000) end, 100),
            ?assertNotEqual(Pid, whereis(redwall_coordinator_app_test)),
            %% The application stops and starts again.
            ok = application:stop(redwall),
            ?assertEqual(undefined, whereis(redwall_coordinator_app_test)),
            {ok, _} = application:ensure_all_started(redwall),
            #{count := 1} = await(fun() -> redwall_cstate:read([Id], 1000) end, 100)
        after
            _ = application:stop(redwall),
            ok = application:unset_env(redwall, coordinator)
        end
    end}.
