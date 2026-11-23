%% This module is the machine of the peace cluster of a simulation run.
%%
%% A run with the config `cstate => peace' has a peace cluster of 3 servers
%% as the coordinated state. Each server applies the values of
%% {@link deadlift_cstate} through this module. The module adds two things
%% to `deadlift_cstate':
%%
%% <ul>
%% <li>It records the state after each value in the table of the run, with
%%   the index of the value in the log of peace. The row `cstate' has the
%%   state with the highest index that a server has applied. The harness
%%   reads this row for its invariants, without a message to a
%%   server.</li>
%% <li>It records the state of each index, and whether two servers have
%%   two states for one index.</li>
%% <li>It plants the two defects of {@link deadlift_cstate_local}:
%%   `cas => false' and `elect_cas => false'.</li>
%% </ul>
%%
%% The state of the machine is the state of `deadlift_cstate', so that a
%% read of the coordinated state returns it. The module therefore keeps
%% its arguments in the named table `deadlift_dst_machine', which the
%% harness creates with `configure/1' before the run and deletes with
%% `clear/0' after the run. A test node performs one run at a time.
%%
%% Arguments: `#{tab := ets:table(), cas => boolean(), elect_cas =>
%% boolean()}'.
-module(deadlift_dst_machine).

-behaviour(peace_machine).

-export([init/1, apply/3]).
-export([configure/1, clear/0]).

%% Keeps the arguments for the servers of the run.
configure(Args = #{tab := _Tab}) ->
    ok = clear(),
    ?MODULE = ets:new(?MODULE, [named_table, public]),
    true = ets:insert(?MODULE, {args, Args}),
    ok.

clear() ->
    _ = ets:whereis(?MODULE) =/= undefined andalso ets:delete(?MODULE),
    ok.

init(_Args) ->
    deadlift_cstate:init([]).

apply(Meta = #{index := Index}, Value, State0) ->
    [{args, Args = #{tab := Tab}}] = ets:lookup(?MODULE, args),
    {State, Reply} = deadlift_cstate:apply(Meta, Value, without_cas(Value, State0, Args)),
    %% Every server applies the same values in the same order, so the
    %% state of an index is the same on every server. The row
    %% `{cstate_at, Index}' has the state of the first server that applied
    %% the index. A server that has another state for the index records
    %% the row `{cstate_differs, Index}', for the invariant
    %% `coordinators_agree'.
    case ets:lookup(Tab, {cstate_at, Index}) of
        [] -> true = ets:insert(Tab, {{cstate_at, Index}, State});
        [{_, State}] -> ok;
        [{_, Other}] -> true = ets:insert(Tab, {{cstate_differs, Index}, #{index => Index, first => Other, second => State, value => Value}})
    end,
    case ets:lookup(Tab, cstate) of
        [{cstate, Applied, _State}] when Applied >= Index -> ok;
        _ -> true = ets:insert(Tab, {cstate, Index, State})
    end,
    {State, Reply}.

%% The planted defects. See `deadlift_cstate_local'.
without_cas({write, Epoch, Owner, _LogSystem, _EndVersion}, State, #{cas := false}) ->
    State#{count := Epoch, owner := Owner};
without_cas({elect, Term, _Pid}, State, #{elect_cas := false}) ->
    State#{controller := {Term, undefined}};
without_cas(_Value, State, _Args) ->
    State.
