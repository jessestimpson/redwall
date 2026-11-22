%% @doc The coordinated state in one process, without peace.
%%
%% This module is for tests and for the simulation. The process applies the
%% values of {@link deadlift_cstate} to a state, one at a time. This is the
%% register with atomic operations of `formal/Recovery.tla'.
%%
%% The functions of `deadlift_cstate' accept `{local, Server}' in the place
%% of the servers of a peace cluster, where `Server' is this process.
%%
%% The coordinated state of this process is lost when the process stops.
%%
%% == Options ==
%%
%% `cas => false' plants a defect for the simulation: the process accepts a
%% write from a Sequencer whose epoch is not the recovery count. The config
%% `RecoveryNoCas' of the model has the same defect.
-module(deadlift_cstate_local).

-behaviour(gen_server).

-include("deadlift_eta.hrl").
-eta_observe(all).

-export([start/1, start_link/1, stop/1]).
-export([read/2, propose/3]).
-export([init/1, handle_call/3, handle_cast/2]).

-type options() :: #{cas => boolean()}.

-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Server) ->
    gen_server:stop(Server).

%% @private
-spec read(gen_server:server_ref(), timeout()) -> {ok, deadlift_cstate:state()} | {error, term()}.
read(Server, Timeout) ->
    call(Server, read, Timeout).

%% @private
-spec propose(gen_server:server_ref(), term(), timeout()) -> term().
propose(Server, Value, Timeout) ->
    call(Server, {propose, Value}, Timeout).

call(Server, Request, Timeout) ->
    try
        gen_server:call(Server, Request, Timeout)
    catch
        exit:{timeout, _} -> {error, {timeout, Server}};
        exit:Reason -> {error, Reason}
    end.

%% @private
%% The state is `{CoordinatedState, Cas}'.
init(Options) ->
    {ok, {deadlift_cstate:init([]), maps:get(cas, Options, true)}}.

%% @private
handle_call(read, _From, State = {CState, _Cas}) ->
    {reply, {ok, CState}, State};
handle_call({propose, Value}, _From, {CState, Cas}) ->
    {CState1, Reply} = deadlift_cstate:apply(#{}, Value, without_cas(Value, CState, Cas)),
    {reply, Reply, {CState1, Cas}}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% The planted defect: the write of any Sequencer is accepted.
without_cas({write, Epoch, Owner, _LogSystem, _EndVersion}, CState, false) ->
    CState#{count := Epoch, owner := Owner};
without_cas(_Value, CState, _Cas) ->
    CState.
