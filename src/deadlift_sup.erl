%% @private
%% The supervisor of the deadlift application. Its children are the
%% coordinator, the worker and the StorageServer of this node, if the
%% application environment has the options `coordinator', `worker' and
%% `storage'. See README.md, "Start".
-module(deadlift_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% The coordinator is the first child, because the worker and the
    %% StorageServer of the node read the coordinated state.
    Coordinator = [
        #{id => deadlift_coordinator, start => {deadlift_coordinator, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(deadlift, coordinator)]
    ],
    Worker = [
        #{id => deadlift_worker, start => {deadlift_worker, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(deadlift, worker)]
    ],
    Storage = [
        #{id => deadlift_storage, start => {deadlift_storage, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(deadlift, storage)]
    ],
    {ok, {#{strategy => one_for_one}, Coordinator ++ Worker ++ Storage}}.
