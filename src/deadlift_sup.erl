%% @private
%% The supervisor of the deadlift application. Its children are the worker
%% and the StorageServer of this node, if the application environment has
%% the options `worker' and `storage'. See README.md, "Start".
-module(deadlift_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Worker = [
        #{id => deadlift_worker, start => {deadlift_worker, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(deadlift, worker)]
    ],
    Storage = [
        #{id => deadlift_storage, start => {deadlift_storage, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(deadlift, storage)]
    ],
    {ok, {#{strategy => one_for_one}, Worker ++ Storage}}.
