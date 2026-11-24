%% @private
%% The supervisor of the redwall application. Its children are the
%% coordinator, the worker and the StorageServer of this node, if the
%% application environment has the options `coordinator', `worker' and
%% `storage'. See README.md, "Start".
-module(redwall_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% The coordinator is the first child, because the worker and the
    %% StorageServer of the node read the coordinated state.
    Coordinator = [
        #{id => redwall_coordinator, start => {redwall_coordinator, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(redwall, coordinator)]
    ],
    Worker = [
        #{id => redwall_worker, start => {redwall_worker, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(redwall, worker)]
    ],
    Storage = [
        #{id => redwall_storage, start => {redwall_storage, start_link, [Options]}}
     || {ok, Options} <- [application:get_env(redwall, storage)]
    ],
    {ok, {#{strategy => one_for_one}, Coordinator ++ Worker ++ Storage}}.
