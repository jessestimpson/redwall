%% @private
%% The supervisor of the deadlift application. It has no child. The roles
%% of the transaction system are not under this supervisor yet.
-module(deadlift_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {#{strategy => one_for_one}, []}}.
