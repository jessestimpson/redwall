%% A storage engine for the tests of `redwall_storage': it calls the
%% engine `ets' for each function, counts the calls of `write/3', and
%% reports the durability `disk'.
-module(redwall_storage_test_engine).

-behaviour(redwall_storage_engine).

-export([durability/0, features/0, new/1, view/1, clear/1, write/3, rollback/2, advance/2, read/3, page/4, load/2, versions/1]).
-export([writes/1]).

durability() -> disk.

features() -> [].

new(Options) ->
    {redwall_storage_ets:new(Options), counters:new(1, [])}.

%% The number of calls of `write/3'.
writes({_Store, Counter}) ->
    counters:get(Counter, 1).

view({Store, Counter}) -> {redwall_storage_ets:view(Store), Counter}.

clear({Store, _}) -> redwall_storage_ets:clear(Store).

write({Store, Counter}, Version, Mutations) ->
    ok = counters:add(Counter, 1, 1),
    redwall_storage_ets:write(Store, Version, Mutations).

rollback({Store, _}, End) -> redwall_storage_ets:rollback(Store, End).

advance({Store, _}, Oldest) -> redwall_storage_ets:advance(Store, Oldest).

read({Store, _}, Query, Version) -> redwall_storage_ets:read(Store, Query, Version).

page({Store, _}, Next, Limit, Version) -> redwall_storage_ets:page(Store, Next, Limit, Version).

load({Store, _}, Rows) -> redwall_storage_ets:load(Store, Rows).

versions({Store, _}) -> redwall_storage_ets:versions(Store).
