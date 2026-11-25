%% @doc The storage engine of a StorageServer.
%%
%% A StorageServer ({@link redwall_storage}) reads the log and answers
%% reads at a version. A storage engine is the module that keeps the keys
%% and the values for it. FoundationDB has several storage engines, and a
%% cluster is configured with one of them (`configure ssd',
%% `configure memory'). redwall follows this: a cluster has one storage
%% engine.
%%
%% <ul>
%% <li>The key `storage_engine' of the environment of the application is
%%   the engine of the node. Every node of a cluster has the same value,
%%   as for `coordinators'. The default is `ets'.</li>
%% <li>The supervisor gives the value to the StorageServer of the node as
%%   the option `engine' of `redwall_storage:start_link/1'.</li>
%% <li>`redwall_status:status/1' reports the engine of the cluster in the
%%   key `storage_engine'.</li>
%% </ul>
%%
%% == Engines ==
%%
%% <ul>
%% <li>`ets': {@link redwall_storage_ets}. The data is in ETS tables in
%%   memory. A StorageServer that stops loses its data. This is the
%%   default, and the only engine of redwall.</li>
%% <li>A module name: a module with this behaviour.</li>
%% </ul>
%%
%% == Interface ==
%%
%% A StorageServer creates one store with `new/1' and is the only process
%% that changes it. The store has versions: a read names a version, and
%% returns the last write of each key at or below that version.
%%
%% <ul>
%% <li>`write/3' adds the mutations of one transaction at a version. The
%%   versions of the writes rise.</li>
%% <li>`rollback/2' removes every write above a version. The StorageServer
%%   calls it only with a version at or above the oldest version.</li>
%% <li>`advance/2' sets the oldest version. The engine can remove the
%%   versions that no read at or above the oldest version returns.</li>
%% <li>`read/3' answers a query of `redwall_storage:query()' at a
%%   version. A process other than the StorageServer can call it at the
%%   same time as a write, with the rules of "Direct reads" in
%%   {@link redwall_storage}. Such a process has the result of `view/1'
%%   and not the store: the view has only the parts that a reader
%%   needs.</li>
%% <li>`page/4' and `load/2' are the two sides of a copy from another
%%   StorageServer: the rows of a page at a version, and the rows that a
%%   new StorageServer stores.</li>
%% <li>`clear/1' removes every key.</li>
%% <li>`durability/0' states whether the data of the engine is there
%%   after the stop of the StorageServer: `memory' or `disk'.</li>
%% <li>`features/0' names the kinds of read that the engine has in
%%   addition to keys and ranges.</li>
%% </ul>
%%
%% == Limitations ==
%%
%% <ul>
%% <li>The interface is that of the engine `ets'. The engine keeps the
%%   versions of the MVCC window itself. A StorageServer of FoundationDB
%%   keeps these versions in memory for every engine, and writes only
%%   older versions to the engine. An engine on a disk will need that
%%   division, and this interface will change for it.</li>
%% <li>A StorageServer with an engine of the durability `disk' does not
%%   read its data at its start yet.</li>
%% <li>The coordinated state does not record the engine. No part of
%%   redwall stops a StorageServer with another engine than the other
%%   StorageServers of its cluster. `redwall_status:status/1' reports
%%   the engines, and the problem `storage_engines_differ'.</li>
%% <li>The LogServers keep their records in memory with every engine
%%   (`redwall_log:durability/0'). An engine of the durability `disk'
%%   alone does not keep the data of a cluster over the stop of every
%%   node: the recovery resets the database after the loss of the
%%   LogServers. See "Reset" in {@link redwall_recovery}.</li>
%% </ul>
-module(redwall_storage_engine).

-export([module/1, name/1, with_key/1, select_rows/2]).

-export_type([engine/0, store/0, row/0]).

%% The option `engine' of a StorageServer.
-type engine() :: ets | module().
-type store() :: term().
%% A row of a copy: the key, the version of the write, and the value.
-type row() :: {Key :: term(), Written :: non_neg_integer(), Value :: term()}.

-callback durability() -> memory | disk.
-callback features() -> [select | mapped_range | scan].
-callback new(Options :: map()) -> store().
%% The part of the store that a process other than the StorageServer
%% uses: the argument of `read/3', `page/4' and `versions/1' in such a
%% process.
-callback view(store()) -> store().
-callback clear(store()) -> ok.
-callback write(store(), Version :: non_neg_integer(), Mutations :: [tuple()]) -> ok.
-callback rollback(store(), End :: non_neg_integer()) -> ok.
-callback advance(store(), Oldest :: non_neg_integer()) -> ok.
-callback read(store(), redwall_storage:query(), Version :: non_neg_integer()) -> term().
-callback page(store(), first | {after_key, term()}, Limit :: pos_integer(), Version :: non_neg_integer()) ->
    {ok, [row()], done | {next, term()}} | {error, gone}.
-callback load(store(), [row()]) -> ok.
%% Every version of every key, for tests and for the simulation.
-callback versions(store()) -> #{term() => [{non_neg_integer(), {set, term()} | clear}]}.

%% @doc The module of an engine.
-spec module(engine()) -> module().
module(ets) -> redwall_storage_ets;
module(Module) when is_atom(Module) -> Module.

%% @doc The name of the engine with the module `Module'.
-spec name(module()) -> engine().
name(redwall_storage_ets) -> ets;
name(Module) -> Module.

%% @doc Runs the match spec on the rows `{Key, Value}', and returns
%% `{Key, Result}' for each row that matches.
-spec select_rows([{term(), term()}], ets:match_spec()) -> [{term(), term()}].
select_rows(Rows, MatchSpec) ->
    ets:match_spec_run(Rows, ets:match_spec_compile(with_key(MatchSpec))).

%% The match spec with the key of the row in each result: the result of a
%% clause is `{Key, Result}', where `Result' is the result of the body of
%% the caller. The row of the match spec is `{Key, Value}', so the key is
%% the first element of `$_'.
-spec with_key(ets:match_spec()) -> ets:match_spec().
with_key(MatchSpec) when is_list(MatchSpec) ->
    [
        {Head, Guards, lists:droplast(Body) ++ [{{{element, 1, '$_'}, lists:last(Body)}}]}
     || {Head, Guards, Body = [_ | _]} <- MatchSpec
    ];
with_key(_NoList) ->
    error(badarg).
