%% @doc The coordinated state.
%%
%% In FoundationDB, the Coordinators store the coordinated state: the
%% configuration of the transaction system. A Sequencer reads and locks the
%% coordinated state at the start of a recovery, and writes the new log
%% system to the coordinated state at the end. See section 2.4.4 of Zhou et
%% al., "FoundationDB: A Distributed Unbundled Transactional Key Value
%% Store", SIGMOD 2021, and `design/recovery-internals.md' in the
%% FoundationDB repository.
%%
%% In redwall, a peace cluster stores the coordinated state. This module is
%% the `peace_machine' of that cluster, and the client for that machine.
%% FoundationDB uses Active Disk Paxos on the Coordinators. redwall uses
%% the replicated log of peace. This is the only use of consensus in
%% redwall: no commit of a transaction goes through peace.
%%
%% == State ==
%%
%% <ul>
%% <li>`count' is the recovery count. A Sequencer that locks the
%%   coordinated state raises the count by 1. The new count is the epoch of
%%   that Sequencer.</li>
%% <li>`owner' identifies the Sequencer that raised the count last.</li>
%% <li>`current' is the log system of the epoch that accepts commits, or
%%   `undefined' before the first epoch.</li>
%% <li>`old' holds the log systems of the epochs that have ended, newest
%%   first. Each has `end_version', the Recovery Version that ended the
%%   epoch. StorageServers read the records up to that version from these
%%   LogServers.</li>
%% <li>`controller' is `{Term, Pid}': the number of elections of a
%%   ClusterController, and the process that won the last election. See
%%   {@link redwall_cluster_controller}.</li>
%% </ul>
%%
%% == Values ==
%%
%% <ul>
%% <li>`{lock, Count, Owner}' raises the recovery count from `Count' to
%%   `Count + 1'. The reply is `{ok, Epoch, State}'. If the count is not
%%   `Count', the reply is `{error, {count, Actual}}' and the state does not
%%   change.</li>
%% <li>`{write, Epoch, Owner, LogSystem, EndVersion}' makes `LogSystem' the
%%   current log system of `Epoch', and moves the previous current log
%%   system to `old' with `EndVersion'. The reply is `ok'. If the count is
%%   not `Epoch', or `Owner' is not the owner, the reply is
%%   `{error, {count, Actual}}' and the state does not change.</li>
%% <li>`{elect, Term, Pid}' makes `Pid' the ClusterController and raises
%%   the term from `Term' to `Term + 1'. The reply is `{ok, Term + 1}'. If
%%   the term is not `Term', the reply is `{error, {term, Actual, Current}}'
%%   and the state does not change. This value does not change the
%%   recovery count. FoundationDB does not store the ClusterController in
%%   the coordinated state. This part is not in the paper.</li>
%% </ul>
%%
%% The values `lock' and `write' are compare-and-set operations on the recovery count. Two
%% Sequencers that recover at the same time cannot both write: the second
%% lock raises the count, and the write of the first Sequencer then fails.
%% `formal/Recovery.tla' models these two operations as `LockCState' and
%% `WriteCState'. The config `RecoveryNoCas' shows that a write without the
%% comparison lets two epochs acknowledge the same version.
%%
%% Each value can be proposed again after a timeout. A value that is
%% applied a second time for the same owner gives the same reply and does
%% not change the state.
-module(redwall_cstate).

-behaviour(peace_machine).

-include("redwall_eta.hrl").

-export([machine/0]).
-export([read/1, read/2, lock/3, lock/4, write/5, write/6, elect/3, elect/4]).
-export([storage/4, forget_storage/3, reset/4]).
-export([init/1, apply/3]).

-export_type([state/0, log_system/0, old_log_system/0, epoch/0, version/0, owner/0]).

-type epoch() :: pos_integer().
-type version() :: non_neg_integer().
%% A term that identifies one Sequencer, for example a reference.
-type owner() :: term().

%% The LogServers of one epoch. `logs' holds one entry `{Id, Pid}' for
%% each LogServer: the identifier (`redwall_log:id()') and the process
%% that the recovery started. A process that reads the coordinated state
%% has the process of each LogServer. `logs' and `replication' are not
%% interpreted by this module.
-type log_system() :: #{
    epoch := epoch(),
    logs := [term()],
    replication := pos_integer()
}.

-type old_log_system() :: #{
    epoch := epoch(),
    logs := [term()],
    replication := pos_integer(),
    end_version := version()
}.

-type state() :: #{
    count := non_neg_integer(),
    owner := owner() | undefined,
    current := log_system() | undefined,
    old := [old_log_system()],
    %% The term and the process of the ClusterController. The term is the
    %% number of elections.
    controller := {non_neg_integer(), pid() | undefined},
    %% The StorageServer of each node of the cluster with one. The
    %% ClusterController records a StorageServer that a worker reports.
    %% A StorageServer that has stopped stays, until a StorageServer of
    %% its node takes its place or `forget_storage/3' removes the node.
    storages := #{node() => pid()},
    %% The number of resets of the database. A reset removes every log
    %% system, after the loss of the LogServers of an epoch. A
    %% StorageServer removes its data when this number changes.
    generation := non_neg_integer()
}.

%% The servers of a peace cluster, or `{local, Server}' for a
%% `redwall_cstate_local' process.
-type servers() :: peace:server_id() | [peace:server_id()] | {local, gen_server:server_ref()}.

-export_type([servers/0]).

-define(TIMEOUT_MS, 5000).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc The machine argument for `peace:start_cluster/3'.
-spec machine() -> peace_machine:machine().
machine() ->
    {module, ?MODULE, []}.

%% @equiv read(Servers, 5000)
-spec read(servers()) -> {ok, state()} | {error, term()}.
read(Servers) ->
    read(Servers, ?TIMEOUT_MS).

%% @doc Reads the coordinated state. The read is linearizable.
-spec read(servers(), timeout()) -> {ok, state()} | {error, term()}.
read({local, Server}, Timeout) ->
    redwall_cstate_local:read(Server, Timeout);
read(Servers, Timeout) ->
    case peace:consistent_query(Servers, fun(State) -> State end, Timeout) of
        {ok, State, _Leader} -> {ok, State};
        {timeout, Server} -> {error, {timeout, Server}};
        {error, Reason} -> {error, Reason}
    end.

%% @equiv lock(Servers, Count, Owner, 5000)
-spec lock(servers(), non_neg_integer(), owner()) ->
    {ok, epoch(), state()} | {error, {count, non_neg_integer()}} | {error, term()}.
lock(Servers, Count, Owner) ->
    lock(Servers, Count, Owner, ?TIMEOUT_MS).

%% @doc Raises the recovery count from `Count' to `Count + 1'. `Count' is
%% the count that the caller read. The result `{ok, Epoch, State}' gives the
%% epoch of the caller and the coordinated state that the caller recovers.
-spec lock(servers(), non_neg_integer(), owner(), timeout()) ->
    {ok, epoch(), state()} | {error, {count, non_neg_integer()}} | {error, term()}.
lock(Servers, Count, Owner, Timeout) ->
    propose(Servers, {lock, Count, Owner}, Timeout).

%% @equiv write(Servers, Epoch, Owner, LogSystem, EndVersion, 5000)
-spec write(servers(), epoch(), owner(), log_system(), version() | undefined) ->
    ok | {error, {count, non_neg_integer()}} | {error, term()}.
write(Servers, Epoch, Owner, LogSystem, EndVersion) ->
    write(Servers, Epoch, Owner, LogSystem, EndVersion, ?TIMEOUT_MS).

%% @doc Writes the log system of `Epoch' as the current log system.
%% `EndVersion' is the Recovery Version that ends the previous epoch. It is
%% `undefined' if there is no previous epoch.
-spec write(servers(), epoch(), owner(), log_system(), version() | undefined, timeout()) ->
    ok | {error, {count, non_neg_integer()}} | {error, term()}.
write(Servers, Epoch, Owner, LogSystem, EndVersion, Timeout) ->
    propose(Servers, {write, Epoch, Owner, LogSystem, EndVersion}, Timeout).

%% @equiv elect(Servers, Term, Pid, 5000)
-spec elect(servers(), non_neg_integer(), pid()) ->
    {ok, pos_integer()} | {error, {term, non_neg_integer(), pid() | undefined}} | {error, term()}.
elect(Servers, Term, Pid) ->
    elect(Servers, Term, Pid, ?TIMEOUT_MS).

%% @doc Makes the process `Pid' the ClusterController. `Term' is the term
%% that the caller read. The result `{ok, Term + 1}' gives the term of the
%% new ClusterController. The result `{error, {term, Actual, Current}}'
%% means that another election has changed the term.
-spec elect(servers(), non_neg_integer(), pid(), timeout()) ->
    {ok, pos_integer()} | {error, {term, non_neg_integer(), pid() | undefined}} | {error, term()}.
elect(Servers, Term, Pid, Timeout) ->
    propose(Servers, {elect, Term, Pid}, Timeout).

%% @doc Removes every log system from the coordinated state, and raises
%% the generation. The caller is the Sequencer with the lock of the epoch
%% `Epoch'. See "Reset" in {@link redwall_recovery}.
-spec reset(servers(), epoch(), owner(), timeout()) -> ok | {error, term()}.
reset(Servers, Epoch, Owner, Timeout) ->
    propose(Servers, {reset, Epoch, Owner}, Timeout).

%% @doc Records `Pid' as the StorageServer of the node `Node'. A second
%% call for the node replaces the first.
-spec storage(servers(), node(), pid(), timeout()) -> ok | {error, term()}.
storage(Servers, Node, Pid, Timeout) ->
    propose(Servers, {storage, Node, Pid}, Timeout).

%% @doc Removes the StorageServer of the node `Node' from the list of the
%% StorageServers of the cluster, for a node that does not start again.
-spec forget_storage(servers(), node(), timeout()) -> ok | {error, term()}.
forget_storage(Servers, Node, Timeout) ->
    propose(Servers, {forget_storage, Node}, Timeout).

propose({local, Server}, Value, Timeout) ->
    redwall_cstate_local:propose(Server, Value, Timeout);
propose(Servers, Value, Timeout) ->
    case peace:propose(Servers, Value, Timeout) of
        {ok, Reply, _Leader} -> Reply;
        {timeout, Server} -> {error, {timeout, Server}};
        {error, Reason} -> {error, Reason}
    end.

%% ---------------------------------------------------------------------------
%% Machine
%% ---------------------------------------------------------------------------

%% @private
-spec init(term()) -> state().
init(_Args) ->
    #{count => 0, owner => undefined, current => undefined, old => [], controller => {0, undefined}, storages => #{}, generation => 0}.

%% @private
-spec apply(peace_machine:meta(), term(), state()) -> {state(), term()}.
apply(_Meta, {lock, Count, Owner}, State = #{count := Count}) ->
    State1 = State#{count := Count + 1, owner := Owner},
    {State1, {ok, Count + 1, State1}};
apply(_Meta, {lock, Count, Owner}, State = #{count := Actual, owner := Owner}) when Actual =:= Count + 1 ->
    %% The same value, applied a second time.
    {State, {ok, Actual, State}};
apply(_Meta, {lock, _Count, _Owner}, State = #{count := Actual}) ->
    {State, {error, {count, Actual}}};
apply(_Meta, {write, Epoch, Owner, LogSystem = #{epoch := Epoch}, EndVersion}, State = #{count := Epoch, owner := Owner}) ->
    case State of
        #{current := LogSystem} ->
            %% The same value, applied a second time.
            {State, ok};
        #{current := undefined} when EndVersion =:= undefined ->
            {State#{current := LogSystem}, ok};
        #{current := Previous = #{epoch := E}, old := Old} when E < Epoch, is_integer(EndVersion), EndVersion >= 0 ->
            {State#{current := LogSystem, old := [Previous#{end_version => EndVersion} | Old]}, ok};
        _ ->
            {State, {error, badarg}}
    end;
apply(_Meta, {write, Epoch, Owner, _LogSystem, _EndVersion}, State = #{count := Epoch, owner := Owner}) ->
    %% The log system is not a log system of this epoch.
    {State, {error, badarg}};
apply(_Meta, {write, _Epoch, _Owner, _LogSystem, _EndVersion}, State = #{count := Actual}) ->
    {State, {error, {count, Actual}}};
apply(_Meta, {elect, Term, Pid}, State = #{controller := {Term, _Previous}}) when is_pid(Pid) ->
    {State#{controller := {Term + 1, Pid}}, {ok, Term + 1}};
apply(_Meta, {elect, Term, Pid}, State = #{controller := {Actual, Pid}}) when Actual =:= Term + 1 ->
    %% The same value, applied a second time.
    {State, {ok, Actual}};
apply(_Meta, {elect, _Term, _Pid}, State = #{controller := {Actual, Current}}) ->
    {State, {error, {term, Actual, Current}}};
apply(_Meta, {reset, Epoch, Owner}, State = #{count := Epoch, owner := Owner, generation := Generation}) ->
    %% The Sequencer of the epoch `Epoch' has the lock. The database
    %% starts again without a log system and without data.
    {State#{current := undefined, old := [], generation := Generation + 1}, ok};
apply(_Meta, {reset, _Epoch, _Owner}, State = #{count := Actual}) ->
    {State, {error, {count, Actual}}};
apply(_Meta, {storage, Node, Pid}, State = #{storages := Storages}) when is_atom(Node), is_pid(Pid) ->
    {State#{storages := Storages#{Node => Pid}}, ok};
apply(_Meta, {forget_storage, Node}, State = #{storages := Storages}) ->
    {State#{storages := maps:remove(Node, Storages)}, ok};
apply(_Meta, _Other, State) ->
    {State, {error, badarg}}.
