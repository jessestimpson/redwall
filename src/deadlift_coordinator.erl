%% @doc A coordinator: one server of the peace cluster that holds the
%% coordinated state ({@link deadlift_cstate}).
%%
%% The coordinated state names the LogServers of every epoch that a
%% StorageServer can still need, and the ClusterController. A cluster that
%% loses it cannot recover. A coordinator of this module therefore keeps
%% its data on a disk, in the write-ahead log of peace
%% (`peace_storage_wal'), and the option `dir' is required. A coordinator
%% that starts again reads its data from the directory and is the same
%% member of the peace cluster as before.
%%
%% FoundationDB has the same rule: a coordinator keeps the coordinated
%% state on a disk (`fdbserver/Coordination.actor.cpp', `OnDemandStore').
%%
%% == Options ==
%%
%% <ul>
%% <li>`id' (required): the server of peace, `{Name, Node}'. `Node' is the
%%   node of the caller.</li>
%% <li>`dir' (required): the directory of the data. The coordinator keeps
%%   its files in a directory below `dir' that has the name of the
%%   server, so several coordinators of one computer can have one
%%   `dir'.</li>
%% <li>`wal': further arguments of `peace_storage_wal', for example
%%   `checkpoint_chunk_bytes'.</li>
%% <li>Another option of `peace:start_server/1', for example
%%   `election_timeout_ms'.</li>
%% </ul>
%%
%% The options `storage' and `machine' are not allowed. `config/1' returns
%% `{error, {missing_option, dir}}' without `dir', and `{error,
%% {option_not_allowed, Key}}' for the two options.
%%
%% == Start of a cluster ==
%%
%% <ol>
%% <li>Start each coordinator: `start_link/1', or the key `coordinator' of
%%   the application environment (see README.md, "Start").</li>
%% <li>Call `form/1' once with every coordinator. peace stores the
%%   configuration of the cluster on the disk of each coordinator.</li>
%% </ol>
%%
%% A coordinator that starts again needs no second step.
%%
%% == Limitations ==
%%
%% <ul>
%% <li>A coordinator whose directory is lost is a new server. It does not
%%   become a member of the cluster by its start. peace replaces a member
%%   with `peace:replace_member/3'. This module has no function for
%%   it.</li>
%% <li>The module has no function that changes the coordinators of a
%%   cluster.</li>
%% </ul>
-module(deadlift_coordinator).

-export([start_link/1, config/1, form/1, form/2]).
-export_type([options/0]).

-type options() :: #{
    id := peace:server_id(),
    dir := file:name_all(),
    wal => map(),
    atom() => term()
}.

%% @doc Starts the coordinator, linked to the caller.
-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    case config(Options) of
        {ok, Config} -> peace_server:start_link(Config);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Returns the configuration of the peace server of a coordinator.
-spec config(options()) -> {ok, peace:server_config()} | {error, term()}.
config(Options = #{id := {Name, Node}}) when is_atom(Name), is_atom(Node) ->
    case Options of
        #{storage := _} ->
            {error, {option_not_allowed, storage}};
        #{machine := _} ->
            {error, {option_not_allowed, machine}};
        #{dir := Dir} when is_list(Dir), Dir =/= []; is_binary(Dir), Dir =/= <<>> ->
            Wal = maps:get(wal, Options, #{}),
            Server = maps:without([dir, wal], Options),
            {ok, Server#{machine => deadlift_cstate:machine(), storage => {peace_storage_wal, Wal#{dir => Dir}}}};
        _ ->
            {error, {missing_option, dir}}
    end;
config(_Options) ->
    {error, {missing_option, id}}.

%% @equiv form(Coordinators, 5000)
-spec form([peace:server_id()]) -> ok | {error, term()}.
form(Coordinators) ->
    form(Coordinators, 5000).

%% @doc Forms the peace cluster of the coordinators, with majority
%% quorums. Every coordinator must run. The function returns `ok' when
%% every coordinator has stored the configuration. It can be called again
%% after an error.
-spec form([peace:server_id()], timeout()) -> ok | {error, term()}.
form(Coordinators, Timeout) ->
    case peace:form_cluster(Coordinators, majority, Timeout) of
        {ok, _Formed, []} -> ok;
        {ok, _Formed, NotFormed} -> {error, {not_formed, NotFormed}};
        {error, Reason} -> {error, Reason}
    end.
