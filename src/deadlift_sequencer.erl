%% @doc The Sequencer.
%%
%% The Sequencer of an epoch performs the recovery, and then assigns the
%% commit versions of the epoch. See sections 2.3.1, 2.4.1 and 2.4.4 of
%% Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
%% Value Store", SIGMOD 2021. `formal/Recovery.tla' models the Sequencer
%% with the recovery actions and with `RecoveryTxn', `Accept' and `Assign'.
%%
%% A Sequencer starts with a recovery ({@link deadlift_recovery}). If the
%% recovery fails, the Sequencer stops with the reason
%% `{shutdown, {recovery, Reason}}'. A request that arrives during the
%% recovery waits for the end of the recovery.
%%
%% After the recovery, the Sequencer starts the Resolver of the epoch and
%% commits the recovery transaction. The Sequencer then answers
%% `commit_version/1' with a new LSN and the previous LSN. A Proxy sends
%% both to the Resolver and to the LogServers.
%%
%% == Recovery transaction ==
%%
%% The recovery transaction is the first record of an epoch (paper, section
%% 2.4.4). Its LSN is the first commit version of the epoch, and its
%% previous LSN is the Recovery Version. The Sequencer sends it to the
%% Resolver and to every LogServer of the epoch, and waits for every
%% answer. If one of them does not answer, the Sequencer stops with the
%% reason `{shutdown, {recovery, {recovery_transaction, Reason}}}'.
%%
%% The recovery transaction has two functions in deadlift:
%%
%% <ul>
%% <li>The read version at the start of an epoch is the LSN of a record
%%   that exists. A StorageServer reaches that version.</li>
%% <li>The Resolver has handled a version of the epoch, so a transaction
%%   with the first read version of the epoch is inside the MVCC
%%   window.</li>
%% </ul>
%%
%% == Versions ==
%%
%% The rules below are the rules of FoundationDB 7.3
%% (`fdbserver/ClusterRecovery.actor.cpp', `fdbserver/masterserver.actor.cpp'
%% and `fdbclient/ServerKnobs.cpp'). The paper does not state them.
%%
%% <ul>
%% <li>The first commit version of an epoch is the Recovery Version plus
%%   `max_versions_in_flight' (default: the versions of 100 seconds). It is
%%   1 if the Recovery Version is 0. Its previous LSN is the Recovery
%%   Version. The recovery transaction has this version.</li>
%% <li>Each later commit version adds the versions of the time since the
%%   last commit version, at `versions_per_second' (default 1,000,000). The
%%   Sequencer adds at least 1, and at most the versions of 5 seconds.</li>
%% <li>The read version is the highest commit version that a Proxy has
%%   reported as committed. It starts at the first commit version of the
%%   epoch.</li>
%% </ul>
%%
%% == Versions in flight ==
%%
%% The Sequencer assigns a commit version only if the version is less than
%% `max_versions_in_flight' above the read version. Otherwise
%% `commit_version/1' returns `{error, versions_in_flight}'.
%%
%% The read version is at or below the Recovery Version of the recovery
%% that ends the epoch, because every LogServer of the epoch has stored the
%% record with the read version. A LogServer of the epoch therefore stores
%% no record of a client at or above the Recovery Version plus
%% `max_versions_in_flight', which is the first commit version of the next
%% epoch. A record of a client that the recovery discards has an LSN below
%% every version of the next epoch. A StorageServer depends on this rule
%% (see {@link deadlift_storage}).
%%
%% The rule does not hold for the recovery transaction, which the
%% Sequencer sends before it has a read version. If an epoch never accepts
%% commits, the next epoch can have the same Recovery Version, and then the
%% two recovery transactions have the same LSN. A recovery transaction has
%% no content, so a read is not affected. `formal/Recovery.tla' has this
%% case (`BelowNextEpoch').
%%
%% FoundationDB uses the constant `MAX_VERSIONS_IN_FLIGHT' for both
%% purposes. The limit in the Sequencer of deadlift is not taken from the
%% FoundationDB source. This part is not in the paper.
%%
%% The gap at the start of an epoch is larger than the MVCC window of the
%% Resolver (`mvcc_window', default: the versions of 5 seconds). The
%% Resolver of the new epoch has no history. Because of the gap, the
%% Resolver treats a transaction with a read version of the previous epoch
%% as too old. See {@link deadlift_resolver}.
%%
%% With `versions_per_second => 0', each commit version is the previous
%% one plus 1, `max_versions_in_flight' is 1000, and the MVCC window has no
%% limit. The unit tests use this option.
%%
%% `epoch_gap' replaces `max_versions_in_flight' in the first commit
%% version of an epoch. It plants a defect for the simulation.
%%
%% A Sequencer does not learn that a later epoch exists. The Sequencer of
%% an ended epoch still assigns versions and still answers `read_version/1'.
%% The LogServers of the ended epoch are stopped, so no record with these
%% versions is stored. A Proxy confirms with the LogServers that the epoch
%% is live before the Proxy gives the read version to a client. See
%% {@link deadlift_proxy}.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Sequencer of deadlift sends the recovery transaction itself,
%%   after the write of the coordinated state. The master of FoundationDB
%%   sends it through a proxy, before the write of the coordinated state.
%%   The recovery transaction of FoundationDB also contains the
%%   configuration of the transaction system. The recovery transaction of
%%   deadlift has no content.</li>
%% <li>FoundationDB also keeps the commit versions near the time of the
%%   wall clock (`figureVersion' in the source). deadlift does not.</li>
%% </ul>
-module(deadlift_sequencer).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1]).
-export([commit_version/1, commit_version/2, recovered/1, recovered/2]).
-export([read_version/1, read_version/2, defaults/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-type epoch() :: deadlift_cstate:epoch().
-type version() :: deadlift_cstate:version().

-record(state, {
    options :: deadlift_recovery:options(),
    recovered :: deadlift_recovery:result() | undefined,
    %% The last LSN that the Sequencer assigned. Before the first LSN of
    %% the epoch, this is the Recovery Version.
    last = 0 :: version(),
    %% The first commit version of the epoch.
    first = 1 :: version(),
    %% The time of the last assignment, in microseconds.
    last_time = 0 :: integer(),
    %% The highest version that a Proxy has reported as committed.
    live = 0 :: version()
}).

-define(TIMEOUT_MS, 10000).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts a Sequencer without a link to the caller. The function
%% returns before the recovery ends.
-spec start(deadlift_recovery:options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(deadlift_recovery:options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Sequencer) ->
    gen_server:stop(Sequencer).

%% @equiv commit_version(Sequencer, 10000)
-spec commit_version(gen_server:server_ref()) -> {ok, epoch(), version(), version()} | {error, versions_in_flight}.
commit_version(Sequencer) ->
    commit_version(Sequencer, ?TIMEOUT_MS).

%% @doc Assigns the next LSN. The result is `{ok, Epoch, Prev, LSN}', where
%% `Prev' is the previous LSN. The result is `{error, versions_in_flight}'
%% if the LSN would be `max_versions_in_flight' or more above the read
%% version.
-spec commit_version(gen_server:server_ref(), timeout()) ->
    {ok, epoch(), version(), version()} | {error, versions_in_flight}.
commit_version(Sequencer, Timeout) ->
    gen_server:call(Sequencer, commit_version, Timeout).

%% @equiv read_version(Sequencer, 10000)
-spec read_version(gen_server:server_ref()) -> {ok, version()}.
read_version(Sequencer) ->
    read_version(Sequencer, ?TIMEOUT_MS).

%% @doc Returns the read version: the highest LSN that a Proxy has reported
%% as stored on every LogServer of the epoch. A client must use `deadlift_proxy:read_version/1',
%% which also confirms that the epoch is live.
-spec read_version(gen_server:server_ref(), timeout()) -> {ok, version()}.
read_version(Sequencer, Timeout) ->
    gen_server:call(Sequencer, read_version, Timeout).

%% @doc The version options with their defaults: `versions_per_second',
%% `max_versions_in_flight', `max_read_life' and `mvcc_window'.
-spec defaults(map()) -> map().
defaults(Options) ->
    PerSecond = maps:get(versions_per_second, Options, 1000000),
    Window =
        case PerSecond of
            0 -> infinity;
            _ -> 5 * PerSecond
        end,
    InFlight =
        case PerSecond of
            0 -> 1000;
            _ -> 100 * PerSecond
        end,
    #{
        versions_per_second => PerSecond,
        max_versions_in_flight => maps:get(max_versions_in_flight, Options, InFlight),
        max_read_life => maps:get(max_read_life, Options, max(1, 5 * PerSecond)),
        mvcc_window => maps:get(mvcc_window, Options, Window)
    }.

%% @equiv recovered(Sequencer, 10000)
-spec recovered(gen_server:server_ref()) -> {ok, deadlift_recovery:result()}.
recovered(Sequencer) ->
    recovered(Sequencer, ?TIMEOUT_MS).

%% @doc Returns the result of the recovery: the epoch, the log system, the
%% Recovery Version, the previous epoch's end version, and the first commit
%% version of the epoch (`recovery_transaction_version'). A Proxy starts
%% with the previous epoch's end version as its Known Committed Version.
-spec recovered(gen_server:server_ref(), timeout()) -> {ok, deadlift_recovery:result()}.
recovered(Sequencer, Timeout) ->
    gen_server:call(Sequencer, recovered, Timeout).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options) ->
    {ok, #state{options = Options}, {continue, recover}}.

%% @private
handle_continue(recover, State = #state{options = Options}) ->
    case deadlift_recovery:recover(Options) of
        {ok, Recovered = #{epoch := Epoch, recovery_version := RV}} ->
            #{max_versions_in_flight := InFlight, mvcc_window := Window} = defaults(Options),
            %% The rule of `readTransactionSystemState' in the source.
            First =
                case RV of
                    0 -> 1;
                    _ -> RV + maps:get(epoch_gap, Options, InFlight)
                end,
            Recruit = maps:get(recruit_resolver, Options, fun deadlift_resolver:start/1),
            maybe
                {ok, _Resolver} ?= recruit_resolver(Recruit, #{epoch => Epoch, version => RV, mvcc_window => Window}),
                ok ?= recovery_transaction(Recovered, First),
                Recovered1 = Recovered#{recovery_transaction_version => First},
                {noreply, State#state{
                    recovered = Recovered1,
                    last = First,
                    first = First,
                    last_time = erlang:monotonic_time(microsecond),
                    live = First
                }}
            else
                {error, Reason} ->
                    {stop, {shutdown, {recovery, Reason}}, State}
            end;
        {error, Reason} ->
            {stop, {shutdown, {recovery, Reason}}, State}
    end.

%% @private
handle_call(commit_version, _From, State = #state{recovered = #{epoch := Epoch}, options = Options}) ->
    #state{last = Last, last_time = LastTime, live = Live} = State,
    #{versions_per_second := PerSecond, max_read_life := MaxReadLife, max_versions_in_flight := InFlight} = defaults(Options),
    Now = erlang:monotonic_time(microsecond),
    %% The rule of `getVersion' in the source.
    Add = max(1, min(MaxReadLife, PerSecond * (Now - LastTime) div 1000000)),
    case Last + Add < Live + InFlight of
        true -> {reply, {ok, Epoch, Last, Last + Add}, State#state{last = Last + Add, last_time = Now}};
        false -> {reply, {error, versions_in_flight}, State}
    end;
handle_call({committed, Version}, _From, State = #state{live = Live}) ->
    {reply, ok, State#state{live = max(Live, Version)}};
handle_call(read_version, _From, State = #state{live = Live}) ->
    {reply, {ok, Live}, State};
handle_call(recovered, _From, State = #state{recovered = Recovered}) ->
    {reply, {ok, Recovered}, State}.

recruit_resolver(Recruit, ResolverOptions = #{epoch := Epoch}) ->
    case Recruit(ResolverOptions) of
        {ok, Resolver} ->
            %% A Resolver on another node has registered on that node. The
            %% registry of this node receives the name later, so the
            %% Sequencer adds the name itself.
            ok =
                case is_pid(Resolver) of
                    true -> deadlift_registry:learn({resolver, Epoch}, Resolver);
                    false -> ok
                end,
            {ok, Resolver};
        {error, Reason} -> {error, {recruit_resolver, Reason}}
    end.

%% Commits the recovery transaction: the record `First' with the previous
%% LSN `RV'. The KCV of the record is the previous epoch's end version.
recovery_transaction(Recovered, First) ->
    #{
        epoch := Epoch,
        recovery_version := RV,
        previous_epoch_end_version := PEV,
        log_system := #{logs := Logs, replication := K}
    } = Recovered,
    Replicas = deadlift_log:replicas(Logs, K, First),
    try
        [committed] = call(deadlift_resolver:ref(Epoch), {resolve, Epoch, RV, First, [#{read_version => 0, reads => [], writes => []}]}),
        lists:foreach(
            fun(Log) ->
                Sent =
                    case lists:member(Log, Replicas) of
                        true -> {body, recovery_transaction};
                        false -> empty
                    end,
                {ok, _DV} = call(deadlift_log:ref(Log), {push, Epoch, RV, First, PEV, Sent})
            end,
            Logs
        )
    catch
        Class:Reason when Class =:= exit; Class =:= error ->
            {error, {recovery_transaction, reason(Reason)}}
    end.

call(undefined, _Request) ->
    exit(noproc);
call(Pid, Request) ->
    gen_server:call(Pid, Request, ?TIMEOUT_MS).

%% An exit reason without the description of the call.
reason({Reason, {_Module, call, _Arguments}}) -> Reason;
reason(Reason) -> Reason.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
%% An answer to a lock that arrives after the recovery has ended.
handle_info(_Message, State) ->
    {noreply, State}.
