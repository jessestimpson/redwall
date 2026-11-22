%% @doc The Sequencer.
%%
%% The Sequencer of an epoch performs the recovery, and then assigns the
%% commit versions of the epoch. See sections 2.3.1, 2.4.1 and 2.4.4 of
%% Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
%% Value Store", SIGMOD 2021. `formal/Recovery.tla' models the assignment
%% of a version with the action `Assign'.
%%
%% A Sequencer starts with a recovery ({@link deadlift_recovery}). If the
%% recovery fails, the Sequencer stops with the reason
%% `{shutdown, {recovery, Reason}}'. A request that arrives during the
%% recovery waits for the end of the recovery.
%%
%% After the recovery, the Sequencer starts the Resolver of the epoch, and
%% answers `commit_version/1' with a new LSN and the previous LSN. A Proxy
%% sends both to the Resolver and to the LogServers.
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
%%   Version.</li>
%% <li>Each later commit version adds the versions of the time since the
%%   last commit version, at `versions_per_second' (default 1,000,000). The
%%   Sequencer adds at least 1, and at most the versions of 5 seconds.</li>
%% <li>The read version is the highest commit version that a Proxy has
%%   reported as committed. It starts at the first commit version of the
%%   epoch.</li>
%% </ul>
%%
%% The gap at the start of an epoch is larger than the MVCC window of the
%% Resolver (`mvcc_window', default: the versions of 5 seconds). The
%% Resolver of the new epoch has no history. Because of the gap, the
%% Resolver treats a transaction with a read version of the previous epoch
%% as too old. See {@link deadlift_resolver}.
%%
%% With `versions_per_second => 0', each commit version is the previous
%% one plus 1, the first commit version of an epoch is the Recovery Version
%% plus 1, and the MVCC window has no limit. The unit tests use this
%% option.
%%
%% A Sequencer does not learn that a later epoch exists. The Sequencer of
%% an ended epoch still assigns versions. The LogServers of the ended epoch
%% are stopped, so no record with these versions is stored.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>FoundationDB also keeps the commit versions near the time of the
%%   wall clock (`figureVersion' in the source). deadlift does not.</li>
%% <li>A Proxy of FoundationDB confirms that its epoch is live before it
%%   gives a read version. deadlift does not. A read version from the
%%   Sequencer of an ended epoch can be below a commit of a later
%%   epoch.</li>
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
    %% Whether the Sequencer has assigned the first commit version.
    assigned = false :: boolean(),
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
-spec commit_version(gen_server:server_ref()) -> {ok, epoch(), version(), version()}.
commit_version(Sequencer) ->
    commit_version(Sequencer, ?TIMEOUT_MS).

%% @doc Assigns the next LSN. The result is `{ok, Epoch, Prev, LSN}', where
%% `Prev' is the previous LSN.
-spec commit_version(gen_server:server_ref(), timeout()) -> {ok, epoch(), version(), version()}.
commit_version(Sequencer, Timeout) ->
    gen_server:call(Sequencer, commit_version, Timeout).

%% @equiv read_version(Sequencer, 10000)
-spec read_version(gen_server:server_ref()) -> {ok, version()}.
read_version(Sequencer) ->
    read_version(Sequencer, ?TIMEOUT_MS).

%% @doc Returns the read version: the highest commit version that a Proxy
%% has reported as committed.
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
    #{
        versions_per_second => PerSecond,
        max_versions_in_flight => maps:get(max_versions_in_flight, Options, max(1, 100 * PerSecond)),
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
                    _ -> RV + InFlight
                end,
            Recruit = maps:get(recruit_resolver, Options, fun deadlift_resolver:start/1),
            case Recruit(#{epoch => Epoch, version => RV, mvcc_window => Window}) of
                {ok, _Resolver} ->
                    Recovered1 = Recovered#{recovery_transaction_version => First},
                    {noreply, State#state{recovered = Recovered1, last = RV, first = First, live = First}};
                {error, Reason} ->
                    {stop, {shutdown, {recovery, {recruit_resolver, Reason}}}, State}
            end;
        {error, Reason} ->
            {stop, {shutdown, {recovery, Reason}}, State}
    end.

%% @private
handle_call(commit_version, _From, State = #state{recovered = #{epoch := Epoch}, assigned = false}) ->
    #state{last = RV, first = First} = State,
    State1 = State#state{last = First, assigned = true, last_time = erlang:monotonic_time(microsecond)},
    {reply, {ok, Epoch, RV, First}, State1};
handle_call(commit_version, _From, State = #state{recovered = #{epoch := Epoch}, options = Options}) ->
    #state{last = Last, last_time = LastTime} = State,
    #{versions_per_second := PerSecond, max_read_life := MaxReadLife} = defaults(Options),
    Now = erlang:monotonic_time(microsecond),
    %% The rule of `getVersion' in the source.
    Add = max(1, min(MaxReadLife, PerSecond * (Now - LastTime) div 1000000)),
    {reply, {ok, Epoch, Last, Last + Add}, State#state{last = Last + Add, last_time = Now}};
handle_call({committed, Version}, _From, State = #state{live = Live}) ->
    {reply, ok, State#state{live = max(Live, Version)}};
handle_call(read_version, _From, State = #state{live = Live}) ->
    {reply, {ok, Live}, State};
handle_call(recovered, _From, State = #state{recovered = Recovered}) ->
    {reply, {ok, Recovered}, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
%% An answer to a lock that arrives after the recovery has ended.
handle_info(_Message, State) ->
    {noreply, State}.
