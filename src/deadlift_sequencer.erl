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
%% After the recovery, the Sequencer answers `commit_version/1' with a new
%% LSN and the previous LSN. A Proxy sends both to the LogServers. The first
%% LSN of an epoch is the Recovery Version plus 1, and its previous LSN is
%% the Recovery Version.
%%
%% A Sequencer does not learn that a later epoch exists. The Sequencer of
%% an ended epoch still assigns versions. The LogServers of the ended epoch
%% are stopped, so no record with these versions is stored.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Sequencer raises the LSN by 1 for each commit version.
%%   FoundationDB raises the version with the time, by about 1,000,000 for
%%   each second.</li>
%% <li>The Sequencer does not give read versions yet.</li>
%% </ul>
-module(deadlift_sequencer).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1]).
-export([commit_version/1, commit_version/2, recovered/1, recovered/2]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-type epoch() :: deadlift_cstate:epoch().
-type version() :: deadlift_cstate:version().

-record(state, {
    options :: deadlift_recovery:options(),
    recovered :: deadlift_recovery:result() | undefined,
    %% The last LSN that the Sequencer assigned.
    last = 0 :: version()
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

%% @equiv recovered(Sequencer, 10000)
-spec recovered(gen_server:server_ref()) -> {ok, deadlift_recovery:result()}.
recovered(Sequencer) ->
    recovered(Sequencer, ?TIMEOUT_MS).

%% @doc Returns the result of the recovery: the epoch, the log system, the
%% Recovery Version and the previous epoch's end version. A Proxy starts
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
        {ok, Recovered = #{recovery_version := RV}} ->
            {noreply, State#state{recovered = Recovered, last = RV}};
        {error, Reason} ->
            {stop, {shutdown, {recovery, Reason}}, State}
    end.

%% @private
handle_call(commit_version, _From, State = #state{recovered = #{epoch := Epoch}, last = Last}) ->
    {reply, {ok, Epoch, Last, Last + 1}, State#state{last = Last + 1}};
handle_call(recovered, _From, State = #state{recovered = Recovered}) ->
    {reply, {ok, Recovered}, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

%% @private
%% An answer to a lock that arrives after the recovery has ended.
handle_info(_Message, State) ->
    {noreply, State}.
