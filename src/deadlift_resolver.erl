%% @doc The Resolver.
%%
%% A Resolver checks a transaction for conflicts with the transactions that
%% committed after the read version of the transaction. See section 2.4.2
%% and Algorithm 1 of Zhou et al., "FoundationDB: A Distributed Unbundled
%% Transactional Key Value Store", SIGMOD 2021. The rules that the paper
%% does not state are taken from `fdbserver/Resolver.actor.cpp' and
%% `fdbserver/SkipList.cpp' of FoundationDB 7.3. The comments below call
%% them "the source". `formal/Resolver.tla' models the Resolver with the
%% actions `Commit' and `NewEpoch'.
%%
%% == Algorithm 1 ==
%%
%% The Resolver keeps `lastCommit': for each key, the commit version of the
%% last transaction that wrote the key. For a transaction with the read
%% version `Rv', the read keys `R', the write keys `W' and the commit
%% version `Cv':
%%
%% <ol>
%% <li>If `lastCommit[K] > Rv' for a key `K' in `R', the transaction has a
%%   conflict and is not committed.</li>
%% <li>Otherwise the transaction is committed, and `lastCommit[K]' becomes
%%   `Cv' for each key `K' in `W'.</li>
%% </ol>
%%
%% == MVCC window ==
%%
%% The Resolver keeps `lastCommit' only for the versions of the MVCC
%% window. With the commit version `Cv' and the window `Window', the oldest
%% version that the Resolver keeps is `Cv - Window'. This is the rule of the
%% source:
%%
%% <ul>
%% <li>A transaction that has read keys and a read version below
%%   `Cv - Window' is too old, and is not committed.</li>
%% <li>A transaction without read keys is never too old.</li>
%% <li>The Resolver removes the entries of `lastCommit' below
%%   `Cv - Window'.</li>
%% </ul>
%%
%% The Resolver of a new epoch has no history. The first commit version of
%% a new epoch is more than `Window' above the last version of the previous
%% epoch (see {@link deadlift_sequencer}). A transaction with a read version
%% of the previous epoch is therefore too old for the new Resolver.
%%
%% == Order ==
%%
%% The Resolver handles the commit versions in their order. A request has
%% the previous commit version. A request that arrives before the request
%% with the previous version waits. The source has the same rule
%% (`prevVersion').
%%
%% A Resolver records the writes of a transaction that it admits. If the
%% Proxy then fails before the LogServers store the transaction, a later
%% transaction can get a conflict with a transaction that did not commit.
%% The paper accepts this (section 2.4.2).
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Resolver checks keys. The Resolver of the paper checks key
%%   ranges.</li>
%% <li>A request has one transaction. A request of FoundationDB has a batch
%%   of transactions with one commit version.</li>
%% <li>One Resolver handles every key. FoundationDB divides the key space
%%   over several Resolvers.</li>
%% </ul>
-module(deadlift_resolver).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1, ref/1]).
-export([new/1, check/3]).
-export([init/1, handle_call/3, handle_cast/2]).

-export_type([options/0, transaction/0, result/0, history/0]).

-type epoch() :: deadlift_cstate:epoch().
-type version() :: deadlift_cstate:version().

%% `version' is the last version of the previous epoch: the previous
%% version of the first request. `mvcc_window' is a number of versions, or
%% `infinity'. `check => false' plants a defect for the simulation: the
%% Resolver admits every transaction.
-type options() :: #{
    epoch := epoch(),
    version := version(),
    mvcc_window => pos_integer() | infinity,
    check => boolean()
}.

-type transaction() :: #{
    read_version := version(),
    reads := [term()],
    writes := [term()]
}.

-type result() :: committed | conflict | too_old.

-opaque history() :: #{
    last_commit := #{term() => version()},
    oldest := integer(),
    window := pos_integer() | infinity
}.

-record(state, {
    epoch :: epoch(),
    %% The last commit version that the Resolver has handled.
    version :: version(),
    history :: history(),
    check :: boolean(),
    %% PreviousVersion -> {Version, Transaction, Callers}: the requests that
    %% wait for the request with the previous version.
    waiting = #{} :: #{version() => {version(), transaction(), [gen_server:from()]}}
}).

%% ---------------------------------------------------------------------------
%% Client
%% ---------------------------------------------------------------------------

%% @doc Starts the Resolver of an epoch without a link to the caller. The
%% Resolver registers with `deadlift_registry'.
-spec start(options()) -> {ok, pid()} | {error, term()}.
start(Options) ->
    gen_server:start(?MODULE, Options, []).

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec stop(gen_server:server_ref()) -> ok.
stop(Resolver) ->
    gen_server:call(Resolver, stop).

%% @doc The process of the Resolver of `Epoch', or `undefined'. A Proxy
%% sends the request `{resolve, Epoch, Prev, Version, Transaction}' to this
%% process. The answer is a `result()', or `{error, {epoch, Epoch}}'.
-spec ref(epoch()) -> pid() | undefined.
ref(Epoch) ->
    deadlift_registry:whereis({resolver, Epoch}).

%% ---------------------------------------------------------------------------
%% Algorithm 1
%% ---------------------------------------------------------------------------

%% @doc A history without a transaction.
-spec new(pos_integer() | infinity) -> history().
new(Window) ->
    #{last_commit => #{}, oldest => 0, window => Window}.

%% @doc Checks the transaction `Transaction' with the commit version
%% `Version'. Returns the result and the history after the transaction.
-spec check(transaction(), version(), history()) -> {result(), history()}.
check(#{read_version := ReadVersion, reads := Reads, writes := Writes}, Version, History) ->
    #{last_commit := LastCommit0, oldest := Oldest0, window := Window} = History,
    NewOldest = new_oldest(Version, Window, Oldest0),
    LastCommit =
        case NewOldest > Oldest0 of
            true -> maps:filter(fun(_Key, V) -> V >= NewOldest end, LastCommit0);
            false -> LastCommit0
        end,
    Result =
        case ReadVersion < NewOldest andalso Reads =/= [] of
            true ->
                too_old;
            false ->
                case lists:any(fun(Key) -> maps:get(Key, LastCommit, 0) > ReadVersion end, Reads) of
                    true -> conflict;
                    false -> committed
                end
        end,
    LastCommit1 =
        case Result of
            committed -> lists:foldl(fun(Key, Acc) -> Acc#{Key => Version} end, LastCommit, Writes);
            _ -> LastCommit
        end,
    {Result, History#{last_commit := LastCommit1, oldest := NewOldest}}.

%% The oldest version only rises.
new_oldest(_Version, infinity, Oldest) -> Oldest;
new_oldest(Version, Window, Oldest) -> max(Oldest, Version - Window).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

%% @private
init(Options = #{epoch := Epoch, version := Version}) ->
    case deadlift_registry:register({resolver, Epoch}) of
        yes ->
            {ok, #state{
                epoch = Epoch,
                version = Version,
                history = new(maps:get(mvcc_window, Options, infinity)),
                check = maps:get(check, Options, true)
            }};
        no ->
            {stop, {already_registered, Epoch}}
    end.

%% @private
handle_call({resolve, Epoch, _Prev, _Version, _Transaction}, _From, State = #state{epoch = Mine}) when Epoch =/= Mine ->
    {reply, {error, {epoch, Mine}}, State};
handle_call({resolve, _Epoch, Prev, Version, Transaction}, From, State = #state{version = Prev}) when Version > Prev ->
    {Result, State1} = resolve(Version, Transaction, State),
    gen_server:reply(From, Result),
    {noreply, release(State1)};
handle_call({resolve, _Epoch, Prev, Version, Transaction}, From, State = #state{version = Mine, waiting = Waiting}) when Prev > Mine, Version > Prev ->
    %% The request with the previous version has not arrived.
    case Waiting of
        #{Prev := {Version, Transaction, Callers}} ->
            {noreply, State#state{waiting = Waiting#{Prev := {Version, Transaction, [From | Callers]}}}};
        #{Prev := _Other} ->
            {reply, {error, {conflicting_request, Version}}, State};
        _ ->
            {noreply, State#state{waiting = Waiting#{Prev => {Version, Transaction, [From]}}}}
    end;
handle_call({resolve, _Epoch, _Prev, Version, _Transaction}, _From, State) ->
    %% A version that the Resolver has handled. The Resolver does not keep
    %% the result of a transaction.
    {reply, {error, {handled, Version}}, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

resolve(Version, Transaction, State = #state{history = History, check = Check}) ->
    {Result0, History1} = check(Transaction, Version, History),
    case Check of
        true ->
            {Result0, State#state{version = Version, history = History1}};
        false ->
            %% The planted defect: every transaction is admitted, and its
            %% writes are recorded.
            {_, History2} = check(Transaction#{reads := []}, Version, History),
            {committed, State#state{version = Version, history = History2}}
    end.

%% Handles the waiting requests whose previous version is now the last
%% version.
release(State = #state{version = Version, waiting = Waiting}) ->
    case maps:take(Version, Waiting) of
        {{Next, Transaction, Callers}, Waiting1} ->
            {Result, State1} = resolve(Next, Transaction, State#state{waiting = Waiting1}),
            [gen_server:reply(Caller, Result) || Caller <- Callers],
            release(State1);
        error ->
            State
    end.
