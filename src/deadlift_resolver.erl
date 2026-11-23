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
%% The Resolver keeps `lastCommit': for each key range, the commit version
%% of the last transaction that wrote the range. For a transaction with the
%% read version `Rv', the read ranges `R', the write ranges `W' and the
%% commit version `Cv':
%%
%% <ol>
%% <li>If `lastCommit[K] > Rv' for a key `K' in a range of `R', the
%%   transaction has a conflict and is not committed.</li>
%% <li>Otherwise the transaction is committed, and `lastCommit[K]' becomes
%%   `Cv' for each key `K' in a range of `W'.</li>
%% </ol>
%%
%% == Key ranges ==
%%
%% A key is an Erlang term. The order of the keys is the order of Erlang
%% terms. A transaction has two kinds of read entry and two kinds of write
%% entry:
%%
%% <ul>
%% <li>`reads' and `writes' are lists of keys.</li>
%% <li>`read_ranges' and `write_ranges' are lists of `{Begin, End}'. A
%%   range has every key `K' with `Begin =< K' and `K < End'. A range
%%   with `Begin >= End' has no key.</li>
%% </ul>
%%
%% A read range has the keys that are absent from the database. A
%% transaction that reads a range and finds no value for a key therefore
%% gets a conflict when another transaction writes that key.
%%
%% `lastCommit' is an ordered set of bounds. Each bound has the commit
%% version that holds from the bound to the next bound. The version below
%% the first bound is 0. A bound is `{Key, 0}' for the position of `Key',
%% or `{Key, 1}' for the position directly after `Key'. The key `K' is the
%% interval from `{K, 0}' to `{K, 1}'. The range `{Begin, End}' is the
%% interval from `{Begin, 0}' to `{End, 0}'. FoundationDB keeps the same
%% information in a skip list (`fdbserver/SkipList.cpp'). FoundationDB has
%% byte strings as keys, and the key `K' is the range from `K' to `K'
%% followed by a zero byte.
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
%% == Batch ==
%%
%% A request has the transactions of one batch of a Proxy, in the order of
%% the batch. Every transaction of the batch has the commit version of the
%% batch. The Resolver checks the transactions in the order of the batch:
%%
%% <ul>
%% <li>A transaction has a conflict if a key that it read was written by an
%%   earlier transaction of the batch that the Resolver admitted. The
%%   commit version of the batch is above the read version of every
%%   transaction of the batch.</li>
%% <li>The writes of a transaction that the Resolver did not admit give no
%%   conflict.</li>
%% </ul>
%%
%% The source has the same rules (`checkIntraBatchConflicts' in
%% `fdbserver/SkipList.cpp').
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
%% <li>A key is an Erlang term. A key of FoundationDB is a byte
%%   string.</li>
%% <li>One Resolver handles every key. FoundationDB divides the key space
%%   over several Resolvers.</li>
%% </ul>
-module(deadlift_resolver).

-behaviour(gen_server).

-include("deadlift_eta.hrl").

-export([start/1, start_link/1, stop/1, ref/1]).
-export([new/1, check/3, check_batch/3]).
-export([init/1, handle_call/3, handle_cast/2]).

-export_type([options/0, transaction/0, key_range/0, result/0, history/0]).

-type epoch() :: deadlift_cstate:epoch().
-type version() :: deadlift_cstate:version().

%% `version' is the last version of the previous epoch: the previous
%% version of the first request. `mvcc_window' is a number of versions, or
%% `infinity'. Two options plant a defect for the simulation. With
%% `check => false' the Resolver admits every transaction. With
%% `check_ranges => false' the Resolver does not check the read ranges of a
%% transaction. With `check_batch => false' the Resolver does not check a
%% transaction against the earlier transactions of its batch. With
%% `check_write_ranges => false' the Resolver does not record the write
%% ranges of a transaction.
-type options() :: #{
    epoch := epoch(),
    version := version(),
    mvcc_window => pos_integer() | infinity,
    check => boolean(),
    check_ranges => boolean(),
    check_batch => boolean(),
    check_write_ranges => boolean()
}.

-type key_range() :: {Begin :: term(), End :: term()}.

%% The default of `read_ranges' and of `write_ranges' is `[]'.
-type transaction() :: #{
    read_version := version(),
    reads := [term()],
    writes := [term()],
    read_ranges => [key_range()],
    write_ranges => [key_range()]
}.

%% A position in the order of the keys: `{Key, 0}' is the position of
%% `Key', and `{Key, 1}' is the position directly after `Key'.
-type bound() :: {term(), 0 | 1}.

-type result() :: committed | conflict | too_old.

-opaque history() :: #{
    last_commit := gb_trees:tree(bound(), version()),
    oldest := integer(),
    window := pos_integer() | infinity
}.

-record(state, {
    epoch :: epoch(),
    %% The last commit version that the Resolver has handled.
    version :: version(),
    history :: history(),
    check :: boolean(),
    check_ranges :: boolean(),
    check_batch :: boolean(),
    check_write_ranges :: boolean(),
    %% PreviousVersion -> {Version, Transactions, Callers}: the requests that
    %% wait for the request with the previous version.
    waiting = #{} :: #{version() => {version(), [transaction()], [gen_server:from()]}}
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
%% sends the request `{resolve, Epoch, Prev, Version, Transactions}' to
%% this process, with the transactions of one batch. The answer is a list
%% with one `result()' for each transaction, or `{error, {epoch, Epoch}}'.
-spec ref(epoch()) -> pid() | undefined.
ref(Epoch) ->
    deadlift_registry:whereis({resolver, Epoch}).

%% ---------------------------------------------------------------------------
%% Algorithm 1
%% ---------------------------------------------------------------------------

%% @doc A history without a transaction.
-spec new(pos_integer() | infinity) -> history().
new(Window) ->
    #{last_commit => gb_trees:empty(), oldest => 0, window => Window}.

%% @doc Checks the transaction `Transaction' with the commit version
%% `Version'. Returns the result and the history after the transaction.
-spec check(transaction(), version(), history()) -> {result(), history()}.
check(Transaction = #{read_version := ReadVersion}, Version, History) ->
    #{last_commit := LastCommit0, oldest := Oldest0, window := Window} = History,
    Reads = intervals(reads, read_ranges, Transaction),
    Writes = intervals(writes, write_ranges, Transaction),
    NewOldest = new_oldest(Version, Window, Oldest0),
    LastCommit =
        case NewOldest > Oldest0 of
            true -> prune(NewOldest, LastCommit0);
            false -> LastCommit0
        end,
    Result =
        case ReadVersion < NewOldest andalso Reads =/= [] of
            true ->
                too_old;
            false ->
                case lists:any(fun({Begin, End}) -> max_version(Begin, End, LastCommit) > ReadVersion end, Reads) of
                    true -> conflict;
                    false -> committed
                end
        end,
    LastCommit1 =
        case Result of
            committed -> lists:foldl(fun({Begin, End}, Acc) -> set_version(Begin, End, Version, Acc) end, LastCommit, Writes);
            _ -> LastCommit
        end,
    {Result, History#{last_commit := LastCommit1, oldest := NewOldest}}.

%% @doc Checks the transactions of one batch, in their order, with the
%% commit version `Version'. Returns one result for each transaction, and
%% the history after the batch.
-spec check_batch([transaction()], version(), history()) -> {[result()], history()}.
check_batch(Transactions, Version, History) ->
    lists:mapfoldl(fun(Transaction, Acc) -> check(Transaction, Version, Acc) end, History, Transactions).

%% The keys and the ranges of a transaction as intervals of bounds. A range
%% without a key gives no interval.
-spec intervals(reads | writes, read_ranges | write_ranges, transaction()) -> [{bound(), bound()}].
intervals(Keys, Ranges, Transaction) ->
    [{{Key, 0}, {Key, 1}} || Key <- maps:get(Keys, Transaction)] ++
        [{{Begin, 0}, {End, 0}} || {Begin, End} <- maps:get(Ranges, Transaction, []), Begin < End].

%% The commit version at the bound `Bound': the version of the last bound
%% at or below `Bound', or 0.
version_at(Bound, LastCommit) ->
    case gb_trees:lookup(Bound, LastCommit) of
        {value, Version} ->
            Version;
        none ->
            case gb_trees:smaller(Bound, LastCommit) of
                {_Below, Version} -> Version;
                none -> 0
            end
    end.

%% The highest commit version in the interval from `Begin' to `End'.
max_version(Begin, End, LastCommit) ->
    max_until(gb_trees:next(gb_trees:iterator_from(Begin, LastCommit)), End, version_at(Begin, LastCommit)).

max_until({Bound, Version, Iterator}, End, Max) when Bound < End ->
    max_until(gb_trees:next(Iterator), End, max(Max, Version));
max_until(_NoBoundInTheInterval, _End, Max) ->
    Max.

%% Sets the commit version of the interval from `Begin' to `End'. The
%% version from `End' on does not change.
set_version(Begin, End, Version, LastCommit) ->
    AtEnd = version_at(End, LastCommit),
    Inside = bounds_in(gb_trees:next(gb_trees:iterator_from(Begin, LastCommit)), End),
    LastCommit1 = lists:foldl(fun gb_trees:delete/2, LastCommit, Inside),
    gb_trees:enter(End, AtEnd, gb_trees:enter(Begin, Version, LastCommit1)).

bounds_in({Bound, _Version, Iterator}, End) when Bound < End ->
    [Bound | bounds_in(gb_trees:next(Iterator), End)];
bounds_in(_NoBoundInTheInterval, _End) ->
    [].

%% Removes the commit versions below `Oldest'. A removed version becomes
%% 0. A bound with the version of the bound before it is removed.
prune(Oldest, LastCommit) ->
    {Kept, _Last} = lists:foldl(
        fun({Bound, Version0}, {Acc, Before}) ->
            Version =
                case Version0 < Oldest of
                    true -> 0;
                    false -> Version0
                end,
            case Version =:= Before of
                true -> {Acc, Before};
                false -> {[{Bound, Version} | Acc], Version}
            end
        end,
        {[], 0},
        gb_trees:to_list(LastCommit)
    ),
    gb_trees:from_orddict(lists:reverse(Kept)).

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
                check = maps:get(check, Options, true),
                check_ranges = maps:get(check_ranges, Options, true),
                check_batch = maps:get(check_batch, Options, true),
                check_write_ranges = maps:get(check_write_ranges, Options, true)
            }};
        no ->
            {stop, {already_registered, Epoch}}
    end.

%% @private
handle_call({resolve, Epoch, _Prev, _Version, _Transactions}, _From, State = #state{epoch = Mine}) when Epoch =/= Mine ->
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
handle_call({resolve, _Epoch, _Prev, Version, _Transactions}, _From, State) ->
    %% A version that the Resolver has handled. The Resolver does not keep
    %% the result of a transaction.
    {reply, {error, {handled, Version}}, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% @private
handle_cast(_Message, State) ->
    {noreply, State}.

resolve(Version, Transactions0, State = #state{history = History}) ->
    #state{check = Check, check_ranges = CheckRanges, check_batch = CheckBatch} = State,
    Transactions1 =
        case CheckRanges of
            true -> Transactions0;
            %% The planted defect: the read ranges are not checked.
            false -> [Transaction#{read_ranges => []} || Transaction <- Transactions0]
        end,
    Transactions =
        case State#state.check_write_ranges of
            true -> Transactions1;
            %% The planted defect: the write ranges are not recorded.
            false -> [Transaction#{write_ranges => []} || Transaction <- Transactions1]
        end,
    WithoutReads = fun(Transaction) -> Transaction#{reads := [], read_ranges => []} end,
    {Results, History1} =
        case {Check, CheckBatch} of
            {true, true} ->
                check_batch(Transactions, Version, History);
            {false, _} ->
                %% The planted defect: every transaction is admitted, and
                %% its writes are recorded.
                check_batch([WithoutReads(Transaction) || Transaction <- Transactions], Version, History);
            {true, false} ->
                %% The planted defect: each transaction is checked against
                %% the history before the batch. The writes of the admitted
                %% transactions are recorded after the checks.
                Checked = [{element(1, check(Transaction, Version, History)), Transaction} || Transaction <- Transactions],
                Admitted = [WithoutReads(Transaction) || {committed, Transaction} <- Checked],
                {_, History2} = check_batch(Admitted, Version, History),
                %% The oldest version rises also when no transaction is
                %% admitted.
                {_, History3} = check(#{read_version => 0, reads => [], writes => []}, Version, History2),
                {[Result || {Result, _Transaction} <- Checked], History3}
        end,
    {Results, State#state{version = Version, history = History1}}.

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
