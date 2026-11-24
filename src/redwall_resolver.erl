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
%%   `Cv - Window', for each transaction.</li>
%% </ul>
%%
%% == Tables ==
%%
%% `lastCommit' is an ETS table of the kind `ordered_set', with a bound as
%% the key. A second ETS table of the kind `ordered_set' has the bounds in
%% the order of their versions. The Resolver finds the entries below the
%% oldest version at the start of the second table, and reads no other
%% entry for the removal. Both tables are outside the heap of the Resolver
%% process, so a garbage collection of the process does not copy them.
%%
%% A benchmark on 2026-11-23 compared this structure with a `gb_trees'
%% tree that was rebuilt at an interval. At 100,000 writes per second, the
%% longest batch took 0.3 ms with the tables and 89 ms with the tree. The
%% README has the results.
%%
%% The functions `check/3' and `check_batch/3' change the tables of the
%% history. A caller uses the history that the last call returned.
%%
%% The Resolver of a new epoch has no history. The first commit version of
%% a new epoch is more than `Window' above the last version of the previous
%% epoch (see {@link redwall_sequencer}). A transaction with a read version
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
%% The Resolver handles the commit versions in their order. In redwall,
%% the process of an epoch ({@link redwall_epoch}) assigns the commit
%% version of a batch and calls `resolve/4' for the batch in one step, so
%% the calls are in the order of the versions. In the source, a request
%% to the Resolver process has the previous commit version, and a request
%% that arrives before the request with the previous version waits
%% (`prevVersion').
%%
%% == Process ==
%%
%% The Resolver is not a process in redwall. This module has functions on
%% a history, and the process of an epoch owns the history. FoundationDB
%% has a process for each role, because a process of FoundationDB runs on
%% one thread. The process of an epoch does the work of the Sequencer, of
%% the Resolver and of the Proxy of the paper. The three have the same
%% lifetime, and the failure of each of them starts a recovery. This part
%% is not in the paper.
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
-module(redwall_resolver).

-include("redwall_eta.hrl").

-export([new/1, check/3, check_batch/3, resolve/4]).

-export_type([flags/0, transaction/0, key_range/0, result/0, history/0]).

-type version() :: redwall_cstate:version().

%% Planted defects for the simulation. See `resolve/4'.
-type flags() :: #{
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

%% `last_commit' is an ETS table of the kind `ordered_set' with one row
%% `{Bound, Version}' for each bound. `versions' is an ETS table of the
%% kind `ordered_set' with one row `{{Version, Bound}}' for each bound
%% with a version above 0.
-opaque history() :: #{
    last_commit := ets:table(),
    versions := ets:table(),
    oldest := integer(),
    window := pos_integer() | infinity
}.

%% ---------------------------------------------------------------------------
%% Algorithm 1
%% ---------------------------------------------------------------------------

%% @doc A history without a transaction. The history is two ETS tables
%% that the calling process owns. `check/3' and `check_batch/3' change
%% the tables. A caller uses the history that the last call returned.
-spec new(pos_integer() | infinity) -> history().
new(Window) ->
    #{
        last_commit => ets:new(redwall_resolver_last_commit, [ordered_set, private]),
        versions => ets:new(redwall_resolver_versions, [ordered_set, private]),
        oldest => 0,
        window => Window
    }.

%% @doc Checks the transaction `Transaction' with the commit version
%% `Version'. Returns the result and the history after the transaction.
-spec check(transaction(), version(), history()) -> {result(), history()}.
check(Transaction, Version, History) ->
    check(Transaction, Version, History, true).

%% With `Record' false, the function does not record the writes of an
%% admitted transaction.
check(Transaction = #{read_version := ReadVersion}, Version, History, Record) ->
    #{last_commit := LastCommit, versions := Versions, oldest := Oldest0, window := Window} = History,
    Reads = intervals(reads, read_ranges, Transaction),
    Writes = intervals(writes, write_ranges, Transaction),
    NewOldest = new_oldest(Version, Window, Oldest0),
    ok =
        case NewOldest > Oldest0 of
            true -> expire(NewOldest, LastCommit, Versions);
            false -> ok
        end,
    Result =
        %% A read version at or above the commit version is not a read
        %% version of this epoch. After a reset of the database, the
        %% versions start again, and a transaction of the time before the
        %% reset has such a read version.
        case (ReadVersion < NewOldest orelse ReadVersion >= Version) andalso Reads =/= [] of
            true ->
                too_old;
            false ->
                case lists:any(fun({Begin, End}) -> max_version(Begin, End, LastCommit) > ReadVersion end, Reads) of
                    true -> conflict;
                    false -> committed
                end
        end,
    ok =
        case Result of
            committed when Record ->
                lists:foreach(fun({Begin, End}) -> set_version(Begin, End, Version, LastCommit, Versions) end, Writes);
            _ ->
                ok
        end,
    {Result, History#{oldest := NewOldest}}.

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
    %% The table compares two bounds with `==', so the bound of the row
    %% can be another term than `Bound': an integer and a float with the
    %% same value are one key.
    case ets:lookup(LastCommit, Bound) of
        [{_Stored, Version}] ->
            Version;
        [] ->
            case ets:prev(LastCommit, Bound) of
                '$end_of_table' -> 0;
                Below -> ets:lookup_element(LastCommit, Below, 2)
            end
    end.

%% The highest commit version in the interval from `Begin' to `End'. The
%% function reads the bounds of the interval only.
max_version(Begin, End, LastCommit) ->
    max_until(ets:next(LastCommit, Begin), End, LastCommit, version_at(Begin, LastCommit)).

max_until(Bound, End, LastCommit, Max) when Bound =/= '$end_of_table', Bound < End ->
    max_until(ets:next(LastCommit, Bound), End, LastCommit, max(Max, ets:lookup_element(LastCommit, Bound, 2)));
max_until(_NoBoundInTheInterval, _End, _LastCommit, Max) ->
    Max.

%% Sets the commit version of the interval from `Begin' to `End'. The
%% version from `End' on does not change.
set_version(Begin, End, Version, LastCommit, Versions) ->
    AtEnd = version_at(End, LastCommit),
    ok = delete_until(ets:next(LastCommit, Begin), End, LastCommit, Versions),
    ok = put_bound(Begin, Version, LastCommit, Versions),
    put_bound(End, AtEnd, LastCommit, Versions).

delete_until(Bound, End, LastCommit, Versions) when Bound =/= '$end_of_table', Bound < End ->
    Next = ets:next(LastCommit, Bound),
    ok = unindex(Bound, LastCommit, Versions),
    true = ets:delete(LastCommit, Bound),
    delete_until(Next, End, LastCommit, Versions);
delete_until(_NoBoundInTheInterval, _End, _LastCommit, _Versions) ->
    ok.

%% Writes the version of a bound, in both tables. `Versions' has one row
%% `{{Version, Bound}}' for each bound with a version above 0.
put_bound(Bound, Version, LastCommit, Versions) ->
    ok = unindex(Bound, LastCommit, Versions),
    _ = Version > 0 andalso ets:insert(Versions, {{Version, Bound}}),
    true = ets:insert(LastCommit, {Bound, Version}),
    ok.

unindex(Bound, LastCommit, Versions) ->
    case ets:lookup(LastCommit, Bound) of
        [{_Stored, Old}] when Old > 0 ->
            true = ets:delete(Versions, {Old, Bound}),
            ok;
        _ ->
            ok
    end.

%% Removes the commit versions below `Oldest'. `Versions' has the bounds in
%% the order of their versions, so the function reads only the bounds that
%% it changes. A removed version becomes 0. A bound with the version 0
%% directly after a bound with the version 0, or after no bound, is
%% removed.
expire(Oldest, LastCommit, Versions) ->
    case ets:first(Versions) of
        {Version, Bound} when Version < Oldest ->
            true = ets:delete(Versions, {Version, Bound}),
            Before =
                case ets:prev(LastCommit, Bound) of
                    '$end_of_table' -> 0;
                    Below -> ets:lookup_element(LastCommit, Below, 2)
                end,
            true =
                case Before of
                    0 -> ets:delete(LastCommit, Bound);
                    _ -> ets:insert(LastCommit, {Bound, 0})
                end,
            %% The next bound has the version 0 only if it ended an
            %% interval with a version above 0, which this bound was.
            _ =
                case ets:next(LastCommit, Bound) of
                    '$end_of_table' ->
                        true;
                    Next ->
                        ets:lookup_element(LastCommit, Next, 2) =:= 0 andalso ets:delete(LastCommit, Next)
                end,
            expire(Oldest, LastCommit, Versions);
        _ ->
            ok
    end.

%% The oldest version only rises.
new_oldest(_Version, infinity, Oldest) -> Oldest;
new_oldest(Version, Window, Oldest) -> max(Oldest, Version - Window).

%% ---------------------------------------------------------------------------
%% Batch of a Proxy
%% ---------------------------------------------------------------------------

%% @doc Checks the transactions of one batch with the commit version
%% `Version', as `check_batch/3' does. `Flags' plants defects for the
%% simulation:
%%
%% <ul>
%% <li>`check => false': every transaction is admitted, and its writes
%%   are recorded.</li>
%% <li>`check_ranges => false': the read ranges are not checked.</li>
%% <li>`check_batch => false': a transaction is not checked against the
%%   earlier transactions of its batch.</li>
%% <li>`check_write_ranges => false': the write ranges are not
%%   recorded.</li>
%% </ul>
-spec resolve([transaction()], version(), history(), flags()) -> {[result()], history()}.
resolve(Transactions0, Version, History, Flags) ->
    Check = maps:get(check, Flags, true),
    CheckRanges = maps:get(check_ranges, Flags, true),
    CheckBatch = maps:get(check_batch, Flags, true),
    Transactions1 =
        case CheckRanges of
            true -> Transactions0;
            %% The planted defect: the read ranges are not checked.
            false -> [Transaction#{read_ranges => []} || Transaction <- Transactions0]
        end,
    Transactions =
        case maps:get(check_write_ranges, Flags, true) of
            true -> Transactions1;
            %% The planted defect: the write ranges are not recorded.
            false -> [Transaction#{write_ranges => []} || Transaction <- Transactions1]
        end,
    WithoutReads = fun(Transaction) -> Transaction#{reads := [], read_ranges => []} end,
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
            Checked = [{element(1, check(Transaction, Version, History, false)), Transaction} || Transaction <- Transactions],
            Admitted = [WithoutReads(Transaction) || {committed, Transaction} <- Checked],
            {_, History2} = check_batch(Admitted, Version, History),
            %% The oldest version rises also when no transaction is
            %% admitted.
            {_, History3} = check(#{read_version => 0, reads => [], writes => []}, Version, History2),
            {[Result || {Result, _Transaction} <- Checked], History3}
    end.
