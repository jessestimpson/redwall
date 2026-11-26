%% @doc The storage engine `ets': the data of a StorageServer in ETS
%% tables in memory. See {@link redwall_storage_engine} for the interface.
%%
%% The data is lost when the StorageServer stops (`durability/0' is
%% `memory').
%%
%% == Tables ==
%%
%% FoundationDB has an ordered keyspace. The engine keeps the keys in
%% their order, so that a read or a clear of a key range examines only
%% the keys of the range. The order of the keys is the order of Erlang
%% terms.
%%
%% <ul>
%% <li>The data is an ETS table of type `ordered_set' with one row for each
%%   key: `{Key, Version, Change}', the newest version of the key.
%%   `Change' is `{set, Value}', or `clear' for a version without a
%%   value. The table is `protected', so another process can read it.</li>
%% <li>A second ETS table of type `ordered_set' has one row
%%   `{{Key, -Version}, Change}' for each older version of a key. The
%%   rows of a key are in the order from the highest version to the
%%   lowest one, so the first row at or after `{Key, -V}' is the last
%%   version of the key at or below `V'. A read or a write of a key
%%   therefore does not depend on the number of versions of the key.</li>
%% <li>A third ETS table of type `ordered_set' has one row
%%   `{{Version, Key}}' for each version above the oldest version. With
%%   this table, a rollback examines only the versions that it removes,
%%   and a rise of the oldest version examines only the versions that
%%   leave the MVCC window.</li>
%% </ul>
%%
%% The process that calls `new/1' owns the tables. Two benchmarks compared
%% layouts of the tables. See "The store of the StorageServer" in
%% `docs/storage-server.md'.
%%
%% == Equal keys ==
%%
%% The tables compare two keys with `=='. See "Equal keys" in
%% {@link redwall_storage}.
-module(redwall_storage_ets).

-behaviour(redwall_storage_engine).

-include("redwall_eta.hrl").

-export([durability/0, features/0, new/1, view/1, clear/1, write/3, rollback/2, advance/2, read/3, page/4, load/2, versions/1]).

%% The number of rows that a select reads before it runs the match spec
%% on them.
-define(SELECT_BATCH, 100).

-record(store, {
    %% The table of the data, the table of the older versions, and the
    %% table with a row for each version above the oldest version.
    data :: ets:tid(),
    history :: ets:tid(),
    %% `undefined' in a view. See `view/1'.
    changes :: ets:tid() | undefined
}).

-spec durability() -> memory.
durability() -> memory.

-spec features() -> [select | mapped_range | scan].
features() -> [select, mapped_range, scan].

-spec new(map()) -> #store{}.
new(_Options) ->
    #store{
        data = ets:new(redwall_storage_data, [ordered_set, protected, {read_concurrency, true}]),
        history = ets:new(redwall_storage_history, [ordered_set, protected, {read_concurrency, true}]),
        changes = ets:new(redwall_storage_changes, [ordered_set, private])
    }.

%% @doc The store without the table of the versions above the oldest
%% version. That table is private, and only a write, a rollback and a
%% rise of the oldest version use it.
-spec view(#store{}) -> #store{}.
view(Store) ->
    Store#store{changes = undefined}.

-spec clear(#store{}) -> ok.
clear(#store{data = Data, history = History, changes = Changes}) ->
    true = ets:delete_all_objects(Data),
    true = ets:delete_all_objects(History),
    true = ets:delete_all_objects(Changes),
    ok.

-spec write(#store{}, non_neg_integer(), [tuple()]) -> ok.
write(Store, Version, Mutations) ->
    write_entry(Mutations, Version, Store).

-spec rollback(#store{}, non_neg_integer()) -> ok.
rollback(Store = #store{changes = Changes}, End) ->
    remove_above(End, ets:last(Changes), Store).

-spec advance(#store{}, non_neg_integer()) -> ok.
advance(Store = #store{changes = Changes}, Oldest) ->
    remove_old(Oldest, ets:first(Changes), Store).

-spec read(#store{}, redwall_storage:query(), non_neg_integer()) -> term().
read(#store{data = Data, history = History}, Query, Version) ->
    lookup(Query, Version, {Data, History}).

-spec page(#store{}, first | {after_key, term()}, pos_integer(), non_neg_integer()) ->
    {ok, [redwall_storage_engine:row()], done | {next, term()}} | {error, gone}.
page(#store{data = Data, history = History}, Next, Limit, Version) ->
    copy_page(Next, Limit, Version, {Data, History}).

-spec load(#store{}, [redwall_storage_engine:row()]) -> ok.
load(#store{data = Data}, Rows) ->
    true = ets:insert(Data, [{Key, Written, {set, Value}} || {Key, Written, Value} <- Rows]),
    ok.

%% @doc Every version of every key: a map from the key to the list of
%% `{Version, Change}' with the highest version first. `Change' is
%% `{set, Value}' or `clear'. The function reads every row.
%%
%% The key of the map is the key of the row of the table of the data,
%% which is the key of the last write. A row of the table of the older
%% versions has the key of the write that moved the version to that
%% table. That key can be equal to the key of the map and not identical.
%% The function compares the two keys with `=='.
-spec versions(#store{}) -> #{term() => [{non_neg_integer(), {set, term()} | clear}]}.
versions(#store{data = Data, history = History}) ->
    %% The two lists are in the order of the keys. The older versions of
    %% one key are in the order of their versions, the highest first.
    maps:from_list(with_older(lists:sort(ets:tab2list(Data)), lists:sort(ets:tab2list(History)))).

with_older([{Key, Version, Change} | Rows], Older0) ->
    %% An older version of a key without a row in the table of the data
    %% is not in the result.
    Older1 = lists:dropwhile(fun({{Other, _Negative}, _Change}) -> Other < Key end, Older0),
    {Mine, Older} = lists:splitwith(fun({{Other, _Negative}, _Change}) -> Other == Key end, Older1),
    [{Key, [{Version, Change} | [{-Negative, Earlier} || {{_Other, Negative}, Earlier} <- Mine]]} | with_older(Rows, Older)];
with_older([], _Older) ->
    [].

%% Applies the mutations of one transaction, in their order.
write_entry(Entry, LSN, State = #store{data = Data}) when is_list(Entry) ->
    lists:foreach(
        fun
            ({set, Key, Value}) ->
                put_version(Key, LSN, {set, Value}, ets:lookup(Data, Key), State);
            ({clear, Key}) ->
                clear_key(Key, LSN, ets:lookup(Data, Key), State);
            ({clear_range, Begin, End}) ->
                %% The rows of the range are read before the first change.
                lists:foreach(fun(Row = {Key, _Version, _Change}) -> clear_key(Key, LSN, [Row], State) end, rows(Begin, End, Data));
            (_Other) ->
                ok
        end,
        Entry
    );
write_entry(_NoTransaction, _LSN, _State) ->
    ok.

%% Adds the version `LSN' of `Key'. `Change' is `{set, Value}' or `clear'.
%% `Row' is the result of the lookup of the key. If a mutation of the same
%% record changed the key before, the new change replaces that one.
%% Otherwise the version of the row moves to the table of the older
%% versions. That table gets its row first, so a direct read finds the
%% version in one of the two tables.
put_version(Key, LSN, Change, Row, #store{data = Data, history = History, changes = Changes}) ->
    case Row of
        [{_Stored, Version, Earlier}] when Version < LSN -> true = ets:insert(History, {{Key, -Version}, Earlier});
        _SameRecordOrNoRow -> true
    end,
    true = ets:insert(Data, {Key, LSN, Change}),
    true = ets:insert(Changes, {{LSN, Key}}),
    ok.

%% A clear of a key without a value adds no version.
clear_key(Key, LSN, Row = [{_Stored, _Version, {set, _Value}}], State) ->
    put_version(Key, LSN, clear, Row, State);
clear_key(_Key, _LSN, _NoRowOrNoValue, _State) ->
    ok.

%% The rows of the keys `K' with `Begin =< K' and `K < End', in the order
%% of the keys. The function reads only these rows and the row after
%% them.
rows(Begin, End, Data) ->
    First =
        case ets:lookup(Data, Begin) of
            [Row] -> {Begin, [Row]};
            [] -> ets:next_lookup(Data, Begin)
        end,
    rows_until(First, End, Data).

rows_until({Key, [Row]}, End, Data) when Key < End ->
    [Row | rows_until(ets:next_lookup(Data, Key), End, Data)];
rows_until(_RowAtOrAfterEndOrEndOfTable, _End, _Data) ->
    [].

%% Removes the versions above `End', from the highest version down. The
%% oldest version is at or below `End', so the table of the versions has a
%% row for each of them. The highest version of a key is in the row of the
%% key. After its removal, the next version of the key moves from the
%% table of the older versions to the row of the key.
remove_above(End, {Version, Key} = Row, State = #store{data = Data, history = History, changes = Changes}) when Version > End ->
    Previous = ets:prev(Changes, Row),
    true = ets:delete(Changes, Row),
    case ets:lookup(Data, Key) of
        [{_Stored, Version, _Change}] ->
            case ets:next_lookup(History, {Key, -Version}) of
                {{Same, Negative} = Older, [{Older, Change}]} when Same == Key ->
                    %% The row of the key changes before the older version
                    %% leaves its table, so a direct read finds the
                    %% version in one of the two.
                    true = ets:insert(Data, {Key, -Negative, Change}),
                    true = ets:delete(History, Older);
                _NoOlderVersionOfTheKey ->
                    true = ets:delete(Data, Key)
            end;
        _ ->
            true
    end,
    remove_above(End, Previous, State);
remove_above(_End, _RowAtOrBelowEndOrNoRow, _State) ->
    ok.

%% Handles the versions at or below `Oldest', from the lowest version up.
%% For the key of each such version, the function removes the versions
%% that no read at or above `Oldest' returns. The row of the version leaves
%% the table of the versions. The last version of a key at or below
%% `Oldest' stays in the data without such a row, until a later version of
%% the key is at or below a later oldest version.
remove_old(Oldest, {Version, Key} = Row, State = #store{data = Data, history = History, changes = Changes}) when Version =< Oldest ->
    Next = ets:next(Changes, Row),
    true = ets:delete(Changes, Row),
    %% No read at or above `Oldest' returns a version of the key below
    %% `Version'.
    ok = delete_older(ets:next(History, {Key, -Version}), Key, History),
    %% `Version' is the last version of the key at or below `Oldest', or a
    %% later row of this pass removes it. If it is a clear, the key has no
    %% value at `Version', and the row is not needed.
    case ets:lookup(Data, Key) of
        [{_Stored, Version, clear}] ->
            true = ets:delete(Data, Key);
        [{_Stored, Newest, _Change}] when Newest > Version ->
            case ets:lookup(History, {Key, -Version}) of
                [{Older, clear}] -> true = ets:delete(History, Older);
                _ -> true
            end;
        _ ->
            true
    end,
    remove_old(Oldest, Next, State);
remove_old(_Oldest, _RowAboveOldestOrNoRow, _State) ->
    ok.

delete_older({Same, _Negative} = Older, Key, History) when Same == Key ->
    Next = ets:next(History, Older),
    true = ets:delete(History, Older),
    delete_older(Next, Key, History);
delete_older(_RowOfAnotherKeyOrEndOfTable, _Key, _History) ->
    ok.

%% One page of a copy: up to `Limit' keys after `Next' with their values
%% at `Version', as `{Key, Written, Value}'. `Written' is the version of
%% the write. The result also has `{next, LastKey}', or `done' after the
%% last key.
copy_page(Next, Limit, Version, {Data, History}) ->
    First =
        case Next of
            first -> ets:first_lookup(Data);
            {after_key, Key} -> ets:next_lookup(Data, Key)
        end,
    try
        copy_rows(First, Limit, Version, {Data, History}, [], none)
    catch
        %% The StorageServer has stopped.
        error:badarg -> {error, gone}
    end.

copy_rows('$end_of_table', _Left, _Version, _Tables, Acc, _Last) ->
    {ok, lists:reverse(Acc), done};
copy_rows(_Row, 0, _Version, _Tables, Acc, Last) ->
    {ok, lists:reverse(Acc), {next, Last}};
copy_rows({Key, [Row]}, Left, Version, Tables = {Data, History}, Acc, _Last) ->
    Acc1 =
        case visible(Row, Version, Data, History) of
            {ok, Value, Written} -> [{Key, Written, Value} | Acc];
            not_found -> Acc
        end,
    copy_rows(ets:next_lookup(Data, Key), Left - 1, Version, Tables, Acc1, Key).

lookup({key, Key}, Version, {Data, History}) ->
    case ets:lookup(Data, Key) of
        [Row] -> visible(Row, Version, Data, History);
        [] -> not_found
    end;
lookup({range, Begin, End}, Version, {Data, History}) ->
    {ok, [
        {Key, Value, Written}
     || Row = {Key, _Newest, _Change} <- rows(Begin, End, Data), {ok, Value, Written} <- [visible(Row, Version, Data, History)]
    ]};
lookup({scan, From, To, Options}, Version, Tables = {Data, _History}) ->
    Limit = maps:get(limit, Options, infinity),
    case maps:get(reverse, Options, false) of
        false ->
            First =
                case From of
                    {incl, Key} ->
                        %% The key of the row can be equal to `Key' and
                        %% not identical. The result has the stored key.
                        case ets:lookup(Data, Key) of
                            [Row] -> {element(1, Row), [Row]};
                            [] -> ets:next_lookup(Data, Key)
                        end;
                    {excl, Key} ->
                        ets:next_lookup(Data, Key)
                end,
            {ok, scan_walk(First, next_lookup, To, Version, Tables, Limit, [])};
        true ->
            Last =
                case To of
                    {incl, Key} ->
                        case ets:lookup(Data, Key) of
                            [Row] -> {element(1, Row), [Row]};
                            [] -> ets:prev_lookup(Data, Key)
                        end;
                    {excl, Key} ->
                        ets:prev_lookup(Data, Key)
                end,
            {ok, scan_walk(Last, prev_lookup, From, Version, Tables, Limit, [])}
    end;
lookup({select, Begin, End, MatchSpec, Limit}, Version, {Data, History}) ->
    Compiled = ets:match_spec_compile(redwall_storage_engine:with_key(MatchSpec)),
    First =
        case ets:lookup(Data, Begin) of
            [Row] -> {Begin, [Row]};
            [] -> ets:next_lookup(Data, Begin)
        end,
    {ok, select_walk(First, End, Version, {Data, History}, Compiled, Limit, [], 0, [], 0)};

lookup({mapped, Begin, End, Mapper, Limit}, Version, Tables = {Data, _History}) ->
    case redwall_mapper:compile(Mapper) of
        {ok, Compiled} ->
            First =
                case ets:lookup(Data, Begin) of
                    [Row] -> {Begin, [Row]};
                    [] -> ets:next_lookup(Data, Begin)
                end,
            mapped_walk(First, End, Version, Tables, Compiled, Limit, []);
        {error, Reason} ->
            {error, {mapper, Reason}}
    end.

%% Reads the rows from `{Key, [Row]}' on with the function `Step' of
%% `ets', which is `next_lookup' or `prev_lookup', until the bound `Stop'
%% or until the result has `Left' keys with a value at `Version'.
scan_walk({Key, [Row]}, Step, Stop, Version, Tables = {Data, History}, Left, Acc) when Left =/= 0 ->
    case within(Key, Step, Stop) of
        true ->
            Next = ets:Step(Data, Key),
            case visible(Row, Version, Data, History) of
                {ok, Value, Written} ->
                    Left1 =
                        case Left of
                            infinity -> infinity;
                            _ -> Left - 1
                        end,
                    scan_walk(Next, Step, Stop, Version, Tables, Left1, [{Key, Value, Written} | Acc]);
                not_found ->
                    scan_walk(Next, Step, Stop, Version, Tables, Left, Acc)
            end;
        false ->
            lists:reverse(Acc)
    end;
scan_walk(_EndOfTableOrNoneLeft, _Step, _Stop, _Version, _Tables, _Left, Acc) ->
    lists:reverse(Acc).

%% Whether the key is on the side of the bound that the walk comes from.
within(Key, next_lookup, {excl, To}) -> Key < To;
within(Key, next_lookup, {incl, To}) -> Key =< To;
within(Key, prev_lookup, {excl, From}) -> Key > From;
within(Key, prev_lookup, {incl, From}) -> Key >= From.

%% Reads the rows of the range in the order of the keys, and performs the
%% second read for each row that has a value at `Version'. `Left' is the
%% number of rows that the result can still take.
mapped_walk({Key, [Row]}, End, Version, Tables = {Data, History}, Compiled, Left, Acc) when Key < End, Left =/= 0 ->
    Next = ets:next_lookup(Data, Key),
    case visible(Row, Version, Data, History) of
        {ok, Value, _Written} ->
            case redwall_mapper:key(Compiled, Key, Value) of
                {ok, Mapped} ->
                    Mode = redwall_mapper:mode(Compiled),
                    Range = redwall_mapper:range(Mode, Mapped),
                    Found = second_read(Mode, Range, Version, Tables),
                    Left1 =
                        case Left of
                            infinity -> infinity;
                            _ -> Left - 1
                        end,
                    mapped_walk(Next, End, Version, Tables, Compiled, Left1, [{{Key, Value}, Range, Found} | Acc]);
                {error, Reason} ->
                    {error, {mapper, Reason}}
            end;
        not_found ->
            mapped_walk(Next, End, Version, Tables, Compiled, Left, Acc)
    end;
mapped_walk(_RowAtOrAfterEndOrEndOfTableOrNoneLeft, _End, _Version, _Tables, _Compiled, _Left, Acc) ->
    {ok, lists:reverse(Acc)}.

second_read(point, {Key, _After}, Version, Tables) ->
    case lookup({key, Key}, Version, Tables) of
        {ok, Value, _Written} -> [{Key, Value}];
        not_found -> []
    end;
second_read(range, {Begin, End}, Version, Tables) ->
    {ok, Rows} = lookup({range, Begin, End}, Version, Tables),
    [{Key, Value} || {Key, Value, _Written} <- Rows].

%% Reads the rows of the range in the order of the keys, and runs the
%% match spec on batches of `{Key, Value}' with the value of each key at
%% `Version'. The function reads only the rows of the range, and stops
%% when it has `Limit' results. `Batch' has the rows of the open batch,
%% and `Acc' has the results of the earlier batches, both with the last
%% one first.
select_walk({Key, [Row]}, End, Version, Tables = {Data, History}, Compiled, Limit, Batch, Size, Acc, Count) when Key < End ->
    Batch1 =
        case visible(Row, Version, Data, History) of
            {ok, Value, _Written} -> [{Key, Value} | Batch];
            not_found -> Batch
        end,
    Next = ets:next_lookup(Data, Key),
    case Size + 1 of
        ?SELECT_BATCH ->
            Results = ets:match_spec_run(lists:reverse(Batch1), Compiled),
            case Count + length(Results) of
                Enough when Enough >= Limit -> select_results([Results | Acc], Limit);
                Count1 -> select_walk(Next, End, Version, Tables, Compiled, Limit, [], 0, [Results | Acc], Count1)
            end;
        Size1 ->
            select_walk(Next, End, Version, Tables, Compiled, Limit, Batch1, Size1, Acc, Count)
    end;
select_walk(_RowAtOrAfterEndOrEndOfTable, _End, _Version, _Tables, Compiled, Limit, Batch, _Size, Acc, _Count) ->
    select_results([ets:match_spec_run(lists:reverse(Batch), Compiled) | Acc], Limit).

select_results(Acc, infinity) -> lists:append(lists:reverse(Acc));
select_results(Acc, Limit) -> lists:sublist(lists:append(lists:reverse(Acc)), Limit).


%% The value of the last version of the key at or below `Version'. `Row'
%% is the row of the key, which has the newest version. An older version
%% is in the table of the older versions, with the key `{Key, -Version}',
%% so the first row at or after `{Key, -Version}' is the last version at
%% or below `Version'.
%%
%% A direct read runs in another process while the StorageServer changes
%% the tables. A rollback moves a version from the table of the older
%% versions to the row of the key. The read therefore reads the row of the
%% key again after the table of the older versions, and starts again if
%% the row has changed.
visible({_Key, Written, Change}, Version, _Data, _History) when Written =< Version ->
    value(Written, Change);
visible(Row = {Key, _Newest, _Change}, Version, Data, History) ->
    Older =
        case ets:lookup(History, {Key, -Version}) of
            [{_, Change}] ->
                value(Version, Change);
            [] ->
                case ets:next_lookup(History, {Key, -Version}) of
                    {{Same, Negative}, [{_, Change}]} when Same == Key -> value(-Negative, Change);
                    _NoVersionAtOrBelow -> not_found
                end
        end,
    case ets:lookup(Data, Key) of
        [Row] -> Older;
        [Changed] -> visible(Changed, Version, Data, History);
        [] -> not_found
    end.

value(Written, {set, Value}) -> {ok, Value, Written};
value(_Written, clear) -> not_found.
