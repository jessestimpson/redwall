%% @doc The keys of redwall, and the key range of a prefix.
%%
%% A key of redwall is an Erlang term, and the keys are in the order of
%% the Erlang terms. FoundationDB has keys that are byte strings, and its
%% tuple layer encodes a tuple so that the keys with a common prefix of
%% elements are one key range. An Erlang tuple does not have this
%% property: Erlang compares two tuples by their sizes first, so `{a, 1}'
%% is not between `{a}' and `{b}'.
%%
%% redwall has two kinds of key with the property:
%%
%% <ul>
%% <li>A list, for example `[users, 42, name]'. Erlang compares two lists
%%   element by element, and a list is below each list that it is a prefix
%%   of. The keys with the prefix `P' are the keys from `P' up to the
%%   improper list with the elements of `P' and the tail `<<>>'. A binary
%%   is above every list, so this bound is above every list with the
%%   prefix and below every other list above them. A list key needs no
%%   encoding. The elements of a list are in the order of the Erlang
%%   terms: numbers, atoms, tuples, maps, lists, binaries.</li>
%% <li>A binary, for example a tuple that `redwall_tuple:pack/1' has
%%   packed, as in FoundationDB and erlfdb. The keys with the prefix `P'
%%   are the keys from `P' up to `strinc(P)'.</li>
%% </ul>
%%
%% `range/1' returns the key range of a prefix of either kind.
%%
%% == Kinds of key of a database ==
%%
%% The option `keys' of `redwall:open/1' names the kind of key of a
%% database:
%%
%% <ul>
%% <li>`list': every key is a list.</li>
%% <li>`binary': every key is a binary.</li>
%% <li>`any' (the default): a key is any term. The application then has to
%%   know the order of the Erlang terms for its key ranges.</li>
%% </ul>
%%
%% With `list' or `binary', a function of `redwall' with another key
%% raises `badarg'. A database with keys of two kinds has key ranges that
%% do not have the keys that the application expects, so a database has
%% one kind.
%%
%% == Reserved keys ==
%%
%% A tuple key of the form `{'$redwall', _, _}' is a key of redwall
%% itself, for example the record of an idempotency id. A tuple is below
%% every list and every binary, so no key range of a database with the
%% kind `list' or `binary' has such a key.
%%
%% == Key selectors ==
%%
%% A key selector names a key by its position relative to another key
%% (the developer guide of FoundationDB, "Key selectors"). `redwall:get_key/2'
%% resolves a key selector, and `redwall:get_range/4' takes a key selector
%% as a bound. A key selector has the forms of erlfdb:
%%
%% <ul>
%% <li>`{Key, lt}': the last key below `Key' (`last_less_than/1').</li>
%% <li>`{Key, lteq}': the last key at or below `Key'
%%   (`last_less_or_equal/1').</li>
%% <li>`{Key, gt}': the first key above `Key' (`first_greater_than/1').</li>
%% <li>`{Key, gteq}': the first key at or above `Key'
%%   (`first_greater_or_equal/1').</li>
%% <li>`{Key, Selector, Offset}' with one of the four atoms: the key
%%   `Offset' keys after the key of `{Key, Selector}', or before it for a
%%   negative `Offset'.</li>
%% <li>`{Key, OrEqual, Offset}' with a boolean, 0 or 1: the form of
%%   FoundationDB. The key at the offset 0 is the last key below `Key',
%%   or at or below `Key' with `OrEqual'.</li>
%% </ul>
%%
%% A key selector needs a lowest and a highest bound of the keys, for a
%% selector that names a key before the first key or after the last key.
%% The Erlang terms have no lowest and no highest term, so only a
%% database with the kind `list' or `binary' has key selectors
%% (`space/1'):
%%
%% <ul>
%% <li>`list': the keys from `[]' up to `<<>>', which is above every
%%   list.</li>
%% <li>`binary': the keys from `<<>>' up to `<<255>>'. A key selector does
%%   not find a key at or above `<<255>>'. In FoundationDB the keys from
%%   `<<255>>' are the system keys.</li>
%% </ul>
%%
%% == Differences from FoundationDB ==
%%
%% <ul>
%% <li>The elements of a list key of different types are in the order of
%%   the Erlang terms. The tuple layer has another order of the types.
%%   The two orders agree for two integers and for two binaries.</li>
%% <li>An integer and a float with the same value are the same key in the
%%   StorageServer with ETS tables, for example `[1]' and `[1.0]'. The
%%   tuple layer has two keys for them.</li>
%% </ul>
-module(redwall_key).

-export([range/1, strinc/1, kind/1, check/2, check_bound/2]).
-export([last_less_than/1, last_less_or_equal/1, first_greater_than/1, first_greater_or_equal/1]).
-export([selector/1, is_selector/1, space/1]).
-export_type([kind/0, selector/0]).

-type selector() ::
    {term(), lt | lteq | gt | gteq | boolean() | 0 | 1}
    | {term(), lt | lteq | gt | gteq | boolean() | 0 | 1, integer()}.

-type kind() :: list | binary | any.

%% @doc Returns the key range `{Begin, End}' of the keys with the prefix
%% `Prefix': the keys `K' with `Begin =< K' and `K < End'. `Prefix' is a
%% list or a binary. See the module documentation.
-spec range(list() | binary()) -> {term(), term()}.
range(Prefix) when is_binary(Prefix) ->
    {Prefix, strinc(Prefix)};
range(Prefix) when is_list(Prefix) ->
    {Prefix, upper(Prefix)};
range(_Other) ->
    error(badarg).

%% The improper list with the elements of the list and the tail `<<>>'.
upper([]) -> <<>>;
upper([Head | Tail]) when is_list(Tail) -> [Head | upper(Tail)];
upper(_Improper) -> error(badarg).

%% @doc Returns the first binary that is above every binary with the
%% prefix `Prefix' (`strinc' of FoundationDB). The call raises `badarg'
%% for a prefix without a byte below 255.
-spec strinc(binary()) -> binary().
strinc(Prefix) when is_binary(Prefix) ->
    case strip(Prefix, byte_size(Prefix)) of
        <<>> ->
            error(badarg);
        Stripped ->
            Size = byte_size(Stripped) - 1,
            <<Head:Size/binary, Last>> = Stripped,
            <<Head/binary, (Last + 1)>>
    end.

strip(_Prefix, 0) ->
    <<>>;
strip(Prefix, Size) ->
    case binary:at(Prefix, Size - 1) of
        16#FF -> strip(Prefix, Size - 1);
        _ -> binary:part(Prefix, 0, Size)
    end.

%% @doc Returns the kind of a key.
-spec kind(term()) -> list | binary | other.
kind(Key) when is_list(Key) -> list;
kind(Key) when is_binary(Key) -> binary;
kind(_Key) -> other.

%% @doc Raises `badarg' if the key is not a key of the kind.
-spec check(kind(), term()) -> ok.
check(any, _Key) -> ok;
check(list, Key) when is_list(Key), length(Key) >= 0 -> ok;
check(binary, Key) when is_binary(Key) -> ok;
check(_Kind, _Key) -> error(badarg).

%% @doc Raises `badarg' if the term is not a bound of a key range of the
%% kind. A bound of the kind `list' is a list, or the end of the range of
%% a prefix, which is an improper list.
-spec check_bound(kind(), term()) -> ok.
check_bound(any, _Bound) -> ok;
check_bound(list, Bound) when is_list(Bound); Bound =:= <<>> -> ok;
check_bound(binary, Bound) when is_binary(Bound) -> ok;
check_bound(_Kind, _Bound) -> error(badarg).

%% @doc Returns the key selector for the last key below `Key'.
-spec last_less_than(term()) -> selector().
last_less_than(Key) -> {Key, lt}.

%% @doc Returns the key selector for the last key at or below `Key'.
-spec last_less_or_equal(term()) -> selector().
last_less_or_equal(Key) -> {Key, lteq}.

%% @doc Returns the key selector for the first key above `Key'.
-spec first_greater_than(term()) -> selector().
first_greater_than(Key) -> {Key, gt}.

%% @doc Returns the key selector for the first key at or above `Key'.
-spec first_greater_or_equal(term()) -> selector().
first_greater_or_equal(Key) -> {Key, gteq}.

%% @doc Returns the key selector in the form of FoundationDB:
%% `{Key, OrEqual, Offset}'. The key at the offset 0 is the last key below
%% `Key', or at or below `Key' with `OrEqual'. The key at the offset 1 is
%% the key after that one. The call raises `badarg' for another term.
-spec selector(selector()) -> {term(), boolean(), integer()}.
selector({Key, lt}) -> {Key, false, 0};
selector({Key, lteq}) -> {Key, true, 0};
selector({Key, gt}) -> {Key, true, 1};
selector({Key, gteq}) -> {Key, false, 1};
selector({Key, OrEqual}) when is_boolean(OrEqual) -> {Key, OrEqual, 0};
selector({Key, OrEqual}) when OrEqual =:= 0; OrEqual =:= 1 -> {Key, OrEqual =:= 1, 0};
selector({Key, Selector, Offset}) when is_integer(Offset) ->
    {Key, OrEqual, Base} = selector({Key, Selector}),
    {Key, OrEqual, Base + Offset};
selector(_Other) ->
    error(badarg).

%% @doc Returns whether the term has the form of a key selector.
-spec is_selector(term()) -> boolean().
is_selector({_Key, Selector}) -> is_comparison(Selector);
is_selector({_Key, Selector, Offset}) -> is_comparison(Selector) andalso is_integer(Offset);
is_selector(_Other) -> false.

is_comparison(Selector) ->
    lists:member(Selector, [lt, lteq, gt, gteq, true, false, 0, 1]).

%% @doc Returns the bounds `{Lowest, Highest}' of the keys of a kind for
%% key selectors: every key `K' that a key selector finds has
%% `Lowest =< K' and `K < Highest'. The call raises
%% `{unsupported, key_selector}' for the kind `any'. See "Key selectors"
%% in the module documentation.
-spec space(kind()) -> {term(), term()}.
space(list) -> {[], <<>>};
space(binary) -> {<<>>, <<255>>};
space(any) -> error({unsupported, key_selector}).
