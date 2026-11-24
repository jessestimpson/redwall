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
-export_type([kind/0]).

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
