-module(deadlift_mapper_tests).

-include_lib("eunit/include/eunit.hrl").

pack(Tuple) -> deadlift_tuple:pack(Tuple).

mapped(Mapper, Key, Value) ->
    case deadlift_mapper:compile({tuple, pack(Mapper)}) of
        {ok, Compiled} ->
            case deadlift_mapper:key(Compiled, Key, Value) of
                {ok, Mapped} -> {deadlift_mapper:mode(Compiled), deadlift_tuple:unpack(Mapped)};
                {error, Reason} -> Reason
            end;
        {error, Reason} ->
            Reason
    end.

%% The cases of the unit test `/fdbserver/storageserver/constructMappedKey'
%% of `fdbserver/storageserver.actor.cpp', FoundationDB 7.3.62.
foundationdb_test() ->
    Key = pack({<<"key-0">>, <<"key-1">>, <<"key-2">>}),
    Value = pack({<<"value-0">>, <<"value-1">>, <<"value-2">>}),
    ?assertEqual(
        {range, {<<"normal">>, <<"{escaped}">>, <<"key-2">>, <<"value-0">>}},
        mapped({<<"normal">>, <<"{{escaped}}">>, <<"{K[2]}">>, <<"{V[0]}">>, <<"{...}">>}, Key, Value)
    ),
    ?assertEqual({point, {<<"{{}">>, <<"}">>}}, mapped({<<"{{{{}}">>, <<"}}">>}, Key, Value)),
    ?assertEqual({point, {<<"{{}">>, <<"}">>}}, mapped({<<"{{{{}}">>, <<"}}">>}, no_tuple, no_tuple)),
    ?assertEqual(mapper_bad_index, mapped({<<"{K[100]}">>}, Key, Value)),
    ?assertEqual(mapper_bad_index, mapped({<<"{K[not-a-number]}">>}, Key, Value)),
    ?assertEqual(mapper_bad_range_descriptor, mapped({<<"{...}">>, <<"last-element">>}, Key, Value)).

%% An element of the key or of the value is copied with its type. An
%% element of the mapper that is not a string is copied too.
types_test() ->
    Nested = {<<"a">>, null, {<<0, 1>>, 2}},
    Elements = [
        null,
        <<"bytes", 0, 255, 0>>,
        {utf8, <<"text">>},
        Nested,
        0,
        1,
        -1,
        255,
        -256,
        1 bsl 40,
        -(1 bsl 40),
        1 bsl 70,
        -(1 bsl 70),
        1.5,
        {float, 2.5},
        true,
        false,
        {uuid, <<1:128>>},
        {id64, 7},
        {versionstamp, 5, 6},
        {versionstamp, 5, 6, 7}
    ],
    Key = pack(list_to_tuple(Elements)),
    {ok, Raw} = deadlift_mapper:elements(Key),
    ?assertEqual(length(Elements), length(Raw)),
    ?assertEqual(Key, iolist_to_binary(Raw)),
    [
        ?assertEqual({point, {Element, 42, Element}}, mapped({iolist_to_binary(io_lib:format("{K[~b]}", [I])), 42, iolist_to_binary(io_lib:format("{V[~b]}", [I]))}, Key, Key))
     || {I, Element} <- lists:enumerate(0, Elements)
    ],
    %% A literal keeps its type, and a string with an escape becomes a byte
    %% string.
    ?assertEqual({point, {{utf8, <<"{K">>}, <<"{x}">>, Nested}}, mapped({{utf8, <<"{K">>}, {utf8, <<"{{x}}">>}, Nested}, Key, Key)),
    %% A placeholder in a Unicode string.
    ?assertEqual({range, {1}}, mapped({{utf8, <<"{K[5]}">>}, {utf8, <<"{...}">>}}, Key, Key)).

%% The index is read as `std::stoi' reads it.
index_test() ->
    Key = pack({<<"a">>, <<"b">>}),
    Index = fun(Text) -> mapped({<<"{K[", Text/binary, "]}">>}, Key, Key) end,
    ?assertEqual({point, {<<"b">>}}, Index(<<"1">>)),
    ?assertEqual({point, {<<"b">>}}, Index(<<"1x">>)),
    ?assertEqual({point, {<<"b">>}}, Index(<<" +1">>)),
    ?assertEqual({point, {<<"b">>}}, Index(<<"01">>)),
    ?assertEqual({point, {<<"a">>}}, Index(<<"-0">>)),
    ?assertEqual(mapper_bad_index, Index(<<"-1">>)),
    ?assertEqual(mapper_bad_index, Index(<<"2">>)),
    ?assertEqual(mapper_bad_index, Index(<<"x1">>)),
    ?assertEqual(mapper_bad_index, Index(<<"+">>)),
    ?assertEqual(mapper_bad_index, Index(<<"99999999999999999999">>)),
    %% `{K[]}' has 5 characters, and is a literal.
    ?assertEqual({point, {<<"{K[]}">>}}, mapped({<<"{K[]}">>}, Key, Key)),
    ?assertEqual({point, {<<"{X[0]}">>, <<"{K[0]">>}}, mapped({<<"{X[0]}">>, <<"{K[0]">>}, Key, Key)).

errors_test() ->
    Key = pack({<<"a">>}),
    ?assertEqual({error, mapper_not_tuple}, deadlift_mapper:compile({tuple, <<"no tuple">>})),
    ?assertEqual({error, mapper_not_tuple}, deadlift_mapper:compile({tuple, <<1, "open">>})),
    ?assertEqual({error, mapper_not_tuple}, deadlift_mapper:compile(no_mapper)),
    ?assertEqual({error, unsupported_mapper}, deadlift_mapper:compile({term, anything})),
    ?assertEqual(mapper_bad_range_descriptor, mapped({<<"{...}">>}, Key, Key)),
    ?assertEqual(key_not_tuple, mapped({<<"{K[0]}">>}, <<"no tuple">>, Key)),
    ?assertEqual(key_not_tuple, mapped({<<"{K[0]}">>}, {a, term}, Key)),
    ?assertEqual(value_not_tuple, mapped({<<"{V[0]}">>}, Key, <<16#FF>>)),
    ?assertEqual(value_not_tuple, mapped({<<"{V[0]}">>}, Key, 17)),
    %% An empty value is an empty tuple.
    ?assertEqual(mapper_bad_index, mapped({<<"{V[0]}">>}, Key, <<>>)),
    %% The error of the index comes before the error of the key.
    ?assertEqual(mapper_bad_index, mapped({<<"{K[x]}">>}, no_tuple, Key)),
    %% A mapper of literals uses no key and no value.
    ?assertEqual({point, {<<"x">>}}, mapped({<<"x">>}, no_tuple, no_tuple)),
    ?assertEqual({point, {}}, mapped({}, no_tuple, no_tuple)),
    ?assertEqual([2030, 2032, 2041, 2042, 2043, 2108], [
        deadlift_mapper:code(Reason)
     || Reason <- [mapper_bad_index, mapper_bad_range_descriptor, key_not_tuple, value_not_tuple, mapper_not_tuple, unsupported_mapper]
    ]).

%% The key range of the second read.
range_test() ->
    ?assertEqual({<<1, "a", 0>>, <<1, "a", 0, 0>>}, deadlift_mapper:range(point, <<1, "a", 0>>)),
    ?assertEqual({<<1, "a", 0>>, <<1, "a", 1>>}, deadlift_mapper:range(range, <<1, "a", 0>>)),
    ?assertEqual({<<16#15, 16#FF>>, <<16#16>>}, deadlift_mapper:range(range, pack({255}))),
    %% Every key that starts with the mapped key is in the range, and the
    %% range is not the range of `deadlift_tuple:range/1', which does not
    %% have the mapped key itself.
    Prefix = pack({<<"p">>, 1}),
    {Begin, End} = deadlift_mapper:range(range, Prefix),
    ?assert(Begin =< Prefix andalso Prefix < End),
    ?assert(pack({<<"p">>, 1, <<16#FF, 16#FF>>}) < End),
    ?assert(pack({<<"p">>, 2}) >= End).
