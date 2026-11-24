%% @doc The mapper of a mapped range read.
%%
%% A mapped range read (`redwall:get_mapped_range/5') reads a key range,
%% and performs a second read for each row of the range. The mapper
%% describes how the key of the second read is built from the key and the
%% value of the row. This module evaluates a mapper. The StorageServer
%% calls it for each row (see "Mapped range" in {@link redwall_storage}).
%%
%% == Kinds of mapper ==
%%
%% <ul>
%% <li>`{tuple, Binary}': the mapper of FoundationDB. `Binary' is a tuple
%%   of the tuple layer. This module follows `preprocessMappedKey' and
%%   `constructMappedKey' of `fdbserver/storageserver.actor.cpp' of
%%   FoundationDB 7.3.62.</li>
%% <li>`{term, Term}': reserved for a mapper that is an Erlang term.
%%   redwall defines no such mapper yet, and `compile/1' returns
%%   `{error, unsupported_mapper}' for it. FoundationDB has no such
%%   mapper. This part is not in the paper.</li>
%% </ul>
%%
%% == The mapper of FoundationDB ==
%%
%% The mapper is a packed tuple. The mapped key is a packed tuple with one
%% element for each element of the mapper, in the order of the mapper:
%%
%% <ul>
%% <li>`{K[i]}': element `i' of the key of the row. The key of the row
%%   must be a packed tuple. The first element has the index 0.</li>
%% <li>`{V[i]}': element `i' of the value of the row. The value of the row
%%   must be a packed tuple.</li>
%% <li>`{...}': adds no element. It is allowed only as the last element.
%%   With it, the second read is a read of every key that starts with the
%%   mapped key. Without it, the second read is a read of the mapped
%%   key.</li>
%% <li>A string with `{{' or `}}': the string with each `{{' replaced by
%%   `{' and each `}}' replaced by `}'. The result is a byte string, also
%%   when the element of the mapper is a Unicode string.</li>
%% <li>Another element, of any type: the element itself.</li>
%% </ul>
%%
%% The three forms with braces are recognised in a byte string and in a
%% Unicode string. An element of the key or of the value is copied with
%% its encoding, so it can have any type of the tuple layer.
%%
%% The index is read as `std::stoi' reads it: white space, an optional
%% sign, and digits. Characters after the digits are ignored, so `{K[1x]}'
%% has the index 1. `{K[]}' has 5 characters and is a literal, as in
%% FoundationDB.
%%
%% == Errors ==
%%
%% <ul>
%% <li>`mapper_not_tuple' (2043): the mapper is not a packed tuple.</li>
%% <li>`mapper_bad_range_descriptor' (2032): `{...}' is not the last
%%   element, or `{...}' is the only element.</li>
%% <li>`mapper_bad_index' (2030): the index is not a number, or the key or
%%   the value has no element with that index.</li>
%% <li>`key_not_tuple' (2041) and `value_not_tuple' (2042): the mapper
%%   uses the key or the value of a row, and it is not a packed
%%   tuple.</li>
%% </ul>
%%
%% `compile/1' returns the first two errors. `key/3' returns the other
%% errors for a row, so a range without rows gives no such error, as in
%% FoundationDB.
%%
%% == Differences from FoundationDB ==
%%
%% <ul>
%% <li>A mapper with `{...}' as the only element gives
%%   `mapper_bad_range_descriptor'. The StorageServer of FoundationDB
%%   fails an assertion for it (`strinc' of an empty key).</li>
%% <li>This module accepts every type code of the tuple layer
%%   specification (`design/tuple.md'). `Tuple::unpack' of FoundationDB
%%   7.3.62 accepts a part of them.</li>
%% </ul>
-module(redwall_mapper).

-export([compile/1, key/3, mode/1, range/2, code/1]).
-export([elements/1]).
-export_type([mapper/0, compiled/0, reason/0]).

-type mapper() :: {tuple, binary()} | {term, term()}.

%% `point': the second read is a read of the mapped key. `range': the
%% second read is a read of the keys that start with the mapped key.
-type mode() :: point | range.

-type index() :: non_neg_integer() | bad.
-type part() :: {literal, binary()} | {key, index()} | {value, index()}.

-opaque compiled() :: {mode(), [part()]}.

-type reason() ::
    mapper_not_tuple
    | mapper_bad_range_descriptor
    | mapper_bad_index
    | key_not_tuple
    | value_not_tuple
    | unsupported_mapper.

-define(NULL, 16#00).
-define(BYTES, 16#01).
-define(STRING, 16#02).
-define(NESTED, 16#05).
-define(ESCAPE, 16#FF).

%% @doc Examines the mapper once for a read.
-spec compile(mapper() | term()) -> {ok, compiled()} | {error, reason()}.
compile({tuple, Mapper}) when is_binary(Mapper) ->
    case elements(Mapper) of
        {ok, Elements} -> parts(Elements, []);
        error -> {error, mapper_not_tuple}
    end;
compile({term, _Term}) ->
    {error, unsupported_mapper};
compile(_Other) ->
    {error, mapper_not_tuple}.

-spec mode(compiled()) -> mode().
mode({Mode, _Parts}) -> Mode.

%% @doc Returns the mapped key of a row.
-spec key(compiled(), Key :: term(), Value :: term()) -> {ok, binary()} | {error, reason()}.
key({_Mode, Parts}, Key, Value) ->
    build(Parts, {key_not_tuple, Key}, {value_not_tuple, Value}, []).

%% @doc Returns the key range `{Begin, End}' of the second read: the keys
%% `K' with `Begin =< K' and `K < End'. For `point' the range has the
%% mapped key only. For `range' the range has every key that starts with
%% the mapped key (`prefixRange' of FoundationDB).
-spec range(mode(), binary()) -> {binary(), binary()}.
range(point, Mapped) -> {Mapped, <<Mapped/binary, 0>>};
range(range, Mapped) -> redwall_key:range(Mapped).

%% @doc Returns the error code of FoundationDB for a reason
%% (`flow/include/flow/error_definitions.h').
-spec code(reason()) -> pos_integer().
code(mapper_bad_index) -> 2030;
code(mapper_bad_range_descriptor) -> 2032;
code(key_not_tuple) -> 2041;
code(value_not_tuple) -> 2042;
code(mapper_not_tuple) -> 2043;
%% `unsupported_operation'.
code(unsupported_mapper) -> 2108.

%% ---------------------------------------------------------------------------
%% The elements of the mapper
%% ---------------------------------------------------------------------------

parts([], Acc) ->
    {ok, {point, lists:reverse(Acc)}};
parts([Raw = <<Code, _/binary>> | Elements], Acc) when Code =:= ?BYTES; Code =:= ?STRING ->
    String = string(Raw),
    case unescape(String) of
        String ->
            case placeholder(String) of
                range when Elements =:= [], Acc =/= [] -> {ok, {range, lists:reverse(Acc)}};
                range -> {error, mapper_bad_range_descriptor};
                literal -> parts(Elements, [{literal, Raw} | Acc]);
                Part -> parts(Elements, [Part | Acc])
            end;
        Unescaped ->
            parts(Elements, [{literal, bytes(Unescaped)} | Acc])
    end;
parts([Raw | Elements], Acc) ->
    parts(Elements, [{literal, Raw} | Acc]).

unescape(String) ->
    binary:replace(binary:replace(String, <<"{{">>, <<"{">>, [global]), <<"}}">>, <<"}">>, [global]).

%% `singleKeyOrValue' and `rangeQuery' of FoundationDB.
placeholder(<<"{...}">>) ->
    range;
placeholder(<<"{", Source, "[", Rest/binary>>) when (Source =:= $K orelse Source =:= $V), byte_size(Rest) > 2 ->
    Size = byte_size(Rest) - 2,
    case Rest of
        <<Index:Size/binary, "]}">> when Source =:= $K -> {key, index(Index)};
        <<Index:Size/binary, "]}">> -> {value, index(Index)};
        _ -> literal
    end;
placeholder(_String) ->
    literal.

%% The index as `std::stoi' reads it.
index(<<Space, Rest/binary>>) when Space =:= $\s; Space >= $\t, Space =< $\r -> index(Rest);
index(<<"+", Rest/binary>>) -> digits(Rest, none);
index(<<"-", Rest/binary>>) ->
    case digits(Rest, none) of
        0 -> 0;
        _NegativeOrBad -> bad
    end;
index(Rest) -> digits(Rest, none).

digits(<<Digit, Rest/binary>>, Acc) when Digit >= $0, Digit =< $9 ->
    Sum =
        case Acc of
            none -> Digit - $0;
            _ -> Acc * 10 + Digit - $0
        end,
    %% `std::stoi' fails for a number above the largest `int'.
    case Sum > 16#7FFFFFFF of
        true -> bad;
        false -> digits(Rest, Sum)
    end;
digits(_NoDigit, none) -> bad;
digits(_NoDigit, Acc) -> Acc.

%% ---------------------------------------------------------------------------
%% The mapped key
%% ---------------------------------------------------------------------------

%% `Key' and `Value' are `{Reason, Term}' until the first use, and the
%% list of the elements after it.
build([], _Key, _Value, Acc) ->
    {ok, iolist_to_binary(lists:reverse(Acc))};
build([{literal, Raw} | Parts], Key, Value, Acc) ->
    build(Parts, Key, Value, [Raw | Acc]);
build([{_Source, bad} | _Parts], _Key, _Value, _Acc) ->
    {error, mapper_bad_index};
build([{key, Index} | Parts], Key, Value, Acc) ->
    case unpacked(Key) of
        {error, Reason} -> {error, Reason};
        Elements when Index < length(Elements) -> build(Parts, Elements, Value, [lists:nth(Index + 1, Elements) | Acc]);
        _Elements -> {error, mapper_bad_index}
    end;
build([{value, Index} | Parts], Key, Value, Acc) ->
    case unpacked(Value) of
        {error, Reason} -> {error, Reason};
        Elements when Index < length(Elements) -> build(Parts, Key, Elements, [lists:nth(Index + 1, Elements) | Acc]);
        _Elements -> {error, mapper_bad_index}
    end.

unpacked(Elements) when is_list(Elements) ->
    Elements;
unpacked({Reason, Packed}) when is_binary(Packed) ->
    case elements(Packed) of
        {ok, Elements} -> Elements;
        error -> {error, Reason}
    end;
unpacked({Reason, _NoBinary}) ->
    {error, Reason}.

%% ---------------------------------------------------------------------------
%% The tuple layer
%% ---------------------------------------------------------------------------

%% @doc Splits a packed tuple into its elements. Each element keeps its
%% encoding (`Tuple::subTupleRawString' of FoundationDB). The result is
%% `error' if the binary is not a packed tuple.
-spec elements(binary()) -> {ok, [binary()]} | error.
elements(Packed) ->
    elements(Packed, []).

elements(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
elements(Packed, Acc) ->
    case size_of(Packed) of
        Size when is_integer(Size), Size =< byte_size(Packed) ->
            <<Raw:Size/binary, Rest/binary>> = Packed,
            elements(Rest, [Raw | Acc]);
        _ErrorOrTooShort ->
            error
    end.

%% The number of bytes of the first element. The type codes are from
%% `design/tuple.md' of FoundationDB.
size_of(<<?NULL, _/binary>>) -> 1;
size_of(<<Code, Rest/binary>>) when Code =:= ?BYTES; Code =:= ?STRING -> string_end(Rest, 1);
size_of(<<?NESTED, Rest/binary>>) -> nested_end(Rest, 1);
%% An integer of more than 8 bytes has its length in the second byte.
size_of(<<16#0B, Length, _/binary>>) -> 2 + (Length bxor 16#FF);
size_of(<<16#1D, Length, _/binary>>) -> 2 + Length;
size_of(<<Code, _/binary>>) when Code >= 16#0C, Code =< 16#1C -> 1 + abs(Code - 16#14);
%% A float and a double.
size_of(<<16#20, _/binary>>) -> 5;
size_of(<<16#21, _/binary>>) -> 9;
%% False and true.
size_of(<<Code, _/binary>>) when Code =:= 16#26; Code =:= 16#27 -> 1;
%% A UUID and a 64 bit identifier.
size_of(<<16#30, _/binary>>) -> 17;
size_of(<<16#31, _/binary>>) -> 9;
%% A versionstamp of 80 bits and of 96 bits.
size_of(<<16#32, _/binary>>) -> 11;
size_of(<<16#33, _/binary>>) -> 13;
size_of(_Other) -> error.

%% A string ends at a 0 byte that is not followed by 0xFF.
string_end(<<0, ?ESCAPE, Rest/binary>>, Size) -> string_end(Rest, Size + 2);
string_end(<<0, _/binary>>, Size) -> Size + 1;
string_end(<<_, Rest/binary>>, Size) -> string_end(Rest, Size + 1);
string_end(<<>>, _Size) -> error.

%% A nested tuple ends at a 0 byte that is not followed by 0xFF. A null
%% in a nested tuple is the two bytes 0 and 0xFF.
nested_end(<<?NULL, ?ESCAPE, Rest/binary>>, Size) ->
    nested_end(Rest, Size + 2);
nested_end(<<?NULL, _/binary>>, Size) ->
    Size + 1;
nested_end(<<>>, _Size) ->
    error;
nested_end(Packed, Size) ->
    case size_of(Packed) of
        Element when is_integer(Element), Element =< byte_size(Packed) ->
            <<_:Element/binary, Rest/binary>> = Packed,
            nested_end(Rest, Size + Element);
        _ErrorOrTooShort ->
            error
    end.

%% The content of a packed byte string or Unicode string.
string(<<_Code, Rest/binary>>) ->
    binary:replace(binary:part(Rest, 0, byte_size(Rest) - 1), <<0, ?ESCAPE>>, <<0>>, [global]).

%% A packed byte string.
bytes(String) ->
    <<?BYTES, (binary:replace(String, <<0>>, <<0, ?ESCAPE>>, [global]))/binary, 0>>.
