%% The client history of a simulated run, for an external checker of
%% strict serializability.
%%
%% `redwall_dst' checks the transactions of a run with its own invariants
%% (`no_conflict', `read_value' and `read_version_order'), which use the
%% commit versions and the records of the LogServers. This module exports
%% only what the clients observed, so that a checker that shares no code
%% with redwall can check the same runs. The checker is Porcupine. See
%% `dst/porcupine/main.go' and `redwall_dst_porcupine_tests'.
%%
%% `history => Table' in the config of a run names an ETS table that the
%% caller owns. When the run ends, the harness copies the history into the
%% table.
%%
%% == Rows ==
%%
%% A client of a transaction (`redwall_dst_client:transaction/5') writes
%% one row `{{history, C}, Row}' to the table of the run. `Row' is a map:
%%
%% <ul>
%% <li>`start' - a number that the client took before it asked for its
%%   read version. The numbers of a run increase with the order of the
%%   events of the run.</li>
%% <li>`read_end' - a number that the client took after its last read.</li>
%% <li>`reads' - a list of `{Key, not_found}' and
%%   `{Key, {{client, W}, unknown}}': the value that the client read for
%%   each key, also for each key of a read range.</li>
%% <li>`writes' and `clears' - the keys that the transaction set, and the
%%   keys that it cleared and did not set. The transaction of the client
%%   `C' sets a key to the value `{client, C}'.</li>
%% <li>`outcome' - `read', `unknown', `committed' or `not_committed'.</li>
%% <li>`end' - a number that the client took after the commit
%%   returned, for the outcome `committed'.</li>
%% </ul>
%%
%% | Outcome | Meaning |
%% |---|---|
%% | `read' | The client read its keys and did not start its commit. |
%% | `unknown' | The client started its commit. The commit returned the error 1021, or the client received no answer, or the client stopped. |
%% | `committed' | The commit returned without an error. |
%% | `not_committed' | The commit returned the error 1020 or 1007. |
%%
%% A client whose reads did not end has no row. The transactions of the
%% watches are not in the history. They use other keys.
-module(redwall_dst_history).

-export([copy/2, rows/1, write/2]).

%% Copies the history from the table of the harness to the table of the
%% caller.
copy(_Tab, undefined) ->
    ok;
copy(Tab, Table) ->
    true = ets:insert(Table, [{C, Row} || [C, Row] <- ets:match(Tab, {{history, '$1'}, '$2'})]),
    ok.

%% Returns the rows in the order of the clients.
rows(Table) ->
    lists:sort(ets:tab2list(Table)).

%% Writes the history as a JSON array with one object for each client.
%%
%% A key is a number from 0: the position of the key in the sorted list of
%% the keys of the history. A value is the number of the client that wrote
%% the value, and 0 is `not_found'. `end' is 0 without the outcome
%% `committed'.
write(File, Rows) ->
    Keys = lists:usort(
        lists:append([
            [Key || {Key, _Value} <- maps:get(reads, Row)] ++ maps:get(writes, Row, []) ++ maps:get(clears, Row, [])
         || {_C, Row} <- Rows
        ])
    ),
    Ids = maps:from_list(lists:zip(Keys, lists:seq(0, length(Keys) - 1))),
    Objects = [object(C, Row, Ids) || {C, Row} <- Rows],
    ok = filelib:ensure_dir(File),
    file:write_file(File, json:encode(Objects)).

object(C, Row = #{start := Start, read_end := ReadEnd, reads := Reads, outcome := Outcome}, Ids) when is_integer(C), C > 0 ->
    #{
        client => C,
        start => Start,
        read_end => ReadEnd,
        'end' => maps:get('end', Row, 0),
        outcome => Outcome,
        reads => [[maps:get(Key, Ids), value(Value)] || {Key, Value} <- Reads],
        writes => [maps:get(Key, Ids) || Key <- maps:get(writes, Row, [])],
        clears => [maps:get(Key, Ids) || Key <- maps:get(clears, Row, [])]
    }.

value(not_found) -> 0;
value({{client, W}, unknown}) when is_integer(W), W > 0 -> W.
