%% @doc Idempotency ids.
%%
%% A client that does not receive the answer to a commit does not know
%% whether the transaction is committed (the error 1021,
%% `commit_unknown_result'). A transaction with an idempotency id leaves a
%% record of its commit in the database, so that the client can find out.
%% The client then does not run a committed transaction a second time. The
%% design follows `design/idempotency_ids.md' and
%% `fdbclient/NativeAPI.actor.cpp' (`determineCommitStatus') of
%% FoundationDB 7.3.62. This module has the keys. {@link deadlift} has the
%% protocol of the client, and {@link deadlift_epoch} writes and removes
%% the records.
%%
%% == Keys ==
%%
%% An idempotency id is `{Milliseconds, Random}': the time of the client
%% at the creation of the id, and a random number of 64 bits.
%%
%% <ul>
%% <li>`{'$deadlift', idmp, Id}': the record of a committed transaction
%%   with the id. The value is `{CommitVersion, Index}', which the process
%%   of the epoch writes with the mutation `{set_versionstamp, Key}' of
%%   the transaction. The ids are in the order of their times, so the
%%   records of old ids are one key range.</li>
%% <li>`{'$deadlift', idmp_lock, Id}': a key without a value. A
%%   transaction with the id has it as a read conflict key. A client that
%%   determines the result of a commit clears it, which is a write for
%%   the Resolver.</li>
%% <li>`{'$deadlift', idmp_expired, 0}': the time below which the records
%%   can be removed. A client whose id is below this time cannot determine
%%   the result of its commit.</li>
%% </ul>
%%
%% A key of the form `{'$deadlift', _, _}' is reserved for deadlift. A key
%% range of an application that contains such keys returns them.
%%
%% == Differences from FoundationDB ==
%%
%% <ul>
%% <li>The key of a record has the id, so a client reads one key.
%%   FoundationDB has the commit version in the key and the ids of a batch
%%   in the value, and a client reads the keys of every possible commit
%%   version.</li>
%% <li>The time of an id is the time of the client. FoundationDB removes
%%   the records by their commit versions.</li>
%% <li>An id is created by deadlift. FoundationDB also takes an id from the
%%   application (the option `IDEMPOTENCY_ID').</li>
%% </ul>
-module(deadlift_idempotency).

-include("deadlift_eta.hrl").

-export([new/0, key/1, lock_key/1, expired_key/0, time/1, clean/2]).
-export_type([id/0]).

-type id() :: {Milliseconds :: non_neg_integer(), Random :: non_neg_integer()}.

%% @doc Returns a new idempotency id.
-spec new() -> id().
new() ->
    {erlang:system_time(millisecond), rand:uniform(1 bsl 64)}.

-spec time(id()) -> non_neg_integer().
time({Milliseconds, _Random}) -> Milliseconds.

-spec key(id()) -> term().
key(Id) -> {'$deadlift', idmp, Id}.

-spec lock_key(id()) -> term().
lock_key(Id) -> {'$deadlift', idmp_lock, Id}.

-spec expired_key() -> term().
expired_key() -> {'$deadlift', idmp_expired, 0}.

%% @doc Returns the mutations that remove the records of the ids below the
%% time `Now - MinAge', and record that time. The result is empty if the
%% time is not above 0.
-spec clean(Now :: integer(), MinAge :: non_neg_integer()) -> [deadlift_epoch:mutation()].
clean(Now, MinAge) when Now - MinAge > 0 ->
    Bound = Now - MinAge,
    [{clear_range, key({0, 0}), key({Bound, 0})}, {set, expired_key(), Bound}];
clean(_Now, _MinAge) ->
    [].
