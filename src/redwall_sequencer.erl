%% @doc The rules of the Sequencer.
%%
%% The Sequencer of an epoch performs the recovery, and then assigns the
%% commit versions of the epoch. See sections 2.3.1, 2.4.1 and 2.4.4 of
%% Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key
%% Value Store", SIGMOD 2021. `formal/Recovery.tla' models the Sequencer
%% with the recovery actions and with `RecoveryTxn', `Accept' and `Assign'.
%%
%% In redwall, the process of an epoch ({@link redwall_epoch}) performs
%% the recovery ({@link redwall_recovery}) at its start, and keeps the
%% last commit version and the read version. This module has the rules for
%% the versions as functions.
%%
%% == Recovery transaction ==
%%
%% The recovery transaction is the first record of an epoch (paper, section
%% 2.4.4). Its LSN is the first commit version of the epoch, and its
%% previous LSN is the Recovery Version. The process of the epoch sends it
%% to every LogServer of the epoch, and waits for every answer. If one of
%% them does not answer, the process stops with the reason
%% `{shutdown, {recovery, {recovery_transaction, Reason}}}'. The process
%% also gives the version to the functions of the Resolver.
%%
%% The recovery transaction has two functions in redwall:
%%
%% <ul>
%% <li>The read version at the start of an epoch is the LSN of a record
%%   that exists. A StorageServer reaches that version.</li>
%% <li>The Resolver has handled a version of the epoch, so a transaction
%%   with the first read version of the epoch is inside the MVCC
%%   window.</li>
%% </ul>
%%
%% == Versions ==
%%
%% The rules below are the rules of FoundationDB 7.3
%% (`fdbserver/ClusterRecovery.actor.cpp', `fdbserver/masterserver.actor.cpp'
%% and `fdbclient/ServerKnobs.cpp'). The paper does not state them.
%%
%% <ul>
%% <li>The first commit version of an epoch is the Recovery Version plus
%%   `max_versions_in_flight' (default: the versions of 100 seconds). It is
%%   1 if the Recovery Version is 0. Its previous LSN is the Recovery
%%   Version. The recovery transaction has this version.</li>
%% <li>Each later commit version adds the versions of the time since the
%%   last commit version, at `versions_per_second' (default 1,000,000). The
%%   Sequencer adds at least 1, and at most the versions of 5 seconds.</li>
%% <li>The read version is the highest commit version that a Proxy has
%%   reported as committed. It starts at the first commit version of the
%%   epoch.</li>
%% </ul>
%%
%% == Versions in flight ==
%%
%% The Sequencer assigns a commit version only if the version is less than
%% `max_versions_in_flight' above the read version. Otherwise
%% `next_version/5' returns `{error, versions_in_flight}'.
%%
%% The read version is at or below the Recovery Version of the recovery
%% that ends the epoch, because every LogServer of the epoch has stored the
%% record with the read version. A LogServer of the epoch therefore stores
%% no record of a client at or above the Recovery Version plus
%% `max_versions_in_flight', which is the first commit version of the next
%% epoch. A record of a client that the recovery discards has an LSN below
%% every version of the next epoch. A StorageServer depends on this rule
%% (see {@link redwall_storage}).
%%
%% The rule does not hold for the recovery transaction, which the
%% Sequencer sends before it has a read version. If an epoch never accepts
%% commits, the next epoch can have the same Recovery Version, and then the
%% two recovery transactions have the same LSN. A recovery transaction has
%% no content, so a read is not affected. `formal/Recovery.tla' has this
%% case (`BelowNextEpoch').
%%
%% FoundationDB uses the constant `MAX_VERSIONS_IN_FLIGHT' for both
%% purposes. The limit in the Sequencer of redwall is not taken from the
%% FoundationDB source. This part is not in the paper.
%%
%% The gap at the start of an epoch is larger than the MVCC window of the
%% Resolver (`mvcc_window', default: the versions of 5 seconds). The
%% Resolver of the new epoch has no history. Because of the gap, the
%% Resolver treats a transaction with a read version of the previous epoch
%% as too old. See {@link redwall_resolver}.
%%
%% With `versions_per_second => 0', each commit version is the previous
%% one plus 1, `max_versions_in_flight' is 1000, and the MVCC window has no
%% limit. The unit tests use this option.
%%
%% `epoch_gap' replaces `max_versions_in_flight' in the first commit
%% version of an epoch. It plants a defect for the simulation.
%%
%% The process of an epoch does not learn that a later epoch exists. The
%% process of an ended epoch still assigns versions. The LogServers of the
%% ended epoch are stopped, so no record with these versions is stored.
%% The process confirms with the LogServers that the epoch is live before
%% it gives the read version to a client. See {@link redwall_epoch}.
%%
%% == Departures from the paper ==
%%
%% <ul>
%% <li>The Sequencer is not a process in redwall. The process of an epoch
%%   ({@link redwall_epoch}) does the work of the Sequencer, of the
%%   Resolver and of the Proxy. See {@link redwall_resolver}. This part
%%   is not in the paper.</li>
%% <li>The process of the epoch sends the recovery transaction itself,
%%   after the write of the coordinated state. The master of FoundationDB
%%   sends it through a proxy, before the write of the coordinated state.
%%   The recovery transaction of FoundationDB also contains the
%%   configuration of the transaction system. The recovery transaction of
%%   redwall has no content.</li>
%% <li>FoundationDB also keeps the commit versions near the time of the
%%   wall clock (`figureVersion' in the source). redwall does not.</li>
%% </ul>
-module(redwall_sequencer).

-export([defaults/1, first_version/2, next_version/5]).

-type version() :: redwall_cstate:version().

%% @doc The version options with their defaults: `versions_per_second',
%% `max_versions_in_flight', `max_read_life' and `mvcc_window'.
-spec defaults(map()) -> map().
defaults(Options) ->
    PerSecond = maps:get(versions_per_second, Options, 1000000),
    Window =
        case PerSecond of
            0 -> infinity;
            _ -> 5 * PerSecond
        end,
    InFlight =
        case PerSecond of
            0 -> 1000;
            _ -> 100 * PerSecond
        end,
    #{
        versions_per_second => PerSecond,
        max_versions_in_flight => maps:get(max_versions_in_flight, Options, InFlight),
        max_read_life => maps:get(max_read_life, Options, max(1, 5 * PerSecond)),
        mvcc_window => maps:get(mvcc_window, Options, Window)
    }.

%% @doc The first commit version of an epoch with the Recovery Version
%% `RV'. This is the rule of `readTransactionSystemState' in the source.
-spec first_version(version(), map()) -> version().
first_version(0, _Options) ->
    1;
first_version(RV, Options) ->
    #{max_versions_in_flight := InFlight} = defaults(Options),
    RV + maps:get(epoch_gap, Options, InFlight).

%% @doc The commit version after `Last'. `LastTime' is the time of the
%% assignment of `Last', and `Now' is the time of this assignment, both
%% in microseconds. `Live' is the read version. The result is
%% `{error, versions_in_flight}' if the version would be
%% `max_versions_in_flight' or more above the read version. This is the
%% rule of `getVersion' in the source.
-spec next_version(version(), integer(), integer(), version(), map()) -> {ok, version()} | {error, versions_in_flight}.
next_version(Last, LastTime, Now, Live, Options) ->
    #{versions_per_second := PerSecond, max_read_life := MaxReadLife, max_versions_in_flight := InFlight} = defaults(Options),
    Add = max(1, min(MaxReadLife, PerSecond * (Now - LastTime) div 1000000)),
    case Last + Add < Live + InFlight of
        true -> {ok, Last + Add};
        false -> {error, versions_in_flight}
    end.
