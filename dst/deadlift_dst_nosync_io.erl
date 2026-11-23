%% This module is a planted defect. It replaces the file operations of
%% `peace_storage_wal' with operations that never sync. The simulated disk
%% of eta loses the data of a node that was not synced when the node is
%% killed. A coordinator with this module can start again without a value
%% that the peace cluster has decided. The defect shows that the invariant
%% `coordinators_agree' of `deadlift_dst' can fail. The module follows
%% `peace_dst_nosync_io' of the harness of peace.
-module(deadlift_dst_nosync_io).

-include("deadlift_eta.hrl").

-export([open/2, write/2, datasync/1, rename/2]).

open(File, Modes) -> file:open(File, Modes).

write(Fd, Data) -> file:write(Fd, Data).

datasync(_Fd) -> ok.

rename(From, To) -> file:rename(From, To).
