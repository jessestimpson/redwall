%% This header opts a module in to deterministic simulation testing with eta.
%%
%% Every module that is part of the system under simulation includes this
%% header. Under the `dst' profile, which defines DST, this header includes
%% the parse transform of eta. The parse transform redirects timers, clock
%% reads, spawns, sends and `rand' to the virtual equivalents of eta. In
%% every other build, this header adds no behaviour, and eta does not have
%% to be resolvable.
-ifndef(DEADLIFT_ETA_HRL).
-define(DEADLIFT_ETA_HRL, true).

-ifdef(DST).
-include_lib("eta/include/eta.hrl").
-else.
%% Neither macro evaluates its argument. Therefore:
%%   - An expression that is logged must have no side effects.
%%   - A variable that is used only in the argument of one of these macros
%%     must have an underscore prefix.
-define(ETA_LABEL(Label), ok).
-define(ETA_LOG(Event), 0).
-endif.

-endif.
