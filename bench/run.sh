#!/bin/sh
# Runs the benchmark, bench/redwall_bench.erl, and prints the results as
# Markdown tables.
#
# Usage: bench/run.sh [SECONDS]
#
#   SECONDS  The run time of each row of the first table. The default
#            is 3.
#
# The benchmark starts 7 more Erlang nodes on this computer, and needs
# `epmd'.
set -e
cd "$(dirname "$0")/.."
seconds=${1:-3}
build=_build/bench
rebar3 as bench compile >/dev/null
paths="-pa $PWD/$build/lib/redwall/ebin -pa $PWD/$build/lib/redwall/bench"
if [ -d "$build/checkouts/peace" ]; then
    paths="$paths -pa $PWD/$build/checkouts/peace/ebin"
else
    paths="$paths -pa $PWD/$build/lib/peace/ebin"
fi
# shellcheck disable=SC2086
exec erl -noshell $paths -kernel logger_level error -eval "
    try redwall_bench:main(#{seconds => $seconds}) of
        _ -> halt(0)
    catch
        Class:Reason:Stack ->
            io:format(\"~p:~p~n~p~n\", [Class, Reason, Stack]),
            halt(1)
    end."
