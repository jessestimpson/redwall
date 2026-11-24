.PHONY: all compile test dst dst-serial bench tlc tlc-long

all: compile

compile:
	rebar3 compile

# Unit tests. One test starts a peace cluster of three servers on one node.
test:
	rebar3 eunit

# Deterministic simulation with eta. See dst/redwall_dst.erl. A node has
# one scheduler thread, so that the scheduler of eta is the only source of
# the order of the processes. `dst' runs each test in an Erlang node of its
# own, and several nodes at the same time. See dst/run.sh. `dst-serial'
# runs the tests one after another in one node.
dst:
	dst/run.sh

dst-serial:
	ERL_FLAGS="+S 1" rebar3 as dst eunit

# The benchmark of a cluster of 8 Erlang nodes on this computer. See
# bench/redwall_bench.erl.
bench:
	bench/run.sh

# Model checking with TLC. This target needs Java. formal/README.md states
# which configs a change needs: `formal/check_all.sh resolver' takes about
# 30 seconds, `make tlc' about 5 minutes and `make tlc-long' about 4 hours.
tlc:
	formal/check_all.sh quick

tlc-long:
	formal/check_all.sh long
