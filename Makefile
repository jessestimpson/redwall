.PHONY: all compile test dst tlc tlc-long

all: compile

compile:
	rebar3 compile

# Unit tests. One test starts a peace cluster of three servers on one node.
test:
	rebar3 eunit

# Deterministic simulation with eta. See dst/deadlift_dst.erl. The node has
# one scheduler thread, so that the scheduler of eta is the only source of
# the order of the processes.
dst:
	ERL_FLAGS="+S 1" rebar3 as dst eunit

# Model checking with TLC. See formal/README.md. This target needs Java.
tlc:
	formal/check_all.sh quick

tlc-long:
	formal/check_all.sh long
