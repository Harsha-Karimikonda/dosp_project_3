ERLC    ?= erlc
ESCRIPT ?= escript
SRC     := $(wildcard src/*.erl)
TESTS   := $(wildcard test/*.erl)
BEAMS   := $(patsubst src/%.erl,ebin/%.beam,$(SRC))

.PHONY: all compile escript test clean

all: escript

compile: ebin/pastry_app.beam $(BEAMS)

ebin/kv_app.beam: ebin/pastry_app.beam

ebin/%.beam: src/%.erl include/pastry.hrl | ebin
	$(ERLC) -Wall -pa ebin -I include -o ebin $<

ebin:
	mkdir -p ebin

# Self-contained executable: ./project3 numNodes numRequests
escript: compile
	$(ESCRIPT) tools/build_escript.escript

test: compile
	mkdir -p ebin/test
	$(ERLC) -I include -o ebin/test $(TESTS)
	$(ESCRIPT) tools/run_tests.escript

clean:
	rm -rf ebin project3
