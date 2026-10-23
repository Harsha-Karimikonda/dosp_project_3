#!/usr/bin/env escript
%%! -pa ebin -pa ebin/test
main(_) ->
    case eunit:test([pastry_id_tests, pastry_state_tests], [verbose]) of
        ok -> halt(0);
        _ -> halt(1)
    end.
