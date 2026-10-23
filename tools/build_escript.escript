#!/usr/bin/env escript
%% Packs ebin/*.beam into a self-contained escript named ./project3
main(_) ->
    Files = [{filename:basename(F), element(2, file:read_file(F))}
             || F <- filelib:wildcard("ebin/*.beam")],
    {ok, {_, Zip}} = zip:create("project3.zip", Files, [memory]),
    ok = escript:create("project3",
                        [shebang,
                         {emu_args, "+P 4000000 -escript main project3"},
                         {archive, Zip}]),
    ok = file:change_mode("project3", 8#755),
    io:format("built ./project3~n").
