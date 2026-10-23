-module(pastry_state_tests).
-include_lib("eunit/include/eunit.hrl").

mk_node(I) -> {pastry_id:hash("n" ++ integer_to_list(I)), list_to_pid("<0.0.0>"), {float(I), 0.0}}.

%% Global ground truth: numerically closest id.
truth(Key, Nodes) -> element(1, pastry_state:closest(Key, Nodes)).

leaf_set_size_test() ->
    Nodes = [mk_node(I) || I <- lists:seq(1, 50)],
    {PS, _} = pastry_state:add_all(tl(Nodes), pastry_state:new(hd(Nodes))),
    ?assertEqual(8, length(pastry_state:smaller(PS))),
    ?assertEqual(8, length(pastry_state:larger(PS))).

leaf_set_is_nearest_test() ->
    [Self | Others] = [mk_node(I) || I <- lists:seq(1, 60)],
    {PS, _} = pastry_state:add_all(Others, pastry_state:new(Self)),
    {S, _, _} = Self,
    Expected = lists:sublist(lists:sort([{pastry_id:cw(S, I), I} || {I, _, _} <- Others]), 8),
    ?assertEqual([I || {_, I} <- Expected], [I || {I, _, _} <- pastry_state:larger(PS)]).

remove_test() ->
    [Self | Others] = [mk_node(I) || I <- lists:seq(1, 30)],
    {PS, _} = pastry_state:add_all(Others, pastry_state:new(Self)),
    {Id, _, _} = hd(pastry_state:larger(PS)),
    {PS2, Info} = pastry_state:remove(Id, PS),
    ?assert(maps:get(larger, Info)),
    ?assertNot(lists:keymember(Id, 1, pastry_state:all_nodes(PS2))).

%% With full knowledge, next_hop must deliver locally exactly when we are
%% the closest node, and otherwise forward to a strictly closer node.
next_hop_progress_test() ->
    Nodes = [mk_node(I) || I <- lists:seq(1, 200)],
    States = [element(1, pastry_state:add_all(Nodes, pastry_state:new(N))) || N <- Nodes],
    Keys = [pastry_id:hash("k" ++ integer_to_list(K)) || K <- lists:seq(1, 200)],
    lists:foreach(
      fun({PS, Key}) ->
              {S, _, _} = pastry_state:self_node(PS),
              case pastry_state:next_hop(Key, PS) of
                  deliver -> ?assertEqual(truth(Key, Nodes), S);
                  {forward, {I, _, _}} ->
                      ?assert(pastry_id:shl(I, Key) > pastry_id:shl(S, Key)
                              orelse pastry_id:dist(I, Key) < pastry_id:dist(S, Key))
              end
      end, [{PS, K} || PS <- lists:sublist(States, 20), K <- Keys]).

single_node_delivers_test() ->
    PS = pastry_state:new(mk_node(1)),
    ?assertEqual(deliver, pastry_state:next_hop(pastry_id:hash("x"), PS)).
