%% Pure operations on Pastry node state (leaf set, routing table, neighborhood set, routing).
-module(pastry_state).
-include("pastry.hrl").

-export([new/1, self_node/1, add/2, add_all/2, remove/2,
         leaves/1, leaf_set/1, smaller/1, larger/1, neighbors/1,
         routing_nodes/1, row/2, rows_upto/2, all_nodes/1,
         in_leaf_range/2, next_hop/2, closest/2, find_pid/2,
         proximity/2]).

new(Self) -> #pst{self = Self}.

self_node(#pst{self = S}) -> S.
smaller(#pst{smaller = S}) -> S.
larger(#pst{larger = L}) -> L.
neighbors(#pst{nbr = N}) -> N.

add_all(Nodes, PS) ->
    lists:foldl(fun(N, {P, Ch}) ->
                        {P2, C2} = add(N, P),
                        {P2, Ch orelse C2}
                end, {PS, false}, Nodes).

add({Id, _, _}, PS = #pst{self = {Id, _, _}}) ->
    {PS, false};
add(N, PS = #pst{self = {S, _, _}, smaller = Sm, larger = La}) ->
    Sm2 = insert_side(N, Sm, fun(I) -> pastry_id:cw(I, S) end),
    La2 = insert_side(N, La, fun(I) -> pastry_id:cw(S, I) end),
    PS2 = PS#pst{smaller = Sm2, larger = La2,
                 rt = add_rt(N, PS), nbr = add_nbr(N, PS)},
    {PS2, Sm2 =/= Sm orelse La2 =/= La}.

insert_side(N = {Id, _, _}, List, DistF) ->
    case lists:keymember(Id, 1, List) of
        true -> List;
        false ->
            Tagged = lists:keysort(1, [{DistF(I), X} || X = {I, _, _} <- [N | List]]),
            [X || {_, X} <- lists:sublist(Tagged, ?L_HALF)]
    end.

add_rt(N = {Id, _, P}, #pst{self = {S, _, SP}, rt = RT}) ->
    L = pastry_id:shl(Id, S),
    Slot = {L, pastry_id:digit(Id, L)},
    case maps:find(Slot, RT) of
        error -> RT#{Slot => N};
        {ok, {Id, _, _}} -> RT;
        {ok, {_, _, OP}} ->
            case proximity(SP, P) < proximity(SP, OP) of
                true  -> RT#{Slot => N};
                false -> RT
            end
    end.

add_nbr(N = {Id, _, P}, #pst{self = {_, _, SP}, nbr = Nb}) ->
    case lists:keymember(Id, 1, Nb) of
        true -> Nb;
        false ->
            Tagged = lists:keysort(1, [{proximity(SP, XP), X} || X = {_, _, XP} <- [N | Nb]]),
            _ = P,
            [X || {_, X} <- lists:sublist(Tagged, ?M_SIZE)]
    end.

remove(Id, PS = #pst{smaller = Sm, larger = La, rt = RT, nbr = Nb}) ->
    Sm2 = lists:keydelete(Id, 1, Sm),
    La2 = lists:keydelete(Id, 1, La),
    Slots = [Slot || {Slot, {I, _, _}} <- maps:to_list(RT), I =:= Id],
    Nb2 = lists:keydelete(Id, 1, Nb),
    Info = #{smaller => Sm2 =/= Sm, larger => La2 =/= La,
             rt => Slots, nbr => Nb2 =/= Nb},
    {PS#pst{smaller = Sm2, larger = La2, rt = maps:without(Slots, RT), nbr = Nb2}, Info}.

leaves(#pst{smaller = Sm, larger = La}) -> uniq(Sm ++ La).

leaf_set(#pst{smaller = Sm, larger = La}) -> {Sm, La}.

routing_nodes(#pst{rt = RT}) -> maps:values(RT).

row(R, #pst{rt = RT}) -> [N || {{R2, _}, N} <- maps:to_list(RT), R2 =:= R].

rows_upto(L, #pst{rt = RT}) -> [N || {{R, _}, N} <- maps:to_list(RT), R =< L].

all_nodes(PS = #pst{nbr = Nb}) -> uniq(leaves(PS) ++ routing_nodes(PS) ++ Nb).

find_pid(Pid, PS) ->
    case [N || N = {_, P, _} <- all_nodes(PS), P =:= Pid] of
        [N | _] -> {ok, N};
        []      -> error
    end.

in_leaf_range(Key, #pst{self = {S, _, _}, smaller = Sm, larger = La}) ->
    MaxCw  = case La of [] -> 0; _ -> pastry_id:cw(S, element(1, lists:last(La))) end,
    MaxCcw = case Sm of [] -> 0; _ -> pastry_id:cw(element(1, lists:last(Sm)), S) end,
    pastry_id:cw(S, Key) =< MaxCw orelse pastry_id:cw(Key, S) =< MaxCcw.

%% Pastry routing decision (Section 2.2 / Table 1): leaf set, routing table, or rare-case fallback.
next_hop(Key, #pst{self = {Key, _, _}}) ->
    deliver;
next_hop(Key, PS = #pst{self = Self = {S, _, _}, rt = RT}) ->
    case in_leaf_range(Key, PS) of
        true ->
            case closest(Key, [Self | leaves(PS)]) of
                {S, _, _} -> deliver;
                N -> {forward, N}
            end;
        false ->
            L = pastry_id:shl(Key, S),
            case maps:find({L, pastry_id:digit(Key, L)}, RT) of
                {ok, N} -> {forward, N};
                error -> rare_case(Key, L, PS)
            end
    end.

rare_case(Key, L, PS = #pst{self = {S, _, _}}) ->
    DS = pastry_id:dist(S, Key),
    Cands = [N || N = {I, _, _} <- all_nodes(PS),
                  pastry_id:shl(I, Key) >= L,
                  pastry_id:dist(I, Key) < DS],
    case Cands of
        [] -> deliver;
        _  -> {forward, closest(Key, Cands)}
    end.

closest(Key, [First | Rest]) ->
    {_, Best} = lists:foldl(
                  fun(N = {I, _, _}, {BD, B}) ->
                          D = {pastry_id:dist(I, Key), I},
                          case D < BD of true -> {D, N}; false -> {BD, B} end
                  end,
                  {{pastry_id:dist(element(1, First), Key), element(1, First)}, First},
                  Rest),
    Best.

proximity({X1, Y1}, {X2, Y2}) ->
    DX = X1 - X2, DY = Y1 - Y2,
    math:sqrt(DX * DX + DY * DY).

uniq(Nodes) -> uniq(Nodes, #{}, []).
uniq([], _, Acc) -> lists:reverse(Acc);
uniq([N = {I, _, _} | T], Seen, Acc) ->
    case maps:is_key(I, Seen) of
        true  -> uniq(T, Seen, Acc);
        false -> uniq(T, Seen#{I => true}, [N | Acc])
    end.
