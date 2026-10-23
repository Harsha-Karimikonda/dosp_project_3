%% Distributed key-value object access service on top of Pastry (1 request/second per peer).
-module(kv_app).
-behaviour(pastry_app).

-export([init/2, deliver/4, forward/4, new_leafs/2, handle_info/2]).

init(Self, Args) ->
    Args#{self => Self,
          store => #{},
          smaller => [], larger => [],
          issued => 0,
          outstanding => #{}}.

handle_info(start_requests, S = #{num_requests := R}) when R > 0 ->
    erlang:send_after(rand:uniform(1000), self(), kv_tick),
    S;
handle_info(kv_tick, S = #{issued := I, num_requests := R}) ->
    S2 = issue_request(S),
    case I + 1 < R of
        true -> erlang:send_after(1000, self(), kv_tick);
        false -> ok
    end,
    S2#{issued := I + 1};
handle_info({kv_timeout, ReqId}, S = #{outstanding := O}) ->
    case maps:take(ReqId, O) of
        {{Type, Key, IssuedAt, _}, O2} ->
            report(S, Type, Key, timeout, undefined, undefined, IssuedAt),
            S#{outstanding := O2};
        error -> S
    end;
handle_info(_Other, S) ->
    S.

issue_request(S = #{self := {MyId, _, _}, keys := K, timeout := TO,
                    outstanding := O, issued := I, stats := Stats}) ->
    Key = pastry_id:hash("key-" ++ integer_to_list(rand:uniform(K))),
    ReqId = make_ref(),
    {Type, Msg} =
        case rand:uniform(2) of
            1 ->
                Value = "value from " ++ pastry_id:short(MyId) ++ " #" ++ integer_to_list(I + 1),
                Stats ! {kv_put_issued, Key, Value},
                {put, {kv_put, Key, Value, self(), ReqId}};
            2 ->
                {get, {kv_get, Key, self(), ReqId}}
        end,
    pastry_node:route(self(), Msg, Key),
    TRef = erlang:send_after(TO, self(), {kv_timeout, ReqId}),
    S#{outstanding := O#{ReqId => {Type, Key, now_ms(), TRef}}}.

deliver({kv_put, Key, Value, Origin, ReqId}, Key, #{hops := H}, S = #{store := St}) ->
    Ver = erlang:unique_integer([monotonic, positive]),
    Entry = {Ver, Value},
    replicate(Key, Entry, replica_targets(S)),
    reply(S, Origin, {kv_reply, ReqId, {put_ok, Ver, Value}, H, my_id(S)}),
    S#{store := put_versioned(Key, Entry, St)};
deliver({kv_get, Key, Origin, ReqId}, Key, #{hops := H}, S = #{store := St}) ->
    Result = case maps:find(Key, St) of
                 {ok, {Ver, V}} -> {ok, Ver, V};
                 error -> not_found
             end,
    reply(S, Origin, {kv_reply, ReqId, Result, H, my_id(S)}),
    S;
deliver({kv_replica, Key, Entry}, _, _, S = #{store := St}) ->
    S#{store := put_versioned(Key, Entry, St)};
deliver({kv_reply, ReqId, Result, Hops, Owner}, _, _, S = #{outstanding := O}) ->
    case maps:take(ReqId, O) of
        {{Type, Key, IssuedAt, TRef}, O2} ->
            erlang:cancel_timer(TRef),
            report(S, Type, Key, Result, Hops, Owner, IssuedAt),
            S#{outstanding := O2};
        error -> S
    end;
deliver(_Msg, _Key, _Meta, S) ->
    S.

forward(Msg, _Key, Next, S) ->
    {forward, Msg, Next, S}.

new_leafs({Sm, La}, S = #{replicas := Rep, store := St}) ->
    Old = replica_targets(S),
    S2 = S#{smaller := Sm, larger := La},
    case Rep > 0 andalso map_size(St) > 0 of
        true ->
            New = replica_targets(S2),
            case New =/= Old of
                false -> ok;
                true ->
                    Mine = [{K, V} || {K, V} <- maps:to_list(St), is_primary(K, S2)],
                    [replicate(K, V, New) || {K, V} <- Mine]
            end;
        false -> ok
    end,
    S2.

reply(_S, Origin, Msg) ->
    pastry_node:send(self(), Msg, Origin).

replicate(Key, Entry, Targets) ->
    [pastry_node:send(self(), {kv_replica, Key, Entry}, P) || P <- Targets],
    ok.

put_versioned(Key, Entry = {Ver, _}, St) ->
    case maps:find(Key, St) of
        {ok, {OldVer, _}} when OldVer > Ver -> St;
        _ -> St#{Key => Entry}
    end.

replica_targets(#{replicas := 0}) -> [];
replica_targets(#{replicas := Rep, smaller := Sm, larger := La}) ->
    Half = Rep div 2,
    Pids = [P || {_, P, _} <- lists:sublist(Sm, Half) ++ lists:sublist(La, Rep - Half)],
    lists:usort(Pids) -- [self()].

is_primary(Key, #{self := Self = {Id, _, _}, smaller := Sm, larger := La}) ->
    element(1, pastry_state:closest(Key, [Self | Sm ++ La])) =:= Id.

my_id(#{self := {Id, _, _}}) -> Id.

report(#{stats := Stats}, Type, Key, Result, Hops, Owner, IssuedAt) ->
    Stats ! {kv_result, self(), Type, Key, Result, Hops, Owner, IssuedAt},
    ok.

now_ms() -> erlang:monotonic_time(millisecond).
