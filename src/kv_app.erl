%% @doc Simple object access service on top of Pastry: associates a key
%% (a 128-bit Pastry id) with a string.
%%
%%   put(Key, String) -> routed to the node numerically closest to Key,
%%                       which stores it (and optionally replicates it on
%%                       its closest leaf set members).
%%   get(Key)         -> routed the same way; the owner replies directly
%%                       to the requester via pastry_node:send/3.
%%
%% Every peer issues one request per second until it has made
%% NumRequests requests. Each completed (or timed out) request is reported
%% to the statistics actor together with the number of hops it took.
-module(kv_app).
-behaviour(pastry_app).

-export([init/2, deliver/4, forward/4, new_leafs/2, handle_info/2]).

init(Self, Args) ->
    Args#{self => Self,          % {Id, Pid, Pos}
          store => #{},          % Key => Value
          smaller => [], larger => [],
          issued => 0,
          outstanding => #{}}.   % ReqId => {Type, Key, IssuedAt, TRef}

%% ---------------------------------------------------------------------
%% Request generation (1 request / second)
%% ---------------------------------------------------------------------

handle_info(start_requests, S = #{num_requests := R}) when R > 0 ->
    %% random phase so that peers don't all fire in the same millisecond
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

%% ---------------------------------------------------------------------
%% Pastry application callbacks
%% ---------------------------------------------------------------------

%% Owner of the key: store the value and acknowledge.
deliver({kv_put, Key, Value, Origin, ReqId}, Key, #{hops := H}, S = #{store := St}) ->
    replicate(Key, Value, replica_targets(S)),
    reply(S, Origin, {kv_reply, ReqId, ok, H, my_id(S)}),
    S#{store := St#{Key => Value}};
%% Owner of the key: look the value up and answer the requester directly.
deliver({kv_get, Key, Origin, ReqId}, Key, #{hops := H}, S = #{store := St}) ->
    Result = case maps:find(Key, St) of
                 {ok, V} -> {ok, V};
                 error -> not_found
             end,
    reply(S, Origin, {kv_reply, ReqId, Result, H, my_id(S)}),
    S;
%% Replica pushed by a key owner (bonus: survives owner failure).
deliver({kv_replica, Key, Value}, _, _, S = #{store := St}) ->
    S#{store := St#{Key => Value}};
%% Reply at the requesting node.
deliver({kv_reply, ReqId, Result, Hops, Owner}, _, _, S = #{outstanding := O}) ->
    case maps:take(ReqId, O) of
        {{Type, Key, IssuedAt, TRef}, O2} ->
            erlang:cancel_timer(TRef),
            report(S, Type, Key, Result, Hops, Owner, IssuedAt),
            S#{outstanding := O2};
        error -> S       % late/duplicate reply
    end;
deliver(_Msg, _Key, _Meta, S) ->
    S.

forward(Msg, _Key, Next, S) ->
    {forward, Msg, Next, S}.

%% Leaf set changed: remember it (replica placement) and push the keys we
%% own to newly added replica holders.
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

%% ---------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------

reply(_S, Origin, Msg) ->
    pastry_node:send(self(), Msg, Origin).

replicate(Key, Value, Targets) ->
    [pastry_node:send(self(), {kv_replica, Key, Value}, P) || P <- Targets],
    ok.

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
