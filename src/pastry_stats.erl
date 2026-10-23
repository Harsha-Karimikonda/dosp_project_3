%% @doc Statistics actor: collects one result per request and, once every
%% peer has completed its requests, sends a report to the coordinator.
%%
%% In verify mode it also keeps the global set of live node ids (only for
%% checking; it is never used for routing) and checks that every message
%% was delivered at the node numerically closest to its key.
-module(pastry_stats).
-include("pastry.hrl").

-export([start/4]).

-record(s, {
    main, n, r, verify,
    registered = false,
    expected,             % requests still expected (shrinks when nodes die)
    done = 0, ok = 0, timeouts = 0,
    sum_hops = 0, max_hops = 0, hist = #{},
    reported = #{},       % Pid => results reported
    alive = 0, killed = 0,
    ring, nring,          % gb_sets of live ids / mirrored ids (verify)
    misdelivered = 0,
    put_values = #{},     % Key => #{Value => true}
    put_acked = #{},      % Key => time a put was first acknowledged
    acked_vers = #{},     % Key => [{AckTimeMs, Ver}]
    gets_found = 0, gets_nf = 0, data_lost = 0,
    corrupt_values = 0, stale_reads = 0,
    counters = #{}
}).

start(Main, N, R, Verify) ->
    spawn(fun() ->
                  loop(#s{main = Main, n = N, r = R, verify = Verify,
                          expected = N * R,
                          ring = gb_sets:new(), nring = gb_sets:new()})
          end).

loop(S) ->
    receive
        {register, Nodes} ->
            [begin erlang:monitor(process, P), put({id, P}, I) end
             || {I, P, _} <- Nodes],
            Ids = [I || {I, _, _} <- Nodes],
            S2 = case S#s.verify of
                     true ->
                         S#s{ring = gb_sets:from_list(Ids),
                             nring = gb_sets:from_list([?RING - 1 - I || I <- Ids])};
                     false -> S
                 end,
            check(S2#s{registered = true, alive = length(Nodes)});
        {kv_put_issued, Key, Value} ->
            PV = S#s.put_values,
            Set = maps:get(Key, PV, #{}),
            loop(S#s{put_values = PV#{Key => Set#{Value => true}}});
        {kv_result, Pid, Type, Key, Result, Hops, Owner, IssuedAt} ->
            check(result(S, Pid, Type, Key, Result, Hops, Owner, IssuedAt));
        {counter, Name} ->
            C = S#s.counters,
            loop(S#s{counters = C#{Name => maps:get(Name, C, 0) + 1}});
        {'DOWN', _, process, Pid, _} ->
            Rep = maps:get(Pid, S#s.reported, 0),
            S2 = S#s{expected = S#s.expected - (S#s.r - Rep),
                     alive = S#s.alive - 1, killed = S#s.killed + 1},
            S3 = case S#s.verify of
                     true -> remove_from_ring(Pid, S2);
                     false -> S2
                 end,
            check(S3)
    end.

result(S, Pid, Type, Key, Result, Hops, Owner, IssuedAt) ->
    S1 = S#s{done = S#s.done + 1,
             reported = maps:update_with(Pid, fun(X) -> X + 1 end, 1, S#s.reported)},
    case Result of
        timeout ->
            S1#s{timeouts = S1#s.timeouts + 1};
        _ ->
            S2 = S1#s{ok = S1#s.ok + 1, sum_hops = S1#s.sum_hops + Hops,
                      max_hops = max(S1#s.max_hops, Hops),
                      hist = maps:update_with(Hops, fun(X) -> X + 1 end, 1, S1#s.hist)},
            S3 = verify_owner(S2, Key, Owner),
            data(S3, Type, Key, Result, IssuedAt)
    end.

data(S, put, Key, {put_ok, Ver, _}, _IssuedAt) ->
    Now = erlang:monotonic_time(millisecond),
    AV = S#s.acked_vers,
    Old = maps:get(Key, AV, []),
    S#s{put_acked = maps:update_with(Key, fun(T) -> T end, Now, S#s.put_acked),
        acked_vers = AV#{Key => [{Now, Ver} | Old]}};
data(S, get, Key, {ok, ReadVer, V}, IssuedAt) ->
    Valid = maps:is_key(V, maps:get(Key, S#s.put_values, #{})),
    Corrupt = case Valid of true -> 0; false -> 1 end,
    Prior = [Ver || {AckAt, Ver} <- maps:get(Key, S#s.acked_vers, []),
                    AckAt < IssuedAt],
    Stale = case Prior =/= [] andalso ReadVer < lists:max(Prior) of
                true -> 1;
                false -> 0
            end,
    S#s{gets_found = S#s.gets_found + 1,
        corrupt_values = S#s.corrupt_values + Corrupt,
        stale_reads = S#s.stale_reads + Stale};
data(S, get, Key, not_found, IssuedAt) ->
    Lost = case maps:find(Key, S#s.put_acked) of
               {ok, T} when T < IssuedAt -> 1;   % stored earlier, now missing
               _ -> 0
           end,
    S#s{gets_nf = S#s.gets_nf + 1, data_lost = S#s.data_lost + Lost};
data(S, _, _, _, _) -> S.

verify_owner(S = #s{verify = false}, _, _) -> S;
verify_owner(S = #s{ring = Ring, nring = NRing}, Key, Owner) ->
    case gb_sets:is_empty(Ring) of
        true -> S;
        false ->
            Succ = case gb_sets:next(gb_sets:iterator_from(Key, Ring)) of
                       {X, _} -> X;
                       none -> gb_sets:smallest(Ring)
                   end,
            Pred = case gb_sets:next(gb_sets:iterator_from(?RING - 1 - Key, NRing)) of
                       {Y, _} -> ?RING - 1 - Y;
                       none -> gb_sets:largest(Ring)
                   end,
            Best = element(2, min({pastry_id:dist(Succ, Key), Succ},
                                  {pastry_id:dist(Pred, Key), Pred})),
            case Best =:= Owner of
                true -> S;
                false -> S#s{misdelivered = S#s.misdelivered + 1}
            end
    end.

remove_from_ring(Pid, S) ->
    %% The pid -> id mapping is only needed for verification.
    case get({id, Pid}) of
        undefined -> S;
        Id -> S#s{ring = gb_sets:delete_any(Id, S#s.ring),
                  nring = gb_sets:delete_any(?RING - 1 - Id, S#s.nring)}
    end.

check(S = #s{registered = true, done = D, expected = E}) when D >= E ->
    S#s.main ! {stats_done, report(S)};
check(S = #s{registered = true}) ->
    loop(S);
check(S) ->
    loop(S).

report(S) ->
    #{expected => S#s.n * S#s.r, active_quota => S#s.expected,
      done => S#s.done, ok => S#s.ok,
      timeouts => S#s.timeouts,
      avg_hops => case S#s.ok of 0 -> 0.0; K -> S#s.sum_hops / K end,
      max_hops => S#s.max_hops, hist => S#s.hist,
      verify => S#s.verify, misdelivered => S#s.misdelivered,
      killed => S#s.killed, alive => S#s.alive,
      gets_found => S#s.gets_found, gets_nf => S#s.gets_nf,
      data_lost => S#s.data_lost, corrupt_values => S#s.corrupt_values,
      stale_reads => S#s.stale_reads,
      counters => S#s.counters}.
