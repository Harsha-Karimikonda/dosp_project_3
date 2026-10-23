%% @doc Entry point:  project3 numNodes numRequests [options]
%%
%% Builds a Pastry overlay with numNodes peers (one actor each), then every
%% peer issues numRequests key/value requests at 1 request/second. When all
%% requests are done the average number of hops is printed.
%%
%% Options (bonus / debugging):
%%   --verify            check every delivery reached the numerically closest node
%%   --kill P            kill P% of the nodes during the request phase
%%   --link-fail P       fail one link on P% of the nodes during the request phase
%%   --link-mode M       temp (default, heals after 1-3 s) | perm
%%   --replicas K        replicate each stored key on K leaf set members
%%   --keys K            size of the key space (default 1000)
%%   --timeout MS        per-request timeout in ms
-module(project3).
-export([main/1]).

main(Args) ->
    case parse(Args) of
        {ok, N, R, Opts} -> run(N, R, Opts);
        {error, Why} ->
            io:format(standard_error, "error: ~s~n~n", [Why]),
            usage(),
            halt(1)
    end.

usage() ->
    io:format(standard_error,
              "usage: project3 numNodes numRequests [--verify] [--kill P] "
              "[--link-fail P] [--link-mode temp|perm] [--replicas K] "
              "[--keys K] [--timeout MS]~n", []).

%% ---------------------------------------------------------------------
%% Argument parsing
%% ---------------------------------------------------------------------

parse([NS, RS | Rest]) ->
    try
        N = list_to_integer(NS),
        R = list_to_integer(RS),
        true = N >= 1 andalso R >= 0,
        Defaults = #{verify => false, kill => 0, link_fail => 0,
                     link_mode => temp, keys => 1000},
        Opts = opts(Rest, Defaults),
        Faulty = maps:get(kill, Opts) > 0 orelse maps:get(link_fail, Opts) > 0,
        Opts2 = maps:merge(#{reliable => Faulty,
                             replicas => case Faulty of true -> 8; false -> 0 end,
                             timeout => case Faulty of true -> 5000; false -> 60000 end},
                           Opts),
        {ok, N, R, Opts2}
    catch
        throw:{bad_opt, O} -> {error, "bad option " ++ O};
        _:_ -> {error, "numNodes must be >= 1 and numRequests >= 0"}
    end;
parse(_) ->
    {error, "missing arguments"}.

opts([], O) -> O;
opts(["--verify" | T], O) -> opts(T, O#{verify => true});
opts(["--kill", P | T], O) -> opts(T, O#{kill => num(P)});
opts(["--link-fail", P | T], O) -> opts(T, O#{link_fail => num(P)});
opts(["--link-mode", "temp" | T], O) -> opts(T, O#{link_mode => temp});
opts(["--link-mode", "perm" | T], O) -> opts(T, O#{link_mode => perm});
opts(["--replicas", K | T], O) -> opts(T, O#{replicas => list_to_integer(K)});
opts(["--keys", K | T], O) -> opts(T, O#{keys => list_to_integer(K)});
opts(["--timeout", K | T], O) -> opts(T, O#{timeout => list_to_integer(K)});
opts([X | _], _) -> throw({bad_opt, X}).

num(S) ->
    try list_to_float(S) catch _:_ -> float(list_to_integer(S)) end.

%% ---------------------------------------------------------------------
%% Simulation
%% ---------------------------------------------------------------------

run(N, R, Opts) ->
    #{verify := Verify, reliable := Rel} = Opts,
    io:format("Pastry: ~p nodes, ~p requests/node (b=4, |L|=16, |M|=16)~s~n",
              [N, R, case Rel of true -> ", failure handling ON"; false -> "" end]),
    Stats = pastry_stats:start(self(), N, R, Verify),
    AppArgs = #{stats => Stats, num_requests => R,
                keys => maps:get(keys, Opts), timeout => maps:get(timeout, Opts),
                replicas => maps:get(replicas, Opts)},
    T0 = now_ms(),
    Nodes = join_all(N, Stats, AppArgs, Rel),
    T1 = now_ms(),
    io:format("Network built: ~p nodes joined in ~.2f s~n", [N, (T1 - T0) / 1000]),
    Stats ! {register, Nodes},
    maybe_faults(Nodes, Stats, R, Opts),
    io:format("Sending requests (1 request/second per peer)...~n"),
    [P ! start_requests || {_, P, _} <- Nodes],
    receive
        {stats_done, Rep} ->
            print_report(N, R, Rep, (now_ms() - T1) / 1000, Opts)
    end,
    halt(0).

%% Nodes join one at a time; each new node bootstraps from the node that
%% is closest in the proximity space among a random sample of joined nodes.
join_all(N, Stats, AppArgs, Rel) ->
    join_all(1, N, Stats, AppArgs, Rel, #{}, max(1, N div 10)).

join_all(I, N, _, _, _, Joined, _) when I > N ->
    maps:values(Joined);
join_all(I, N, Stats, AppArgs, Rel, Joined, Step) ->
    Pos = {rand:uniform() * 1000, rand:uniform() * 1000},
    Boot = case I of
               1 -> none;
               _ -> pick_bootstrap(Pos, Joined, I - 1)
           end,
    {ok, Pid} = pastry_node:start(#{index => I, pos => Pos, app => kv_app,
                                    app_args => AppArgs, bootstrap => Boot,
                                    main => self(), stats => Stats,
                                    reliable => Rel}),
    receive
        {joined, Pid, Id} ->
            case I rem Step of
                0 -> io:format("  joined ~p/~p~n", [I, N]);
                _ -> ok
            end,
            join_all(I + 1, N, Stats, AppArgs, Rel, Joined#{I => {Id, Pid, Pos}}, Step)
    after 60000 ->
            io:format(standard_error, "node ~p failed to join~n", [I]),
            halt(2)
    end.

pick_bootstrap(Pos, Joined, Count) ->
    Sample = [maps:get(rand:uniform(Count), Joined) || _ <- lists:seq(1, min(16, Count))],
    Tagged = [{pastry_state:proximity(Pos, P), Node} || Node = {_, _, P} <- Sample],
    element(2, lists:min(Tagged)).

maybe_faults(Nodes, Stats, R, #{reliable := true} = Opts) ->
    Window = {1000, max(1500, round(R * 1000 * 0.6))},
    io:format("Fault injection: kill ~p% nodes, link failures on ~p% nodes (~p) "
              "between ~p and ~p ms~n",
              [maps:get(kill, Opts), maps:get(link_fail, Opts),
               maps:get(link_mode, Opts), element(1, Window), element(2, Window)]),
    pastry_faults:start(Nodes, Stats,
                        #{kill_pct => maps:get(kill, Opts),
                          link_pct => maps:get(link_fail, Opts),
                          link_mode => maps:get(link_mode, Opts),
                          window => Window});
maybe_faults(_, _, _, _) ->
    ok.

print_report(N, R, Rep, Secs, Opts) ->
    #{done := Done, ok := Ok, timeouts := TO, avg_hops := Avg,
      max_hops := Max, hist := Hist} = Rep,
    io:format("~n================ Results ================~n"),
    io:format("Nodes                 : ~p~n", [N]),
    io:format("Requests per node     : ~p~n", [R]),
    io:format("Requests completed    : ~p (delivered ~p, timed out ~p)~n", [Done, Ok, TO]),
    io:format("Request phase         : ~.2f s~n", [Secs]),
    io:format("log16(N)              : ~.3f~n", [math:log(max(N, 1)) / math:log(16)]),
    io:format("Max hops              : ~p~n", [Max]),
    io:format("Hop distribution      : ~s~n",
              [string:join([io_lib:format("~p:~p", [H, C])
                            || {H, C} <- lists:sort(maps:to_list(Hist))], "  ")]),
    case maps:get(verify, Rep) of
        true -> io:format("Misdelivered (verify) : ~p~n", [maps:get(misdelivered, Rep)]);
        false -> ok
    end,
    case maps:get(reliable, Opts) of
        true ->
            C = maps:get(counters, Rep),
            io:format("---- failure model ----~n"),
            io:format("Nodes killed          : ~p (alive ~p)~n",
                      [maps:get(killed, Rep), maps:get(alive, Rep)]),
            io:format("Links failed          : ~p~n", [maps:get(links_failed, C, 0)]),
            io:format("Delivery success      : ~.2f %~n",
                      [case Done of 0 -> 100.0; _ -> 100 * Ok / Done end]),
            io:format("Re-routes (ack t/o)   : ~p~n", [maps:get(reroutes, C, 0)]),
            io:format("Repairs triggered     : ~p~n", [maps:get(repairs, C, 0)]),
            io:format("Dropped (hop limit)   : ~p~n", [maps:get(dropped, C, 0)]),
            io:format("Gets found/not found  : ~p / ~p~n",
                      [maps:get(gets_found, Rep), maps:get(gets_nf, Rep)]),
            io:format("Data lost (gets)      : ~p~n", [maps:get(data_lost, Rep)]);
        false -> ok
    end,
    io:format("=========================================~n"),
    io:format("Average number of hops: ~.4f~n", [Avg]).

now_ms() -> erlang:monotonic_time(millisecond).
