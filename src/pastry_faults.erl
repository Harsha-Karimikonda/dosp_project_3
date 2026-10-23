%% @doc Fault injector actor (bonus). During the request phase it
%%   * kills a percentage of the peers (node failure: crash-stop), and/or
%%   * fails links between peers and their routing state neighbours,
%%     either temporarily (healed after 1-3 s) or permanently.
%% Failures are spread uniformly over a time window.
-module(pastry_faults).

-export([start/3]).

start(Nodes, Stats, Cfg) ->
    spawn(fun() -> run(Nodes, Stats, Cfg) end).

run(Nodes, Stats, #{kill_pct := KP, link_pct := LP, link_mode := Mode,
                    window := {From, To}}) ->
    N = length(Nodes),
    Pids = [P || {_, P, _} <- Nodes],
    Victims = lists:sublist(shuffle(Pids), round(N * KP / 100)),
    LinkNodes = lists:sublist(shuffle(Pids), round(N * LP / 100)),
    At = fun() -> From + rand:uniform(max(1, To - From)) end,
    [erlang:send_after(At(), self(), {kill, P}) || P <- Victims],
    [erlang:send_after(At(), self(), {link, P}) || P <- LinkNodes],
    loop(length(Victims) + length(LinkNodes), Stats, Mode).

loop(0, _, _) -> ok;
loop(Left, Stats, Mode) ->
    receive
        {kill, P} ->
            exit(P, kill),
            loop(Left - 1, Stats, Mode);
        {link, P} ->
            Peer0 = try gen_server:call(P, random_peer, 2000)
                    catch _:_ -> none
                    end,
            case Peer0 of
                {_, Peer, _} when is_pid(Peer) ->
                    Dur = case Mode of
                              perm -> infinity;
                              temp -> 1000 + rand:uniform(2000)
                          end,
                    P ! {fail_link, Peer, Dur},
                    Peer ! {fail_link, P, Dur},
                    Stats ! {counter, links_failed};
                _ -> ok
            end,
            loop(Left - 1, Stats, Mode)
    end.

shuffle(L) -> [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- L])].
