%% @doc One Pastry peer = one Erlang actor (gen_server).
%%
%% Implements the Pastry API of Section 2.3:
%%   pastryInit  -> start/1   (creates the node and joins the overlay)
%%   route       -> route/3
%%   send        -> send/3
%% and invokes the application callbacks deliver/forward/new_leafs
%% (see pastry_app).
%%
%% Node-to-node traffic is wrapped as {p2p, FromPid, Payload} so that the
%% (bonus) link failure model can drop messages on failed links.
-module(pastry_node).
-behaviour(gen_server).
-include("pastry.hrl").

-export([start/1, route/3, send/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ACK_TIMEOUT, 300).      % per-hop ack timeout (reliable mode)
-define(SUSPECT_TTL, 5000).     % how long a failed node is ignored
-define(MAINT_INTERVAL, 2000).  % leaf set maintenance period
-define(MAX_HOPS, 64).          % safety bound against routing loops

-record(st, {
    ps,                    % #pst{} Pastry state
    app, app_st,           % application module + state
    main, stats,           % coordinator and statistics actors
    reliable = false,      % failure detection / repair enabled (bonus)
    join,                  % undefined | {collect, Map} | {acks, N}
    pending = #{},         % AckRef => {Msg, Key, Hops, Next, TRef}
    dead_links = #{},      % Pid => true   (failed links, bonus)
    suspects = #{},        % Id => ExpiryMs (recently failed nodes)
    monitors = #{}         % Pid => MonitorRef (leaf set keep-alive)
}).

%% =====================================================================
%% Pastry API
%% =====================================================================

%% @doc pastryInit: start a node and join the overlay through Bootstrap
%% (none for the first node). When the join is complete the node sends
%% {joined, Pid, NodeId} to Cfg.main.
start(Cfg) -> gen_server:start(?MODULE, Cfg, [{hibernate_after, 500}]).

%% @doc route(msg, key): route Msg to the node numerically closest to Key.
route(Node, Msg, Key) -> gen_server:cast(Node, {api_route, Msg, Key}).

%% @doc send(msg, node): send Msg directly to the node with pid To.
send(Node, Msg, To) -> gen_server:cast(Node, {api_send, Msg, To}).

%% =====================================================================
%% gen_server
%% =====================================================================

init(#{index := I, pos := Pos, app := App, app_args := AppArgs,
       bootstrap := Boot, main := Main, stats := Stats} = Cfg) ->
    Id = pastry_id:hash("node-" ++ integer_to_list(I)),
    Self = {Id, self(), Pos},
    Rel = maps:get(reliable, Cfg, false),
    St = #st{ps = pastry_state:new(Self), app = App,
             app_st = App:init(Self, AppArgs), main = Main, stats = Stats,
             reliable = Rel,
             join = {collect, #{expected => undefined, got => 0, nodes => []}}},
    case Boot of
        none -> self() ! join_alone;
        {_, BPid, _} -> xmit(BPid, {join_req, Self, 0}, St)
    end,
    case Rel of
        true -> erlang:send_after(?MAINT_INTERVAL + rand:uniform(1000), self(), maint);
        false -> ok
    end,
    {ok, St}.

handle_call(random_peer, _From, St) ->
    Reply = case pastry_state:all_nodes(St#st.ps) of
                [] -> none;
                Ns -> lists:nth(rand:uniform(length(Ns)), Ns)
            end,
    {reply, Reply, St};
handle_call(get_state, _From, St) ->
    {reply, St#st.ps, St};
handle_call(_, _From, St) ->
    {reply, error, St}.

handle_cast({api_route, Msg, Key}, St) ->
    {noreply, do_route(Msg, Key, 0, St)};
handle_cast({api_send, Msg, To}, St) ->
    xmit_direct(To, {direct, Msg}, St),
    {noreply, St};
handle_cast(_, St) ->
    {noreply, St}.

handle_info({p2p, From, Payload}, St) ->
    case maps:is_key(From, St#st.dead_links) of
        true  -> {noreply, St};                 % link is down: drop
        false -> {noreply, p2p(From, Payload, St)}
    end;

handle_info(join_alone, St) ->
    {noreply, join_done(St)};

handle_info({ack_timeout, Ref}, St = #st{pending = P}) ->
    case maps:take(Ref, P) of
        {{Msg, Key, Hops, Next, _}, P2} ->
            counter(St, reroutes),
            St2 = node_failed(Next, St#st{pending = P2}),
            {noreply, do_route(Msg, Key, Hops, St2)};
        error ->
            {noreply, St}
    end;

handle_info({'DOWN', _MRef, process, Pid, _Reason}, St) ->
    St1 = St#st{monitors = maps:remove(Pid, St#st.monitors)},
    case pastry_state:find_pid(Pid, St1#st.ps) of
        {ok, Node} -> {noreply, node_failed(Node, St1)};
        error      -> {noreply, St1}
    end;

handle_info({fail_link, Peer, Dur}, St) ->
    case Dur of
        infinity -> ok;
        _ -> erlang:send_after(Dur, self(), {heal_link, Peer})
    end,
    {noreply, St#st{dead_links = (St#st.dead_links)#{Peer => true}}};

handle_info({heal_link, Peer}, St) ->
    {noreply, St#st{dead_links = maps:remove(Peer, St#st.dead_links)}};

handle_info(maint, St = #st{ps = PS}) ->
    erlang:send_after(?MAINT_INTERVAL, self(), maint),
    Now = now_ms(),
    Sus = maps:filter(fun(_, T) -> T > Now end, St#st.suspects),
    Sm = pastry_state:smaller(PS), La = pastry_state:larger(PS),
    Targets = (case length(Sm) < ?L_HALF of true -> far(Sm, La); false -> [] end)
           ++ (case length(La) < ?L_HALF of true -> far(La, Sm); false -> [] end),
    St2 = St#st{suspects = Sus},
    [xmit(P, leaf_req, St2) || {_, P, _} <- lists:usort(Targets)],
    {noreply, St2};

handle_info(Other, St = #st{app = App, app_st = AS}) ->
    {noreply, St#st{app_st = App:handle_info(Other, AS)}}.

terminate(_Reason, _St) -> ok.
code_change(_Old, St, _Extra) -> {ok, St}.

%% =====================================================================
%% Peer-to-peer protocol
%% =====================================================================

%% ---- Join (Section 2.4) ---------------------------------------------
%% Each node on the route of the join message towards X's id sends X the
%% routing table rows it can contribute (plus itself). The bootstrap node A
%% also sends its neighborhood set, and the final node Z (numerically
%% closest to X) also sends its leaf set.
p2p(_From, {join_req, X = {XId, XPid, _}, Hop}, St = #st{ps = PS}) ->
    Self = {S, _, _} = pastry_state:self_node(PS),
    Rows = pastry_state:rows_upto(pastry_id:shl(XId, S), PS),
    Base = [Self | Rows] ++ case Hop of
                               0 -> pastry_state:neighbors(PS);
                               _ -> []
                           end,
    case pastry_state:next_hop(XId, PS) of
        {forward, {_, NPid, _}} ->
            xmit(XPid, {join_state, Hop, Base, false}, St),
            xmit(NPid, {join_req, X, Hop + 1}, St);
        deliver ->
            xmit(XPid, {join_state, Hop, Base ++ pastry_state:leaves(PS), true}, St)
    end,
    St;

p2p(_From, {join_state, Hop, Nodes, Final}, St = #st{join = {collect, J}}) ->
    #{got := G, nodes := Ns, expected := E} = J,
    E2 = case Final of true -> Hop + 1; false -> E end,
    J2 = J#{got := G + 1, nodes := Nodes ++ Ns, expected := E2},
    case E2 =/= undefined andalso G + 1 >= E2 of
        true  -> finish_join(J2, St);
        false -> St#st{join = {collect, J2}}
    end;

p2p(_From, announce_ack, St = #st{join = {acks, N}}) ->
    case N - 1 of
        0 -> join_done(St);
        N2 -> St#st{join = {acks, N2}}
    end;

%% A newly joined node announces itself to every node in its state.
p2p(_From, {announce, X = {_, XPid, _}}, St) ->
    St2 = add_nodes([X], St),
    xmit(XPid, announce_ack, St2),
    St2;

%% ---- Routing --------------------------------------------------------
p2p(From, {route, Msg, Key, Hops, AckRef}, St) ->
    case AckRef of
        none -> ok;
        _ -> xmit(From, {hop_ack, AckRef}, St)
    end,
    do_route(Msg, Key, Hops, St);

p2p(_From, {hop_ack, Ref}, St = #st{pending = P}) ->
    case maps:take(Ref, P) of
        {{_, _, _, _, TRef}, P2} ->
            erlang:cancel_timer(TRef),
            St#st{pending = P2};
        error -> St
    end;

p2p(_From, {direct, Msg}, St = #st{app = App, app_st = AS}) ->
    St#st{app_st = App:deliver(Msg, undefined, #{direct => true}, AS)};
p2p(_From, {relay, To, Payload}, St) ->
    xmit(To, Payload, St),
    St;

%% ---- State repair (bonus) -------------------------------------------
p2p(From, leaf_req, St = #st{ps = PS}) ->
    xmit(From, {leaf_resp, [pastry_state:self_node(PS) | pastry_state:leaves(PS)]}, St),
    St;
p2p(_From, {leaf_resp, Nodes}, St) ->
    add_nodes(Nodes, St);
p2p(From, {rt_req, R, C}, St = #st{ps = PS}) ->
    Reply = case maps:find({R, C}, PS#pst.rt) of
                {ok, N} -> [N];
                error -> []
            end,
    xmit(From, {rt_resp, Reply}, St),
    St;
p2p(_From, {rt_resp, Nodes}, St) ->
    add_nodes(Nodes, St);
p2p(From, nbr_req, St = #st{ps = PS}) ->
    xmit(From, {nbr_resp, [pastry_state:self_node(PS) | pastry_state:neighbors(PS)]}, St),
    St;
p2p(_From, {nbr_resp, Nodes}, St) ->
    add_nodes(Nodes, St);

p2p(_From, _Unknown, St) ->
    St.

%% =====================================================================
%% Internals
%% =====================================================================

finish_join(#{nodes := Nodes}, St = #st{ps = PS0}) ->
    {PS, _} = pastry_state:add_all(Nodes, PS0),
    St2 = St#st{ps = PS},
    Self = pastry_state:self_node(PS),
    Targets = [P || {_, P, _} <- pastry_state:all_nodes(PS)],
    [xmit(P, {announce, Self}, St2) || P <- Targets],
    case Targets of
        [] -> join_done(St2);
        _  -> St2#st{join = {acks, length(Targets)}}
    end.

join_done(St = #st{ps = PS, main = Main}) ->
    {Id, _, _} = pastry_state:self_node(PS),
    Main ! {joined, self(), Id},
    leafs_changed(St#st{join = undefined}).

%% Core routing step. Hops = number of hops travelled so far.
do_route(_Msg, _Key, Hops, St) when Hops > ?MAX_HOPS ->
    counter(St, dropped),
    St;
do_route(Msg, Key, Hops, St = #st{ps = PS, app = App, app_st = AS}) ->
    case pastry_state:next_hop(Key, PS) of
        deliver ->
            St#st{app_st = App:deliver(Msg, Key, #{hops => Hops}, AS)};
        {forward, Next} ->
            case App:forward(Msg, Key, Next, AS) of
                {forward, Msg2, Next2, AS2} ->
                    forward_route(Next2, Msg2, Key, Hops, St#st{app_st = AS2});
                {stop, AS2} ->
                    St#st{app_st = AS2}
            end
    end.

forward_route({_, NPid, _}, Msg, Key, Hops, St = #st{reliable = false}) ->
    xmit(NPid, {route, Msg, Key, Hops + 1, none}, St),
    St;
forward_route(Next = {_, NPid, _}, Msg, Key, Hops, St = #st{pending = P}) ->
    Ref = make_ref(),
    TRef = erlang:send_after(?ACK_TIMEOUT, self(), {ack_timeout, Ref}),
    xmit(NPid, {route, Msg, Key, Hops + 1, Ref}, St),
    St#st{pending = P#{Ref => {Msg, Key, Hops, Next, TRef}}}.

%% Add nodes learned from peers (skipping recently failed ones).
add_nodes(Nodes, St = #st{ps = PS, suspects = Sus}) ->
    Now = now_ms(),
    Fresh = [N || N = {I, _, _} <- Nodes, not is_suspect(I, Sus, Now)],
    {PS2, Changed} = pastry_state:add_all(Fresh, PS),
    St2 = St#st{ps = PS2},
    case Changed of
        true  -> leafs_changed(St2);
        false -> St2
    end.

is_suspect(Id, Sus, Now) ->
    case maps:find(Id, Sus) of
        {ok, T} -> T > Now;
        error -> false
    end.

leafs_changed(St = #st{ps = PS, app = App, app_st = AS}) ->
    St2 = St#st{app_st = App:new_leafs(pastry_state:leaf_set(PS), AS)},
    ensure_monitors(St2).

%% Keep-alive on leaf set members: in Erlang a process monitor plays the
%% role of the paper's periodic keep-alive messages (crash detection).
ensure_monitors(St = #st{reliable = false}) -> St;
ensure_monitors(St = #st{ps = PS, monitors = M}) ->
    M2 = lists:foldl(fun({_, P, _}, Acc) ->
                             case maps:is_key(P, Acc) of
                                 true -> Acc;
                                 false -> Acc#{P => erlang:monitor(process, P)}
                             end
                     end, M, pastry_state:leaves(PS)),
    St#st{monitors = M2}.

%% A node was detected as failed (crash or unreachable link): remove it
%% and start repairing the affected structures (Section 2.4).
node_failed({Id, _, _}, St = #st{ps = PS}) ->
    {PS2, Info} = pastry_state:remove(Id, PS),
    St2 = St#st{ps = PS2, suspects = (St#st.suspects)#{Id => now_ms() + ?SUSPECT_TTL}},
    LeafHit = maps:get(smaller, Info) orelse maps:get(larger, Info),
    Affected = LeafHit orelse maps:get(rt, Info) =/= [] orelse maps:get(nbr, Info),
    case Affected of
        true -> counter(St2, repairs);
        false -> ok
    end,
    repair(Info, St2),
    case LeafHit of
        true -> leafs_changed(St2);
        false -> St2
    end.

repair(Info, St = #st{ps = PS}) ->
    Sm = pastry_state:smaller(PS), La = pastry_state:larger(PS),
    %% Leaf set: ask the farthest live leaf on the damaged side for its leaf set.
    LeafT = (case maps:get(smaller, Info) of true -> far(Sm, La); false -> [] end)
         ++ (case maps:get(larger, Info) of true -> far(La, Sm); false -> [] end),
    [xmit(P, leaf_req, St) || {_, P, _} <- lists:usort(LeafT)],
    %% Routing table entry R[l][d]: ask the other entries of row l (or l+1).
    lists:foreach(
      fun({R, C}) ->
              Row = case pastry_state:row(R, PS) of
                        [] -> pastry_state:row(R + 1, PS);
                        Rw -> Rw
                    end,
              [xmit(P, {rt_req, R, C}, St) || {_, P, _} <- Row]
      end, maps:get(rt, Info)),
    %% Neighborhood set: ask the remaining members for theirs.
    case maps:get(nbr, Info) of
        true -> [xmit(P, nbr_req, St) || {_, P, _} <- pastry_state:neighbors(PS)];
        false -> ok
    end,
    ok.

far([], []) -> [];
far([], Other) -> [lists:last(Other)];
far(Side, _) -> [lists:last(Side)].

%% Transmit a peer-to-peer message, honouring failed links.
xmit(Pid, Payload, #st{dead_links = DL}) ->
    case maps:is_key(Pid, DL) of
        true  -> ok;
        false -> Pid ! {p2p, self(), Payload}, ok
    end.

%% Direct application send: if the direct link to Pid has failed, relay
%% through a live leaf/neighbour whose link is up.
xmit_direct(Pid, Payload, St = #st{ps = PS, dead_links = DL}) ->
    case maps:is_key(Pid, DL) of
        false ->
            Pid ! {p2p, self(), Payload}, ok;
        true ->
            Live = [P || {_, P, _} <- pastry_state:all_nodes(PS),
                         P =/= Pid, not maps:is_key(P, DL)],
            case Live of
                [Relay | _] -> xmit(Relay, {relay, Pid, Payload}, St);
                [] -> ok
            end
    end.

counter(#st{reliable = true, stats = S}, Name) -> S ! {counter, Name}, ok;
counter(_, _) -> ok.

now_ms() -> erlang:monotonic_time(millisecond).
