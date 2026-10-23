%% @doc Behaviour for applications running on top of Pastry
%% (the application-side API of Section 2.3 of the Pastry paper).
%%
%% Callbacks run inside the Pastry node actor, so an application can call
%% the Pastry API with the node's own pid: pastry_node:route(self(), Msg, Key)
%% and pastry_node:send(self(), Msg, TargetPid).
-module(pastry_app).

%% Called once when the node starts (part of pastryInit).
-callback init(Self :: {integer(), pid(), {float(), float()}}, Args :: term()) ->
    State :: term().

%% deliver(msg, key): Msg arrived at the node numerically closest to Key
%% (or was sent directly with send/3, in which case Key = undefined).
-callback deliver(Msg :: term(), Key :: integer() | undefined,
                  Meta :: map(), State :: term()) -> State :: term().

%% forward(msg, key, nextId): called before a message is forwarded. The
%% application may modify the message, change the next hop, or stop it.
-callback forward(Msg :: term(), Key :: integer(), Next :: tuple(), State :: term()) ->
    {forward, Msg :: term(), Next :: tuple(), State :: term()} | {stop, State :: term()}.

%% newLeafs(leafSet): the node's leaf set changed.
-callback new_leafs(LeafSet :: {list(), list()}, State :: term()) -> State :: term().

%% Non-Pastry messages delivered to the node (e.g. application timers).
-callback handle_info(Msg :: term(), State :: term()) -> State :: term().
