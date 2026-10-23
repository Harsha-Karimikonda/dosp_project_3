%% Application behaviour for services built on top of Pastry (Section 2.3 API).
-module(pastry_app).

-callback init(Self :: {integer(), pid(), {float(), float()}}, Args :: term()) ->
    State :: term().

-callback deliver(Msg :: term(), Key :: integer() | undefined,
                  Meta :: map(), State :: term()) -> State :: term().

-callback forward(Msg :: term(), Key :: integer(), Next :: tuple(), State :: term()) ->
    {forward, Msg :: term(), Next :: tuple(), State :: term()} | {stop, State :: term()}.

-callback new_leafs(LeafSet :: {list(), list()}, State :: term()) -> State :: term().

-callback handle_info(Msg :: term(), State :: term()) -> State :: term().
