%% Shared Pastry constants and records.
%%
%% Parameters follow Rowstron & Druschel, "Pastry: Scalable, decentralized
%% object location and routing for large-scale peer-to-peer systems".

-define(B, 4).                       % bits per digit (base 2^b = 16)
-define(DIGITS, 32).                 % 128-bit ids => 32 hex digits
-define(RING, (1 bsl 128)).          % size of the circular id space
-define(L_HALF, 8).                  % |L| = 16 -> 8 smaller + 8 larger
-define(M_SIZE, 16).                 % |M| neighborhood set size

%% A node reference is the tuple {Id, Pid, Pos} where Pos = {X, Y} is a
%% simulated network coordinate used as the proximity metric.

%% Pastry node state (routing table, leaf set, neighborhood set).
-record(pst, {
    self,            % {Id, Pid, Pos} of the local node
    smaller = [],    % leaf set, numerically smaller side (nearest first)
    larger  = [],    % leaf set, numerically larger side (nearest first)
    rt      = #{},   % routing table: {Row, Col} => NodeRef
    nbr     = []     % neighborhood set (closest by proximity first)
}).
