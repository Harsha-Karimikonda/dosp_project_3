%% Shared Pastry constants and state record (b = 4, 128-bit IDs, |L| = 16, |M| = 16).

-define(B, 4).
-define(DIGITS, 32).
-define(RING, (1 bsl 128)).
-define(L_HALF, 8).
-define(M_SIZE, 16).

-record(pst, {
    self,            % {Id, Pid, Pos}
    smaller = [],    % numerically smaller leaf set (nearest first)
    larger  = [],    % numerically larger leaf set (nearest first)
    rt      = #{},   % routing table: {Row, Col} => {Id, Pid, Pos}
    nbr     = []     % neighborhood set (closest by proximity first)
}).
