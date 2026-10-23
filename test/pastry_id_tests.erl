-module(pastry_id_tests).
-include_lib("eunit/include/eunit.hrl").

-define(R, (1 bsl 128)).

digit_test() ->
    Id = 16#0123456789abcdef0123456789abcdef,
    ?assertEqual(0, pastry_id:digit(Id, 0)),
    ?assertEqual(1, pastry_id:digit(Id, 1)),
    ?assertEqual(15, pastry_id:digit(Id, 15)),
    ?assertEqual(15, pastry_id:digit(Id, 31)).

shl_test() ->
    A = 16#abcd0000000000000000000000000000,
    B = 16#abce0000000000000000000000000000,
    ?assertEqual(3, pastry_id:shl(A, B)),
    ?assertEqual(32, pastry_id:shl(A, A)),
    ?assertEqual(0, pastry_id:shl(0, ?R - 1)).

dist_wraps_test() ->
    ?assertEqual(2, pastry_id:dist(?R - 1, 1)),
    ?assertEqual(2, pastry_id:dist(1, ?R - 1)),
    ?assertEqual(5, pastry_id:dist(10, 15)),
    ?assertEqual(?R - 1, pastry_id:cw(1, 0)).

hash_range_test() ->
    H = pastry_id:hash("node-1"),
    ?assert(H >= 0 andalso H < ?R),
    ?assertEqual(32, length(pastry_id:to_hex(H))).
