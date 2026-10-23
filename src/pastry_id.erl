%% @doc Pure helpers for 128-bit Pastry identifiers (base 2^b digits, b = 4).
-module(pastry_id).
-include("pastry.hrl").

-export([hash/1, digit/2, shl/2, cw/2, dist/2, to_hex/1, short/1]).

%% @doc Map a string/binary onto the 128-bit id space (MD5).
hash(Term) when is_list(Term) -> hash(list_to_binary(Term));
hash(Bin) when is_binary(Bin) ->
    <<I:128/unsigned>> = erlang:md5(Bin),
    I.

%% @doc The I-th base-16 digit of Id (0 = most significant).
digit(Id, I) when I >= 0, I < ?DIGITS ->
    (Id bsr (?B * (?DIGITS - 1 - I))) band 15.

%% @doc Length of the shared prefix (in digits) of A and B.
shl(A, B) -> shl(A, B, 0).

shl(_, _, ?DIGITS) -> ?DIGITS;
shl(A, B, I) ->
    case digit(A, I) =:= digit(B, I) of
        true  -> shl(A, B, I + 1);
        false -> I
    end.

%% @doc Clockwise distance going from A to B on the ring.
cw(A, B) -> (B - A + ?RING) rem ?RING.

%% @doc Circular numeric distance between A and B.
dist(A, B) ->
    D = cw(A, B),
    min(D, ?RING - D).

%% @doc Full 32-digit hex representation.
to_hex(Id) -> lists:flatten(io_lib:format("~32.16.0b", [Id])).

%% @doc First 8 hex digits (for logging).
short(Id) -> lists:sublist(to_hex(Id), 8).
