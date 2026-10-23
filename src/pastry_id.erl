%% 128-bit Pastry identifier arithmetic (base-16 digits, prefix length, ring distance).
-module(pastry_id).
-include("pastry.hrl").

-export([hash/1, digit/2, shl/2, cw/2, dist/2, to_hex/1, short/1]).

hash(Term) when is_list(Term) -> hash(list_to_binary(Term));
hash(Bin) when is_binary(Bin) ->
    <<I:128/unsigned>> = erlang:md5(Bin),
    I.

digit(Id, I) when I >= 0, I < ?DIGITS ->
    (Id bsr (?B * (?DIGITS - 1 - I))) band 15.

shl(A, B) -> shl(A, B, 0).

shl(_, _, ?DIGITS) -> ?DIGITS;
shl(A, B, I) ->
    case digit(A, I) =:= digit(B, I) of
        true  -> shl(A, B, I + 1);
        false -> I
    end.

cw(A, B) -> (B - A + ?RING) rem ?RING.

dist(A, B) ->
    D = cw(A, B),
    min(D, ?RING - D).

to_hex(Id) -> lists:flatten(io_lib:format("~32.16.0b", [Id])).

short(Id) -> lists:sublist(to_hex(Id), 8).
