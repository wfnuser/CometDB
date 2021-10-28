-module(comet_util).

-export([split_packet/2]).

split_packet(Size, P) when byte_size(P) >= Size ->
    {Chunk, Rest} = split_binary(P, Size),
    [Chunk | split_packet(Size, Rest)];

split_packet(_Size, <<>>) ->
    [];

split_packet(_Size, P)  ->
    [P].
