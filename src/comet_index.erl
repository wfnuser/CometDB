-module(comet_index).

-export([
    last_index/2
]).

last_index(DbOrTx, QueueName) ->
    Options = [ {snapshot, true}
              , {limit, 1}
              , {reverse, true}
              ],
    case erlfdb:get_range_startswith(DbOrTx, QueueName, Options) of
        [] -> -1;
        [{K, _}] -> 
            case erlfdb_tuple:unpack(K, QueueName) of
                {Index} -> Index;
                {Index, _} -> Index
            end
    end.
