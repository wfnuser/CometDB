-module(comet).

-export([
    new_queue/2,
    insert/2,
    fetchOne/1,
    fetchN/2,
    delete/2,
    rangeDelete/3
]).

new_queue(ClusterFile, QueueName) -> 
    Db = erlfdb:open(ClusterFile),
    {Db, QueueName}.

insert(Queue, Value) ->
    case Queue of
        {Db, QueueName} ->
            erlfdb:transactional(Db, fun(Tx) ->
                LastIndex = comet_index:last_index(Tx, QueueName),
                K = erlfdb_tuple:pack({LastIndex+1}, QueueName),
                erlfdb:set(Db, K, Value)
            end)
    end.

fetchOne(Queue) -> fetchN(Queue, 1).
fetchN(Queue, N) ->
    case Queue of
        {Db, QueueName} -> 
            Opts = [
                {limit, N}
            ],
            KVs = erlfdb:get_range_startswith(Db, QueueName, Opts),
            lists:map(
                fun({K, V}) ->
                    {Index} = erlfdb_tuple:unpack(K, QueueName),
                    {Index, V}
                end,
                KVs
            )
    end.


delete(Queue, Key) ->
    case Queue of
        {Db, QueueName} -> 
            erlfdb:clear(Db, erlfdb_tuple:pack({Key}, QueueName))
    end.
rangeDelete(Queue, Start, End) ->
    case Queue of
        {Db, QueueName} -> 
            erlfdb:clear_range(Db, erlfdb_tuple:pack({Start}, QueueName), erlfdb_tuple:pack({End}, QueueName))
    end.





