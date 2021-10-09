-module(comet).

-export([
    new_queue/2,
    insert/2,
    fetchN/2,
    fetchOne/1,
    rangeDelete/3
]).

new_queue(ClusterFile, QueueName) -> 
    Db = erlfdb:open(ClusterFile),
    {Db, QueueName}.

insert(Queue, Value) ->
    case Queue of
        {Db, QueueName} ->
            Key = erlfdb_tuple:pack({uuid:uuid1()},QueueName),
            erlfdb:set(Db, Key, Value)
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
                    {UUID} = erlfdb_tuple:unpack(K, QueueName),
                    {UUID, V}
                end,
                KVs
            )
    end.

rangeDelete(Queue, Start, End) ->
    case Queue of
        {Db, QueueName} -> 
            erlfdb:clear_range(Db, erlfdb_tuple:pack({Start}, QueueName), erlfdb_tuple:pack({End}, QueueName))
    end.    







