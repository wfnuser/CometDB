-module(comet).

-export([
    new_queue/2,
    insert/2,
    batch_insert/2,
    fetchOne/1,
    fetchN/2,
    delete/2,
    rangeDelete/3
]).

% FDB_SIZE_LIMIT is 100,000 bytes
-define(FDB_SIZE_LIMIT, 100000). 

new_queue(ClusterFile, QueueName) -> 
    Db = erlfdb:open(ClusterFile),
    {Db, QueueName}.

insert_by_index(DbOrTx, QueueName, Value, Index) ->
    if
        byte_size(Value) > ?FDB_SIZE_LIMIT ->
            Chunks = split_binary(Value, ?FDB_SIZE_LIMIT),
            lists:foldl(
                fun(E, I) ->
                    K = erlfdb_tuple:pack({Index, I}, QueueName),
                    erlfdb:set(DbOrTx, K, E),
                    I+1
                end,
                1,
                Chunks
            ),
            K = erlfdb_tuple:pack({erlfdb:decode(<<"chunked">>), Index}, QueueName),
            erlfdb:set(DbOrTx, K, true);
        true -> 
            K = erlfdb_tuple:pack({Index}, QueueName),
            erlfdb:set(DbOrTx, K, Value)
    end.

insert(Queue, Value) ->
    case Queue of
        {Db, QueueName} ->
            erlfdb:transactional(Db, fun(Tx) ->
                LastIndex = comet_index:last_index(Tx, QueueName),
                insert_by_index(Tx, QueueName, Value, LastIndex+1)
            end)
    end.
batch_insert(Queue, Values) when is_list(Values) ->
    case Queue of
        {Db, QueueName} ->
            erlfdb:transactional(Db, fun(Tx) ->
                LastIndex = comet_index:last_index(Tx, QueueName),
                lists:foldl(
                    fun(Value, I) ->
                        insert_by_index(Tx, QueueName, Value, LastIndex+I),
                        I+1
                    end,
                    1,
                    Values
                )
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





