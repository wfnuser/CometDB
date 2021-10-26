-module(comet_queue).

-export([
    new_queue/2,
    insert/2,
    batch_insert/2,
    fetch/2,
    delete/2,
    range_delete/3
]).

% FDB_SIZE_LIMIT is 100,000 bytes
-define(FDB_SIZE_LIMIT, 10). 

% FoundationDB can only be intiailized once
% in a given OS process.
new_queue(ClusterFile, QueueName) -> 
    Db = erlfdb:open(ClusterFile),
    {Db, QueueName}.
insert_by_index(DbOrTx, QueueName, Value, Index) ->
    if
        byte_size(Value) > ?FDB_SIZE_LIMIT ->
            Chunks = comet_util:split_packet(?FDB_SIZE_LIMIT, Value),
            lists:foldl(
                fun(E, I) ->
                    K = erlfdb_tuple:pack({Index, I}, QueueName),
                    erlfdb:set(DbOrTx, K, E),
                    I+1
                end,
                1,
                Chunks
            );
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

fetch(Queue, N) ->
    case Queue of
        {Db, QueueName} -> 
            Opts = [
                {limit, N}
            ],
            KVs = erlfdb:get_range_startswith(Db, QueueName, Opts),
            % Unpack All KVs
            % Blob will be represent as series of {Index, I, V}; which I is the Ith Chunk of Indexth {Key, Value} pair
            Blobs = lists:map(
                fun({K, V}) ->
                    case erlfdb_tuple:unpack(K, QueueName) of 
                        {Index} -> {Index, V};
                        {Index, I} -> {Index, I, V}
                    end
                end,
                KVs
            ),
            % We should combine all the chunks of one blob
            lists:reverse(lists:foldl(
                fun(T, Res) -> 
                    case T of
                        {Index, V} -> [{Index, V} | Res];
                        {Index, 1, V} ->  [{Index, V} | Res];
                        {Index, _, V} -> 
                            case Res of
                                [{_, HV} | Remain] -> [{Index, <<HV/binary, V/binary>>} | Remain]
                            end
                    end
                end,
                [],
                Blobs
            ))
    end.


delete(Queue, Key) ->
    case Queue of
        {Db, QueueName} -> 
            erlfdb:clear(Db, erlfdb_tuple:pack({Key}, QueueName))
    end.
range_delete(Queue, Start, End) ->
    case Queue of
        {Db, QueueName} -> 
            erlfdb:clear_range(Db, erlfdb_tuple:pack({Start}, QueueName), erlfdb_tuple:pack({End}, QueueName))
    end.
