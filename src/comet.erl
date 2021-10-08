-module(comet).

-export([
    new_queue/2,
    enqueue/2,
    dequeue/1
]).

timestamp() ->
    {M, S, _} = os:timestamp(),
    M * 1000000 + S.

new_queue(ClusterFile, QueueName) -> 
    Db = erlfdb:open(ClusterFile),
    {Db, QueueName}.

enqueue(Queue, Value) ->
    case Queue of
        {Db, QueueName} ->
            Key = erlfdb_tuple:pack({timestamp()},QueueName),
            erlfdb:set(Db, Key, Value)
    end.
        
dequeue(Queue) ->
    case Queue of
        {Db, QueueName} -> 
            Opts = [
                % {snapshot, true},
                % {reverse, true},
                % {streaming_mode, exact},
                {limit, 1}
            ],
            KVs = erlfdb:get_range_startswith(Db, QueueName, Opts),
            lists:map(
                fun({K, V}) ->
                    io:format("key: ~p value: ~p ~n", [K, V])
                end,
                KVs
            )
    end.







