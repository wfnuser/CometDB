-module(comet).
-behaviour(gen_server).

-export([start_link/0]).

-export([start/0
        , stop/0
        , new_queue/2
        , insert/2
        , batch_insert/2
        , fetch_one/1
        , fetch/2
        , delete/2
        , range_delete/3
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
]).

-define(SERVER, comet).

start() -> 
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() -> 
    gen_server:call(?MODULE, stop).

start_link() -> 
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) -> 
    {ok, ets:new(?MODULE, [])}.

new_queue(ClusterFile, QueueName) ->
    gen_server:call(?MODULE, {new_queue, ClusterFile, QueueName}).

insert(Queue, Value) ->    
    gen_server:call(?MODULE, {insert, Queue, Value}).

batch_insert(Queue, Values) ->
    gen_server:call(?MODULE, {batch_insert, Queue, Values}).

fetch_one(Queue) ->
    gen_server:call(?MODULE, {fetch_one, Queue}).

fetch(Queue, N) ->
    gen_server:call(?MODULE, {fetch, Queue, N}).

delete(Queue, Key) ->
    gen_server:call(?MODULE, {delete, Queue, Key}).

range_delete(Queue, Start, End) ->
    gen_server:call(?MODULE, {range_delete, Queue, Start, End}).

handle_call({new_queue, ClusterFile, QueueName}, _From, State) -> 
    {reply, comet_queue:new_queue(ClusterFile, QueueName), State};

handle_call({insert, Queue, Value}, _From, State) -> 
    {reply, comet_queue:insert(Queue, Value), State};

handle_call({batch_insert, Queue, Values}, _From, State) -> 
    {reply, comet_queue:batch_insert(Queue, Values), State};

handle_call({fetch_one, Queue}, _From, State) -> 
    {reply, comet_queue:fetch(Queue, 1), State};

handle_call({fetch, Queue, N}, _From, State) -> 
    {reply, comet_queue:fetch(Queue, N), State};

handle_call({delete, Queue, Key}, _From, State) -> 
    {reply, comet_queue:delete(Queue, Key), State};

handle_call({range_delete, Queue, Start, End}, _From, State) -> 
    {reply, comet_queue:range_delete(Queue, Start, End), State}.

handle_cast(_Msg, State) -> 
    {noreply, State}.

handle_info(_Info, State) -> 
    {noreply, State}.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

terminate(_Reason, _State) -> ok.
