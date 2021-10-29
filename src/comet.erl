-module(comet).
-behaviour(gen_server).

-export([start_link/0]).

-export([ start/0
        , start/1
        , stop/0
        , new_queue/1
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    start(<<>>).

start(ClusterFile) -> 
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ClusterFile], []).

stop() -> 
    gen_server:call(?MODULE, stop).

start_link() -> 
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

% FoundationDB can only be intiailized once
% in a given OS process.
init([ClusterFile]) -> 
    Db = erlfdb:open(ClusterFile),
    {ok, Db}.

new_queue(QueueName) ->
    gen_server:call(?MODULE, {new_queue, QueueName}).

insert(QueueName, Value) ->    
    gen_server:call(?MODULE, {insert, QueueName, Value}).

batch_insert(QueueName, Values) ->
    gen_server:call(?MODULE, {batch_insert, QueueName, Values}).

fetch_one(QueueName) ->
    gen_server:call(?MODULE, {fetch_one, QueueName}).

fetch(QueueName, N) ->
    gen_server:call(?MODULE, {fetch, QueueName, N}).

delete(QueueName, Key) ->
    gen_server:call(?MODULE, {delete, QueueName, Key}).

range_delete(QueueName, Start, End) ->
    gen_server:call(?MODULE, {range_delete, QueueName, Start, End}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call({new_queue, QueueName}, _From, Db) -> 
    {reply, comet_queue:new_queue(QueueName, Db), Db};

handle_call({insert, QueueName, Value}, _From, Db) -> 
    {reply, comet_queue:insert({Db, QueueName}, Value), Db};

handle_call({batch_insert, QueueName, Values}, _From, Db) -> 
    {reply, comet_queue:batch_insert({Db, QueueName}, Values), Db};

handle_call({fetch_one, QueueName}, _From, Db) -> 
    {reply, comet_queue:fetch({Db, QueueName}, 1), Db};

handle_call({fetch, QueueName, N}, _From, Db) -> 
    {reply, comet_queue:fetch({Db, QueueName}, N), Db};

handle_call({delete, QueueName, Key}, _From, Db) -> 
    {reply, comet_queue:delete({Db, QueueName}, Key), Db};

handle_call({range_delete, QueueName, Start, End}, _From, Db) -> 
    {reply, comet_queue:range_delete({Db, QueueName}, Start, End), Db}.

handle_cast(_Msg, Db) -> 
    {noreply, Db}.

handle_info(_Info, Db) -> 
    {noreply, Db}.

code_change(_OldVsn, Db, _Extra) -> 
    {ok, Db}.

terminate(_Reason, _Db) -> ok.
