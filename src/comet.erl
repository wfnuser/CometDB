-module(comet).
-behaviour(gen_server).

-export([start_link/0]).

%% API:
-export([ start/0
        , start/1
        , stop/0
        , insert/3
        , batch_insert/3
        , fetch_one/2
        , fetch/3
        , delete/3
        , range_delete/4
        ]).

%% gen_server callbacks:
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-define(SERVER, comet).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    start(<<>>).

start(ClusterFile) -> 
    gen_server:start_link(?MODULE, [ClusterFile], []).

stop() -> 
    gen_server:call(?MODULE, stop).

start_link() -> 
    gen_server:start_link(?MODULE, [], []).

% FoundationDB can only be intiailized once
% in a given OS process.
-spec init([comet_types:cluster_file()]) -> {ok, any()}.
init([ClusterFile]) -> 
    process_flag(trap_exit, true),
    Db = erlfdb:open(ClusterFile),
    {ok, Db}.

-spec insert(pid(), comet_types:comet_queue_name(), comet_types:comet_kv()) -> ok.
insert(Pid, QueueName, Value) ->    
    gen_server:call(Pid, {insert, QueueName, Value}).

-spec batch_insert(pid(), comet_types:comet_queue_name(), list(comet_types:comet_kv())) -> ok.
batch_insert(Pid, QueueName, Values) ->
    gen_server:call(Pid, {batch_insert, QueueName, Values}).

-spec fetch_one(pid(), comet_types:comet_queue_name()) -> list({integer, comet_types:comet_kv()}).
fetch_one(Pid, QueueName) ->
    gen_server:call(Pid, {fetch_one, QueueName}).

-spec fetch(pid(), comet_types:comet_queue_name(), integer()) -> list({integer, comet_types:comet_kv()}).
fetch(Pid, QueueName, N) ->
    gen_server:call(Pid, {fetch, QueueName, N}).

-spec delete(pid(), comet_types:comet_queue_name(), integer()) -> ok.
delete(Pid, QueueName, Key) ->
    gen_server:call(Pid, {delete, QueueName, Key}).

-spec range_delete(pid(), comet_types:comet_queue_name(), integer(), integer()) -> ok.
range_delete(Pid, QueueName, Start, End) ->
    gen_server:call(Pid, {range_delete, QueueName, Start, End}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

terminate(_Reason, _Db) -> ok.
