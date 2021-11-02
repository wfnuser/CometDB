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
        , get_state/1
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

handle_cast(_Msg, Db) -> 
    {noreply, Db}.

handle_info(_Info, Db) -> 
    {noreply, Db}.

terminate(_Reason, _Db) -> ok.

% FoundationDB can only be intiailized once
% in a given OS process.
-spec init([comet_types:cluster_file()]) -> {ok, any()}.
init([ClusterFile]) -> 
    process_flag(trap_exit, true),
    Db = erlfdb:open(ClusterFile),
    {ok, Db}.

-spec insert(comet_types:comet_pid(), comet_types:comet_queue_name(), comet_types:comet_kv()) -> ok.
insert(Pid, QueueName, Value) ->    
    gen_server:call(Pid, {insert, get_state(Pid), QueueName, Value}).

-spec batch_insert(comet_types:comet_pid(), comet_types:comet_queue_name(), list(comet_types:comet_kv())) -> ok.
batch_insert(Pid, QueueName, Values) ->
    gen_server:call(Pid, {batch_insert, get_state(Pid), QueueName, Values}).

-spec fetch_one(comet_types:comet_pid(), comet_types:comet_queue_name()) -> list({integer, comet_types:comet_kv()}).
fetch_one(Pid, QueueName) ->
    gen_server:call(Pid, {fetch_one, get_state(Pid), QueueName}).

-spec fetch(comet_types:comet_pid(), comet_types:comet_queue_name(), integer()) -> list({integer, comet_types:comet_kv()}).
fetch(Pid, QueueName, N) ->
    gen_server:call(Pid, {fetch, get_state(Pid), QueueName, N}).

-spec delete(comet_types:comet_pid(), comet_types:comet_queue_name(), integer()) -> ok.
delete(Pid, QueueName, Key) ->
    gen_server:call(Pid, {delete, get_state(Pid), QueueName, Key}).

-spec range_delete(comet_types:comet_pid(), comet_types:comet_queue_name(), integer(), integer()) -> ok.
range_delete(Pid, QueueName, Start, End) ->
    gen_server:call(Pid, {range_delete, get_state(Pid), QueueName, Start, End}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call(get_state, _From, Db) -> 
    {reply, Db, Db};
handle_call({insert, Conn, QueueName, Value}, _From, Db) -> 
    {reply, comet_queue:insert({Conn, QueueName}, Value), Db};
handle_call({batch_insert, Conn, QueueName, Values}, _From, Db) -> 
    {reply, comet_queue:batch_insert({Conn, QueueName}, Values), Db};
handle_call({fetch_one, Conn, QueueName}, _From, Db) -> 
    {reply, comet_queue:fetch({Conn, QueueName}, 1), Db};
handle_call({fetch, Conn, QueueName, N}, _From, Db) -> 
    {reply, comet_queue:fetch({Conn, QueueName}, N), Db};
handle_call({delete, Conn, QueueName, Key}, _From, Db) -> 
    {reply, comet_queue:delete({Conn, QueueName}, Key), Db};
handle_call({range_delete, Conn, QueueName, Start, End}, _From, Db) -> 
    {reply, comet_queue:range_delete({Conn, QueueName}, Start, End), Db}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_state(Pid) ->
  gen_server:call(Pid, get_state).
