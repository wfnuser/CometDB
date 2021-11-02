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
    Db = get_state(Pid),
    comet_queue:insert({Db, QueueName}, Value).

-spec batch_insert(comet_types:comet_pid(), comet_types:comet_queue_name(), list(comet_types:comet_kv())) -> ok.
batch_insert(Pid, QueueName, Values) ->
    Db = get_state(Pid),
    comet_queue:batch_insert({Db, QueueName}, Values).

-spec fetch_one(comet_types:comet_pid(), comet_types:comet_queue_name()) -> list({integer, comet_types:comet_kv()}).
fetch_one(Pid, QueueName) ->
    Db = get_state(Pid),
    comet_queue:fetch_one({Db, QueueName}).

-spec fetch(comet_types:comet_pid(), comet_types:comet_queue_name(), integer()) -> list({integer, comet_types:comet_kv()}).
fetch(Pid, QueueName, N) ->
    Db = get_state(Pid),
    comet_queue:fetch({Db, QueueName}, N).

-spec delete(comet_types:comet_pid(), comet_types:comet_queue_name(), integer()) -> ok.
delete(Pid, QueueName, Key) ->
    Db = get_state(Pid),
    comet_queue:delete({Db, QueueName}, Key).

-spec range_delete(comet_types:comet_pid(), comet_types:comet_queue_name(), integer(), integer()) -> ok.
range_delete(Pid, QueueName, Start, End) ->
    Db = get_state(Pid),
    comet_queue:range_delete({Db, QueueName}, Start, End).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call(get_state, _From, Db) -> 
    {reply, Db, Db}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_state(Pid) ->
  gen_server:call(Pid, get_state).
