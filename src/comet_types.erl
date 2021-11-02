-module(comet_types).

-export_type([ cluster_file/0
             , comet_queue_name/0
             , comet_kv/0
             , comet_pid/0
             ]).

-type cluster_file() :: bitstring().
-type comet_queue_name() :: bitstring().
-type comet_kv() :: bitstring().
-type comet_pid() :: pid().
