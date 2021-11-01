-module(comet_types).

-export_type([ cluster_file/0
             , comet_queue_name/0
             , fdb_kv/0
             ]).

-type cluster_file() :: bitstring().
-type comet_queue_name() :: bitstring().
-type fdb_kv() :: bitstring().