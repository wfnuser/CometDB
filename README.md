# CometDB
CometDB is an Erlang implementation of queue model over FoundationDB.

## About
This project aims to provide you a set of simple interfaces so that you can use FoundationDB as a Distributed, High Concurrency and Low Latency Queue.

## Features
* You can insert data in batch.
* You can fetch data in batch.
* You can delete data in range.
* You can insert data of any size; we overcome the size limitation of FDB.

## How to use
We need to setup the FDB environment first. You can find details in [FoundationDB official website](https://apple.github.io/foundationdb/getting-started-linux.html).

Then the use is really simple.
The basic API usages are as following:
```erlang

P = comet:start().
comet:insert(P, <<"testq">>, <<"testvalue">>).
comet:batch_insert(P, <<"testq">>, [<<"testvalue_2">>, <<"testvalue_3">>, <<"testvalue_4">>]).
comet:fetch(P, <<"testq">>, 100).
comet:delete(P, <<"testq">>, 1).

```

## TODO
* type checking
* stream
* cursors
* index
* priority queue
* connection management
* performance
* tests
...
