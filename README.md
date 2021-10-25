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
```
Q = new_queue(<<>>, <<"testq">>). # create a queue entity; the first parameter is the cluster file of foundationdb.
insert(Q, <<"testvalue_1">>). # the system will always generate an sequence number for items we insert in each time
batch_insert(Q, [<<"testvalue_2">>, <<"testvalue_3">>, <<"testvalue_4">>]).
fetch(Q, 5).

delete(Q, <<"xxxx_as_the_id_which_will_return_by_insert_or_fetch">>).
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
