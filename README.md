# Antidote Go Client Library

## How to install

Get the library using Go:

```
go get github.com/AntidoteDB/antidote-go-client
```

Then import the library in your code:

```
import (
    antidote "github.com/AntidoteDB/antidote-go-client"
)
```

## How to use

To connect to a running Antidote instance, you have to create a client using ``client, err := antidote.NewClient(...)```.
The function takes one or more host definitions consisting of a host name and the protocol buffer port.
To connect to an Antidote instance running on the same machine with default port, you pass `Host{"127.0.0.1", 8087}` to the `NewClient` function.
Do not forget to defer the close method `defer client.Close()`.

The client manages a connection pool and picks a random connection to a random host whenever a connection is required.
Operations are executed on the data store using a `Bucket` object.

```
bucket := antidote.Bucket{[]byte("test")}
```

The bucket name is given as byte-slice.
A bucket object includes functions for reading and updating persistent objects in the data store.
These functions take as parameter a transaction, which can either be an `InteractiveTransaction` or a `StaticTransaction`.
Interactive transactions all to combine multiple read- and update-operations into an atomic transaction.
Updates issued in the context of an interactive transaction are visible to read operations issued in the same context after the updates.
Interactive transactions have to be committed in order to make the updates visible to subsequent transactions.

```
tx, err := client.StartTransaction()
if err != nil {
    ... // handle error
}
... //use transaction
err = tx.Commit()
```

Static transactions can be seen as one-shot transactions for executing a set of updates or a read operation.
Static transactions do not have to be committed or closed and are mainly handled by the Antidote server.

```
tx := client.CreateStaticTransaction()
```

The `Bucket.Update(...)` function takes, in addition to a transaction, a list of CRDT updates.
These update objects are created using the following functions:

- SetAdd(key Key, elems ...[]byte)
- SetRemove(key Key, elems ...[]byte)
- CounterInc(key Key, inc int64)
- RegPut(key Key, value []byte)
- MVRegPut(key Key, value []byte)
- MapUpdate(key Key, updates ...*CRDTUpdate)

The first 5 updates are straight forward updates of sets, counters, registers and multi-value registers.
The map update is more complex in that it takes update of the keys inside the map as parameter.
To update the key `key1` in map `map1` referring to a counter, the following update is created:

```
antidote.MapUpdate([]byte("map1"),
    antidote.CounterInc([]byte("key1"), 1)
)
```

These updates are executed in the context of a transaction using the `Update` function of the `Bucket`.
