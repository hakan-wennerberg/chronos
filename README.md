# Chronos

A data access layer that is the result of some research into how to scale chronological data. Think Pinterest, time ordered streams of data across multiple shards. The concept is to take a truly tested database server (MySQL) and “dumb it down” to simple `put`, `get`, `delete` and `scan` requests. Ie. buckets with indexes.

## Globally unique identifiers

The GUID can be used for chronologic sorting and includes the full address of its contents, i.e. shard, bucket and entry.

## Application level sharding

To enable horizontal scaling, sharding is implemented at the application level instead of database level. Chronos supports 4096 shards (ie. databases) from the start, but can is configurable by editing the global ID compound.

## Users live on a specific shard

All data for a specific user lives on a specific shard.

## BLOB format versioning

Each stored entry is a BLOB. The data access layer do not care about the data its storing. It just knows the format version of the BLOB content as META-data. It is expected that consumers upgrade the format of the BLOB content whenever it retrieves an outdated version (think Riak).

## Multiple bucket types

### Timeline bucket

A bucket with chronologically sorted data (BLOBs). It has an index and is utilising the previously mentioned GUID.

### Key/value bucket

Simple key/value bucket. Does not use GUID.

### User bucket

Bucket for user data. It has an e-mail field unique constraint to it. Does not use GUID.

## Documentation

No, not really. Usage can be seen in tests :)

