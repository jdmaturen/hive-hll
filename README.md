hive-hll
========

A Hive wrapper of [aggregateknowledge/java-hll](https://github.com/aggregateknowledge/java-hll) which is intended to be API compatible with [aggregateknowledge/postgresql-hll](https://github.com/aggregateknowledge/postgresql-hll) and [storage compatible](https://github.com/aggregateknowledge/hll-storage-spec) with the above and [aggregateknowledge/js-hll](https://github.com/aggregateknowledge/js-hll).


Installation
------------

Git clone and `mvn package`.  

```sql
add jar target/hive-hll-0.1-SNAPSHOT.jar;
create temporary function hll_add_agg as 'com.kresilas.hll.HyperLogLogUDAF';
create temporary function hll_cardinality as 'com.kresilas.hll.CardinalityUDF';
create temporary function hll_hash as 'com.kresilas.hll.HashUDF';
```


Usage
-----

 - `hll_hash(string)` takes a string and returns a Murmur3 hashed long. See [The Importance of Hashing](https://github.com/aggregateknowledge/java-hll#the-importance-of-hashing).
 - `hll_cardinality(hll)` takes a hex representation of an HLL and returns the cardinality. 
 - `hll_add_agg(long)` is an aggregate function over a long which returns the hex representation of an HLL.
 - `hll_union_agg(hll)` aggregate function which unions HLLs and returns a hex representation.
   
*Note:* As there are no custom types in Hive, HLL generating functions return the hex representation as a string.
   
Examples
--------

One `hll` per day over the `user_id` column: 

```sql
SELECT day, hll_add_agg(hll_hash(user_id)) AS users
FROM facts
GROUP BY day;
```

A count of the users seen over the last seven days: 
 
```sql
SELECT day, hll_cardinality(hll_union_agg(users) OVER last_7)
FROM daily_uniques
WINDOW last_7 as (ORDER BY date ASC ROWS 6 PRECEDING)
```


TODOs
-----

 - [ ] Tests (starting with first understanding how to correctly test UD{A,}Fs). 
 - [ ] Support additional input types for `hll_hash`.
