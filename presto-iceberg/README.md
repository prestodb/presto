This plugin allows presto to interact with [iceberg][iceberg]  tables.

[iceberg]: https://github.com/Netflix/iceberg

## Status

Currently this plugin supports create, CTAS, drop and reading from iceberg table. Support for other DDL operation will be added soon.


## How to configure

The plugin extends from hive-plugin so it needs metastore configs and s3 configs if you use s3. In addition the current implementation relies
on [HiveTables][HiveTables]  implementation which relies on `metastore.thrift.uris` and `hive.metastore.warehouse.dir` values from the hive-site.xml. 

You can look at the sample configuration under `presto-main/etc/catalog/iceberg.properties`

[HiveTables]: https://github.com/Netflix/iceberg/tree/master/hive/src/main/java/com/netflix/iceberg/hive

### How to create an iceberg table
Just like a hive table, the only difference is you will specify the iceberg catalog instead of a hive catalog.

## Unpartitioned Tables
``` sql
create table iceberg.testdb.sample (
    i int, 
    s varchar
);
```
## Partitioned Tables
Currently we only support identity partitions so there is no difference in hive vs iceberg syntax. Just like unpartitioned table you must specify iceberg catalog to create iceberg tables.

``` sql
create table iceberg.testdb.sample_partitioned (
    b boolean,
    dateint integer,
    l bigint,
    f real,
    d double,
    de decimal(12,2),
    dt date,
    ts timestamp,
    s varchar,
    bi varbinary
 )
WITH (partitioned_by = ARRAY['dateint', 's']);
```