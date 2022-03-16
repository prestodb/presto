## About hudi-testing-data.zip

The archive file `hudi-testing-data.zip` contains data files for 4 Hudi tables.

- `stock_ticks_cow`: a partitioned copy-on-write table
- `stock_ticks_cow`: a non-partitioned copy-on-write table,
- `stock_ticks_mor`: a partitioned merge-on-read table
- `stock_ticks_morn`: a non-partitioned merge-on-read table;

In `HudiTestingDataGenerator`, each merge-on-read table creates two table entries into hive metastore
(one table name suffixed `_ro` and the other suffixed `_rt`), as hudi-hive-sync tool does.

The table data is generated following the doc at https://hudi.apache.org/docs/docker_demo/

All the tables have the same data columns:

| Name   | Type    |
|--------|---------|
| volume | bigint  |
| ts     | varchar |
| symbol | varchar |
| year   | integer |
| month  | varchar |
| high   | double  |
| low    | double  |
| key    | varchar |
| date   | varchar |
| close  | double  |
| open   | double  |
| day    | varchar |

Each partitioned table has a partition column, named `dt`, of type `varchar`,
and has partition named `dt=2018-08-31` at the subdirectory `2018/08/31`.
