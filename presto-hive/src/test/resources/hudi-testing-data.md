## About hudi-testing-data.zip

The archive file `hudi-testing-data.zip` contains data files for 4 Hudi tables.

- `stock_ticks_cow`: a partitioned copy-on-write table
- `stock_ticks_cown`: a non-partitioned copy-on-write table
- `stock_ticks_mor`: a partitioned merge-on-read table
- `stock_ticks_morn`: a non-partitioned merge-on-read table
- `stock_ticks_morn_only_log`: a non-partitioned merge-on-read table with only log files

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

## About hudi-testing-data-1x.zip

The archive file `hudi-testing-data-1x.zip` contains Hudi 1.x data files for 5 tables, generated using
Hudi 1.1.0 with Spark 3.5. The tables mirror the 0.x tables above to verify Presto can read both versions.

- `stock_ticks_cow`: a partitioned copy-on-write table
- `stock_ticks_cown`: a non-partitioned copy-on-write table
- `stock_ticks_mor`: a partitioned merge-on-read table
- `stock_ticks_morn`: a non-partitioned merge-on-read table
- `stock_ticks_morn_only_log`: a non-partitioned merge-on-read table with only log files

The zip extracts under the subdirectory `hudi-data-1x/` inside the data directory.

In `HudiTestingDataGenerator`, each merge-on-read table creates two table entries into hive metastore
(one table name suffixed `_ro` and the other suffixed `_rt`), as hudi-hive-sync tool does.

The 1.x tables use a different schema than 0.x, reflecting the Hudi 1.x write path defaults:

| Name   | Type    |
|--------|---------|
| key    | varchar |
| symbol | varchar |
| ts     | varchar |
| dt     | varchar |
| hr     | varchar |
| volume | bigint  |
| open   | double  |
| close  | double  |

Each partitioned table has `dt` as the partition column (type `varchar`) with partition `dt=2018-08-31`.
For non-partitioned tables, `dt` is a regular data column.

**Important:** Reading Hudi 1.x partitioned tables requires the session property
`parquet_use_column_names=true` (connector config: `hive.parquet.use-column-names=true`) because
Hudi 1.x writes parquet files in internal Hudi schema order, which differs from the Hive metastore
column order. Without name-based resolution, columns will be read positionally and return wrong data.
