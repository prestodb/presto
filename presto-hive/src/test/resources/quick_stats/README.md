## Test data for Quick Stats tests

### min_max_stats
To generate the `min_max_stats/min_max_fixture.parquet` file, use the python script -
```shell
uv init
uv add pyarrow
uv run min_max_fixture.py
```

The fixture has one row group with 100 rows and no nulls. Each column has either only `min_value`/`max_value`
set, or (for the `*_legacy` columns) only the deprecated `min`/`max` set. The script patches the footer, so the
readers can be tested against both sets of fields for several logical types.

Read from DuckDb here are the stats -
```sql

memory D select path_in_schema, "type", stats_min, stats_max, stats_min_value, stats_max_value  from parquet_metadata('./min_max_stats/min_max_fixture.parquet') ;
┌──────────────────┬─────────┬─────────────────────┬─────────────────────┬─────────────────────┬─────────────────────┐
│  path_in_schema  │  type   │      stats_min      │      stats_max      │   stats_min_value   │   stats_max_value   │
│     varchar      │ varchar │       varchar       │       varchar       │       varchar       │       varchar       │
├──────────────────┼─────────┼─────────────────────┼─────────────────────┼─────────────────────┼─────────────────────┤
│ i32              │ INT32   │ NULL                │ NULL                │ 1                   │ 100                 │
│ i64              │ INT64   │ NULL                │ NULL                │ 1000000000          │ 100000000000        │
│ d                │ INT32   │ NULL                │ NULL                │ 2022-01-09          │ 2022-04-18          │
│ dbl              │ DOUBLE  │ NULL                │ NULL                │ 1.5                 │ 100.5               │
│ flt              │ FLOAT   │ NULL                │ NULL                │ 1.25                │ 100.25              │
│ i8               │ INT32   │ NULL                │ NULL                │ 1                   │ 100                 │
│ i16              │ INT32   │ NULL                │ NULL                │ 100                 │ 10000               │
│ ts               │ INT64   │ NULL                │ NULL                │ 2022-01-09 10:30:00 │ 2022-01-09 12:09:00 │
│ dec32            │ INT32   │ NULL                │ NULL                │ 0.01                │ 1.00                │
│ dec64            │ INT64   │ NULL                │ NULL                │ 1000.000            │ 100000.000          │
│ u8               │ INT32   │ NULL                │ NULL                │ 1                   │ 100                 │
│ u32              │ INT32   │ NULL                │ NULL                │ 10000000            │ 1000000000          │
│ u64              │ INT64   │ NULL                │ NULL                │ 1000000000          │ 100000000000        │
│ i32_legacy       │ INT32   │ 1                   │ 100                 │ NULL                │ NULL                │
│ i64_legacy       │ INT64   │ 1000000000          │ 100000000000        │ NULL                │ NULL                │
│ d_legacy         │ INT32   │ 2022-01-09          │ 2022-04-18          │ NULL                │ NULL                │
│ dbl_legacy       │ DOUBLE  │ 1.5                 │ 100.5               │ NULL                │ NULL                │
│ flt_legacy       │ FLOAT   │ 1.25                │ 100.25              │ NULL                │ NULL                │
│ i8_legacy        │ INT32   │ 1                   │ 100                 │ NULL                │ NULL                │
│ i16_legacy       │ INT32   │ 100                 │ 10000               │ NULL                │ NULL                │
│ ts_legacy        │ INT64   │ 2022-01-09 10:30:00 │ 2022-01-09 12:09:00 │ NULL                │ NULL                │
│ u8_legacy        │ INT32   │ 1                   │ 100                 │ NULL                │ NULL                │
│ u32_legacy       │ INT32   │ 10000000            │ 1000000000          │ NULL                │ NULL                │
│ u64_legacy       │ INT64   │ 1000000000          │ 100000000000        │ NULL                │ NULL                │
│ dec32_legacy     │ INT32   │ 0.01                │ 1.00                │ NULL                │ NULL                │
│ dec64_legacy     │ INT64   │ 1000.000            │ 100000.000          │ NULL                │ NULL                │
│ u32_const_legacy │ INT32   │ 7                   │ 7                   │ NULL                │ NULL                │
└──────────────────┴─────────┴─────────────────────┴─────────────────────┴─────────────────────┴─────────────────────┘
  27 rows                                                                                                  6 columns

```
