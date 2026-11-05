# presto-hudi-common

Connector-independent Hudi helpers for pruning files with metadata table (MDT)
column statistics. The module depends on the Hudi bundle and Presto types only.

- `HudiColumnStatsReader` reads per-file column statistics as Presto domains.
  `LatestHudiColumnStatsReader` reads the latest MDT state; it opens and closes
  one MDT reader per batch, validates column names and schema types, and ignores
  deleted, duplicate or unusable entries.
- `HudiColumnStatsConverter` converts boolean and integral statistics to
  conservative domains, handling null counts, missing bounds, overflow and
  inconsistent statistics. Other types produce an unconstrained domain.
- `HudiStatisticsEvaluator` intersects file domains with the query
  `TupleDomain`. Missing fields and type mismatches retain the file.
- `HudiColumnStatsPruner` limits lookups to 128 files and 32 predicate columns
  and returns the files that may match.

The Hive connector uses these classes for COW Parquet tables when
`hive.hudi-mdt-data-skipping-enabled=true`; see the Hive connector
documentation.

Statistics are not bound to the file-listing snapshot. A snapshot-bound reader
can be added behind `HudiColumnStatsReader` without worker or split changes.
