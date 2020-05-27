=============
Release 0.236
=============

General Changes
_______________
* Fixed translation of searched case expressions to be flat.
* Improve experience by warning user of poorly performant UNION DISTINCT usage.

Verifier Changes
________________
* Add ``ErrorType`` of query failures to verification outputs.
* Add support to auto-resolve result mismatch for structured-type columns.
* Add support to auto-resolve result mismatch in case the query uses functions to be ignored.

Hive Changes
____________
* Fix  ANALYZE table_name failure for tables with map, list or struct columns (:issue:`14494`).
* Fix Parquet schema mismatch when type is upgraded (int32 -> int64).
* Improve planning time for queries that scan large number of partitions across multiple partition columns.
* Add local data caching support with `Alluxio <https://www.alluxio.io/>`_ cache library. To enable it, set config `cache.enabled=true` and `cache.type=ALLUXIO`.

JDBC Changes
____________
* Adds PreparedStatement support for getMetaData.
