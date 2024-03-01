=============
Release 0.236
=============

General Changes
_______________
* Fix query failure for case expression with large number of cases.
* Add warning about inefficient use of ``UNION DISTINCT``.

JDBC Changes
____________
* Add support for ``PreparedStatement#getMetaData``.

Hive Changes
____________
* Fix ``ANALYZE`` query failure for tables with structure-typed columns. (:issue:`14494`).
* Fix Parquet schema mismatch when type is upgraded from ``int32`` to ``int64``.
* Improve planning time for queries that scan large number of partitions across multiple partition columns.
* Add local data caching support with `Alluxio <https://www.alluxio.io/>`_ cache library. This can be enabled by setting the configuration property ``cache.enabled`` to ``true`` and ``cache.type`` to ``ALLUXIO``.
* Improve Hive file caching by introducing Parquet metadata caching.

Verifier Changes
________________
* Add ``ErrorType`` of query failures to verification outputs.
* Add support for auto-resolving result mismatches for array, map, and row columns.
* Add support for auto-resolving result mismatches for queries that use certain functions. The list of functions can be configured by setting the configuration property ``ignored-functions.functions``.
