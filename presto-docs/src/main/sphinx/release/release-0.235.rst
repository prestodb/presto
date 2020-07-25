=============
Release 0.235
=============

General Changes
_______________
* Fix named queries where its output does not match its column list (:issue:`14333`).
* Fix issue where queries with large filters in the where clause would cause excessive memory usage.  Now such queries will fail quickly with ``GENERATED_BYTECODE_TOO_LARGE``.
* Improve performance of ``UNNEST``.
* Improve ``CREATE FUNCTION`` to allow parameter type list and return type to have a length up to 30k characters.
* Add optimization for filters and projections to extract common subexpressions and evaluate them only once.  This optimization can be turned off by the session property ``optimize_common_sub_expressions``.
* Add support for ``SHOW CREATE FUNCTION``.
* Add soft memory limit configuration properties. Soft memory limits are default memory limits given to each query that can be overridden using session properties up to the hard limit set by the existing configuration properties. Available soft memory limit configuration properties are ``query.soft-max-memory-per-node``, ``query.soft-max-total-memory-per-node``, ``query.soft-max-total-memory``, and ``query.soft-max-memory``.
* Add check to disallow invoking SQL functions in SQL function body.
* Add support for limiting the total number of buffers per optimized repartitioning operator. The limit can be set by the configuration property ``driver.max-page-partitioning-buffer-count``.
* Add peak task total memory to query stats.
* Add :func:`scale_qdigest` function to scale a ``qdigest`` to a new weight.
* Add :func:`myanmar_font_encoding` and :func:`myanmar_normalize_unicode` to support working with Burmese text
* Add support for :func:`ST_AsText` to accept Spherical Geographies.
* Add support for :func:`ST_Centroid` to accept Spherical Geography Points and MultiPoints.

Pinot Connector Changes
_______________________
* Add support for mapping Pinot ``BYTES`` data type to Presto ``VARBINARY`` type.
* Add support for mapping Pinot time fields with days since epoch value to Presto ``DATE`` type via the system property ``pinot.infer-date-type-in-schema``.
* Add support for mapping Pinot time fields with milliseconds since epoch value to Presto ``TIMESTAMP`` type via the system prpoerty ``pinot.infer-timestamp-type-in-schema``.
* Add Pinot Field type in to column comment field shown as ``DIMENSION``, ``METRIC``, ``TIME``, ``DATETIME``, to provide more information.
* Add support for pushing down distinct count query to Pinot on a best-effort basis.
* Add support for new Pinot Routing Table APIs.

Hive Connector Changes
______________________
* Fix a bug in Hive split calculation which affects Parquet reader in few corner cases.
* Fix ZSTD compression issue with zero row file for missing buckets.
* Fix AWS client metric reporting when using S3 select.
* Add AWS client retry pause time metrics to ``PrestoS3FileSystemStats``.
* Add table property ``preferred_ordering_columns`` to support writing sorted files for unbucketed table.
* Add native Parquet Writer for Presto.
* Add support for impersonation access by using HMS delegation token.
* Add support for multi-HMS instances load balancing and breakdown metrics by HMS hosts.
* Add support in file status cache to cache all tables. This could be enabled by setting the configuration property ``hive.file-status-cache-tables`` to ``*``.
* Add configuration property ``hive.orc-compression-codec`` to override ``hive.compression-codec`` for ORC and DWRF formats. If specified, ORC and DWRF files are compressed using this codec. RC, Parquet, and other files are compressed using hive.compression-codec.

Druid Connector Changes
_______________________
* Fix druid connector segment scan.
* Fix an issue where distinct is not respected in count aggregation.
* Add support for query processing pushdown via the ``druid.compute-pushdown-enabled`` configuration property.

Verifier Changes
________________
* Fix an issue where resubmitted queries always fail.
* Add support for verifying ``SELECT`` queries that produce columns of ``TIME``, ``TIMESTAMP WITH TIME ZONE``, or ``DECIMAL`` types, or columns of structured types with those types.
* Add support for specifying table properties override for temporary Verifier tables, through configuration property ``control.table-properties`` and ``test.table-properties``.
* Add support to output verification results for failures due to Verifier internal errors.
* Add support to skip teardown queries in case control and test queries succeeds but verification fails. This can be enabled by configuration property ``smart-teardown``, which replaces ``run-teardown-on-result-mismatch``.
