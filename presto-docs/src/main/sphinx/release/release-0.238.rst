=============
Release 0.238
=============

.. warning::

   There is a bug in this release that will cause certain queries with lambda expressions to fail.
   There is a reliability issue in ZSTD compression that causes frequent excessive GC events in this release.

.. warning::
    There is a bug in LambdaDefinitionExpression canonicalization introduced in this release. For more details, go to :issue:`15424`.

**Highlights**
==============
* Fix SQL function parameters to be case-insensitive.
* Add support to create external function. (:issue:`13254`)
* Add support for exchange materialization of table bucketed by non-hive types. This can be enabled by setting the ``bucket_function_type_for_exchange`` session property or ``hive.bucket-function-type-for-exchange `` configuration property to ``PRESTO_NATIVE``.
* Add predicate pushdown support for ``DATE``, ``TIMESTAMP``, and ``TIMESTAMP_WITH_TIME_ZONE`` literals for Pinot connector.

General Changes
_______________
* Fix SQL function parameters to be case-insensitive.
* Add ``target_result_size`` session property to customize data batch sizes being streamed from coordinator.
* Add optimization to push null filters to the INNER side of equijoins. The optimization can be enabled with ``optimize-nulls-in-joins``.
* Add ``sessionProperty`` override to Presto JDBC URI.
* Add support to create external function. (:issue:`13254`)
* Add session property ``query_max_broadcast_memory`` to limit the memory a query can use for broadcast join.
* Remove max buffer count configuration property ``driver.max-page-partitioning-buffer-count`` for optimized repartitioning.

Geospatial Changes
__________________
* Fix error in :func:`geometry_invalid_reason`.
* Fix integer overflow in certain cases with Bing Tiles.

Hive Changes
____________
* Fix a bug in Parquet reader which manifests when there are nested column schema changes.
* Add support for exchange materialization of table bucketed by non-hive types. This can be enabled by setting the ``bucket_function_type_for_exchange`` session property or ``hive.bucket-function-type-for-exchange `` configuration property to ``PRESTO_NATIVE``.

Pinot Changes
_____________
* Add support to retry data fetch exception. This can be enabled by setting the configuration property ``pinot.mark-data-fetch-exceptions-as-retriable``.
* Add predicate pushdown support for ``DATE``, ``TIMESTAMP``, and ``TIMESTAMP_WITH_TIME_ZONE`` literals.

Verifier Changes
________________
* Fix an issue where session properties of control and test queries also affects checksum queries.
* Add support to report peak task memory usage for control and test queries.
