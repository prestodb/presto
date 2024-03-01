=============
Release 0.239
=============

.. warning::

   There is a reliability issue in ZSTD compression that causes frequent excessive GC events in this release.
   classification_precision function returns wrong results, fixed in 0.239.2 release.

.. warning::
    There is a bug in LambdaDefinitionExpression canonicalization introduced since 0.238. For more details, go to :issue:`15424`.

**Highlights**
==============
* Add support for ``DEFINER`` and ``INVOKER`` view security models.
* Add support for caching Glue metastore in Hive connector.
* Add Pinot SQL endpoint support.

**Details**
==============

General Changes
_______________
* Fix incorrect results from :func:`classification_miss_rate`, :func:`classification_fall_out` (:pr:`14740`).
* Fix error in ``/v1/thread`` end point.
* Fix an issue where the property ``ignore_stats_calculator_failures`` would not be honored
  for certain queries.
* Fix missing query completion events for queries which fail prior to executing.
* Fix potential performance regression when setting ``use_legacy_scheduler`` is set to ``false``.
* Optimize queries with repeated expressions in filters or projections by computing the
  common expressions only once. This can be disabled by the session property
  ``optimize_common_sub_expressions``.
* Optimize queries containing only :func:`min` and :func:`max` on columns that can be
  evaluated using metadata (e.g., Hive partitions). This is controlled by configuration property
  ``optimizer.optimize-metadata-queries`` and session property ``optimize_metadata_queries``.
  Note: Enabling this optimization might change query result if there are metadata that refers to
  empty data, see :pr:`14845` for examples.
* Add aggregation function :func:`set_union`.
* Add local disk spilling support for queries with ``ORDER BY`` or ``DISTINCT``.
* Add new unified grouped execution configuration property ``grouped-execution-enabled`` and
  session property ``grouped_execution`` with default set to ``true``. The property
  ``grouped-execution-for-join-enabled`` will be removed in a future release (:pr:`14886`).
* Remove experimental feature to perform grouped execution for eligible table scans and its
  associated configuration property ``experimental.grouped-execution-for-eligible-table-scans``
  and session property ``grouped_execution_for_eligible_table_scans``.
* Enable ``dynamic-schedule-for-grouped-execution`` by default.  This property will be removed
  in a future release.
* Enable ``grouped-execution-for-aggregation-enabled`` by default. This property will be removed in
  a future release.
* Enable async page transport with non-blocking IO for exchange by default. This can be disabled by
  setting the configuration property ``exchange.async-page-transport-enabled`` to ``false``.


Security Changes
----------------
* Add support for ``DEFINER`` and ``INVOKER`` view security modes. While querying a view the former
  uses the permissions of the view owner and the latter uses the query runner's permissions.
  See :doc:`/sql/create-view`.

JDBC Changes
____________
* Implemented ``DatabaseMetaData#getClientInfoProperties`` API.

Web UI Changes
--------------
* Fix worker thread snapshot UI to correctly display the stack trace.

Hive Changes
____________
* Add support for caching the Glue metastore.
* Add support for warning on unfiltered partition keys. This can be enabled using the configuration
  property ``partition-keys-to-warn-on-no-filtering``.

Elasticsearch Changes
_____________________
* Add connector configuration ``elasticsearch.max-http-connections`` to control maximum number of
  persistent connections to Elasticsearch.
* Add connector configuration ``elasticsearch.http-thread-count`` to control the number of threads
  handling HTTP connections to Elasticsearch.
* Add support for numeric keyword.
* Add support for composite ``publish_address`` (:pr:`14811`).

Pinot Changes
_____________
* Add Pinot SQL endpoint support.
* Add support to pushdown ``DistinctLimitNode`` to Pinot Query in SQL mode.

SPI Changes
___________
* Move ``DistinctLimitNode`` to ``presto-spi`` module for connectors to push down.
