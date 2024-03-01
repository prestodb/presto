=============
Release 0.237
=============

**Highlights**
==============

* Add JSON format for ``EXPLAIN`` with type ``LOGICAL`` and ``DISTRIBUTED``.
* Add functions :func:`array_sum`, :func:`array_average`, :func:`map_normalize`, :func:`set_agg` and :func:`flatten_geometry_collections`.
* Support authentication for Druid connector.
* Support for AWS IAM authorization to Elasticsearch connector.
* Improve Elasticsearch query capabilities.
* Improve Elasticsearch connector performance.
* Improve Coordinator RPC performance.

**Notes**
=========

General Changes
_______________
* Fix NPE in common sub-expression optimization triggered by ``CASE-WHEN`` expression.
* Fix compiler failure with function type when common sub-expression optimization is enabled.
* Improve coordinator RPC performance by removing unused entries from ``TaskStatus``.
* Add JSON format for ``EXPLAIN`` with type ``LOGICAL`` and ``DISTRIBUTED``.
* Add functions :func:`array_sum`, :func:`array_average`, :func:`map_normalize` and :func:`set_agg`.
* Add session property ``warning_handling`` to control how warnings are handled. The options are ``SUPPRESS``, ``NORMAL`` and ``AS_ERROR``. The default value is ``NORMAL``.
* Add support for defining SQL-invoked functions in plugins.
* Add support to control which worker can receive tasks by implementing the ``NodeStatusService`` interface. See :pr:`14535`.
* Add warning to use :func:`approx_distinct` when using ``COUNT(DISTINCT x)``.
* Add support for listing functions whose names match a specified pattern using the ``SHOW FUNCTION LIKE`` syntax.
* Add :func:`flatten_geometry_collections` function to recursively flatten GeometryCollections.

Cassandra Connector Changes
___________________________
* Fix missing Netty library introduced in version 0.229 that causes the Cassandra connector to fail to load and queries to fail.

Druid Connector Changes
_______________________
* Add support for Druid's basic and kerberos authentication.

Elasticsearch Connector Changes
_______________________________
* Fix predicate pushdown for Elasticsearch.
* Improve Elasticsearch (save an extra hop) by using shard primary host in Elasticsearch connector.
* Add Elasticsearch array support using definitions in the _meta field.
* Add support for AWS IAM authorization to Elasticsearch connector.
* Add support for Elasticsearch query string syntax.
* Add support for nested types in Elasticsearch.
* Add support for querying Elasticsearch aliases.
* Add system.nodes table to Elasticsearch.
* Add handling for mixed-case columns in Elasticsearch.
* Add support to load tables dynamically in Elasticsearch.
* Add support to refresh Elasticsearch nodes periodically.

Hive Connector Changes
______________________
* Add functionality to specify cache quota with respect to a scope. A scope could be at global, schema, table, or partition level. Cache quota prevents queries scanning too much
  data to disrupt cache locality. Such queries can only use the cache within their own scopes. Cache quota now only works with ``FILE_MERGE`` cache. Turn it on with config
  ``cache.cache-quota-scope`` and ``cache.default-cache-quota``.

Verifier Changes
________________
* Fix an internal error when session properties of a control or a test query contains ``query_max_execution_time``.
* Fix auto-resolution of checksum query failure due to query complexity.
* Add control query IDs, test query IDs, and peak total memory to verification outputs.
* Add support to resubmit verification for ``CLUSTER_OUT_OF_MEMORY`` and ``ADMINISTRATIVELY_PREEMPTED`` errors.
* Add support to retry ``DESCRIBE`` queries failed with ``TIME_LIMIT_EXCEEDED``.

Resource Groups Changes
_______________________
* Fix existing resource group to handle transition of leaf resource group to internal and vice versa.