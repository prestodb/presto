=============
Release 0.232
=============

.. warning::

   There is a bug in this release that will cause queries with the predicate ``IS NULL`` on
   bucketed columns to produce incorrect results.

General Changes
_______________
* Fix an issue where ``DATE_TRUNC`` may produce incorrect results at certain timestamp in ``America/Sao_Paulo``.
* Improve built-in function resolution performance by caching function resolution results.
* Add a MySQL-based function namespace manager implementation that supports creating, altering, and dropping SQL functions (:doc:`/admin/function-namespace-managers`).
* Add support for retrying failed stages from a materialized point instead of failing the entire query. The number of retries allowed can be configured using the configuration property ``max-stage-retries`` and session property ``max_stage_retries``. The default value is zero. To take advantage of this feature, exchange_materialization_strategy must be set to ``ALL``.
* Add configuration property ``use-legacy-scheduler`` and session property ``use_legacy_scheduler`` to use a version of the query scheduler before refactorings to enable full stage retries. The default value is false. This is a temporary property to provide an easy way to roll back in case of bugs in the new scheduler. This property will be removed in a couple releases once we have confidence in the stability of the new scheduler.
* Add ``query_max_total_memory_per_node`` and ``query_max_memory_per_node`` session properties.
* Add support to show whether functions have variable arity in ``SHOW FUNCTIONS``.
* Add support to show whether functions are built-in in ``SHOW FUNCTIONS``.
* Add ``use_exact_partitioning`` session property that forces repartitioning if repartitioning is possible.
* Add support for ``ALTER FUNCTION``.
* Add configuration property ``resource-groups.reload-refresh-interval`` to control the frequency of reloading resource group information from the database. The default value is 10 seconds.
* Add support for using the Thrift protocol to shuffle data. This can be configured using the configuration property ``internal-communication.task-communication-protocol``. Possible values are HTTP or Thrift.
* Add support for using the Thrift protocol to announce node state. This can be configured using the configuration property ``internal-communication.server-info-communication-protocol``. Possible values are HTTP or Thrift.
* Add session property ``list_built_in_functions_only`` to support hiding user-defined SQL functions in ``SHOW FUNCTIONS``.
* Add experimental functions ``tdigest_agg``, ``merge(tdigest)``, ``value_at_quantile(tdigest, quantile)``, ``values_at_quantiles(tdigest, quantiles)``, ``quantile_at_value(tdigest, quantile)``, ``quantiles_at_values(tdigest, quantile)`` for creating, merging, and querying t-digests.
  These can be enabled by using the session property ``experimental_functions_enabled`` and the configuration property ``experimental-functions-enabled``.

Hive Changes
____________
* Fix an issue where queries could fail with buffer overflow when writing ORC files.
* Add support for handling statistics to the Alluxio metastore.
* Add Alluxio metastore which connects to the Alluxio catalog service <https://docs.alluxio.io/os/user/2.1/en/core-services/Catalog.html>.
* Add session property ``shuffle_partitioned_columns_for_table_write`` to make Presto shuffle data on the partition columns before writing to partitioned unbucketed Hive tables.
  This increases the maximum number of partitions that can be written in a single query by a factor of the total number of writing workers.
  The property is ``false`` by default. (:pr:`14010`).
* Expose Hive table properties via system table$properties table.
* Change error code from ``HIVE_METASTORE_ERROR`` to ``HIVE_TABLE_DROPPED_DURING_QUERY`` when a ``DROP TABLE`` query fails due to another query dropping the table before this query has finished.
* Upgrade Alluxio version from 2.1.1 to 2.1.2.

Kudu Changes
_____________
* Add ``Kerberos`` authentication.

Kafka Changes
_____________
* Update ``Kafka`` connector to 2.3.1, which improves implementation and performance (:pr:`13709`).

Pinot Changes
_____________
* Replace config ``pinot.prefer-broker-queries`` with the inverse config ``pinot.forbid-broker-queries``.

Verifier Changes
________________
* Add specific validation checks for the individual fields when validating a row column.

SPI Changes
___________
* Replace ``IsHidden`` attribute on AggregationFunction and ScalarFunction with ``visibility`` which can be of the following values ``PUBLIC``, ``EXPERIMENTAL``, ``HIDDEN``.
