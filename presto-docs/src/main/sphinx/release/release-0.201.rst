=============
Release 0.201
=============

General Changes
---------------

* Change grouped aggregations to use ``IS NOT DISTINCT FROM`` semantics rather than equality
  semantics. This fixes incorrect results and degraded performance when grouping on ``NaN``
  floating point values, and adds support for grouping on structural types that contain nulls.
* Fix planning error when column names are reused in ``ORDER BY`` query.
* System memory pool is now unused by default and it will eventually be removed completely.
  All memory allocations will now be served from the general/user memory pool. The old behavior
  can be restored with the ``deprecated.legacy-system-pool-enabled`` config option.
* Improve performance and memory usage for queries using :func:`row_number` followed by a
  filter on the row numbers generated.
* Improve performance and memory usage for queries using ``ORDER BY`` followed by a ``LIMIT``.
* Improve performance of queries that process structural types and contain joins, aggregations,
  or table writes.
* Add session property ``prefer-partial-aggregation`` to allow users to disable partial
  aggregations for queries that do not benefit.
* Add support for ``current_user`` (see :doc:`/functions/session`).

Security Changes
----------------

* Change rules in the :doc:`/security/built-in-system-access-control` for enforcing matches
  between authentication credentials and a chosen username to allow more fine-grained
  control and ability to define superuser-like credentials.

Hive Changes
------------

* Replace ORC writer stripe minimum row configuration ``hive.orc.writer.stripe-min-rows``
  with stripe minimum data size ``hive.orc.writer.stripe-min-size``.
* Change ORC writer validation configuration ``hive.orc.writer.validate`` to switch to a
  sampling percentage ``hive.orc.writer.validation-percentage``.
* Fix optimized ORC writer writing incorrect data of type `map` or `array`.
* Fix ``SHOW PARTITIONS`` and the ``$partitions`` table for tables that have null partition
  values.
* Fix impersonation for the simple HDFS authentication to use login user rather than current
  user.

SPI Changes
-----------

* Support resource group selection based on resource estimates.
