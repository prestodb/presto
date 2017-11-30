=============
Release 0.182
=============

General Changes
---------------

* Fix correctness issue that causes :func:`corr` to return positive numbers for inverse correlations.
* Fix the :doc:`/sql/explain` query plan for tables that are partitioned
  on ``TIMESTAMP`` or ``DATE`` columns.
* Fix query failure when when using certain window functions that take arrays or maps as arguments (e.g., :func:`approx_percentile`).
* Implement subtraction for all ``TIME`` and ``TIMESTAMP`` types.
* Improve planning performance for queries that join multiple tables with
  a large number columns.
* Improve the performance of joins with only non-equality conditions by using
  a nested loops join instead of a hash join.
* Improve the performance of casting from ``JSON`` to ``ARRAY`` or ``MAP`` types.
* Add a new :ref:`ipaddress_type` type to represent IP addresses.
* Add :func:`to_milliseconds` function to convert intervals (day to second) to milliseconds.
* Add support for column aliases in ``CREATE TABLE AS`` statements.
* Add a config option to reject queries during cluster initialization.
  Queries are rejected if the active worker count is less than the
  ``query-manager.initialization-required-workers`` property while the
  coordinator has been running for less than ``query-manager.initialization-timeout``.
* Add :doc:`/connector/tpcds`. This connector provides a set of schemas to
  support the TPC Benchmark™ DS (TPC-DS).

CLI Changes
-----------

* Fix an issue that would sometimes prevent queries from being cancelled when exiting from the pager.

Hive Changes
------------

* Fix reading decimal values in the optimized Parquet reader when they are backed
  by the ``int32`` or ``int64`` types.
* Add a new experimental ORC writer implementation optimized for Presto.
  We have some upcoming improvements, so we recommend waiting a few releases before
  using this in production. The new writer can be enabled with the
  ``hive.orc.optimized-writer.enabled`` configuration property or with the
  ``orc_optimized_writer_enabled`` session property.
