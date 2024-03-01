=============
Release 0.222
=============

.. warning::

    This release contains a regression which may cause Presto to incorrectly
    remove certain ``CAST`` operations.

General Changes
---------------

* Fix incorrect results when dividing certain decimal numbers (:pr:`12930`).
* Fix planning failure for certain join queries caused by invalid distribution
  type (:pr:`12354`).
* Add support for automatically retrying failed buckets when using grouped
  execution. Currently this is supported for certain ``INSERT`` queries using
  the Hive connector. This can be enabled with the
  ``recoverable_grouped_execution`` session property or the
  ``recoverable-grouped-execution-enabled`` configuration property (:issue:`12124`).
* Add support for grouped execution for queries with no joins or aggregations.
  This can be enabled with the ``grouped_execution_for_eligible_table_scans``
  session property or the
  ``experimental.grouped-execution-for-eligible-table-scans-enabled``
  configuration property (:pr:`12934`).
* Add configuration property ``max-concurrent-materializations`` and session
  property ``max_concurrent_materializations`` to limit the number of plan
  sections that will run concurrently when using materialized exchanges.
* Add support for computing :func:`approx_distinct` over BingTile values.
* Add :func:`merge_hll` to merge an array of HyperLogLogs.
* Add bitwise shift operations, :func:`bitwise_arithmetic_shift_right`,
  :func:`bitwise_logical_shift_right` and :func:`bitwise_shift_left`.

Web UI Changes
--------------

* Add completed and total lifespans to the Presto Coordinator UI.

Hive Connector Changes
----------------------

* Fix failures for ``information_schema`` queries when a table has an invalid storage format.
* Improve query execution time over bucketed table with large buckets.
* Add config property ``hive.metastore.glue.catalogid`` to configure the Glue catalog ID.

SPI Changes
-----------

* Add experimental interface ``ConnectorPlanOptimizer`` to allow connectors to
  participate in query plan optimization (e.g., filter pushdown) (:issue:`13102`).
* Rename ``LogicalRowExpressions::TRUE`` and ``LogicalRowExpressions::FALSE`` to
  ``LogicalRowExpressions::TRUE_CONSTANT`` and ``LogicalRowExpressions::FALSE_CONSTANT``
  respectively to avoid collision with ``java.lang.Boolean``.
