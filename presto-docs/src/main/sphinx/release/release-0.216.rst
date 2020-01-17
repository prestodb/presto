=============
Release 0.216
=============

General Changes
---------------

* Fix correctness issue for :func:`array_intersect` and :func:`array_distinct` when input contains
  both zeros and nulls.
* Fix ``count(*)`` aggregation on empty relation when ``optimize_mixed_distinct_aggregation`` is enabled.
* Improve table scan performance for structural types.
* Improve performance for :func:`array_intersect`.
* Add :func:`reduce_agg` aggregate function.
* Add :func:`millisecond` function.
* Add an optimizer rule to filter the window partitions before executing the window operators.
* Remove ``ON`` keyword for :doc:`/sql/show-stats`.
* Restrict ``WHERE`` clause in :doc:`/sql/show-stats` to filters that can be pushed down to the connectors.
* Remove ``node_id`` column from ``system.runtime.queries`` table.
* Return final results to clients immediately for failed queries.

Web UI
------

* Fix rendering of live plan view for queries involving index joins.

Hive Connector Changes
----------------------

* Fix accounting of time spent reading Parquet data.
* Fix a corner case where the ORC writer fails with integer overflow when writing
  highly compressible data using dictionary encoding (:issue:`11930`).
* Fail queries reading Parquet files if statistics in those Parquet files are
  corrupt (e.g., min > max). To disable this behavior, set the configuration
  property ``hive.parquet.fail-on-corrupted-statistics``
  or session property ``parquet_fail_with_corrupted_statistics`` to false.
* Add support for :ref:`S3 select pushdown <s3selectpushdown>`, which enables pushing down
  projections and predicates into S3 for text files.

Kudu Connector Changes
----------------------

* Add ``number_of_replicas`` table property to ``SHOW CREATE TABLE`` output.

Cassandra Connector Changes
---------------------------

* Add ``cassandra.splits-per-node`` and ``cassandra.protocol-version`` configuration properties
  to allow connecting to Cassandra servers older than 2.1.5.

MySQL, PostgreSQL, Redshift, and SQL Server Changes
---------------------------------------------------

* Add support for predicate pushdown for columns of ``char(x)`` type.

Verifier Changes
----------------

* Add ``run-teardown-on-result-mismatch`` configuration property to facilitate debugging.
  When set to false, temporary tables will not be dropped after checksum failures.

SPI Changes
-----------

* Make ``ConnectorBucketNodeMap`` a top level class.
* Use list instead of map for bucket-to-node mapping.

.. note::

    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector that uses bucketing, you will need to
    update your code before deploying this release.
