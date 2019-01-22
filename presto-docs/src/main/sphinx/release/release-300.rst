===========
Release 300
===========

General Changes
---------------

* Fix :func:`array_intersect` and :func:`array_distinct`
  skipping zeros when input also contains nulls.
* Fix ``count(*)`` aggregation returning null on empty relation
  when ``optimize_mixed_distinct_aggregation`` is enabled.
* Improve table scan performance for structured types.
* Improve performance for :func:`array_intersect`.
* Improve performance of window functions by filtering partitions early.
* Add :func:`reduce_agg` aggregate function.
* Add :func:`millisecond` function.
* Remove ``ON`` keyword from :doc:`/sql/show-stats` (use ``FOR`` instead).
* Restrict ``WHERE`` clause in :doc:`/sql/show-stats`
  to filters that can be pushed down to connectors.
* Return final results to clients immediately for failed queries.

JMX MBean Naming Changes
------------------------

* The base domain name for server MBeans is now ``presto``. The old names can be
  used by setting the configuration property ``jmx.base-name`` to ``com.facebook.presto``.
* The base domain name for the Hive, Raptor, and Thrift connectors is ``presto.plugin``.
  The old names can be used by setting the catalog configuration property
  ``jmx.base-name`` to ``com.facebook.presto.hive``, ``com.facebook.presto.raptor``,
  or ``com.facebook.presto.thrift``, respectively.

Web UI
------

* Fix rendering of live plan view for queries involving index joins.

JDBC Driver Changes
-------------------

* Change driver class name to ``io.prestosql.jdbc.PrestoDriver``.

System Connector Changes
------------------------

* Remove ``node_id`` column from ``system.runtime.queries`` table.

Hive Connector Changes
----------------------

* Fix accounting of time spent reading Parquet data.
* Fix corner case where the ORC writer fails with integer overflow when writing
  highly compressible data using dictionary encoding (:issue:`x11930`).
* Fail queries reading Parquet files if statistics in those files are corrupt
  (e.g., min > max). To disable this behavior, set the configuration
  property ``hive.parquet.fail-on-corrupted-statistics``
  or session property ``parquet_fail_with_corrupted_statistics`` to false.
* Add support for :ref:`s3selectpushdown`, which enables pushing down
  column selection and range filters into S3 for text files.

Kudu Connector Changes
----------------------

* Add ``number_of_replicas`` table property to ``SHOW CREATE TABLE`` output.

Cassandra Connector Changes
---------------------------

* Add ``cassandra.splits-per-node`` and ``cassandra.protocol-version`` configuration
  properties to allow connecting to Cassandra servers older than 2.1.5.

MySQL Connector Changes
-----------------------

* Add support for predicate pushdown for columns of ``char(x)`` type.

PostgreSQL Connector Changes
----------------------------

* Add support for predicate pushdown for columns of ``char(x)`` type.

Redshift Connector Changes
---------------------------

* Add support for predicate pushdown for columns of ``char(x)`` type.

SQL Server Connector Changes
----------------------------

* Add support for predicate pushdown for columns of ``char(x)`` type.

Raptor Legacy Connector Changes
-------------------------------

* Change name of connector to ``raptor-legacy``.

Verifier Changes
----------------

* Add ``run-teardown-on-result-mismatch`` configuration property to facilitate debugging.
  When set to false, temporary tables will not be dropped after checksum failures.

SPI Changes
-----------

* Change base package to ``io.prestosql.spi``.
* Move connector related classes to package ``io.prestosql.spi.connector``.
* Make ``ConnectorBucketNodeMap`` a top level class.
* Use list instead of map for bucket-to-node mapping.

.. note::

    These are backwards incompatible changes with the previous SPI.
    If you have written a plugin, you will need to update your code
    before deploying this release.
