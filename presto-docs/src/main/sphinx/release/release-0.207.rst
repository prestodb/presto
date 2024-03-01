=============
Release 0.207
=============

General Changes
---------------

* Fix a planning issue for queries where correlated references were used in ``VALUES``.
* Remove support for legacy ``JOIN ... USING`` behavior.
* Change behavior for unnesting an array of ``row`` type to produce multiple columns.
* Deprecate the ``reorder_joins`` session property and the ``reorder-joins``
  configuration property. They are replaced by the ``join_reordering_strategy``
  session property and the ``optimizer.join-reordering-strategy`` configuration
  property. ``NONE`` maintains the order of the joins as written and is equivalent
  to ``reorder_joins=false``. ``ELIMINATE_CROSS_JOINS`` will eliminate any
  unnecessary cross joins from the plan and is equivalent to ``reorder_joins=true``.
  ``AUTOMATIC`` will use the new cost-based optimizer to select the best join order.
  To simplify migration, setting the ``reorder_joins`` session property overrides the
  new session and configuration properties.
* Deprecate the ``distributed_joins`` session property and the
  ``distributed-joins-enabled`` configuration property. They are replaced by the
  ``join_distribution_type`` session property and the ``join-distribution-type``
  configuration property. ``PARTITIONED`` turns on hash partitioned joins and
  is equivalent to ``distributed_joins-enabled=true``. ``BROADCAST`` changes the
  join strategy to broadcast and is equivalent to ``distributed_joins-enabled=false``.
  ``AUTOMATIC`` will use the new cost-based optimizer to select the best join
  strategy. If no statistics are available, ``AUTOMATIC`` is the same as
  ``REPARTITIONED``. To simplify migration, setting the ``distributed_joins``
  session property overrides the new session and configuration properties.
* Add support for column properties.
* Add ``optimizer.max-reordered-joins`` configuration property to set the maximum number of joins that
  can be reordered at once using cost-based join reordering.
* Add support for ``char`` type to :func:`approx_distinct`.

Security Changes
----------------

* Fail on startup when configuration for file based system access control is invalid.
* Add support for securing communication between cluster nodes with Kerberos authentication.

Web UI Changes
--------------

* Add peak total (user + system) memory to query details UI.

Hive Connector Changes
----------------------

* Fix handling of ``VARCHAR(length)`` type in the optimized Parquet reader. Previously, predicate pushdown
  failed with ``Mismatched Domain types: varchar(length) vs varchar``.
* Fail on startup when configuration for file based access control is invalid.
* Add support for HDFS wire encryption.
* Allow ORC files to have struct columns with missing fields. This allows the table schema to be changed
  without rewriting the ORC files.
* Change collector for columns statistics to only consider a sample of partitions. The sample size can be
  changed by setting the ``hive.partition-statistics-sample-size`` property.

Memory Connector Changes
------------------------

* Add support for dropping schemas.

SPI Changes
-----------

* Remove deprecated table/view-level access control methods.
* Change predicate in constraint for accessing table layout to be optional.
* Change schema name in ``ConnectorMetadata`` to be optional rather than nullable.
