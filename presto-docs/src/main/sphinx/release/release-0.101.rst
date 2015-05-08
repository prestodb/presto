=============
Release 0.101
=============

General Changes
---------------

* Add support for :doc:`/sql/create-table` (in addition to :doc:`/sql/create-table-as`).
* Add ``IF EXISTS`` support to :doc:`/sql/drop-table` and :doc:`/sql/drop-view`.
* Add :func:`array_agg` function.
* Add :func:`array_intersect` function.
* Add :func:`array_position` function.
* Add :func:`regexp_split` function.
* Add support for ``millisecond`` to :func:`date_diff` and :func:`date_add`.
* Fix excessive memory usage in :func:`map_agg`.
* Fix excessive memory usage in queries that perform partitioned top-N operations
  with :func:`row_number`.
* Optimize :ref:`array_type` comparison operators.
* Fix analysis of ``UNION`` queries for tables with hidden columns.
* Fix ``JOIN`` associativity to be left-associative instead of right-associative.
* Add ``source`` column to ``runtime.queries`` table in :doc:`/connector/system`.
* Add ``coordinator`` column to ``runtime.nodes`` table in :doc:`/connector/system`.
* Add ``errorCode``, ``errorName`` and ``errorType`` to ``error`` object in REST API
  (``errorCode`` previously existed but was always zero).
* Fix ``DatabaseMetaData.getIdentifierQuoteString()`` in JDBC driver.
* Handle thread interruption in JDBC driver ``ResultSet``.
* Add ``history`` command and support for running previous commands via ``!n`` to the CLI.
* Change Driver to make as much progress as possible before blocking.  This improves
  responsiveness of some limit queries.
* Add predicate push down support to JMX connector.
* Add support for unary ``PLUS`` operator.
* Improve scheduling speed by reducing lock contention.
* Extend optimizer to understand physical properties such as local grouping and sorting.
* Add support for streaming execution of window functions.
* Make ``UNION`` run partitioned, if underlying plan is partitioned.
* Add ``hash_partition_count`` session property to control hash partitions.

Web UI Changes
--------------

The main page of the web UI has been completely rewritten to use ReactJS. It also has
a number of new features, such as the ability to pause auto-refresh via the "Z" key and
also with a toggle in the UI.

Hive Changes
------------

* Add support for connecting to S3 using EC2 instance credentials.
  This feature is enabled by default. To disable it, set
  ``hive.s3.use-instance-credentials=false`` in your Hive catalog properties file.
* Treat ORC files as splittable.
* Change PrestoS3FileSystem to use lazy seeks, which improves ORC performance.
* Fix ORC ``DOUBLE`` statistic for columns containing ``NaN``.
* Lower the Hive metadata refresh interval from two minutes to one second.
* Invalidate Hive metadata cache for failed operations.
* Support ``s3a`` file system scheme.
* Fix discovery of splits to correctly backoff when the queue is full.
* Add support for non-canonical Parquet structs.
* Add support for accessing Parquet columns by name. By default, columns in Parquet
  files are accessed by their ordinal position in the Hive table definition. To access
  columns based on the names recorded in the Parquet file, set
  ``hive.parquet.use-column-names=true`` in your Hive catalog properties file.
* Add JMX stats to PrestoS3FileSystem.
* Add ``hive.recursive-directories`` config option to recursively scan
  partition directories for data.

SPI Changes
-----------

* Add connector callback for rollback of ``INSERT`` and ``CREATE TABLE AS``.
* Introduce an abstraction for representing physical organizations of a table
  and describing properties such as partitioning, grouping, predicate and columns.
  ``ConnectorPartition`` and related interfaces are deprecated and will be removed
  in a future version.
* Rename ``ConnectorColumnHandle`` to ``ColumnHandle``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
