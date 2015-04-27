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
* Fix analysis of ``UNION`` queries for tables with hidden columns.
* Fix ``JOIN`` associativity to be left-associative instead of right-associative.
* Add ``source`` column to ``runtime.queries`` table in :doc:`/connector/system`.
* Add ``coordinator`` column to ``runtime.nodes`` table in :doc:`/connector/system`.
* Add ``errorName`` and ``errorType`` to ``error`` object in REST API.
* Fix ``DatabaseMetaData.getIdentifierQuoteString()`` in JDBC driver.
* Handle thread interruption in JDBC driver ``ResultSet``.
* Add ``history`` command and support for running previous commands via ``!n`` to the CLI.
* Change Driver to make as much progress as possible before blocking.  This improves
  responsiveness of some limit queries.
* Add connector callback for rollback of ``INSERT`` and ``CREATE TABLE AS``.
* Add predicate push down support to JMX connector.
* Add support for unary ``PLUS`` operator.
* Improve scheduling speed by reducing lock contention.

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
* Add support for accessing Parquet columns by name. By default, columns in Parquet
  files are accessed by their ordinal position in the Hive table definition. To access
  columns based on the names recorded in the Parquet file, set
  ``hive.parquet.use-column-names=true`` in your Hive catalog properties file.
* Add JMX stats to PrestoS3FileSystem.
* Add ``hive.recursive-directories`` config option to recursively scan
  partition directories for data.
