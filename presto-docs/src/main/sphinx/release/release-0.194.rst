=============
Release 0.194
=============

General Changes
---------------

* Fix planning performance regression that can affect queries over Hive tables
  with many partitions.
* Fix deadlock in memory management logic introduced in the previous release.
* Add :func:`word_stem` function.
* Restrict ``n`` (number of result elements) to 10,000 or less for
  ``min(col, n)``, ``max(col, n)``, ``min_by(col1, col2, n)``, and ``max_by(col1, col2, n)``.
* Improve error message when a session property references an invalid catalog.
* Reduce memory usage of :func:`histogram` aggregation function.
* Improve coordinator CPU efficiency when discovering splits.
* Include minimum and maximum values for columns in ``SHOW STATS``.

Web UI Changes
--------------

* Fix previously empty peak memory display in the query details page.

CLI Changes
-----------

* Fix regression in CLI that makes it always print "query aborted by user" when
  the result is displayed with a pager, even if the query completes successfully.
* Return a non-zero exit status when an error occurs.
* Add ``--client-info`` option for specifying client info.
* Add ``--ignore-errors`` option to continue processing in batch mode when an error occurs.

JDBC Driver Changes
-------------------

* Allow configuring connection network timeout with ``setNetworkTimeout()``.
* Allow setting client tags via the ``ClientTags`` client info property.
* Expose update type via ``getUpdateType()`` on ``PrestoStatement``.

Hive Changes
------------

* Consistently fail queries that attempt to read partitions that are offline.
  Previously, the query can have one of the following outcomes: fail as expected,
  skip those partitions and finish successfully, or hang indefinitely.
* Allow setting username used to access Hive metastore via the ``hive.metastore.username`` config property.
* Add ``hive_storage_format`` and ``respect_table_format`` session properties, corresponding to
  the ``hive.storage-format`` and ``hive.respect-table-format`` config properties.
* Reduce ORC file reader memory consumption by allocating buffers lazily.
  Buffers are only allocated for columns that are actually accessed.

Cassandra Changes
-----------------

* Fix failure when querying ``information_schema.columns`` when there is no equality predicate on ``table_name``.
