=============
Release 0.163
=============

General Changes
---------------

* Fix data corruption when transporting dictionary-encoded data.
* Fix potential deadlock when resource groups are configured with memory limits.
* Improve performance for ``OUTER JOIN`` queries.
* Improve exchange performance by reading from buffers in parallel.
* Improve performance when only a subset of the columns resulting from a ``JOIN`` are referenced.
* Make ``ALL``, ``SOME`` and ``ANY`` non-reserved keywords.
* Add :func:`from_big_endian_64` function.
* Change :func:`xxhash64` return type from ``BIGINT`` to ``VARBINARY``.
* Change subscript operator for map types to fail if the key is not present in the map. The former
  behavior (returning ``NULL``) can be restored by setting the ``deprecated.legacy-map-subscript``
  config option.
* Improve ``EXPLAIN ANALYZE`` to render stats more accurately and to include input statistics.
* Improve tolerance to communication errors for long running queries. This can be adjusted
  with the ``query.remote-task.max-error-duration`` config option.

Accumulo Changes
----------------

* Fix issue that could cause incorrect results for large rows.

MongoDB Changes
---------------

* Fix NullPointerException when a field contains a null.

Cassandra Changes
-----------------

* Add support for ``VARBINARY``, ``TIMESTAMP`` and ``REAL`` data types.

Hive Changes
------------

* Fix issue that would prevent predicates from being pushed into Parquet reader.
* Fix Hive metastore user permissions caching when tables are dropped or renamed.
* Add experimental file based metastore which stores information in HDFS or S3 instead of a database.
