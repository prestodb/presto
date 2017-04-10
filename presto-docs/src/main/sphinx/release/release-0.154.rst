=============
Release 0.154
=============

General Changes
---------------

* Fix planning issue that could cause ``JOIN`` queries involving functions
  that return null on non-null input to produce incorrect results.
* Fix regression that would cause certain queries involving uncorrelated
  subqueries in ``IN`` predicates to fail during planning.
* Fix potential *"Input symbols do not match output symbols"*
  error when writing to bucketed tables.
* Fix potential *"Requested array size exceeds VM limit"* error
  that triggers the JVM's ``OutOfMemoryError`` handling.
* Improve performance of window functions with identical partitioning and
  ordering but different frame specifications.
* Add ``code-cache-collection-threshold`` config which controls when Presto
  will attempt to force collection of the JVM code cache and reduce the
  default threshold to ``40%``.
* Add support for using ``LIKE`` with :doc:`/sql/create-table`.
* Add support for ``DESCRIBE INPUT`` to describe the requirements for
  the input parameters to a prepared statement.

Hive Changes
------------

* Fix handling of metastore cache TTL. With the introduction of the
  per-transaction cache, the cache timeout was reset after each access,
  which means cache entries might never expire.
