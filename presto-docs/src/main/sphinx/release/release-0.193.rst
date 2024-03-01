=============
Release 0.193
=============

General Changes
---------------

* Fix an infinite loop during planning for queries containing non-trivial predicates.
* Fix ``row_number()`` optimization that causes query failure or incorrect results
  for queries that constrain the result of ``row_number()`` to be less than one.
* Fix failure during query planning when lambda expressions are used in ``UNNEST`` or ``VALUES`` clauses.
* Fix ``Tried to free more revocable memory than is reserved`` error for queries that have spilling enabled
  and run in the reserved memory pool.
* Improve the performance of the :func:`ST_Contains` function.
* Add :func:`map_zip_with` lambda function.
* Add :func:`normal_cdf` function.
* Add ``SET_DIGEST`` type and related functions.
* Add query stat that tracks peak total memory.
* Improve performance of queries that filter all data from a table up-front (e.g., due to partition pruning).
* Turn on new local scheduling algorithm by default (see :doc:`release-0.181`).
* Remove the ``information_schema.__internal_partitions__`` table.

Security Changes
----------------

* Apply the authentication methods in the order they are listed in the
  ``http-server.authentication.type`` configuration.

CLI Changes
-----------

* Fix rendering of maps of Bing tiles.
* Abort the query when the result pager exits.

JDBC Driver Changes
-------------------

* Use SSL by default for port 443.

Hive Changes
------------

* Allow dropping any column in a table. Previously, dropping columns other
  than the last one would fail with ``ConcurrentModificationException``.
* Correctly write files for text format tables that use non-default delimiters.
  Previously, they were written with the default delimiter.
* Fix reading data from S3 if the data is in a region other than ``us-east-1``.
  Previously, such queries would fail with
  ``"The authorization header is malformed; the region 'us-east-1' is wrong; expecting '<region_name>'"``,
  where ``<region_name>`` is the S3 region hosting the bucket that is queried.
* Enable ``SHOW PARTITIONS FROM <table> WHERE <condition>`` to work for tables
  that have more than ``hive.max-partitions-per-scan`` partitions as long as
  the specified ``<condition>`` reduces the number of partitions to below this limit.

Blackhole Changes
-----------------

* Do not allow creating tables in a nonexistent schema.
* Add support for ``CREATE SCHEMA``.

Memory Connector Changes
------------------------

* Allow renaming tables across schemas. Previously, the target schema was ignored.
* Do not allow creating tables in a nonexistent schema.

MongoDB Changes
---------------

* Add ``INSERT`` support. It was previously removed in 0.155.
