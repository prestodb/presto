=============
Release 0.200
=============

General Changes
---------------

* Disable early termination of inner or right joins when the right side
  has zero rows. This optimization can cause indefinite query hangs
  for queries that join against a small number of rows.
  This regression was introduced in 0.199.
* Fix query execution failure for :func:`bing_tile_coordinates`.
* Remove the ``log()`` function. The arguments to the function were in the
  wrong order according to the SQL standard, resulting in incorrect results
  when queries were translated to or from other SQL implementations. The
  equivalent to ``log(x, b)`` is ``ln(x) / ln(b)``. The function can be
  restored with the ``deprecated.legacy-log-function`` config option.
* Allow including a comment when adding a column to a table with ``ALTER TABLE``.
* Add :func:`from_ieee754_32` and :func:`from_ieee754_64` functions.
* Add :func:`ST_GeometryType` geospatial function.

Hive Changes
------------

* Fix reading min/max statistics for columns of ``REAL`` type in partitioned tables.
* Fix failure when reading Parquet files with optimized Parquet reader
  related with the predicate push down for structural types.
  Predicates on structural types are now ignored for Parquet files.
* Fix failure when reading ORC files that contain UTF-8 Bloom filter streams.
  Such Bloom filters are now ignored.

MySQL Changes
-------------

* Avoid reading extra rows from MySQL at query completion.
  This typically affects queries with a ``LIMIT`` clause.
