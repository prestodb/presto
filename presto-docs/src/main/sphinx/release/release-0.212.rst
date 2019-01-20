=============
Release 0.212
=============

General Changes
---------------

* Fix query failures when the :func:`ST_GeomFromBinary` function is run on multiple rows.
* Fix memory accounting for the build side of broadcast joins.
* Fix occasional query failures when running ``EXPLAIN ANALYZE``.
* Enhance :func:`ST_ConvexHull` and :func:`convex_hull_agg` functions to support geometry collections.
* Improve performance for some queries using ``DISTINCT``.
* Improve performance for some queries that perform filtered global aggregations.
* Remove ``round(x, d)`` and ``truncate(x, d)`` functions where ``d`` is a ``BIGINT`` (:issue:`x11462`).
* Add :func:`ST_LineString` function to form a ``LineString`` from an array of points.

Hive Connector Changes
----------------------

* Prevent ORC writer from writing stripes larger than the max configured size for some rare data
  patterns (:issue:`x11526`).
* Restrict the maximum line length for text files. The default limit of 100MB can be changed
  using the ``hive.text.max-line-length`` configuration property.
* Add sanity checks that fail queries if statistics read from the metastore are corrupt. Corrupt
  statistics can be ignored by setting the ``hive.ignore-corrupted-statistics``
  configuration property or the ``ignore_corrupted_statistics`` session property.

Thrift Connector Changes
------------------------

* Fix retry for network errors that occur while sending a Thrift request.
* Remove failed connections from connection pool.

Verifier Changes
----------------

* Record the query ID of the test query regardless of query outcome.
