=============
Release 0.209
=============

General Changes
---------------

* Fix incorrect predicate pushdown when grouping sets contain the empty grouping set (:issue:`11296`).
* Fix ``X-Forwarded-Proto`` header handling for requests to the ``/`` path (:issue:`11168`).
* Fix a regression that results in execution failure when at least one
  of the arguments to :func:`min_by` or :func:`max_by` is a constant ``NULL``.
* Fix failure when some buckets are completely filtered out during bucket-by-bucket execution.
* Fix execution failure of queries due to a planning deficiency involving
  complex nested joins where a join that is not eligible for bucket-by-bucket
  execution feeds into the build side of a join that is eligible.
* Improve numerical stability for :func:`corr`, :func:`covar_samp`,
  :func:`regr_intercept`, and :func:`regr_slope`.
* Do not include column aliases when checking column access permissions.
* Eliminate unnecessary data redistribution for scalar correlated subqueries.
* Remove table scan original constraint information from ``EXPLAIN`` output.
* Introduce distinct error codes for global and per-node memory limit errors.
* Include statistics and cost estimates for ``EXPLAIN (TYPE DISTRIBUTED)`` and ``EXPLAIN ANALYZE``.
* Support equality checks for ``ARRAY``, ``MAP``, and ``ROW`` values containing nulls.
* Improve statistics estimation and fix potential negative nulls fraction
  estimates for expressions that include ``NOT`` or ``OR``.
* Completely remove the ``SHOW PARTITIONS`` statement.
* Add :func:`bing_tiles_around` variant that takes a radius.
* Add the :func:`convex_hull_agg` and :func:`geometry_union_agg` geospatial aggregation functions.
* Add ``(TYPE IO, FORMAT JSON)`` option for :doc:`/sql/explain` that shows
  input tables with constraints and the output table in JSON format.
* Add :doc:`/connector/kudu`.
* Raise required Java version to 8u151. This avoids correctness issues for
  map to map cast when running under some earlier JVM versions, including 8u92.

Web UI Changes
--------------

* Fix the kill query button on the live plan and stage performance pages.

CLI Changes
-----------

* Prevent spurious *"No route to host"* errors on macOS when using IPv6.

JDBC Driver Changes
-------------------

* Prevent spurious *"No route to host"* errors on macOS when using IPv6.

Hive Connector Changes
----------------------

* Fix data loss when writing bucketed sorted tables. Partitions would
  be missing arbitrary rows if any of the temporary files for a bucket
  had the same size. The ``numRows`` partition property contained the
  correct number of rows and can be used to detect if this occurred.
* Fix cleanup of temporary files when writing bucketed sorted tables.
* Allow creating schemas when using ``file`` based security.
* Reduce the number of cases where tiny ORC stripes will be written when
  some columns are highly dictionary compressed.
* Improve memory accounting when reading ORC files. Previously, buffer
  memory and object overhead was not tracked for stream readers.
* ORC struct columns are now mapped by name rather than ordinal.
  This correctly handles missing or extra struct fields in the ORC file.
* Add procedure ``system.create_empty_partition()`` for creating empty partitions.

Kafka Connector Changes
-----------------------

* Support Avro formatted Kafka messages.
* Support backward compatible Avro schema evolution.

SPI Changes
-----------

* Allow using ``Object`` as a parameter type or return type for SQL
  functions when the correponding SQL type is an unbounded generic.
