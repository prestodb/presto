=============
Release 0.202
=============

General Changes
---------------

* Fix correctness issue for queries involving aggregations over the result of an outer join (:issue:`x10592`).
* Fix :func:`map` to raise an error on duplicate keys rather than silently producing a corrupted map.
* Fix :func:`map_from_entries` to raise an error when input array contains a ``null`` entry.
* Fix out-of-memory error for bucketed execution by scheduling new splits on the same worker as
  the recently finished one.
* Fix query failure when performing a ``GROUP BY`` on ``json`` or ``ipaddress`` types.
* Fix correctness issue in :func:`line_locate_point`, :func:`ST_IsValid`, and :func:`geometry_invalid_reason`
  functions to not return values outside of the expected range.
* Fix failure in :func:`geometry_to_bing_tiles` and :func:`ST_NumPoints` functions when
  processing geometry collections.
* Fix query failure in aggregation spilling (:issue:`x10587`).
* Remove support for ``SHOW PARTITIONS`` statement.
* Improve support for correlated subqueries containing equality predicates.
* Improve performance of correlated ``EXISTS`` subqueries.
* Limit the number of grouping sets in a ``GROUP BY`` clause.
  The default limit is ``2048`` and can be set via the ``analyzer.max-grouping-sets``
  configuration property or the ``max_grouping_sets`` session property.
* Allow coercion between row types regardless of field names.
  Previously, a row type is coercible to another only if the field name in the source type
  matches the target type, or when target type has anonymous field name.
* Increase default value for ``experimental.filter-and-project-min-output-page-size`` to ``500kB``.
* Improve performance of equals operator on ``array(bigint)`` and ``array(double)`` types.
* Respect ``X-Forwarded-Proto`` header in client protocol responses.
* Add support for column-level access control.
  Connectors have not yet been updated to take advantage of this support.
* Add support for correlated subqueries with correlated ``OR`` predicates.
* Add :func:`multimap_from_entries` function.
* Add :func:`bing_tiles_around`, :func:`ST_NumGeometries`, :func:`ST_GeometryN`, and :func:`ST_ConvexHull` geospatial functions.
* Add :func:`wilson_interval_lower` and :func:`wilson_interval_upper` functions.
* Add ``IS DISTINCT FROM`` for ``json`` and ``ipaddress`` type.

Hive Changes
------------

* Fix optimized ORC writer encoding of ``TIMESTAMP`` before ``1970-01-01``.  Previously, the
  written value was off by one second.
* Fix query failure when a Hive bucket has no splits. This commonly happens when a
  predicate filters some buckets out entirely.
* Remove the ``hive.bucket-writing`` config property.
* Add support for creating and writing bucketed sorted tables. The list of
  sorting columns may be specified using the ``sorted_by`` table property.
  Writing to sorted tables can be disabled using the ``hive.sorted-writing``
  config property or the ``sorted_writing_enabled`` session property. The
  maximum number of temporary files for can be controlled using the
  ``hive.max-sort-files-per-bucket`` property.
* Collect and store basic table statistics (``rowCount``, ``fileCount``, ``rawDataSize``,
  ``totalSize``) when writing.
* Add ``hive.orc.tiny-stripe-threshold`` config property and ``orc_tiny_stripe_threshold``
  session property to control the stripe/file size threshold when ORC reader decides to
  read multiple consecutive stripes or entire fires at once. Previously, this feature
  piggybacks on other properties.

CLI Changes
-----------

* Add peak memory usage to ``--debug`` output.

SPI Changes
-----------

* Make ``PageSorter`` and ``PageIndexer`` supported interfaces.
