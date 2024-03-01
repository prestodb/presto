=============
Release 0.155
=============

General Changes
---------------

* Fix incorrect results when queries contain multiple grouping sets that
  resolve to the same set.
* Fix incorrect results when using ``map`` with ``IN`` predicates.
* Fix compile failure for outer joins that have a complex join criteria.
* Fix error messages for failures during commit.
* Fix memory accounting for simple aggregation, top N and distinct queries.
  These queries may now report higher memory usage than before.
* Reduce unnecessary memory usage of :func:`map_agg`, :func:`multimap_agg`
  and :func:`map_union`.
* Make ``INCLUDING``, ``EXCLUDING`` and ``PROPERTIES`` non-reserved keywords.
* Remove support for the experimental feature to compute approximate queries
  based on sampled tables.
* Properly account for time spent creating page source.
* Various optimizations to reduce coordinator CPU usage.

Hive Changes
------------

* Fix schema evolution support in new Parquet reader.
* Fix ``NoClassDefFoundError`` when using Hadoop KMS.
* Add support for Avro file format.
* Always produce dictionary blocks for DWRF dictionary encoded streams.

SPI Changes
-----------

* Remove legacy connector API.
