=============
Release 0.184
=============

General Changes
---------------

* Fix query execution failure for ``split_to_map(...)[...]``.
* Fix issue that caused queries containing ``CROSS JOIN`` to continue using CPU resources
  even after they were killed.
* Fix planning failure for some query shapes containing ``count(*)`` and a non-empty
  ``GROUP BY`` clause.
* Fix communication failures caused by lock contention in the local scheduler.
* Improve performance of :func:`element_at` for maps to be constant time rather than
  proportional to the size of the map.
* Improve performance of queries with gathering exchanges.
* Require ``coalesce()`` to have at least two arguments, as mandated by the SQL standard.
* Add :func:`hamming_distance` function.

JDBC Driver Changes
-------------------

* Always invoke the progress callback with the final stats at query completion.

Web UI Changes
--------------

* Add worker status page with information about currently running threads
  and resource utilization (CPU, heap, memory pools). This page is accessible
  by clicking a hostname on a query task list.

Hive Changes
------------

* Fix partition filtering for keys of ``CHAR``, ``DECIMAL``, or ``DATE`` type.
* Reduce system memory usage when reading table columns containing string values
  from ORC or DWRF files. This can prevent high GC overhead or out-of-memory crashes.

TPCDS Changes
-------------

* Fix display of table statistics when running ``SHOW STATS FOR ...``.

SPI Changes
-----------

* Row columns or values represented with ``ArrayBlock`` and ``InterleavedBlock`` are
  no longer supported. They must be represented as ``RowBlock`` or ``SingleRowBlock``.
* Add ``source`` field to ``ConnectorSession``.
