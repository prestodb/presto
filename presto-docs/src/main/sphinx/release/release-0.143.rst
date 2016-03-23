=============
Release 0.143
=============

General Changes
---------------

* Add config option ``query.max-cpu-time`` to limit CPU time used by a query.
* Add loading indicator and error message to query detail page in UI.
* Add query teardown to query timeline visualizer.
* Add string padding functions :func:`lpad` and :func:`rpad`.
* Improve query startup time in large clusters.

Hive Changes
------------

* Fix native memory leak when reading or writing gzip compressed data.
