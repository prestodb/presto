=============
Release 0.176
=============

General Changes
---------------

* Fix an issue where a query (and some of its tasks) continues to
  consume CPU/memory on the coordinator and workers after the query fails.
* Fix a regression that cause the GC overhead and pauses to increase significantly when processing maps.
* Fix a memory tracking bug that causes the memory to be overestimated for ``GROUP BY`` queries on ``bigint`` columns.
* Improve the performance of the :func:`transform_values` function.
* Add support for casting from ``JSON`` to ``REAL`` type.
* Add :func:`parse_duration` function.

MySQL Changes
-------------

* Disallow having a database in the ``connection-url`` config property.

Accumulo Changes
----------------

* Decrease planning time by fetching index metrics in parallel.

MongoDB Changes
---------------

* Allow predicate pushdown for ObjectID.
