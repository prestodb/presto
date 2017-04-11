=============
Release 0.169
=============

General Changes
---------------

* Fix regression that could cause queries involving ``JOIN`` and certain language features
  such as ``current_date``, ``current_time`` or ``extract`` to fail during planning.
* Limit the maximum allowed input size to :func:`levenshtein_distance`.
* Improve performance of :func:`map_agg` and :func:`multimap_agg`.
* Improve memory accounting when grouping on a single ``BIGINT`` column.

JDBC Driver Changes
-------------------

* Return correct class name for ``ARRAY`` type from ``ResultSetMetaData.getColumnClassName()``.

CLI Changes
-----------

* Fix support for non-standard offset time zones (e.g., ``GMT+01:00``).

Cassandra Changes
-----------------

* Add custom error codes.
