=============
Release 0.158
=============

General Changes
---------------

* Fix regression that could cause high CPU and heap usage on coordinator,
  when processing certain types of long running queries.
* Fix incorrect pruning of output columns in ``EXPLAIN ANALYZE``.
* Fix incorrect ordering of ``CHAR`` values. Previously, values with trailing spaces
  were ordered before values with trailing control-characters.
* Fix failure of queries that have non-remotely accessible splits.
* Fix non-linear parsing issue for certain SQL expressions.
* Fix case-sensitivity issues when operating on columns of ``ROW`` data type.
* Validate resource groups configuration file on startup. This ensures that all
  selectors reference a configured resource group.
* Add experimental on-disk merge sort for aggregations. This can be enabled with
  the ``experimental.spill-enabled`` configuration flag.
* Push down predicates for ``DECIMAL``, ``TINYINT``, ``SMALLINT`` and ``REAL`` data types.
* Produce simpler query plans, which can reduce data transfers over the network and reduce CPU requirements.
* Return ``NULL`` from ::func::`element_at` for out-of-range indices instead of failing.

Hive Changes
------------

* Add a hidden column ``$bucket`` for bucketed tables.
