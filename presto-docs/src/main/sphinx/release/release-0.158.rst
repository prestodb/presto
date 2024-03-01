=============
Release 0.158
=============

General Changes
---------------

* Fix regression that could cause high CPU and heap usage on coordinator
  when processing certain types of long running queries.
* Fix incorrect pruning of output columns in ``EXPLAIN ANALYZE``.
* Fix ordering of ``CHAR`` values so that trailing spaces are ordered after control characters.
* Fix query failures for connectors that produce non-remotely accessible splits.
* Fix non-linear performance issue when parsing certain SQL expressions.
* Fix case-sensitivity issues when operating on columns of ``ROW`` data type.
* Fix failure when creating views for tables names that need quoting.
* Return ``NULL`` from :func:`element_at` for out-of-range indices instead of failing.
* Remove redundancies in query plans, which can reduce data transfers over the network and reduce CPU requirements.
* Validate resource groups configuration file on startup to ensure that all
  selectors reference a configured resource group.
* Add experimental on-disk merge sort for aggregations. This can be enabled with
  the ``experimental.spill-enabled`` configuration flag.
* Push down predicates for ``DECIMAL``, ``TINYINT``, ``SMALLINT`` and ``REAL`` data types.

Hive Changes
------------

* Add hidden ``$bucket`` column for bucketed tables that
  contains the bucket number for the current row.
* Prevent inserting into non-managed (i.e., external) tables.
* Add configurable size limit to Hive metastore cache to avoid using too much
  coordinator memory.

Cassandra Changes
-----------------

* Allow starting the server even if a contact point hostname cannot be resolved.
