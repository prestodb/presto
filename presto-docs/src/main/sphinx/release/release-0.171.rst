=============
Release 0.171
=============

General Changes
---------------

* Fix planning regression for queries that compute a mix of distinct and non-distinct aggregations.
* Fix casting from certain complex types to ``JSON`` when source type contains ``JSON`` or ``DECIMAL``.
* Fix issue for data definition queries that prevented firing completion events or purging them from
  the coordinator's memory.
* Add support for capture in lambda expressions.
* Add support for ``ARRAY`` and ``ROW`` type as the compared value in :func:`min_by` and :func:`max_by`.
* Add support for ``CHAR(n)`` data type to common string functions.
* Add :func:`codepoint`, :func:`skewness` and :func:`kurtosis` functions.
* Improve validation of resource group configuration.
* Fail queries when casting unsupported types to JSON; see :doc:`/functions/json` for supported types.

Web UI Changes
--------------

* Fix the threads UI (``/ui/thread``).

Hive Changes
------------

* Fix issue where some files are not deleted on cancellation of ``INSERT`` or ``CREATE`` queries.
* Allow writing to non-managed (external) Hive tables. This is disabled by default but can be
  enabled via the ``hive.non-managed-table-writes-enabled`` configuration option.
