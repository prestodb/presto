=============
Release 0.164
=============

General Changes
---------------

* Fix correctness issue for queries that perform ``DISTINCT`` and ``LIMIT`` on the results of a ``JOIN``.
* Fix correctness issue when casting between maps where the key or value is the ``REAL`` type.
* Fix correctness issue in :func:`min_by` and :func:`max_by` when nulls are present in the comparison column.
* Fail queries when ``FILTER`` clause is specified for scalar functions.
* Fix planning failure for certain correlated subqueries that contain aggregations.
* Fix planning failure when arguments to selective aggregates are derived from other selective aggregates.
* Fix boolean expression optimization bug that can cause long planning times, planning failures and coordinator instability.
* Fix query failure when ``TRY`` or lambda expression with the exact same body is repeated in an expression.
* Fix split source resource leak in coordinator that can occur when a query fails.
* Improve :func:`array_join` performance.
* Improve error message for map subscript operator when key is not present in the map.
* Improve client error message for invalid session.
* Add ``VALIDATE`` mode for :doc:`/sql/explain`.

Web UI Changes
--------------

* Add resource group to query detail page.

Hive Changes
------------

* Fix handling of ORC files containing extremely large metadata.
* Fix failure when creating views in file based metastore.
* Improve performance for queries that read bucketed tables by optimizing scheduling.
