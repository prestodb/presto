=============
Release 0.108
=============

General Changes
---------------

* Fix incorrect query results when a window function follows a :func:`row_number`
  function and both are partitioned on the same column(s).
* Fix planning issue where queries that apply a ``false`` predicate
  to the result of a non-grouped aggregation produce incorrect results.
* Fix exception when ``ORDER BY`` clause contains duplicate columns.
* Fix issue where a query (read or write) that should fail can instead
  complete successfully with zero rows.
* Add :func:`normalize`, :func:`from_iso8601_timestamp`, :func:`from_iso8601_date`
  and :func:`to_iso8601` functions.
* Add support for :func:`position` syntax.
* Add Teradata compatibility functions: :func:`index`, :func:`char2hexint`,
  :func:`to_char`, :func:`to_date` and :func:`to_timestamp`.
* Make ``ctrl-C`` in CLI cancel the query (rather than a partial cancel).
* Allow calling ``Connection.setReadOnly(false)`` in the JDBC driver.
  The read-only status for the connection is currently ignored.
* Add missing ``CAST`` from ``VARCHAR`` to ``TIMESTAMP WITH TIME ZONE``.
* Allow optional time zone in ``CAST`` from ``VARCHAR`` to ``TIMESTAMP`` and
  ``TIMESTAMP WITH TIME ZONE``.
* Trim values when converting from ``VARCHAR`` to date/time types.
* Add support for fixed time zones ``+00:00`` and ``-00:00``.
* Properly account for query memory when using the :func:`row_number` function.
* Skip execution of inner join when the join target is empty.
* Improve query detail UI page.
* Fix printing of table layouts in :doc:`/sql/explain`.
* Add :doc:`/connector/blackhole`.

Cassandra Changes
-----------------

* Randomly select Cassandra node for split generation.
* Fix handling of ``UUID`` partition keys.
