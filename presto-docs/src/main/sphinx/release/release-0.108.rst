=============
Release 0.108
=============

General Changes
---------------

* Fix incorrect query results when a window function follows a :func:`row_number`
  function and both are partitioned on the same column(s).
* Add :func:`normalize`, :func:`from_iso8601_timestamp`, :func:`from_iso8601_date`
  and :func:`to_iso8601` functions.
* Make ``ctrl-C`` in CLI cancel the query (rather than a partial cancel).
* Allow calling ``Connection.setReadOnly(false)`` in the JDBC driver.
  The read-only status for the connection is currently ignored.
* Add missing ``CAST`` from ``VARCHAR`` to ``TIMESTAMP WITH TIME ZONE``.
* Allow optional time zone in ``CAST`` from ``VARCHAR`` to ``TIMESTAMP`` and
  ``TIMESTAMP WITH TIME ZONE``.
* Trim in ``CAST`` from ``VARCHAR`` to date time types.
* Add support for fixed time zones ``+00:00`` and ``-00:00``.
* Properly account for query memory when using the :func:`row_number` function.
* Fix issue where a query (read or write) that should fail can instead
  complete successfully with zero rows.
* Skip execution of inner join when the join target is empty.
* Improve query detail UI page.
* Fix printing of table layouts in :doc:`/sql/explain`.

Cassandra Changes
-----------------

* Randomly select Cassandra node for split generation.
* Fix handling of UUID partition keys.
