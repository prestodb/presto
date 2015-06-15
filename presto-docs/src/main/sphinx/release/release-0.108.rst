=============
Release 0.108
=============

General Changes
---------------

* Fix incorrect query results when a window function follows a :func:`row_number`
  function and both are partitioned on the same column(s).
* Add :func:`from_iso8601_timestamp`, :func:`from_iso8601_date`, and
  :func:`to_iso8601` function.
* Make ``ctrl-C`` in CLI cancel the query (rather than a partial cancel).
* Allow calling ``Connection.setReadOnly(false)`` in the JDBC driver.
  The read-only status for the connection is currently ignored.
* Add missing ``CAST`` from ``VARCHAR`` to ``TIMESTAMP WITH TIME ZONE``.
* Allow optional time zone in ``CAST`` from ``VARCHAR`` to ``TIMESTAMP`` and
  ``TIMESTAMP WITH TIME ZONE``.
* Trim in ``CAST`` from ``VARCHAR`` to date time types.
* Add support for fixed time zones ``+00:00`` and ``-00:00``.

Cassandra Changes
-----------------

* Randomly select Cassandra node for split generation.
