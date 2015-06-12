=============
Release 0.108
=============

General Changes
---------------

* Fix incorrect query results when one window function follows another row_number() window function and are
  both partitioned on the same columns.
* Add :func:`from_iso8601_timestamp`, :func:`from_iso8601_date`, and
  :func:`to_iso8601` function.
* Make ``ctrl-C`` in CLI cancel the query (rather than a partial cancel).
