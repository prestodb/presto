============
Release 0.96
============

General Changes
---------------

* Fix :func:`try_cast` for ``TIMESTAMP`` and other types that
  need access to session information.
* Fix planner bug that could result in incorrect results for tables containing columns with the same prefix, underscores and numbers.
* ``MAP`` type is now comparable.

Hive Changes
------------

* Add support for tables partitioned by ``DATE``.
