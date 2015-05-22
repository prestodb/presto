============
Release 0.96
============

General Changes
---------------

* Fix :func:`try_cast` for ``TIMESTAMP`` and other types that
  need access to session information.
* Fix planner bug that could result in incorrect results for
  tables containing columns with the same prefix, underscores and numbers.
* ``MAP`` type is now comparable.
* Fix output buffer leak in ``StatementResource.Query``.
* Fix leak in ``SqlTasks`` caused by invalid heartbeats .
* Fix double logging of queries submitted while the queue is full.
* Fixed "running queries" JMX stat.
* Add ``distributed_join`` session property to enable/disable distributed joins.

Hive Changes
------------

* Add support for tables partitioned by ``DATE``.
