=============
Release 0.128
=============

Graceful Shutdown
-----------------

Workers can now be instructed to shutdown. This is done by submiting a ``PUT``
request to ``/v1/info/state`` with the body ``"SHUTTING_DOWN"``. Once instructed
to shutdown, the worker will no longer receive new tasks, and will exit once
all existing tasks have completed.

General Changes
---------------

* Fix cast from json to structural types when rows or maps have arrays,
  rows, or maps nested in them.
* Fix Example HTTP connector.
  It would previously fail with a JSON deserialization error.
* Optimize memory usage in TupleDomain.
* Fix an issue that can occur when an ``INNER JOIN`` has equi-join clauses that
  align with the grouping columns used by a preceding operation such as
  ``GROUP BY``, ``DISTINCT``, etc. When this triggers, the join may fail to
  produce some of the output rows.

MySQL Changes
-------------

* Fix handling of MySQL database names with underscores.
