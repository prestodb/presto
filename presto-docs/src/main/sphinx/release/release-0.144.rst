=============
Release 0.144
=============

General Changes
---------------

* Fix already exists check when adding a column to be case-insensitive.
* Fix correctness issue when complex grouping operations have a partitioned source.
* Fix missing coercion when using ``INSERT`` with ``NULL`` literals.
* Fix regression that the queries fail when aggregation functions present in ``AT TIME ZONE``.
* Fix potential memory starvation when a query is run with ``resource_overcommit=true``.
* Queries run with ``resource_overcommit=true`` may now be killed before
  they reach ``query.max-memory`` if the cluster is low on memory.
* Discard output stage JSON from completion event when it is very long.
  This limit can be configured with ``event.max-output-stage-size``.
* Add support for :doc:`/sql/explain-analyze`.
* Change ``infoUri`` field of ``/v1/statement`` to point to query HTML page instead of JSON.
* Improve performance when processing results in CLI and JDBC driver.
* Improve performance of ``GROUP BY`` queries.

Hive Changes
------------

* Fix ORC reader to actually use ``hive.orc.stream-buffer-size`` configuration property.
* Add support for creating and inserting into bucketed tables.
