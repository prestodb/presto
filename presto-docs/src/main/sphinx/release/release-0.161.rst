=============
Release 0.161
=============

General Changes
---------------

* Fix correctness issue for queries involving multiple nested EXCEPT clauses.
  A query such as ``a EXCEPT (b EXCEPT c)`` was incorrectly evaluated as
  ``a EXCEPT b EXCEPT c`` and thus could return the wrong result.
* Fix failure when executing prepared statements that contain parameters in the join criteria.
* Fix failure when describing the output of prepared statements that contain aggregations.
* Fix planning failure when a lambda is used in the context of an aggregation or subquery.
* Fix column resolution rules for ``ORDER BY`` to match the behavior expected
  by the SQL standard. This is a change in semantics that breaks
  backwards compatibility. To ease migration of existing queries, the legacy
  behavior can be restored by the ``deprecated.legacy-order-by`` config option
  or the ``legacy_order_by`` session property.
* Improve error message when coordinator responds with ``403 FORBIDDEN``.
* Improve performance for queries containing expressions in the join criteria
  that reference columns on one side of the join.
* Improve performance of :func:`map_concat` when one argument is empty.
* Remove ``/v1/execute`` resource.
* Add new column to :doc:`/sql/show-columns` (and :doc:`/sql/describe`)
  to show extra information from connectors.
* Add :func:`map` to construct an empty :ref:`map_type`.

Hive Connector
--------------

* Remove ``"Partition Key: "`` prefix from column comments and
  replace it with the new extra information field described above.

JMX Connector
-------------

* Add support for escaped commas in ``jmx.dump-tables`` config property.
