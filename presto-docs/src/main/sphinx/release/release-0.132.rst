=============
Release 0.132
=============

.. warning::

   :func:`concat` on :ref:`array_type`, or enabling ``columnar_processing_dictionary``
   may cause queries to fail in this release. This is fixed in :doc:`/release/release-0.133`.

General Changes
---------------

* Fix a correctness issue that can occur when any join depends on the output
  of another outer join that has an inner side (or either side for the full outer
  case) for which the connector declares that it has no data during planning.
* Improve error messages for unresolved operators.
* Add support for creating constant arrays with more than 255 elements.
* Fix analyzer for queries with ``GROUP BY ()`` such that errors are raised
  during analysis rather than execution.
* Add ``resource_overcommit`` session property. This disables all memory
  limits for the query. Instead it may be killed at any time, if the coordinator
  needs to reclaim memory.
* Add support for transactional connectors.
* Add support for non-correlated scalar sub-queries.
* Add support for SQL binary literals.
* Add variant of :func:`random` that produces an integer number between 0 and a
  specified upper bound.
* Perform bounds checks when evaluating :func:`abs`.
* Improve accuracy of memory accounting for :func:`map_agg` and :func:`array_agg`.
  These functions will now appear to use more memory than before.
* Various performance optimizations for functions operating on :ref:`array_type`.
* Add server version to web UI.

CLI Changes
-----------

* Fix sporadic *"Failed to disable interrupt character"* error after exiting pager.

Hive Changes
------------

* Report metastore and namenode latency in milliseconds rather than seconds in
  JMX stats.
* Fix ``NullPointerException`` when inserting a null value for a partition column.
* Improve CPU efficiency when writing data.
