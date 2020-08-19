=============
Release 0.221
=============

General Changes
---------------
* Fix error during stats collection phase of query planning.
* Fix a performance regression for some outer joins without equality predicates when
  ``join_distribution_type`` is set to ``AUTOMATIC``.
* Improve performance for queries that have constant ``VARCHAR`` predicates on join columns.
* Add a variant of :func:`strpos` that returns the position of the N-th instance of the substring.
* Add :func:`strrpos` that returns the position of the N-th instance of a substring from the back of a string.
* Add aggregation function :func:`entropy`.
* Add classification aggregation functions :func:`classification_miss_rate`, :func:`classification_precision`,
  :func:`classification_recall`, :func:`classification_thresholds`.
* Add overload of :func:`approx_set` which takes in the maximum standard error.
* Add ``max_tasks_per_stage`` session property and ``stage.max-tasks-per-stage`` config property to
  limit the number of tasks per stage for grouped execution.  Setting this session property allows queries
  running with grouped execution to use a predictable amount of memory independent of the cluster size.
* Add encryption for spill files (see :doc:`/admin/spill`).

Web UI Changes
--------------
* Add information about query warnings to the web UI.

Raptor Changes
--------------
* Revert the change introduced in 0.219 to rebalance bucket assignment after restarting
  the cluster. Automatic rebalancing can cause unexpected downtime when restarting the cluster
  to resolve emergent issues.

Hive Connector Changes
----------------------
* Improve coordinator memory utilization for Hive splits.
* Improve performance of writing large ORC files.

SPI Changes
-----------
* Add ``pageSinkContext`` for ``createPageSink`` in ``PageSinkProvider`` and
  ``ConnectorPageSinkProvider``. It contains a boolean ``partitionCommitRequired``, which is
  false by default.  See the note below about ``commitPartition`` for more information.
* Add ``commitPartition`` to ``Metadata`` and ``ConnectorMetadata``. This SPI is coupled with
  ``pageSinkContext#partitionCommitRequired`` and is used by the engine to commit a partition of data to the target
  connector. The connector that implements this SPI should ensure that if ``pageSinkContext#isPartitionCommitRequired``
  is true in ``ConnectorPageSinkProvider#createPageSink``, the written data is not published until
  ``ConnectorMetadata#commitPartition`` is called. Also, it is expected for the connector to add ``SUPPORTS_PARTITION_COMMIT``
  in ``Connector#getCapabilities``.
* Add ``ExpressionOptimizer`` in ``RowExpressionService``. ``ExpressionOptimizer`` simplifies a ``RowExpression``
  and prunes redundant part of it.
* Add ``pushNegationToLeaves`` method to ``LogicalRowExpressions`` to push negation down below conjunction or disjunction
  for a logical expression.
* Replace ``SplitSchedulingStrategy`` with ``SplitSchedulingContext`` in ``ConnectorSplitManager``.  ``SplitSchedulingContext``
  contains the ``SplitSchedulingStrategy`` and a boolean ``schedulerUsesHostAddresses`` that indicates whether the network topology
  is used during scheduling.  If false, the connector doesn't need to provide the host addresses for remotely accessible splits.
