====================
Properties Reference
====================

This section describes the most important config properties that
may be used to tune Presto or alter its behavior when required.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

.. _tuning-pref-general:

General Properties
------------------

``distributed-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Use hash distributed joins instead of broadcast joins. Distributed joins
    require redistributing both tables using a hash of the join key. This can
    be slower (sometimes substantially) than broadcast joins, but allows much
    larger joins. Broadcast joins require that the tables on the right side of
    the join after filtering fit in memory on each node, whereas distributed joins
    only need to fit in distributed memory across all nodes. This can also be
    specified on a per-query basis using the ``distributed_join`` session property.

``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    This property enables redistribution of data before writing. This can
    eliminate the performance impact of data skew when writing by hashing it
    across nodes in the cluster. It can be disabled when it is known that the
    output data set is not skewed in order to avoid the overhead of hashing and
    redistributing all the data across the network. This can also be specified
    on a per-query basis using the ``redistribute_writes`` session property.

``resources.reserved-system-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``JVM max memory * 0.4``

    The amount of JVM memory reserved, for accounting purposes, for things
    that are not directly attributable to or controllable by a user query.
    For example, output buffers, code caches, etc. This also accounts for
    memory that is not tracked by the memory tracking system.

    The purpose of this property is to prevent the JVM from running out of
    memory (OOM). The default value is suitable for smaller JVM heap sizes or
    clusters with many concurrent queries. If running fewer queries with a
    large heap, a smaller value may work. Basically, set this value large
    enough that the JVM does not fail with ``OutOfMemoryError``.

``optimizer.reorder-windows``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``Boolean``
    * **Default value:** ``true``
    * **Description:** Allow reordering windows in order to put those with the same partitioning next to each other. This will sometimes decrease the number of repartitionings. This can also be specified on a per-query basis using the ``reorder_windows`` session property.

.. _tuning-spilling:

Properties controlling spilling
-------------------------------

``experimental.spill-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``Boolean``
    * **Default value:** ``false``
    * **Description:** Try spilling memory to disk to avoid exceeding memory limits for the query.

    Spilling works by offloading memory to disk. This process can allow some queries with large memory
    footprint to pass at the cost of slower execution times. Currently, spilling is supported only for
    aggregations, so this property will not reduce memory usage required for joins, window functions and
    sorting.

    Be aware that this is an experimental feature and should be used with care.

    Currently, all queries with aggregations will slow down after enabling spilling. It is recommended
    to use the spill session property to selectively turn on spilling only for queries that would run
    out of memory otherwise.

    This config property can be overridden by the ``spill_enabled`` session property.


``experimental.spiller-spill-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``String``
    * **No default value.** Must be overriden before enabling spilling
    * **Description:** Directory where spilled content will be written. It can be a comma separated list to

    spill simultaneously to multiple directories, which helps to utilize multiple drives installed in the system.
    It is highly unrecommended to spill to system drives. Especially do not spill on a drive, to which are
    written JVM logs. Otherwise disks overutilization might cause JVM to pause for lengthy periods causing
    presto workers to disconnect.


``experimental.spiller-minimum-free-space-threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Double``
 * **Default value:** ``0.9``
 * **Description:** If disk space usage of a given spill path is above this threshold, this spill path will not be eligible for spilling.


``experimental.spiller-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``4``
 * **Description:** Number of spiller threads. Increase this value if the default is not able to saturate the underlying spilling device (for example, when using a RAID matrix with multiple disks)


``experimental.max-spill-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``100 GB``
 * **Description:** Max spill space to be used by all queries on a single node.


``experimental.query-max-spill-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``100 GB``
 * **Description:** Max spill space to be used by a single query on a single node.

.. _tuning-pref-query:

Query execution properties
--------------------------

``query.execution-policy``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``all-at-once``
 * **Description:**

  Setting this value to ``phased`` will allow the query scheduler to split a single
  query execution between different time slots. This will allow Presto to switch context
  more often and possibly stage the partially executed query in order to increase robustness.
  Average time to execute a query may slightly increase after setting this to ``phased``,
  but query execution time will be more consistent. This can also be specified on a
  per-query basis using the ``execution_policy`` session property.


``query.initial-hash-partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:**

  This value is used to determine how many nodes may share the same query when fixed
  partitioning is chosen by Presto. Manipulating this value will affect the distribution
  of work between nodes. A value lower then the number of Presto nodes may lower the utilization
  of the cluster in a low traffic environment. An excessively high value will cause multiple
  partitions of the same query to be assigned to a single node, or Presto may ignore
  the setting if ``node-scheduler.multiple-tasks-per-node-enabled`` is set to false -
  the value is internally capped at the number of available worker nodes in such scenario.
  This can also be specified on a per-query basis using the ``hash_partition_count``
  session property.


``query.low-memory-killer.delay``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``5s``)
 * **Default value:** ``5 m``
 * **Description:**

  Delay between a cluster running low on memory and invoking a query killer.
  A lower value may cause more queries to fail fast, but fewer queries to
  fail in an unexpected way.


``query.low-memory-killer.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:**

  This property controls whether a query killer should be triggered when a cluster
  is running out of memory. The killer will drop the largest queries first so enabling
  this option may cause problems with executing large queries in a highly loaded cluster,
  but should increase stability of smaller queries.


``query.manager-executor-pool-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5``
 * **Description:**

  Size of the thread pool used for garbage collecting after queries. Threads from this
  pool are used to free resources from canceled queries, as well as enforce memory limits,
  queries timeouts etc. More threads will allow for more efficient memory management,
  and so may help avoid out of memory exceptions in some scenarios. However, having more
  threads may also increase CPU usage for garbage collecting and will have an additional
  constant memory cost even if the threads have nothing to do.


``query.min-expire-age``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``15 m``
 * **Description:**

  This property describes the minimum time after which the query metadata may be removed
  from the server. If the value is too low, the client may not be able to receive information
  about query completion. The value describes minimum time, but if there is space available
  in the history queue the query data will be kept longer. The size of the history queue is
  defined by the ``query.max-history property``.


``query.max-concurrent-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:**

  **Deprecated** Describes how many queries can be processed simultaneously in a single cluster node.
  In new configurations, the ``query.queue-config-file`` should be used instead.


.. _query-max-memory:

``query.max-memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``20 GB``
 * **Description:**

  Serves as the default value for the ``query_max_memory`` session property. This property also describes
  the strict limit of total memory that may be used to process a single query. A query is dropped if the
  limit is reached unless the ``resource_overcommit`` session property is set. This property helps ensure
  that a single query cannot use all resources in a cluster. It should be set higher than what is expected
  to be needed for a typical query in the system. It is important to set this to higher than the default
  if Presto will be running complex queries on large datasets. It is possible to decrease the query memory
  limit for a session by setting ``query_max_memory`` to a smaller value. Setting ``query_max_memory`` to
  a greater value than ``query.max-memory`` will not have any effect.


``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.1``
 * **Description:**

  The purpose of that is same as of :ref:`query.max-memory<query-max-memory>` but the memory is not counted
  cluster-wise but node-wise instead. This should not be any lower than ``query.max-memory / number of nodes``.
  It may be required to increase this value if data are skewed.


``query.max-queued-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5000``
 * **Description:**

  **Deprecated** Describes how many queries may wait in Presto coordinator queue. If the limit is reached the
  server will drop all new incoming queries. Setting this value high may allow to order a lot of queries at
  once with the cost of additional memory needed to keep informations about tasks to process. Lowering this
  value will decrease system capacity but will allow to utilize memory for real processing of data instead
  of queuing. It shouldn't be used in new configuration, the ``query.queue-config-file`` can be used instead.


``query.max-run-time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``100 d``
 * **Description:**

  Used as default for session property ``query_max_run_time``. If the Presto works in environment where there
  are mostly very long queries (over 100 days) than it may be a good idea to increase this value to avoid
  dropping clients that didn't set their session property correctly. On the other hand in the Presto works
  in environment where they are only very short queries this value set to small value may be used to detect
  user errors in queries. It may also be decreased in poor Presto cluster configuration with mostly short
  queries to increase garbage collection efficiency and by that lowering memory usage in cluster.


``query.queue-config-file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String``
 * **Default value:**
 * **Description:**

  The path to the queue config file. Queues are used to manage the number of concurrent queries across the
  system. More information on queues and how to configure them can be found in :doc:/admin/queue.


``query.remote-task.max-callback-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:**

  This value describes the maximum size of the thread pool used to handle responses to HTTP requests for
  each task. Increasing this value will cause more resources to be used for handling HTTP communication
  itself, but may also improve response time when Presto is distributed across many hosts or there are
  a lot of small queries being run.


``query.remote-task.min-error-duration``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``1s``)
 * **Default value:** ``2 m``
 * **Description:**

  The minimal time that HTTP worker must be unavailable before the coordinator assumes the worker crashed.
  A higher value may be recommended in unstable connection conditions. This value is only a bottom line
  so there is no guarantee that a node will be considered dead after the ``query.remote-task.min-error-duration``.
  In order to consider a node dead, the defined time must pass between two failed attempts of HTTP communication,
  with no successful communication in between.


``query.schedule-split-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:**

  The size of single data chunk expressed in split that will be processed in a single stage. Higher value may
  be used if system works in reliable environment and the responsiveness is less important then average answer
  time, it will require more memory reserve though. Decreasing this value may have a positive effect if
  there are lots of nodes in system and calculations are relatively heavy for each of splits.

Exchange properties
-------------------

Exchanges transfer data between Presto nodes for different stages of
a query. Adjusting these properties may help to resolve inter-node
communication issues or improve network utilization.

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``25``

    Number of threads used by exchange clients to fetch data from other Presto
    nodes. A higher value can improve performance for large clusters or clusters
    with very high concurrency, but excessively high values may cause a drop
    in performance due to context switches and additional memory usage.

``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``3``

    Multiplier determining the number of concurrent requests relative to
    available buffer memory. The maximum number of requests is determined
    using a heuristic of the number of clients that can fit into available
    buffer space based on average buffer usage per request times this
    multiplier. For example, with an ``exchange.max-buffer-size`` of ``32 MB``
    and ``20 MB`` already used and average size per request being ``2MB``,
    the maximum number of clients is
    ``multiplier * ((32MB - 20MB) / 2MB) = multiplier * 6``. Tuning this
    value adjusts the heuristic, which may increase concurrency and improve
    network utilization.

``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``32MB``

    Size of buffer in the exchange client that holds data fetched from other
    nodes before it is processed. A larger buffer can increase network
    throughput for larger clusters and thus decrease query processing time,
    but will reduce the amount of memory available for other usages.

``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Minimum value:** ``1MB``
    * **Default value:** ``16MB``

    Maximum size of a response returned from an exchange request. The response
    will be placed in the exchange client buffer which is shared across all
    concurrent requests for the exchange.

    Increasing the value may improve network throughput if there is high
    latency. Decreasing the value may improve query performance for large
    clusters as it reduces skew due to the exchange client buffer holding
    responses for more tasks (rather than hold more data from fewer tasks).

``sink.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``32MB``

    Output buffer size for task data that is waiting to be pulled by upstream
    tasks. If the task output is hash partitioned, then the buffer will be
    shared across all of the partitioned consumers. Increasing this value may
    improve network throughput for data transferred between stages if the
    network has high latency or if there are many nodes in the cluster.

.. _tuning-pref-task:

Task Properties
---------------

.. _task-concurrency:

``task.concurrency``
^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Restrictions:** must be a power of two
    * **Default value:** ``16``

    Default local concurrency for parallel operators such as joins and aggregations.
    This value should be adjusted up or down based on the query concurrency and worker
    resource utilization. Lower values are better for clusters that run many queries
    concurrently because the cluster will already be utilized by all the running
    queries, so adding more concurrency will result in slow downs due to context
    switching and other overhead. Higher values are better for clusters that only run
    one or a few queries at a time. This can also be specified on a per-query basis
    using the ``task_concurrency`` session property.

``task.http-response-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``100``

    Maximum number of threads that may be created to handle HTTP responses. Threads are
    created on demand and are cleaned up when idle, thus there is no overhead to a large
    value if the number of requests to be handled is small. More threads may be helpful
    on clusters with a high number of concurrent queries, or on clusters with hundreds
    or thousands of workers.

``task.http-timeout-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``3``

    Number of threads used to handle timeouts when generating HTTP responses. This value
    should be increased if all the threads are frequently in use. This can be monitored
    via the ``com.facebook.presto.server:name=AsyncHttpExecutionMBean:TimeoutExecutor``
    JMX object. If ``ActiveCount`` is always the same as ``PoolSize``, increase the
    number of threads.

``task.info-update-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``duration``
    * **Minimum value:** ``1ms``
    * **Maximum value:** ``10s``
    * **Default value:** ``3s``

    Controls staleness of task information, which is used in scheduling. Larger values
    can reduce coordinator CPU load, but may result in suboptimal split scheduling.

``task.max-partial-aggregation-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``data size``
    * **Default value:** ``16MB``

    Maximum size of partial aggregation results for distributed aggregations. Increasing this
    value can result in less network transfer and lower CPU utilization by allowing more
    groups to be kept locally before being flushed, at the cost of additional memory usage.

``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``Node CPUs * 2``

    Sets the number of threads used by workers to process splits. Increasing this number
    can improve throughput if worker CPU utilization is low and all the threads are in use,
    but will cause increased heap space usage. Setting the value too high may cause a drop
    in performance due to a context switching. The number of active threads is available
    via the ``RunningSplits`` property of the
    ``com.facebook.presto.execution.executor:name=TaskExecutor.RunningSplits`` JXM object.

``task.min-drivers``
^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``task.max-worker-threads * 2``

    The target number of running leaf splits on a worker. This is a minimum value because
    each leaf task is guaranteed at least ``3`` running splits. Non-leaf tasks are also
    guaranteed to run in order to prevent deadlocks. A lower value may improve responsiveness
    for new tasks, but can result in underutilized resources. A higher value can increase
    resource utilization, but uses additional memory.

``task.writer-count``
^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Restrictions:** must be a power of two
    * **Default value:** ``1``

    The number of concurrent writer threads per worker per query. Increasing this value may
    increase write speed, especially when a query is not I/O bound and can take advantage
    of additional CPU for parallel writes (some connectors can be bottlenecked on CPU when
    writing due to compression or other factors). Setting this too high may cause the cluster
    to become overloaded due to excessive resource utilization. This can also be specified on
    a per-query basis using the ``task_writer_count`` session property.


.. _tuning-pref-node:

Node Scheduler Properties
-------------------------

``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``100``

    The target value for the total number of splits that can be running for
    each worker node.

    Using a higher value is recommended if queries are submitted in large batches
    (e.g., running a large group of reports periodically) or for connectors that
    produce many splits that complete quickly. Increasing this value may improve
    query latency by ensuring that the workers have enough splits to keep them
    fully utilized.

    Setting this too high will waste memory and may result in lower performance
    due to splits not being balanced across workers. Ideally, it should be set
    such that there is always at least one split waiting to be processed, but
    not higher.

``node-scheduler.max-pending-splits-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``10``

    The number of outstanding splits that can be queued for each worker node
    for a single stage of a query, even when the node is already at the limit for
    total number of splits. Allowing a minimum number of splits per stage is
    required to prevent starvation and deadlocks.

    This value must be smaller than ``node-scheduler.max-splits-per-node``,
    will usually be increased for the same reasons, and has similar drawbacks
    if set too high.

``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Minimum value:** ``1``
    * **Default value:** ``10``

    The minimum number of candidate nodes that will be evaluated by the
    node scheduler when choosing the target node for a split. Setting
    this value too low may prevent splits from being properly balanced
    across all worker nodes. Setting it too high may increase query
    latency and increase CPU usage on the coordinator.

``node-scheduler.network-topology``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Allowed values:** ``legacy``, ``flat``
    * **Default value:** ``legacy``

.. _tuning-pref-optimizer:

Optimizer Properties
--------------------

``optimizer.dictionary-aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Enables optimization for aggregations on dictionaries. This can also be specified
    on a per-query basis using the ``dictionary_aggregation`` session property.

``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Compute hash codes for distribution, joins, and aggregations early during execution,
    allowing result to be shared between operations later in the query. This can reduce
    CPU usage by avoiding computing the same hash multiple times, but at the cost of
    additional network transfer for the hashes. In most cases it will decrease overall
    query processing time. This can also be specified on a per-query basis using the
    ``optimize_hash_generation`` session property.

    It is often helpful to disable this property when using :doc:`/sql/explain` in order
    to make the query plan easier to read.

``optimizer.optimize-metadata-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

    Enable optimization of some aggregations by using values that are stored as metadata.
    This allows Presto to execute some simple queries in constant time. Currently, this
    optimization applies to ``max``, ``min`` and ``approx_distinct`` of partition
    keys and other aggregation insensitive to the cardinality of the input (including
    ``DISTINCT`` aggregates). Using this may speed up some queries significantly.

    The main drawback is that it can produce incorrect results if the connector returns
    partition keys for partitions that have no rows. In particular, the Hive connector
    can return empty partitions if they were created by other systems (Presto cannot
    create them).

``optimizer.optimize-single-distinct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    The single distinct optimization will try to replace multiple ``DISTINCT`` clauses
    with a single ``GROUP BY`` clause, which can be substantially faster to execute.

``optimizer.push-aggregation-through-join``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``Boolean``
    * **Default value:** ``true``
    * **Description:**

    When an aggregation is above an outer join and all columns from the outer side of the join
    are in the grouping clause, the aggregation is pushed below the outer join. This optimization
    is particularly useful for correlated scalar subqueries, which get rewritten to an aggregation
    over an outer join. For example:

    .. code-block:: sql

        SELECT * FROM item i
            WHERE i.i_current_price > (
                SELECT AVG(j.i_current_price) FROM item j
                    WHERE i.i_category = j.i_category);

    Enabling this optimization can substantially speed up queries by reducing
    the amount of data that needs to be processed by the join.  However, it may slow down some
    queries that have very selective joins. This can also be specified on a per-query basis using
    the ``push_aggregation_through_join`` session property.

``optimizer.push-table-write-through-union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``true``

    Parallelize writes when using ``UNION ALL`` in queries that write data. This improves the
    speed of writing output tables in ``UNION ALL`` queries because these writes do not require
    additional synchronization when collecting results. Enabling this optimization can improve
    ``UNION ALL`` speed when write speed is not yet saturated. However, it may slow down queries
    in an already heavily loaded system. This can also be specified on a per-query basis
    using the ``push_table_write_through_union`` session property.

.. _tuning-pref-session:

Session properties
------------------

``execution_policy``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``query.execution-policy`` (``all-at-once``)
 * **Description:**

  See :ref:`query.execution-policy <tuning-pref-query>`.


``hash_partition_count``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``query.initial-hash-partitions`` (``100``)
 * **Description:**

  See :ref:`query.initial-hash-partitions <tuning-pref-query>`.


``optimize_hash_generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.optimize-hash-generation`` (``true``)
 * **Description:**

  See :ref:`optimizer.optimize-hash-generation <tuning-pref-optimizer>`.


``plan_with_table_node_partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:**

  **Experimental.** Adapt plan to use backend partitioning. When this is set, presto will
  try to partition data for workers such that each worker gets a chunk of data from a single
  backend partition. This enables workers to take advantage of the I/O distribution optimization
  in table partitioning. Note that this property is only used if a given projection uses all
  columns used for table partitioning inside connector.



``push_table_write_through_union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.push-table-write-through-union`` (``true``)
 * **Description:**

  See :ref:`optimizer.push-table-writethrough-union <tuning-pref-optimizer>`.


``query_max_memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``query.max-memory`` (``20 GB``)
 * **Description:**

  This property can be use to be nice to the cluster if a particular query is not as important
  as the usual cluster routines. Setting this value to less than the server property
  ``query.max-memory`` will cause Presto to drop the query in the session if it will require
  more then ``query_max_memory`` memory. Setting this value to higher than ``query.max-memory``
  will not have any effect.



``query_max_run_time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``query.max-run-time`` (``100 d``)
 * **Description:**

  If the expected query processing time is higher than ``query.max-run-time``, it is crucial
  to set this session property to prevent results of long running queries being dropped after
  ``query.max-run-time``. A session may also set this value to less than ``query.max-run-time``
  in order to crosscheck for bugs in the query. Setting this value less than ``query.max-run-time``
  may be particularly useful for a session with a very large number of short-running queries.
  It is important to set this value to much higher than the average query time to avoid problems
  with outliers (some queries may randomly take much longer due to cluster load and other circumstances).
  As the query timed out by this limit immediately returns all used resources this may be particularly
  useful in query management systems to force user limits.


``resource_overcommit``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:**

  Use resources that are not guaranteed to be available to a query. This property allows you to exceed
  the limits of memory available per query and session. It may allow resources to be used more efficiently,
  but may also cause non-deterministic query drops due to insufficient memory on machine. It can be
  particularly useful for performing more demanding queries.


``task_concurrency``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (power of 2).
 * **Default value:** ``task.concurrency`` (``16``)
 * **Description:**

  Default number of local parallel aggregation jobs per worker. Unlike `task.concurrency` this property
  must be power of two. See :ref:`task.concurrency<task-concurrency>`.


``task_writer_count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.writer-count`` (``1``)
 * **Description:**

  See :ref:`task.writer-count <tuning-pref-task>`.
