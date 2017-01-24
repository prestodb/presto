=================
Presto properties
=================

This is a list and description of most important presto properties that may be used to tune Presto or alter it behavior when required.


.. _tuning-pref-general:

General properties
------------------

``distributed-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:**

  Use hash distributed joins instead of broadcast joins. Distributed joins
  require redistributing both tables using a hash of the join key. This can
  be slower (sometimes substantially) than broadcast joins, but allows much
  larger joins. Broadcast joins require that the tables on the right side of
  the join after filtering fit in memory on each node whereas distributed joins
  only need to fit in distributed memory across all nodes. This can also be
  specified on a per-query basis using the ``distributed_join`` session property.


``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:**

  Turning on this property causes additional rehashing of data before writing them
  to connector. This eliminates performance impact of data skewness when writing to
  disk by distributing data before write (usually I/O operation). It can be disabled
  when it's known that data set is not skewed in order to save time on rehashing
  operation. This can also be specified on a per-query basis using the
  ``redistribute_writes`` session property.


``resources.reserved-system-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.4``
 * **Description:**

  Maximum amount of memory available to each Presto node. Reaching this limit
  will cause the server to drop operations. Higher value may increase Presto's
  stability, but may cause problems if physical server is used for other purposes.
  If too much memory is allocated to Presto, the operating system may terminate the process.


``sink.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:**

  Buffer size for IO writes while collecting pipeline results from cluster node.
  Increasing this value may improve the speed of IO operations, but will take memory
  away from other functions. Buffered data will be lost if the node crashes, so using
  a large value is not recommended when the environment is unstable.


.. _tuning-pref-exchange:

Exchange properties
-------------------

The Exchange service is responsible for transferring data between Presto nodes.
Adjusting these properties may help to resolve inter-node communication issues
or improve network utilization.

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``25``
 * **Description:**

  Number of threads that the exchange server can spawn to handle clients.
  Higher value will increase concurrency but excessively high values may cause
  a drop in performance due to context switches and additional memory usage.


``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``3``
 * **Description:**

  Multiplier determining how many clients of the exchange server may be spawned
  relative to available buffer memory. The number of possible clients is determined
  by heuristic as the number of clients that can fit into available buffer space
  based on average buffer usage per request times this multiplier. For example
  with the ``exchange.max-buffer-size`` of ``32 MB`` and ``20 MB`` already used,
  and average bytes per request being ``2MB`` up to
  ``exchange.concurrent-request-multipier`` * ((``32MB`` - ``20MB``) / ``2MB``) = ``exchange.concurrent-request-multiplier`` * ``6``
  may be spawned. Tuning this value adjusts the heuristic, which may increase
  concurrency and improve network utilization.


``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:**

  Size of memory block reserved for the client buffer in exchange server. Lower
  value may increase processing time under heavy load. Increasing this value
  may improve network utilization, but will reduce the amount of memory available
  for other activities.


``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``1 MB``)
 * **Default value:** ``16 MB``
 * **Description:**

  Max size of messages sent through the exchange server. The size of message headers
  is included in this value, so the amount of data sent per message will be a little lower.
  Increasing this value may improve network utilization if the network is stable. In an
  unstable network environment, making this value smaller may improve stability.


.. _tuning-pref-task:

Tasks managment properties
--------------------------

``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``Node CPUs`` * ``2``
 * **Description:**

  Sets the number of threads used by workers to process splits. Increasing this number
  can improve throughput if worker CPU utilization is low and all the threads are in use,
  but will cause increased heap space usage. Too high value may cause drop in performance
  due to a context switching. The number of active threads is available via the
  ``com.facebook.presto.execution.TaskExecutor.RunningSplits`` JMX stat.


.. _tuning-pref-node:

Node scheduler properties
-------------------------

``node-scheduler.max-pending-splits-per-node-per-stage``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``10``
 * **Description:**

  Must be smaller than ``node-scheduler.max-splits-per-node``. This property describes
  how many splits can be queued to each worker node. Having this value higher will
  allow more jobs to be queued but will cause resources to be used for that.

  Using a higher value is recommended if queries are submitted in large batches, (eg.
  running a large group of reports periodically). Increasing this value may help to avoid
  query drops and decrease the risk of short query starvation. High value is also
  recommended if splits are processed relatively quickly compared to a time of generating
  new splits by the connector.

  Too high value may drastically increase processing wall time if node distribution of
  query work will be skew. This is especially important if nodes do have important
  differences in performance. The best value for that is enough to provide at least one
  split always waiting to be process but not higher.


``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:**

  This property limits the number of splits that can be scheduled for each node.

  Increasing this value will allow the cluster to process more queries or reduce visibility
  of problems connected to data skew. High value is also recommended if splits are
  processed relatively quickly compared to a time of generating new splits by the connector.

  Excessively high values may result in poor performance due to context switching and
  higher memory reservation for cluster metadata.


``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``10``
 * **Description:**

  The minimal number of node candidates check by scheduler when looking for a node to schedule
  a split. Having this value to low may increase skew of work distribution between nodes.
  Too high value may increase latency of query and CPU load. The value should be aligned
  with number of nodes in cluster.


``node-scheduler.multiple-tasks-per-node-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:**

  Allow nodes to be selected multiple times by the node scheduler in a single stage.
  With this property set to ``false`` the ``hash_partition_count`` is capped at number of
  nodes in system. Having this set to ``true`` may allow better scheduling and concurrency,
  which would reduce the number of outliers and speed up computations. It may also improve
  reliability in unstable network conditions. The drawbacks are that some optimization may
  work less efficiently on smaller partitions. Also slight hardware efficiency drop is
  expected in heavy loaded system.

.. _node-scheduler-network-topology:

``node-scheduler.network-topology``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``legacy`` or ``flat``)
 * **Default value:** ``legacy``
 * **Description:**

  Sets the network topology to use when scheduling splits. ``legacy`` will ignore
  the topology when scheduling splits. ``flat`` will try to schedule splits on the host
  where the data is located by reserving 50% of the work queue for local splits.
  It is recommended to use ``flat`` for clusters where distributed storage runs on
  the same nodes as Presto workers.
