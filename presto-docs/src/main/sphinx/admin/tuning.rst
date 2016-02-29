=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you if your cluster is facing a specific performance problem.

.. _tuning-pref-general:

General properties
------------------


``distributed-index-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Serves as default value for ``distributed_index_join`` session property. Enabling this property forces to repartition tables used for index join. This causes drop in processing time, but allows to perform much larger joins. Depending of side of index selected for join (preferably right side) only the other must fit into each node memory (after filtering).


``distributed-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Use hash distributed joins instead of broadcast joins. Distributed joins require redistributing both tables using a hash of the join key. This can be slower (sometimes substantially) than broadcast joins but allows much larger joins. Broadcast joins require that the tables on the right side of the join after filtering fit in memory on each machine whereas distributed joins only need to fit in distributed memory across all machines. This can also be specified on a per-query basis using the ``distributed_join`` session property.


``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Force parallel distributed writes. Serves as default value for ``redistribute_writes`` session property. Setting this property will cause write operator to be distributed between nodes. This allows to utilize distributed storage backend especially in case of small number of huge queries.


``resources.reserved-system-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.4``
 * **Description:** Amount of memory set up as allocation limit for single presto node. Reaching this limit will cause operations to start to be dropped by the server. Higher value may increase server stability but may cause problems if physical server is used for other purposes as well. In some configurations of host it may even cause presto to be shut down by host if to much memory will be allocated.


``sink.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:** Buffer size for IO writes while collecting pipeline results. Higher value may increase speed of IO operations with the cost of additional memory. Also higher value may increase number of data lost when presto node will fail effectively slowing down IO in unstable environment.


.. _tuning-pref-exchange:

Exchange properties
-------------------

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``25``
 * **Description:** Number of threads that exchange server can spawn to handle clients. Higher value will increase concurrency but may cause general drop in performance if the value is to high due to context switches and additional memory usage.


``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``3``
 * **Description:** Multiplier determining how many clients of exchange server may be spawned in reference to available buffer memory. Number of possible clients is determined by heuristic as number of clients that can fit into available buffer space based on average buffer usage per request times this multiplier. For example with the ``exchange.max-buffer-size`` of ``32 MB`` and ``20 MB`` already used, and average bytes per request being ``2MB`` up to ``exchange.concurrent-request-multipier`` * ((``32MB`` - ``20MB``) / ``2MB``) = ``exchange.concurrent-request-multiplier`` * ``6`` may be spawned. Tuning this value allows to change the heuristic in order to ensure higher concurrency and possibly better network utilization in the case of dense network architecture.


``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:** Size of memory block reserved for client buffer in exchange server. Lower value may increase processing time under heavy load on cluster. The value may be higher if network connection is not saturated even though it could. The drawback is that it uses more of memory for pure communication purposes.


``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``1 MB``)
 * **Default value:** ``16 MB``
 * **Description:** Max size of chunk of data send through HTTP exchange server. It's adjusted by heuristic to include headers into this value, so one my expect the size of real data sent in one response to be actually smaller. Higher value may increase network utilization if the network is stable. In unstable network environment making this value smaller may increase stability drastically by decreasing number of data lost in network.


.. _tuning-pref-node:

Node scheduler properties
-------------------------

``node-scheduler.max-pending-splits-per-node-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``10``
 * **Description:** Must be smaller then ``node-scheduler.max-splits-per-node``. This property describes how many splits can be queued to every single worker node. Having this value higher will allow more jobs to be queued but will cause resources to be used for that. Higher value here is recommended if system usual routine is to get lots of queries in a row with long time in between (eg. running number of queries once a day) - in such case it may be crucial to avoid query drops. Setting this value higher will also decrease risk of short queries starvation.


``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** This property describes how many splits can each of nodes in cluster have scheduled. Setting this value to higher will allow to handle bigger bulk of queries to be handled when they are not distributed properly. However higher value causes possibility of losing performance for switching contexts and higher memory reservation for cluster metadata.


``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``10``
 * **Description:** The minimal number of nodes candidates proposed by scheduler to do every job in system. Setting this allows to manipulate global parallelism. The higher value is recommended for system having lots of nodes and small number of huge queries. The lower value is recommended in system that have higher number of smaller queries. Also this setting is connected with ``node-scheduler.network-topology`` - while using ``flat`` it is important to align this value with number of nodes that backend required for queries is split between (or higher).


``node-scheduler.multiple-tasks-per-node-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Allow nodes to be selected multiple times by the node scheduler, in a single stage. With that property set to ``false`` the ``node-scheduler.min-candidates`` is capped at number of nodes in system. Having this set may allow better scheduling and concurrency reducing number of outliers and possibly speeding up computations. Also it may allow to collect smaller bulks of result in unstable network conditions. The drawbacks are that some optimization may work less efficiently on smaller partitions. Also slight hardware efficiency drop is expected in heavy loaded system.

.. _node-scheduler-network-topology:

``node-scheduler.network-topology``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``legacy`` or ``flat``)
 * **Default value:** ``legacy``
 * **Description:** Sets the network topology to use when scheduling splits. ``legacy`` will ignore the topology when scheduling splits. ``flat`` will try to schedule splits on the same host as the data is located by reserving 50% of the work queue for local splits. It is recommended to use ``flat`` for clusters where distributed storage runs on same nodes as presto workers.


.. _tuning-pref-optimizer:

Optimizer properties
--------------------

``optimizer.columnar-processing-dictionary``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Serves as default value for ``columnar_processing_dictionary`` session property. Setting this property will allow to use columnar processing with dictionary while performing filtering operators.


``optimizer.columnar-processing``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Serves as default value for ``columnar_processing`` session property. Setting this property will allow to use columnar processing while performing filtering operators. This setup is ignored if ``columnar_processing_dictionary`` is enabled.


``optimizer.dictionary-aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Serves as default value for ``dictionary_aggregation`` session property. Enables optimization for aggregations on dictionaries.


``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Serves as default value for ``optimize_hash_generation`` session property. Compute hash codes for distribution, joins, and aggregations early in query plan which may allow to drop some of computation later in query processing with the cost of increased preprocessing. In most cases it should decrease overall query processing time.


``optimizer.optimize-metadata-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Enables optimization of aggregations that are kept in metadata of data structures. This allow to perform simple queries in ``O(1)`` time using metadata that are kept anyway. Currently this optimization can be use for selecting `max`, `min` and `approx_distinct` of partition keys. Using this may speed some queries significantly with possible drawback on very small data sets.


``optimizer.optimize-single-distinct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Enables single distinct optimization. This optimization allows to perform applying distinct mask only once in cases where it's possible. This optimization will try to use single GROUP BY instead of multiple DISTINCT clauses. Enabling this optimization should speed up some specific selects but analyzing all queries to check if they qualify for this optimization may be a slight overhead.


``optimizer.push-table-write-through-union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Serves as default value for ``push_table_write_through_union`` session property. Parallelize writes when using UNION ALL in queries that write data. This allows to improve speed of writing output tables in UNION ALL clause by making use of the fact, that UNION ALL outputs do not require additional synchronization when collecting results. Enabling this optimization can improve UNION ALL speed when write speed is not yet saturated. However it's may slow down queries in already heavy loaded system.


``optimizer.use-intermediate-aggregations``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Serves as default value for ``task_intermediate_aggregation`` session property. Setting this property allows to reduce amount of data sent over the network for grouped aggregation queries. This has side effect of possibly lower parallelism as well as bigger chunks of data to perform. Also some grouping functions may have higher overall time when splitting aggregation between nodes.


.. _tuning-pref-query:

Query execution properties
--------------------------


``query.execution-policy``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``all-at-once``
 * **Description:** Serves as default value for ``execution_policy`` session property. Setting this value to ``phased`` will allow query scheduler to split a single query execution between different time slots. This will allow to switch context more often and possibly stage the partially executed query in order to increase robustness. Average time of executing query may slightly increase after setting this to ``phased`` due to context switching and more complex scheduling algorithm but drop in variation of query execution time is expected.


``query.initial-hash-partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``8``
 * **Description:** Serves as default value for ``hash_partition_count`` session property. This value is used to determine how many nodes may share the same query when partitioning system is set to ``FIXED``. Manipulating this value will allow to distribute work between nodes properly. Value lower then number of presto nodes may lower the utilization of cluster in low traffic environment. Setting the number to to high value will cause assigning multiple partitions of same query to one node or ignoring the setting - in some configurations the value is internally capped at number of available worker nodes.


``query.low-memory-killer.delay``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``5s``)
 * **Default value:** ``5 m``
 * **Description:** Delay between cluster running low on memory and invoking killer. When this value is low, there will be instant reaction for running out of memory on cluster. This may cause more queries to fail fast but it will be less often that query will fail in unexpected way.


``query.low-memory-killer.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** This property controls if there should be killer of query triggered when cluster is running out of memory. The strategy of the killer is to drop largest queries first so enabling this option may cause problem with executing large queries in highly loaded cluster but should increase stability of smaller queries.


``query.manager-executor-pool-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5``
 * **Description:** Size of thread pool used for garbage collecting after queries. Threads from this pool are used to free resources from canceled queries, enforcing memory limits, queries timeouts etc. Higher number of threads will allow to manage memory more efficiently, so it may be increased to avoid out of memory exceptions in some scenarios. On the other hand higher value here may increase CPU usage for garbage collecting and use additional constant memory even if there is nothing to do for all of the threads.


``query.max-age``
^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``15 m``
 * **Description:** This property describes time after which the query metadata may be removed from server. If value is low, it's possible that client will not be able to receive information about query completion. The value describes minimum time that must pass to remove query (after it's considered completed) but if there is space available in history queue the query data will be kept longer. The size of history queue is defined by ``query.max-history`` property (``100`` by default).


``query.max-concurrent-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** **Deprecated** Describes how many queries be processed simultaneously in single cluster node. It shouldn't be used in new configuration, the ``query.queue-config-file`` can be used instead.


``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``1 GB``
 * **Description:** The purpose of that is same as of ``query.max-memory`` but the memory is not counted cluster-wise but node-wise instead.


``query.max-memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``20 GB``
 * **Description:** Serves as default value for ``query_max_memory`` session property. This property also describes strict limit of total memory allocated around the cluster that may be used to process single query. The query is dropped if the limit is reached unless session want to prevent that by setting session property ``resource_overcommit``. The session may also want to decrease system pressure, so it's possible to decrease query memory limit for session by setting ``query_max_memory`` to smaller value. Setting ``query_max_memory`` to higher value then ``query.max-memory`` will not have any effect. This property may be used to ensure that single query cannot use all resources in cluster. The value should be set to be higher than what typical expected query in system will need - that way system will be resistant to SQL bugs that would cause large unwanted computation. Also if rare queries will require more memory, then the ``resource_overcommit`` session property may be used to break the limit. It is important to set this value to higher then default when presto runs complex queries on large datasets.


``query.max-queued-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5000``
 * **Description:** **Deprecated** Describes how many queries may wait in worker queue. If the limit is reached master server will consider worker blocked and will not push more tasks to him. Setting this value high may allow to order a lot of queries at once with the cost of additional memory needed to keep informations about tasks to process. Lowering this value will decrease system capacity but will allow to utilize memore for real processing of date instead of queuing. It shouldn't be used in new configuration, the ``query.queue-config-file`` can be used instead.


``query.max-run-time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``100 d``
 * **Description:** Used as default for session property ``query_max_run_time``. If the presto works in environment where there are mostly very long queries (over 100 days) than it may be a good idea to increase this value to avoid dropping clients that didn't set their session property correctly. On the other hand in the presto works in environment where they are only very short queries this value set to small value may be used to detect user errors in queries. It may also be decreased in poor presto cluster configuration with mostly short queries to increase garbage collection efficiency and by that lowering memory usage in cluster.


``query.queue-config-file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String``
 * **Default value:** 
 * **Description:** This property may be defined to provide patch to queue config file. This is new way of providing such informations as ``query.max-concurrent-queries`` and ``query.max-queued-queries``. The file should contain JSON configuration described in :ref:`Queue configuration<Queue-configuration>`.


``query.remote-task.max-callback-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** This value describe max size of thread pool used to handle HTTP requests responses for task in cluster. Higher value will cause more of resources to be used for handling HTTP communication itself though increasing this value may improve response time when presto is distributed across many hosts or there is a lot of small queries going on in the system.


``query.remote-task.min-error-duration``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``1s``)
 * **Default value:** ``2 m``
 * **Description:** The minimal time that HTTP worker must be unavailable for server to drop the connection. Higher value may be recommended in unstable connection conditions. This value is only a bottom line so there is no guarantee that node will be considered dead after such amount of time. In order to consider node dead the defined time must pass between two failed attempts of HTTP communication, with no successful communication in between.


``query.schedule-split-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** The size of single data chunk expressed in rows that will be processed as single split. Higher value may be used if system works in reliable environment and there the responsiveness is less important then average answer time. Decreasing this value may have a positive effect if there are lots of nodes in system and calculations are relatively heavy for each of rows. Other scenario may be if there are many nodes with poor stability - lowering this number will allow to react faster and for that reason the lost computation time will be potentially lower.


.. _tuning-pref-task:

Tasks managment properties
--------------------------


``task.default-concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``1``
 * **Description:** Default local concurrency for parallel operators. Serves as default value for ``task_hash_build_concurrency`` and ``task_aggregation_concurrency``. It is also a default value of ``task.join-concurrency`` property. Increasing this value is strongly recommended when any of CPU, IO or memory is not saturated on regular basis. In this scenario it will allow queries to utilize as many resources as possible. Setting this value to high will cause queries to slow down. It may happen even if none of resources is saturated as there are cases in which increasing parallelism is not possible due to algorithms limitations.


``task.http-response-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** Max number of threads that may be created to handle http responses. Threads are created on demand and they ends when there is no response to be sent. That means that there is no overhead if there is only a small number of request handled by system even if this value is big. On the other hand increasing this value may increase utilization of CPU in multicore environment (with the cost of memory usage). Also in systems having a lot of requests, the response time distribution may be manipulated using this property. Higher value may be used to avoid outliers adding the cost of increased average response time.


``task.http-timeout-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``3``
 * **Description:** Number of threads spawned for handling timeouts of http requests. Presto server sends update of query status whenever it is different then the one that client knows about. However in order to ensure client that connection is still alive, server sends this data after delay declared internally in HTTP headers (by default ``200 ms``). This property tells how many threads are designated to handle this delay. If the property turn out to low it's possible that the update time will increase even significantly when comparing to requested value (``200ms``). Increasing this value may solve the problem, but it generate a cost of additional memory even if threads are not used all the time. If there is no problem with updating status of query this value should not be manipulated.


``task.info-refresh-max-wait``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``200 ms``
 * **Description:** Controls staleness of task information which is used in scheduling. Increasing this value can reduce coordinator CPU load but may result in suboptimal split scheduling.


``task.join-concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.default-`` ``concurrency`` (``1``)
 * **Description:** Servers as default value for session property: ``task_join_concurrency``. Describes local concurrency for join operators. This value may be increased to perform join on worker using more then one thread. This will increase CPU utilization with the cost of increased memory usage.


``task.max-index-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``64 MB``
 * **Description:** Max size of index cache in memory used for index based joins. Increasing this value allows to use more memory for such queries which may improve time of huge table joins.


``task.max-partial-aggregation-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``16 MB``
 * **Description:** Max size of partial aggregation result (if it is splitable). Increasing this value will decrease fragmentation of result which may improve general times and CPU utilization with the cost of additional memory usage. Also high value of this property may cause drop in performance in unstable cluster conditions.


``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``Node CPUs`` * ``4``
 * **Description:** Sets the number of threads used by workers to process splits. Increasing this number can improve throughput if worker CPU utilization is low but will cause increased heap space usage.


``task.min-drivers``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.max-`` ``worker-threads`` * ``2``
 * **Description:** This describes how many drivers are kept on worker any time (if there is anything to do). The smaller value may cause better responsiveness for new task but possibly decreases CPU utilization. Higher value makes context switching faster with the cost of additional memory. The general rules of managing drivers is that if there is possibility of assigning a split to driver it is assigned if: there are less then ``3`` drivers assigned to given task OR there is less drivers on worker then ``task.min-drivers`` OR the task has been enqueued with ``force start`` property.


``task.operator-pre-allocated-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``16 MB``
 * **Description:** Memory preallocated for each driver in query execution. Increasing this value may cause less efficient memory usage but allows to fail fast in low memory environment more frequently.


``task.share-index-loading``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** It allows to control whether index lookups join has index shared within a task. This enables the possibility of optimizing for index cache hits or for more CPU parallelism depending on the property value. Serves as default for ``task_share_index_loading`` session property.


``task.writer-count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``1``
 * **Description:** Describes how many parallel writers may try to access I/O while executing queries in session. Serves as default for session property ``task_writer_count``. Setting this value to higher than default may increase write speed especially when query is NOT IO bounded and could use of more CPU cores for parallel writes. However in many cases increasing this value will visibly increase computation time while writing.



.. _tuning-pref-session:

Session properties
------------------

``columnar_processing_dictionary``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.columnar-processing-dictionary`` (``false``)
 * **Description:** See :ref:`optimizer.columnar-processing-dictionary <tuning-pref-optimizer>`.


``columnar_processing``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.columnar-processing`` (``false``)
 * **Description:** See :ref:`optimizer.columnar-processing <tuning-pref-optimizer>`.


``dictionary_aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.dictionary-aggregation`` (``false``)
 * **Description:** See :ref:`optimizer.dictionary-aggregation <tuning-pref-optimizer>`.


``execution_policy``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``query.execution-policy`` (``all-at-once``)
 * **Description:** See :ref:`query.execution-policy <tuning-pref-query>`.


``hash_partition_count``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``query.initial-hash-partitions`` (``8``)
 * **Description:** See :ref:`query.initial-hash-partitions <tuning-pref-query>`.


``optimize_hash_generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.optimize-hash-generation`` (``true``)
 * **Description:** See :ref:`optimizer.optimize-hash-generation <tuning-pref-optimizer>`.


``orc_max_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-buffer-size`` (``8 MB``)
 * **Description:** See :ref:`hive.orc.max-buffer-size <tuning-pref-hive>`.


``orc_max_merge_distance``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-merge-distance`` (``1 MB``)
 * **Description:** See :ref:`hive.orc.max-merge-distance <tuning-pref-hive>`.


``orc_stream_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-buffer-size`` (``8 MB``)
 * **Description:** See :ref:`hive.orc.max-buffer-size <tuning-pref-hive>`.


``plan_with_table_node_partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** **Experimental.** Adapt plan to use backend partitioning. By setting this property you allow to use partitioning provided by table layout itself while collecting required data. This may allow to utilize optimization of table layout provided by specific connector. In particular, when this is set presto will try to partition data for workers in a way that each workers gets a chunk of data that comes from one backend partition. It can be particularly useful due to the I/O distribution optimization in table partitioning. Note that this property may only be utilized if given projection uses all columns used for table partitioning inside connector.


``prefer_streaming_operators``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Prefer source table layouts that produce streaming operators. Setting this property will allow workers not to wait for chunks of data to start processing them while scanning tables. This may cause faster processing  with lower latency and downtime but some operators may do things more efficiently when working with chunks of data.


``push_table_write_through_union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.push-table-write-through-union`` (``true``)
 * **Description:** See :ref:`optimizer.push-table-writethrough-union <tuning-pref-optimizer>`.


``query_max_memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``query.max-memory`` (``20 GB``)
 * **Description:** This property can be use to be nice to the cluster for example when our query is not as important then the usual cluster routines. Setting this value to smaller then server property ``query.max-memory`` will cause server to drop session query if it will require more then ``query_max_memory`` memory instead of ``query.max-memory``. On the other hand setting this value to higher then ``query.max-memory`` will not have effect at all.


``query_max_run_time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``query.max-run-time`` (``100 d``)
 * **Description:** The default value of this is defined by server. If expected query processing time is higher then property ``query.max-run-time`` it's crucial to set this session property - otherwise there is a risk of dropping all result of long processing after ``query.max-run-time`` ends. Session may also set this value to lower than ``query.max-run-time`` in order to crosscheck for bugs in queries. In may be particularly use full when setting up session with very large number of queries each of which should take very short time in order to be able to end all of queries in acceptable time. Even in this scenario it's crucial though, to set this value to much higher value than average query time to avoid problems with outliers (some queries may randomly take much longer then other due to cluster load and many other circumstances).


``resource_overcommit``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Use resources which are not guaranteed to be available to the query. By setting this property you allow to exceed limits of memory available per query processing and session. This may cause resources to be used more efficiently allowing to  but may cause some indeterministic query drops due to lacking memory on machine. perform more demanding queries


``task_aggregation_concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.default-concurrency`` (``1``)
 * **Description:** **Experimental.** Default number of local parallel aggregation jobs per worker. Same as ``task_join_concurrency`` but it is used for aggregation.


``task_hash_build_concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.default-concurrency`` (``1``)
 * **Description:** **Experimental.** Default number of local parallel hash build jobs per worker. Same as ``task_join_concurrency`` but it is used for building hashes. The value is always rounded down to the power of 2  so it's recommended to use such value in order to avoid unexpected behavior.


``task_intermediate_aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.use-intermediate-aggregations`` (``false``)
 * **Description:** See :ref:`optimizer.use-intermediate-aggregations <tuning-pref-optimizer>`.


``task_join_concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.join-concurrency`` (``1``)
 * **Description:** **Experimental.** Default number of local parallel join jobs per worker. This value may be increased to perform join on worker using more then one thread to increase CPU utilization with the cost of increased memory usage.


``task_writer_count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.writer-count`` (``1``)
 * **Description:** See :ref:`task.writer-count <tuning-pref-task>`.



JVM Settings
------------

The following can be helpful for diagnosing GC issues:

.. code-block:: none

    -XX:+PrintGCApplicationConcurrentTime
    -XX:+PrintGCApplicationStoppedTime
    -XX:+PrintGCCause
    -XX:+PrintGCDateStamps
    -XX:+PrintGCTimeStamps
    -XX:+PrintGCDetails
    -XX:+PrintClassHistogramAfterFullGC
    -XX:+PrintClassHistogramBeforeFullGC
    -XX:PrintFLSStatistics=2
    -XX:+PrintAdaptiveSizePolicy
    -XX:+PrintSafepointStatistics
    -XX:PrintSafepointStatisticsCount=1
