=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you if your cluster is facing a specific performance problem.

Config Properties
-----------------

These configuration options may require tuning in specific situations:

+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+              property name               +                   type                   +           default value            +tunning the property                                   +
+==========================================+==========================================+====================================+=======================================================+
+      ``task.info-refresh-max-wait``      +          ``String`` (duration)           +             ``200 ms``             +Controls staleness of task information which is used   +
+                                          +                                          +                                    +in scheduling. Increasing this value can reduce        +
+                                          +                                          +                                    +coordinator CPU load but may result in suboptimal      +
+                                          +                                          +                                    +split scheduling.                                      +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+       ``task.max-worker-threads``        +               ``Integer``                +    ``Number of node CPU's * 4``    +Sets the number of threads used by workers to process  +
+                                          +                                          +                                    +splits. Increasing this number can improve throughput  +
+                                          +                                          +                                    +if worker CPU utilization is low but will cause        +
+                                          +                                          +                                    +increased heap space usage.                            +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+      ``distributed-joins-enabled``       +               ``Boolean``                +              ``true``              +Use hash distributed joins instead of broadcast joins. +
+                                          +                                          +                                    +Distributed joins require redistributing both tables   +
+                                          +                                          +                                    +using a hash of the join key. This can be slower       +
+                                          +                                          +                                    +(sometimes substantially) than broadcast joins but     +
+                                          +                                          +                                    +allows much larger joins. Broadcast joins require that +
+                                          +                                          +                                    +the tables on the right side of the join fit in memory +
+                                          +                                          +                                    +on each machine whereas distributed joins only need to +
+                                          +                                          +                                    +fit in distributed memory across all machines. This    +
+                                          +                                          +                                    +can also be specified on a per-query basis using the   +
+                                          +                                          +                                    +``distributed_join`` session property.                 +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+   ``node-scheduler.network-topology``    +   ``String`` (``legacy`` or ``flat``)    +             ``legacy``             +Sets the network topology to use when scheduling       +
+                                          +                                          +                                    +splits. ``legacy`` will ignore the topology when       +
+                                          +                                          +                                    +scheduling splits. ``flat`` will try to schedule       +
+                                          +                                          +                                    +splits on the same host as the data is located by      +
+                                          +                                          +                                    +reserving 50% of the work queue for local splits.      +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+       ``task.default-concurrency``       +               ``Integer``                +               ``1``                +Default local concurrency for parallel operators. Use  +
+                                          +                                          +                                    +this as default for sessions properties:               +
+                                          +                                          +                                    +``task_hash_build_concurrency`` and                    +
+                                          +                                          +                                    +``task_aggregation_concurrency`` as well as            +
+                                          +                                          +                                    +``task.join-concurrency`` property.                    +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+        ``task.join-concurrency``         +               ``Integer``                +``task.default-concurrency`` (``1``)+Servers as default value for session property:         +
+                                          +                                          +                                    +``task_join_concurrency``. Describes local concurrency +
+                                          +                                          +                                    +for join operators. This value may be increased to     +
+                                          +                                          +                                    +perform join on worker using more then one thread.     +
+                                          +                                          +                                    +This will increase CPU utilization with the cost of    +
+                                          +                                          +                                    +increased memory usage.                                +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+      ``task.http-response-threads``      +               ``Integer``                +              ``100``               +Max number of threads that may be created to handle    +
+                                          +                                          +                                    +http responses. Threads are created on demand and they +
+                                          +                                          +                                    +ends when there is no response to be sent. That means  +
+                                          +                                          +                                    +that there is no overhead if there is only a small     +
+                                          +                                          +                                    +number of request handled by system even if this value +
+                                          +                                          +                                    +is big. On the other hand increasing this value may    +
+                                          +                                          +                                    +increase utilization of CPU in multicore environment   +
+                                          +                                          +                                    +(with the cost of memory usage). Also in systems       +
+                                          +                                          +                                    +having a lot of requests, the response time            +
+                                          +                                          +                                    +distribution may be manipulated using this property.   +
+                                          +                                          +                                    +Higher value may be used to avoid outliers adding      +
+                                          +                                          +                                    +the cost of increased average response time.           +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+      ``task.http-timeout-threads``       +               ``Integer``                +               ``3``                +Number of threads spawned for handling timeouts of     +
+                                          +                                          +                                    +http requests. Presto server sends update of query     +
+                                          +                                          +                                    +status whenever it is different then the one that      +
+                                          +                                          +                                    +client knows about. However in order to ensure client  +
+                                          +                                          +                                    +that connection is still alive, server sends this data +
+                                          +                                          +                                    +after delay declared internally in HTTP headers (by    +
+                                          +                                          +                                    +default ``200 ms``). This property tells how many      +
+                                          +                                          +                                    +threads are designated to handle this delay. If the    +
+                                          +                                          +                                    +property turn out to low it's possible that the update +
+                                          +                                          +                                    +time will increase even significantly when comparing   +
+                                          +                                          +                                    +to requested value (``200ms``). Increasing this value  +
+                                          +                                          +                                    +may solve the problem, but it generate a cost of       +
+                                          +                                          +                                    +additional memory even if threads are not used all the +
+                                          +                                          +                                    +time. If there is no problem with updating status of   +
+                                          +                                          +                                    +query this value should not be manipulated.            +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+        ``task.max-index-memory``         +          ``String`` (data size)          +             ``64 MB``              +Max size of index cache in memory used for index based +
+                                          +                                          +                                    +joins. Increasing this value allows to use more memory +
+                                          +                                          +                                    +for such queries which may improve time of huge table  +
+                                          +                                          +                                    +joins by utilizing memory.                             +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+ ``task.max-partial-aggregation-memory``  +          ``String`` (data size)          +             ``16 MB``              +Max size of partial aggregation result (if it is       +
+                                          +                                          +                                    +splitable). Increasing this value will decrease        +
+                                          +                                          +                                    +fragmentation of result which may improve general      +
+                                          +                                          +                                    +times and CPU utilization with the cost of additional  +
+                                          +                                          +                                    +memory usage. Also high value of this property may     +
+                                          +                                          +                                    +cause drop in performance in unstable cluster          +
+                                          +                                          +                                    +conditions.                                            +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+           ``task.min-drivers``           +               ``Integer``                +  ``task.max-worker-threads * 2``   +This describes how many drivers are kept on worker any +
+                                          +                                          +                                    +time (if there is anything to do). The smaller value   +
+                                          +                                          +                                    +may cause better responsiveness for new task but       +
+                                          +                                          +                                    +possibly decreases CPU utilization. Higher value makes +
+                                          +                                          +                                    +context switching faster with the cost of additional   +
+                                          +                                          +                                    +memory. The general rules of managing drivers is that  +
+                                          +                                          +                                    +if there is possibility of assigning a split to driver +
+                                          +                                          +                                    +it is assigned if: there are less then ``3`` drivers   +
+                                          +                                          +                                    +assigned to given task OR there is less drivers on     +
+                                          +                                          +                                    +worker then ``task.min-drivers`` OR the task has been  +
+                                          +                                          +                                    +enqueued with ``force start`` property.                +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+  ``task.operator-pre-allocated-memory``  +          ``String`` (data size)          +             ``16 MB``              +Memory preallocated for each driver in query           +
+                                          +                                          +                                    +execution. Increasing this value may cause less        +
+                                          +                                          +                                    +efficient memory usage but allows to fail fast in low  +
+                                          +                                          +                                    +memory environment more frequently.                    +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+       ``task.share-index-loading``       +               ``Boolean``                +             ``false``              +It allows to control whether index lookups join has    +
+                                          +                                          +                                    +index shared within a task. This enables the           +
+                                          +                                          +                                    +possibility of optimizing for index cache hits or for  +
+                                          +                                          +                                    +more CPU parallelism depending on the property value.  +
+                                          +                                          +                                    +Serves as default for ``task_share_index_loading``     +
+                                          +                                          +                                    +session property.                                      +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+          ``task.writer-count``           +               ``Integer``                +               ``1``                +Defines default number of writers per task. Serves as  +
+                                          +                                          +                                    +default for session property ``task_writer_count``.    +
+                                          +                                          +                                    +Setting this value to higher than default may increase +
+                                          +                                          +                                    +write speed especially in high IOPS, low write speed   +
+                                          +                                          +                                    +environments allowing to utilize available hardware.   +
+                                          +                                          +                                    +However in many cases increasing this value will       +
+                                          +                                          +                                    +visibly increase computation time while writing.       +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+        ``query.execution-policy``        +``String`` (``all-at-once`` or ``phased``)+          ``all-at-once``           +Serves as default value for ``execution_policy``       +
+                                          +                                          +                                    +session property. Setting this value to ``phased``     +
+                                          +                                          +                                    +will allow query scheduler to split a single query     +
+                                          +                                          +                                    +execution between different time slots. This will      +
+                                          +                                          +                                    +allow to switch context more often and possibly stage  +
+                                          +                                          +                                    +the partially executed query in order to increase      +
+                                          +                                          +                                    +robustness. Average time of executing query may        +
+                                          +                                          +                                    +slightly increase after setting this to ``phased`` due +
+                                          +                                          +                                    +to context switching and more complex scheduling       +
+                                          +                                          +                                    +algorithm but drop in variation of query execution     +
+                                          +                                          +                                    +time is expected.                                      +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+    ``query.initial-hash-partitions``     +               ``Integer``                +               ``8``                +Serves as default value for ``hash_partition_count``   +
+                                          +                                          +                                    +session property. This value is used to determine how  +
+                                          +                                          +                                    +many nodes may share the same query when partitioning  +
+                                          +                                          +                                    +system is set to ``FIXED``. Manipulating this value    +
+                                          +                                          +                                    +will allow to distribute work between nodes properly.  +
+                                          +                                          +                                    +Value lower then number of presto nodes may lower the  +
+                                          +                                          +                                    +utilization of cluster in low traffic environment.     +
+                                          +                                          +                                    +Setting the number to to high value will cause         +
+                                          +                                          +                                    +assigning multiple partitions of same query to one     +
+                                          +                                          +                                    +node or ignoring the setting - in some configuration   +
+                                          +                                          +                                    +the value is internally capped at number of available  +
+                                          +                                          +                                    +worker nodes.                                          +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+    ``query.low-memory-killer.delay``     + ``String`` (data size, at least ``5s``)  +              ``5 m``               +Delay between cluster running low on memory and        +
+                                          +                                          +                                    +invoking killer. When this value is low, there will be +
+                                          +                                          +                                    +instant reaction for running out of memory on cluster. +
+                                          +                                          +                                    +This may cause more queries to fail fast but it will   +
+                                          +                                          +                                    +be less often that query will fail in unexpected way.  +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+   ``query.low-memory-killer.enabled``    +               ``Boolean``                +             ``false``              +This property controls if there should be killer of    +
+                                          +                                          +                                    +query triggered when cluster is running out of memory. +
+                                          +                                          +                                    +The strategy of the killer is to drop largest queries  +
+                                          +                                          +                                    +first so enabling this option may cause problem with   +
+                                          +                                          +                                    +executing large queries in highly loaded cluster but   +
+                                          +                                          +                                    +should increase stability of smaller queries.          +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+   ``query.manager-executor-pool-size``   +       ``Integer`` (at least ``1``)       +               ``5``                +Size of thread pool used for garbage collecting after  +
+                                          +                                          +                                    +queries. Threads from this pool are used to free       +
+                                          +                                          +                                    +resources from canceled queries, enforcing memory      +
+                                          +                                          +                                    +limits, queries timeouts etc. Higher number of threads +
+                                          +                                          +                                    +will allow to manage memory more efficiently, so it    +
+                                          +                                          +                                    +may be increased to avoid out of memory exceptions in  +
+                                          +                                          +                                    +some scenarios. On the other hand higher value here    +
+                                          +                                          +                                    +may increase CPU usage for garbage collecting and use  +
+                                          +                                          +                                    +additional constant memory even if there is nothing to +
+                                          +                                          +                                    +do for all of the threads.                             +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+            ``query.max-age``             +          ``String`` (duration)           +              ``15 m``              +This property describes time after which the query     +
+                                          +                                          +                                    +metadata may be removed from server. If value is low,  +
+                                          +                                          +                                    +it's possible that client will not be able to receive  +
+                                          +                                          +                                    +information about query completion. The value          +
+                                          +                                          +                                    +describes minimum time that must pass to remove query  +
+                                          +                                          +                                    +(after it's considered completed) but if there is      +
+                                          +                                          +                                    +space available in history queue the query data will   +
+                                          +                                          +                                    +be kept longer. The size of history queue is defined   +
+                                          +                                          +                                    +by ``query.max-history`` property (``100`` by          +
+                                          +                                          +                                    +default).                                              +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+     ``query.max-concurrent-queries``     +       ``Integer`` (at least ``1``)       +              ``1000``              +**Deprecated** Describes how many queries be processed +
+                                          +                                          +                                    +simultaneously in single cluster node. It shouldn't be +
+                                          +                                          +                                    +used in new configuration, the                         +
+                                          +                                          +                                    +``query.queue-config-file`` can be used instead.       +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+           ``query.max-memory``           +          ``String`` (data size)          +             ``20 GB``              +Serves as default value for ``query_max_memory``       +
+                                          +                                          +                                    +session property. This property also describes strict  +
+                                          +                                          +                                    +limit of total memory allocated around the cluster     +
+                                          +                                          +                                    +that may be used to process single query. The query is +
+                                          +                                          +                                    +dropped if the limit is reached unless session want to +
+                                          +                                          +                                    +prevent that by setting session property               +
+                                          +                                          +                                    +``resource_overcommit``. The session may also want to  +
+                                          +                                          +                                    +decrease system pressure, so it's possible to decrease +
+                                          +                                          +                                    +query memory limit for session by setting              +
+                                          +                                          +                                    +``query_max_memory`` to smaller value. Setting         +
+                                          +                                          +                                    +``query_max_memory`` to higher value then              +
+                                          +                                          +                                    +``query.max-memory`` will not have any effect. This    +
+                                          +                                          +                                    +property may be used to ensure that single query       +
+                                          +                                          +                                    +cannot use all resources in cluster. The value should  +
+                                          +                                          +                                    +be set to be a little bit over what typical expected   +
+                                          +                                          +                                    +query in system will need - that way system will be    +
+                                          +                                          +                                    +resistant to SQL bugs that would cause large unwanted  +
+                                          +                                          +                                    +large computation. Also if rare usecases will require  +
+                                          +                                          +                                    +more memory, then the ``resource_overcommit`` session  +
+                                          +                                          +                                    +property may be used to break the limit.               +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+      ``query.max-memory-per-node``       +          ``String`` (data size)          +              ``1 GB``              +The purpose of that is same as of ``query.max-memory`` +
+                                          +                                          +                                    +but the memory is not counted cluster-wise but         +
+                                          +                                          +                                    +node-wise instead.                                     +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+       ``query.max-queued-queries``       +       ``Integer`` (at least ``1``)       +              ``5000``              +**Deprecated** Describes how many queries may wait in  +
+                                          +                                          +                                    +worker queue. If the limit is reached master server    +
+                                          +                                          +                                    +will consider worker blocked and will not push more    +
+                                          +                                          +                                    +tasks to him. Setting this value high may allow to     +
+                                          +                                          +                                    +order a lot of queries at once with the cost of        +
+                                          +                                          +                                    +additional memory needed to keep informations about    +
+                                          +                                          +                                    +tasks to process. Lowering this value will decrease    +
+                                          +                                          +                                    +system capacity but will allow to utilize memore for   +
+                                          +                                          +                                    +real processing of date instead of queuing. It         +
+                                          +                                          +                                    +shouldn't be used in new configuration, the            +
+                                          +                                          +                                    +``query.queue-config-file`` can be used instead.       +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+          ``query.max-run-time``          +          ``String`` (duration)           +             ``100 d``              +Used as default for session property                   +
+                                          +                                          +                                    +``query_max_run_time``. If the presto works in         +
+                                          +                                          +                                    +environment where there are mostly very long queries   +
+                                          +                                          +                                    +(over 100 days) than it may be a good idea to increase +
+                                          +                                          +                                    +this value to avoid dropping clients that didn't set   +
+                                          +                                          +                                    +their session property correctly. On the other hand in +
+                                          +                                          +                                    +the presto works in environment where they are only    +
+                                          +                                          +                                    +very short queries this value set to small value may   +
+                                          +                                          +                                    +be used to detect user errors in queries. It may also  +
+                                          +                                          +                                    +be decreased in poor presto cluster configuration with +
+                                          +                                          +                                    +mostly short queries to increase garbage collection    +
+                                          +                                          +                                    +efficiency and by that lowering memory usage in        +
+                                          +                                          +                                    +cluster.                                               +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+       ``query.queue-config-file``        +                ``String``                +               `` ``                +This property may be defined to provide patch to queue +
+                                          +                                          +                                    +config file. This is new way of providing such         +
+                                          +                                          +                                    +informations as ``query.max-concurrent-queries`` and   +
+                                          +                                          +                                    +``query.max-queued-queries``. The file should contain  +
+                                          +                                          +                                    +JSON configuration described in :ref:`Queue            +
+                                          +                                          +                                    +configuration<Queue-configuration>`.                   +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+``query.remote-task.max-callback-threads``+       ``Integer`` (at least ``1``)       +              ``1000``              +This value describe max size of thread pool used to    +
+                                          +                                          +                                    +handle HTTP requests responses for task in cluster.    +
+                                          +                                          +                                    +Higher value will cause more of resources to be used   +
+                                          +                                          +                                    +for handling HTTP communication itself though          +
+                                          +                                          +                                    +increasing this value may improve response time when   +
+                                          +                                          +                                    +presto is distributed across many hosts or there is a  +
+                                          +                                          +                                    +lot of small queries going on in the system.           +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+ ``query.remote-task.min-error-duration`` +  ``String`` (duration, at least ``1s``)  +              ``2 m``               +The minimal time that HTTP worker must be unavailable  +
+                                          +                                          +                                    +for server to drop the connection. Higher value may be +
+                                          +                                          +                                    +recommended in unstable connection conditions. This    +
+                                          +                                          +                                    +value is only a bottom line so there is no guarantee   +
+                                          +                                          +                                    +that node will be considered dead after such amount of +
+                                          +                                          +                                    +time. In order to consider node dead the defined time  +
+                                          +                                          +                                    +must pass between two failed attempts of HTTP          +
+                                          +                                          +                                    +communication, with no successful communication in     +
+                                          +                                          +                                    +between.                                               +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+
+   ``query.schedule-split-batch-size``    +       ``Integer`` (at least ``1``)       +              ``1000``              +The size of single data chunk expressed in rows that   +
+                                          +                                          +                                    +will be processed as single split. Higher value may be +
+                                          +                                          +                                    +used if system works in reliable environment and there +
+                                          +                                          +                                    +the responsiveness is less important then average      +
+                                          +                                          +                                    +answer time. Decreasing this value may have a positive +
+                                          +                                          +                                    +effect if there are lots of nodes in system and        +
+                                          +                                          +                                    +calculations are relatively heavy for each of rows.    +
+                                          +                                          +                                    +Other scenario may be if there are many nodes with     +
+                                          +                                          +                                    +poor stability - lowering this number will allow to    +
+                                          +                                          +                                    +react faster and for that reason the lost computation  +
+                                          +                                          +                                    +time will be potentially lower.                        +
+------------------------------------------+------------------------------------------+------------------------------------+-------------------------------------------------------+


Session properties
------------------

+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+            property name            +                   type                   +               default value                +short description                                   +
+=====================================+==========================================+============================================+====================================================+
+      ``task_join_concurrency``      +               ``Integer``                +     ``task.join-concurrency`` (``1``)      +**Experimental.** Default number of local parallel  +
+                                     +                                          +                                            +join jobs per worker. This value may be increased   +
+                                     +                                          +                                            +to perform join on worker using more then one       +
+                                     +                                          +                                            +thread to increase CPU utilization with the cost of +
+                                     +                                          +                                            +increased memory usage.                             +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+   ``task_hash_build_concurrency``   +               ``Integer``                +    ``task.default-concurrency`` (``1``)    +**Experimental.** Default number of local parallel  +
+                                     +                                          +                                            +hash build jobs per worker. Same as                 +
+                                     +                                          +                                            +``task_join_concurrency`` but it is used for        +
+                                     +                                          +                                            +building hashes. The value is always rounded down   +
+                                     +                                          +                                            +to the power of 2 so it's recommended to use such   +
+                                     +                                          +                                            +value in order to avoid unexpected behavior.        +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+  ``task_aggregation_concurrency``   +               ``Integer``                +    ``task.default-concurrency`` (``1``)    +**Experimental.** Default number of local parallel  +
+                                     +                                          +                                            +aggregation jobs per worker. Same as                +
+                                     +                                          +                                            +``task_join_concurrency`` but it is used for        +
+                                     +                                          +                                            +aggregation.                                        +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+        ``task_writer_count``        +               ``Integer``                +       ``task.writer-count`` (``1``)        +Describes how many parallel writers may try to      +
+                                     +                                          +                                            +access I/O while executing queries in session.      +
+                                     +                                          +                                            +Setting this value to higher than default may       +
+                                     +                                          +                                            +increase write speed especially in high IOPS, low   +
+                                     +                                          +                                            +write speed environments allowing to utilize        +
+                                     +                                          +                                            +available hardware. However in many cases           +
+                                     +                                          +                                            +increasing this value will visibly increase         +
+                                     +                                          +                                            +computation time while writing.                     +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+   ``prefer_streaming_operators``    +               ``Boolean``                +                 ``false``                  +Prefer source table layouts that produce streaming  +
+                                     +                                          +                                            +operators. Setting this property will allow workers +
+                                     +                                          +                                            +not to wait for chunks of data to start processing  +
+                                     +                                          +                                            +them while scanning tables. This may cause faster   +
+                                     +                                          +                                            +processing with lower latency and downtime but some +
+                                     +                                          +                                            +operators may do things more efficiently when       +
+                                     +                                          +                                            +working with chunks of data.                        +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+       ``resource_overcommit``       +               ``Boolean``                +                 ``false``                  +Use resources which are not guaranteed to be        +
+                                     +                                          +                                            +available to the query. By setting this property    +
+                                     +                                          +                                            +you allow to exceed limits of memory available per  +
+                                     +                                          +                                            +query processing and session. This may cause        +
+                                     +                                          +                                            +resources to be used more efficiently allowing to   +
+                                     +                                          +                                            +perform larger queries but may cause some           +
+                                     +                                          +                                            +indeterministic query drops due to lacking memory   +
+                                     +                                          +                                            +on cluster or single workers.                       +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+``plan_with_table_node_partitioning``+               ``Boolean``                +                  ``true``                  +**Experimental.** Adapt plan to pre-partitioned     +
+                                     +                                          +                                            +tables. By setting this property you allow to use   +
+                                     +                                          +                                            +partitioning provided by table layout itself while  +
+                                     +                                          +                                            +collecting required data. This may allow to utilize +
+                                     +                                          +                                            +optimization of table layout provided by specific   +
+                                     +                                          +                                            +connector. However it may only be utilized if given +
+                                     +                                          +                                            +projection uses all columns used for table          +
+                                     +                                          +                                            +partitioning inside connector. It can be            +
+                                     +                                          +                                            +particularly useful due to the I/O distribution     +
+                                     +                                          +                                            +optimization in table partitioning.                 +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+        ``execution_policy``         +``String`` (``all-at-once`` or ``phased``)+``query.execution-policy`` (``all-at-once``)+Setting this value to ``phased`` will allow query   +
+                                     +                                          +                                            +scheduler to split a single query execution between +
+                                     +                                          +                                            +different time slots. This will allow to switch     +
+                                     +                                          +                                            +context more often and possibly stage the partially +
+                                     +                                          +                                            +executed query in order to increase robustness.     +
+                                     +                                          +                                            +Average time of executing query may slightly        +
+                                     +                                          +                                            +increase after setting this to ``phased`` due to    +
+                                     +                                          +                                            +context switching and more complex scheduling       +
+                                     +                                          +                                            +algorithm but drop in variation of query execution  +
+                                     +                                          +                                            +time is expected.                                   +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+      ``hash_partition_count``       +               ``Integer``                + ``query.initial-hash-partitions`` (``8``)  +This value is used to determine how many nodes may  +
+                                     +                                          +                                            +share the same query when partitioning system is    +
+                                     +                                          +                                            +set to ``FIXED``. Manipulating this value will      +
+                                     +                                          +                                            +allow to distribute work between nodes properly.    +
+                                     +                                          +                                            +Value lower then number of presto nodes may lower   +
+                                     +                                          +                                            +the utilization of cluster in low traffic           +
+                                     +                                          +                                            +environment. Setting the number to to high value    +
+                                     +                                          +                                            +will cause assigning multiple partitions of same    +
+                                     +                                          +                                            +query to one node or ignoring the setting - in some +
+                                     +                                          +                                            +configuration the value is internally capped at     +
+                                     +                                          +                                            +number of available worker nodes.                   +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+        ``query_max_memory``         +          ``String`` (data size)          +      ``query.max-memory`` (``20 GB``)      +This property can be use to be nice to the cluster  +
+                                     +                                          +                                            +for example when our query is not as important then +
+                                     +                                          +                                            +the usual cluster routines. Setting this value to   +
+                                     +                                          +                                            +smaller then server property ``query.max-memory``   +
+                                     +                                          +                                            +will cause server to drop session query if it will  +
+                                     +                                          +                                            +require more then ``query_max_memory`` memory       +
+                                     +                                          +                                            +instead of ``query.max-memory``. On the other hand  +
+                                     +                                          +                                            +setting this value to higher then                   +
+                                     +                                          +                                            +``query.max-memory`` will not have effect at all.   +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+
+       ``query_max_run_time``        +          ``String`` (duration)           +     ``query.max-run-time`` (``100 d``)     +The default value of this is defined by server. If  +
+                                     +                                          +                                            +expected query processing time is higher then       +
+                                     +                                          +                                            +property ``query.max-run-time`` it's crucial to set +
+                                     +                                          +                                            +this session property - otherwise there is a risk   +
+                                     +                                          +                                            +of dropping all result of long processing after     +
+                                     +                                          +                                            +``query.max-run-time`` ends. Session may also set   +
+                                     +                                          +                                            +this value to lower than ``query.max-run-time`` in  +
+                                     +                                          +                                            +order to crosscheck for bugs in queries. In may be  +
+                                     +                                          +                                            +particularly useful when setting up session with    +
+                                     +                                          +                                            +very large number of queries each of which should   +
+                                     +                                          +                                            +take very short time in order to be able to end all +
+                                     +                                          +                                            +of queries in acceptable time. Even in this         +
+                                     +                                          +                                            +scenario it's crucial though, to set this value to  +
+                                     +                                          +                                            +much higher value than average query time to avoid  +
+                                     +                                          +                                            +problems with outliers (some queries may randomly   +
+                                     +                                          +                                            +take much longer then other due to cluster load and +
+                                     +                                          +                                            +many other circumstances).                          +
+-------------------------------------+------------------------------------------+--------------------------------------------+----------------------------------------------------+



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
