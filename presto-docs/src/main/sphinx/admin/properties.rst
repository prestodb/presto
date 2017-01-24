====================
Properties Reference
====================

This section describes the most important config properties that
may be used to tune Presto or alter its behavior when required.

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


Tasks managment properties
--------------------------

``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``Node CPUs * 2``

    Sets the number of threads used by workers to process splits. Increasing this number
    can improve throughput if worker CPU utilization is low and all the threads are in use,
    but will cause increased heap space usage. Setting the value too high may cause a drop
    in performance due to a context switching. The number of active threads is available
    via the ``com.facebook.presto.execution.executor.TaskExecutor.RunningSplits`` JMX stat.


Node Scheduler Properties
-------------------------

``node-scheduler.network-topology``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Allowed values:** ``legacy``, ``flat``
    * **Default value:** ``legacy``

    Sets the network topology to use when scheduling splits. ``legacy`` will ignore
    the topology when scheduling splits. ``flat`` will try to schedule splits on the host
    where the data is located by reserving 50% of the work queue for local splits.
    It is recommended to use ``flat`` for clusters where distributed storage runs on
    the same nodes as Presto workers.
