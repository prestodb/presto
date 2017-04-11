=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you if your cluster is facing a specific performance problem.

Config Properties
-----------------

These configuration options may require tuning in specific situations:

* ``task.max-worker-threads``:
  Sets the number of threads used by workers to process splits. Increasing this number
  can improve throughput if worker CPU utilization is low and all the threads are in use,
  but will cause increased heap space usage. The number of active threads is available via
  the ``com.facebook.presto.execution.TaskExecutor.RunningSplits`` JMX stat.

* ``distributed-joins-enabled``:
  Use hash distributed joins instead of broadcast joins. Distributed joins
  require redistributing both tables using a hash of the join key. This can
  be slower (sometimes substantially) than broadcast joins, but allows much
  larger joins. Broadcast joins require that the tables on the right side of
  the join fit in memory on each machine, whereas with distributed joins the
  tables on the right side have to fit in distributed memory. This can also be
  specified on a per-query basis using the ``distributed_join`` session property.

* ``node-scheduler.network-topology``:
  Sets the network topology to use when scheduling splits. "legacy" will ignore
  the topology when scheduling splits. "flat" will try to schedule splits on the same
  host as the data is located by reserving 50% of the work queue for local splits.

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
    -XX:+PrintReferenceGC
    -XX:+PrintClassHistogramAfterFullGC
    -XX:+PrintClassHistogramBeforeFullGC
    -XX:PrintFLSStatistics=2
    -XX:+PrintAdaptiveSizePolicy
    -XX:+PrintSafepointStatistics
    -XX:PrintSafepointStatisticsCount=1
