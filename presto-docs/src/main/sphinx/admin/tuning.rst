=============
Tuning Presto
=============

The default Presto settings should work well for most workloads. The following
information may help you if your cluster is facing a specific performance problem.

When setting up a cluster for a specific workload it may be necessary to adjust the
following properties to ensure optimal performance:

  * :ref:`distributed-joins-enabled <tuning-pref-general>`
  * :ref:`query.max-memory <tuning-pref-query>`
  * :ref:`query.max-memory-per-node <tuning-pref-query>`
  * :ref:`query.initial-hash-partitions <tuning-pref-query>`
  * :ref:`task.concurrency <tuning-pref-task>`

Those and other Presto properties are described in :doc:`properties article<properties>`.

As an example, on a 11-node cluster dedicated to Presto (1 Coordinator + 10 Workers) with 8-core CPU and 128GB of RAM per node, you might want to start tuning with following values:

  * `query.max-memory = 200GB`
  * `query.max-memory-per-node = 32GB`
  * `query.initial-hash-partitions = 10`
  * `task.concurrency = 8`

If this guide does not suit your needs, You may look for more tuning options on
:doc:`/admin/properties` page.

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
