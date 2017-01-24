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
