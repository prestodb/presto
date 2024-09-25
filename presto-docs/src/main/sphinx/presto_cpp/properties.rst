===============================
Presto C++ Properties Reference
===============================

This section describes Presto C++ configuration properties.

The following is not a complete list of all configuration and
session properties, and does not include any connector-specific
catalog configuration properties. For information on catalog 
configuration properties, see :doc:`Connectors </connector/>`.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Coordinator Properties
----------------------

Set the following configuration properties for the Presto coordinator exactly 
as they are shown in this code block to enable the Presto coordinator's use of 
Presto C++ workers. 

.. code-block:: none

    native-execution-enabled=true
    optimizer.optimize-hash-generation=false
    regex-library=RE2J
    use-alternative-function-signatures=true
    experimental.table-writer-merge-operator-enabled=false

These Presto coordinator configuration properties are described here, in 
alphabetical order. 

``driver.cancel-tasks-with-stuck-operators-threshold-ms``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``240000`` (40 minutes)

  Cancels any task when at least one operator has been stuck for at 
  least the time specified by this threshold.
  
  Set this property to ``0`` to disable canceling.

``experimental.table-writer-merge-operator-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  Merge TableWriter output before sending to TableFinishOperator. This property must be set to 
  ``false``. 

``native-execution-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

  This property is required when running Presto C++ workers because of 
  underlying differences in behavior from Java workers.

``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  Set this property to ``false`` when running Presto C++ workers.  
  Velox does not support optimized hash generation, instead using a HashTable 
  with adaptive runtime optimizations that does not use extra hash fields. 

``regex-library``
^^^^^^^^^^^^^^^^^

* **Type:** ``type``
* **Allowed values:** ``RE2J``
* **Default value:** ``JONI``

  Only `RE2J <https://github.com/google/re2j>`_ is currently supported by Velox.

``use-alternative-function-signatures``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

  Some aggregation functions use generic intermediate types which are 
  not compatible with Velox aggregation function intermediate types. One  
  example function is ``approx_distinct``, whose intermediate type is 
  ``VARBINARY``. 
  This property provides function signatures for built-in aggregation 
  functions which are compatible with Velox.

Worker Properties
-----------------

The configuration properties of Presto C++ workers are described here, in alphabetical order. 

``async-cache-persistence-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``0s``

  The interval for persisting in-memory cache to SSD. Setting this config
  to a non-zero value will activate periodic cache persistence.

``async-data-cache-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  In-memory cache.

``runtime-metrics-collection-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``boolean``
* **Default value:** ``false``

  Enables collection of worker level metrics.

``task.max-drivers-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``number of hardware CPUs``

  Number of drivers to use per task. Defaults to hardware CPUs.

``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``4GB``

  Max memory usage for each query.


``system-memory-gb``
^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``40``

  Memory allocation limit enforced by an internal memory allocator. It consists of two parts:
  1) Memory used by the queries as specified in ``query-memory-gb``; 2) Memory used by the
  system, such as disk spilling and cache prefetch.

  Set ``system-memory-gb`` to the available machine memory of the deployment.


``query-memory-gb``
^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``38``

  Specifies the total amount of memory in GB that can be used for all queries on a
  worker node. Memory for system usage such as disk spilling and cache prefetch are
  not counted in it.

``max_spill_bytes``
^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``100UL << 30``

  Specifies the max spill bytes limit set for each query. This is used to cap the
  storage used for spilling. If it is zero, then there is no limit and spilling
  might exhaust the storage or takes too long to run.

``shared-arbitrator.reserved-capacity``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``4GB``

  Specifies the total amount of memory in GB reserved for the queries on
  a worker node. A query can only allocate from this reserved space if
  1) the non-reserved space in ``query-memory-gb`` is used up; and 2) the amount
  it tries to get is less than ``shared-arbitrator.memory-pool-reserved-capacity``.

``shared-arbitrator.memory-pool-initial-capacity``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``128MB``

  The initial memory pool capacity in bytes allocated on creation.

``shared-arbitrator.global-arbitration-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``false``

  If true, it allows shared arbitrator to reclaim used memory across query
  memory pools.

``shared-arbitrator.memory-pool-reserved-capacity``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``64MB``

  The amount of memory in bytes reserved for each query memory pool. When
  a query tries to allocate memory from the reserved space whose size is
  specified by ``shared-arbitrator.reserved-capacity``, it cannot allocate
  more than the value specified in ``shared-arbitrator.memory-pool-reserved-capacity``.

``shared-arbitrator.memory-pool-transfer-capacity``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``32MB``

  The minimal memory capacity in bytes transferred between memory pools
  during memory arbitration.

``shared-arbitrator.memory-reclaim-max-wait-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``5m``

  Specifies the max time to wait for memory reclaim by arbitration. The
  memory reclaim might fail if the max wait time has exceeded. If it is
  zero, then there is no timeout.

``shared-arbitrator.fast-exponential-growth-capacity-limit``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``512MB``

  When shared arbitrator grows memory pool's capacity, the growth bytes will
  be adjusted in the following way:

  * If 2 * current capacity is less than or equal to
    ``shared-arbitrator.fast-exponential-growth-capacity-limit``, grow
    through fast path by at least doubling the current capacity, when
    conditions allow (see below NOTE section).
  * If 2 * current capacity is greater than
    ``shared-arbitrator.fast-exponential-growth-capacity-limit``, grow
    through slow path by growing capacity by at least
    ``shared-arbitrator.slow-capacity-grow-pct`` * current capacity if
    allowed (see below NOTE section).

  NOTE: If original requested growth bytes is larger than the adjusted
  growth bytes or adjusted growth bytes reaches max capacity limit, the
  adjusted growth bytes will not be respected.

  NOTE: Capacity growth adjust is only enabled if both
  ``shared-arbitrator.fast-exponential-growth-capacity-limit`` and
  ``shared-arbitrator.slow-capacity-grow-pct`` are set, otherwise it is
  disabled.

``shared-arbitrator.slow-capacity-grow-pct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``0.25``

  See description for ``shared-arbitrator.fast-exponential-growth-capacity-limit``

``shared-arbitrator.memory-pool-min-free-capacity``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``128MB``

  When shared arbitrator shrinks memory pool's capacity, the shrink bytes
  will be adjusted in a way such that AFTER shrink, the stricter (whichever
  is smaller) of the following conditions is met, in order to better fit the
  pool's current memory usage:

  * Free capacity is greater or equal to capacity *
    ``shared-arbitrator.memory-pool-min-free-capacity-pct``
  * Free capacity is greater or equal to
    ``shared-arbitrator.memory-pool-min-free-capacity``

  NOTE: In the conditions when original requested shrink bytes ends up
  with more free capacity than above two conditions, the adjusted shrink
  bytes is not respected.

  NOTE: Capacity shrink adjustment is enabled when both
  ``shared-arbitrator.memory-pool-min-free-capacity-pct`` and
  ``shared-arbitrator.memory-pool-min-free-capacity`` are set.

``shared-arbitrator.memory-pool-min-free-capacity-pct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``0.25``

  See description for ``shared-arbitrator.memory-pool-min-free-capacity``
