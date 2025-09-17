===================================
Presto C++ Configuration Properties
===================================

This section describes Presto C++ configuration properties.

The following is not a complete list of all configuration properties,
and does not include any connector-specific catalog configuration properties
or session properties.

For information on catalog configuration properties, see :doc:`Connectors </connector/>`.

For information on Presto C++ session properties, see :doc:`properties-session`.

NOTE: While some of the configuration properties below with "-gb" in their names
show gigabytes (gB; 1 gB equals 1000000000 B), it is actually
gibibytes (GiB; 1 GiB equals 1073741824 B).

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

These Presto coordinator configuration properties are described here, in
alphabetical order.

``driver.max-split-preload``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``integer``
* **Default value:** ``2``

  Maximum number of splits to preload per driver.
  Set to 0 to disable preloading.

``driver.cancel-tasks-with-stuck-operators-threshold-ms``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``240000`` (40 minutes)

  Cancels any task when at least one operator has been stuck for at
  least the time specified by this threshold.

  Set this property to ``0`` to disable canceling.

``native-execution-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

  This property is required when running Presto C++ workers because of
  underlying differences in behavior from Java workers.

``native-execution-type-rewrite-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

  When set to ``true``:
    - Custom type names are peeled in the coordinator. Only the actual base type is preserved.
    - ``CAST(col AS EnumType<T>)`` is rewritten as ``CAST(col AS <T>)``.
    - ``ENUM_KEY(EnumType<T>)`` is rewritten as ``ELEMENT_AT(MAP(<T>, VARCHAR))``.

  This property can only be enabled with native execution.

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

``presto.default-namespace``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``presto.default``

  Specifies the namespace prefix for native C++ functions.

Worker Properties
-----------------

The configuration properties of Presto C++ workers are described here, in alphabetical order.

``runtime-metrics-collection-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``boolean``
* **Default value:** ``false``

  Enables collection of worker level metrics.

``task.max-drivers-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``number of concurrent threads supported by the host``

  Number of drivers to use per task. Defaults to the number of concurrent
  threads supported by the host.

``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``4GB``

  Max memory usage for each query.


``system-memory-gb``
^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``57``

  Memory allocation limit enforced by an internal memory allocator. It consists of two parts:
  1) Memory used by the queries as specified in ``query-memory-gb``; 2) Memory used by the
  system, such as disk spilling and cache prefetch.

  Set ``system-memory-gb`` to about 90% of available machine memory of the deployment.
  This allows some buffer room to handle unaccounted memory in order to prevent out-of-memory conditions.
  The default value of 57 gb is calculated based on available machine memory of 64 gb.


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


``spill-enabled``
^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Try spilling memory to disk to avoid exceeding memory limits for the query.

Spilling works by offloading memory to disk. This process can allow a query with a large memory
footprint to pass at the cost of slower execution times. Currently, spilling is supported only for
aggregations and joins (inner and outer), so this property will not reduce memory usage required for
window functions, sorting and other join types.


``join-spill-enabled``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for joins to
avoid exceeding memory limits for the query.


``aggregation-spill-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for aggregations to
avoid exceeding memory limits for the query.


``order-by-spill-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for order by to
avoid exceeding memory limits for the query.

``local-exchange.max-partition-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``65536`` (64KB)

  Specifies the maximum size in bytes to accumulate for a single partition of a local exchange before flushing.


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

``shared-arbitrator.max-memory-arbitration-time``
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

``shared-arbitrator.memory-pool-abort-capacity-limit``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``1GB``

  Specifies the starting memory capacity limit for global arbitration to
  search for victim participant to reclaim used memory by abort. For
  participants with capacity larger than the limit, the global arbitration
  chooses to abort the youngest participant which has the largest
  participant id. This helps to let the old queries to run to completion.
  The abort capacity limit is reduced by half if could not find a victim
  participant until this reaches to zero.

  NOTE: the limit value must be either zero, or a power of 2.

``shared-arbitrator.memory-pool-min-reclaim-bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``128MB``

  Specifies the minimum bytes to reclaim from a participant at a time. The
  global arbitration also avoids reclaiming from a participant if its
  reclaimable used capacity is less than this threshold. This is to
  prevent inefficient memory reclaim operations on a participant with
  small reclaimable used capacity, which could cause a large number of
  small spilled files on disk.

``shared-arbitrator.memory-reclaim-threads-hw-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``0.5``

  Floating point number used in calculating how many threads to use
  for memory reclaim execution: hw_concurrency x multiplier. 0.5 is
  default.

``shared-arbitrator.global-arbitration-memory-reclaim-pct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``10``

  If not zero, specifies the minimum amount of memory to reclaim by global
  memory arbitration as percentage of total arbitrator memory capacity.

``shared-arbitrator.global-arbitration-abort-time-ratio``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``0.5``

  The ratio used with ``shared-arbitrator.memory-reclaim-max-wait-time``,
  beyond which global arbitration will no longer reclaim memory by
  spilling, but instead directly abort. It is only in effect when
  ``global-arbitration-enabled`` is ``true``.

``shared-arbitrator.global-arbitration-without-spill``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``false``

  If ``true``, global arbitration does not reclaim memory by spilling, but
  only by aborting. This flag is only effective if
  ``shared-arbitrator.global-arbitration-enabled`` is ``true``.

Cache Properties
----------------

The configuration properties of AsyncDataCache and SSD cache are described here.

``async-data-cache-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  In-memory cache.

``async-cache-ssd-gb``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

  The size of the SSD. Unit is in GiB (gibibytes).

``async-cache-ssd-path``
^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``/mnt/flash/async_cache.``

  The path of the directory that is mounted onto the SSD.

``async-cache-max-ssd-write-ratio``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``double``
* **Default value:** ``0.7``

  The maximum ratio of the number of in-memory cache entries written to the SSD cache
  over the total number of cache entries. Use this to control SSD cache write rate,
  once the ratio exceeds this threshold then we stop writing to the SSD cache.

``async-cache-ssd-savable-ratio``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``double``
* **Default value:** ``0.125``

  The min ratio of SSD savable (in-memory) cache space over the total cache space.
  Once the ratio exceeds this limit, we start writing SSD savable cache entries
  into SSD cache.

``async-cache-min-ssd-savable-bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``integer``
* **Default value:** ``16777216``

  Min SSD savable (in-memory) cache space to start writing SSD savable cache entries into SSD cache.

  The default value ``16777216`` is 16 MB.

  NOTE: we only write to SSD cache when both ``async-cache-max-ssd-write-ratio`` and
  ``async-cache-ssd-savable-ratio`` conditions are satisfied.

``async-cache-persistence-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``0s``

  The interval for persisting in-memory cache to SSD. Set this configuration to a non-zero value to
  activate periodic cache persistence.

  The following time units are supported:

  ns, us, ms, s, m, h, d

``async-cache-ssd-disable-file-cow``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``bool``
* **Default value:** ``false``

  In file systems such as btrfs that support cow (copy on write), the SSD cache can use all of the SSD
  space and stop working. To prevent that, use this option to disable cow for cache files.

``ssd-cache-checksum-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``bool``
* **Default value:** ``false``

  When enabled, a CRC-based checksum is calculated for each cache entry written to SSD.
  The checksum is stored in the next checkpoint file.

``ssd-cache-read-verification-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``bool``
* **Default value:** ``false``

  When enabled, the checksum is recalculated and verified against the stored value when
  cache data is loaded from the SSD.

``cache.velox.ttl-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``bool``
* **Default value:** ``false``

  Enable TTL for AsyncDataCache and SSD cache.

``cache.velox.ttl-threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``2d``

  TTL duration for AsyncDataCache and SSD cache entries.

  The following time units are supported:

  ns, us, ms, s, m, h, d

``cache.velox.ttl-check-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``string``
* **Default value:** ``1h``

  The periodic duration to apply cache TTL and evict AsyncDataCache and SSD cache entries.

Exchange Properties
-------------------

``exchange.http-client.request-data-sizes-max-wait-sec``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``10``

  Maximum wait time for exchange request in seconds.

Memory Checker Properties
-------------------------

The LinuxMemoryChecker extends from PeriodicMemoryChecker and is used for Linux systems only.
The LinuxMemoryChecker can be enabled by setting the CMake flag ``PRESTO_MEMORY_CHECKER_TYPE=LINUX_MEMORY_CHECKER``.
The following properties for PeriodicMemoryChecker are as follows:

``system-mem-pushback-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, starts memory limit checker to trigger memory pushback when
server is under low memory pressure.

``system-mem-limit-gb``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``60``

Specifies the system memory limit that triggers the memory pushback or heap dump if
the server memory usage is beyond this limit. A value of zero means no limit is set.
This only applies if ``system-mem-pushback-enabled`` is ``true``.
Set ``system-mem-limit-gb`` to be greater than or equal to system-memory-gb but not
higher than the available machine memory of the deployment.
The default value of 60 gb is calculated based on available machine memory of 64 gb.

``system-mem-shrink-gb``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``8``

Specifies the amount of memory to shrink when the memory pushback is
triggered. This only applies if ``system-mem-pushback-enabled`` is ``true``.

``system-mem-pushback-abort-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If true, memory pushback will abort queries with the largest memory usage under
low memory condition. This only applies if ``system-mem-pushback-enabled`` is ``true``.

``worker-overloaded-threshold-mem-gb``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Memory threshold in GB above which the worker is considered overloaded in terms of
memory use. Ignored if zero.

``worker-overloaded-threshold-cpu-pct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

CPU threshold in % above which the worker is considered overloaded in terms of
CPU use. Ignored if zero.

``worker-overloaded-threshold-num-queued-drivers-hw-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``double``
* **Default value:** ``0.0``

Floating point number used in calculating how many drivers must be queued
for the worker to be considered overloaded.
Number of drivers is calculated as hw_concurrency x multiplier. Ignored if zero.

``worker-overloaded-cooldown-period-sec``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``5``

Specifies how many seconds worker has to be not overloaded (in terms of
memory and CPU) before its status changes to not overloaded.
This is to prevent spiky fluctuation of the overloaded status.

``worker-overloaded-task-queuing-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If true, the worker starts queuing new tasks when overloaded, and
starts them gradually when it stops being overloaded.

Environment Variables As Values For Worker Properties
-----------------------------------------------------

This section applies to worker configurations in the ``config.properties`` file
and catalog property files only.

The value in a key-value pair can reference an environment variable by using
a leading `$` followed by enclosing the environment variable name in brackets (`{}`).

``key=${ENV_VAR_NAME}``

The environment variable name must match exactly with the defined variable.

This allows a worker to read sensitive data such as access keys from an
environment variable rather than having the actual value hard coded in a configuration
file on disk, improving the security of deployments.

For example, consider the hive connector's ``hive.s3.aws-access-key`` property.
This is sensitive data and can be stored in an environment variable such as
``AWS_S3_ACCESS_KEY`` which is set to the actual access key value.

One mechanism is to create a preload library that is injected at the time
presto_server is started that decrypts encrypted secrets and sets environment
variables specific to the presto_server process. These can then be referenced
in the properties.

Once decrypted the preloaded library sets the ``AWS_S3_ACCESS_KEY``
environment variable which then can be accessed by providing it in the catalog properties:

``hive.s3.aws-access-key=${AWS_S3_ACCESS_KEY}``
