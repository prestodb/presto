=============
Runtime Stats
=============

Runtime stats are used to collect the per-query velox runtime events for
offline query analysis purpose. The collected stats can provide insights into
the operator level query execution internals, such as how much time a query
operator spent in disk spilling. The collected stats are organized in a
free-form key-value for easy extension. The key is the event name and the
value is defined as RuntimeCounter which is used to store and aggregate a
particular event occurrences during the operator execution. RuntimeCounter has
three types: kNone used to record event count, kNanos used to record event time
in nanoseconds and kBytes used to record memory or storage size in bytes. It
records the count of events, and the min/max/sum of the event values. The stats
are stored in OperatorStats structure. The query system can aggregate the
operator level stats collected from each driver by pipeline and task for
analysis.

Memory Arbitration
------------------
These stats are reported by all operators.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - memoryReclaimCount
     -
     - The number of times that the memory arbitration to reclaim memory from
       an spillable operator.
       This stats only applies for spillable operators.
   * - memoryReclaimWallNanos
     - nanos
     - The memory reclaim execution time of an operator during the memory
       arbitration. It collects time spent on disk spilling or file write.
       This stats only applies for spillable operators.
   * - reclaimedMemoryBytes
     - bytes
     - The reclaimed memory bytes of an operator during the memory arbitration.
       This stats only applies for spillable operators.
   * - globalArbitrationCount
     -
     - The number of times a request for more memory hit the arbitrator's
       capacity limit and initiated a global arbitration attempt where
       memory is reclaimed from viable candidates chosen among all running
       queries based on a criterion.
   * - localArbitrationCount
     -
     - The number of times a request for more memory hit the query memory
       limit and initiated a local arbitration attempt where memory is
       reclaimed from the requestor itself.
   * - localArbitrationQueueWallNanos
     -
     - The time of an operator waiting in local arbitration queue.
   * - localArbitrationLockWaitWallNanos
     -
     - The time of an operator waiting to acquire the local arbitration lock.
   * - globalArbitrationLockWaitWallNanos
     -
     - The time of an operator waiting to acquire the global arbitration lock.

HashBuild, HashAggregation
--------------------------
These stats are reported only by HashBuild and HashAggregation operators.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - hashtable.capacity
     -
     - Number of slots across all buckets in the hash table.
   * - hashtable.numRehashes
     -
     - Number of rehash() calls.
   * - hashtable.numDistinct
     -
     - Number of distinct keys in the hash table.
   * - hashtable.numTombstones
     -
     - Number of tombstone slots in the hash table.
   * - hashtable.buildWallNanos
     - nanos
     - Time spent on building the hash table from rows collected by all the
       hash build operators. This stat is only reported by the HashBuild operator.

TableWriter
-----------
These stats are reported only by TableWriter operator

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - earlyFlushedRawBytes
     - bytes
     - Number of bytes pre-maturely flushed from file writers because of memory reclaiming.

Spilling
--------
These stats are reported by operators that support spilling.

.. list-table::
   :widths: 50 25 50
   :header-rows: 1

   * - Stats
     - Unit
     - Description
   * - spillFillWallNanos
     - nanos
     - The time spent on filling rows for spilling.
   * - spillSortWallNanos
     - nanos
     - The time spent on sorting rows for spilling.
   * - spillSerializationWallNanos
     - nanos
     - The time spent on serializing rows for spilling.
   * - spillFlushWallNanos
     - nanos
     - The time spent on copy out serialized rows for disk write. If compression
       is enabled, this includes the compression time.
   * - spillWrites
     -
     - The number of spill writer flushes, equivalent to number of write calls to
       underlying filesystem.
   * - spillWriteWallNanos
     - nanos
     - The time spent on writing spilled rows to disk.
   * - spillRuns
     -
     - The number of times that spilling runs on an operator.
   * - exceededMaxSpillLevel
     -
     - The number of times that an operator exceeds the max spill limit.
   * - spillReadBytes
     - bytes
     - The number of bytes read from spilled files.
   * - spillReads
     -
     - The number of spill reader reads, equivalent to the number of read calls to the underlying filesystem.
   * - spillReadWallNanos
     - nanos
     - The time spent on read data from spilled files.
   * - spillDeserializationWallNanos
     - nanos
     - The time spent on deserializing rows read from spilled files.
