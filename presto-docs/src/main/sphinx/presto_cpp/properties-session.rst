=============================
Presto C++ Session Properties
=============================

Session Properties
------------------

The following are the native session properties for C++ based Presto.

``driver_cpu_time_slice_limit_ms``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``1000``

Native Execution only. Defines the maximum CPU time in milliseconds that a driver thread
is permitted to run before it must yield to other threads,facilitating fair CPU usage across
multiple threads.

A positive value enforces this limit, ensuring threads do not monopolize CPU resources.

Negative values are considered invalid and are treated as a request to use the system default setting,
which is ``1000`` ms in this case.

Note: Setting the property to ``0`` allows a thread to run indefinitely
without yielding, which is not recommended in a shared environment as it can lead to
resource contention.

``legacy_timestamp``
^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Use legacy TIME and TIMESTAMP semantics.

``native_aggregation_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Native Execution only. Specifies the maximum memory in bytes
that a final aggregation operation can utilize before it starts spilling to disk.
If set to ``0``, there is no limit, allowing the aggregation to consume unlimited memory resources,
which may impact system performance.

``native_debug_validate_output_from_operators``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, then during the execution of tasks, the output vectors of every operator are validated for consistency.
It can help identify issues where a malformed vector causes failures or crashes, facilitating the debugging of operator output issues.

Note: This is an expensive check and should only be used for debugging purposes.

``native_debug_disable_expression_with_peeling``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, disables the optimization in expression evaluation to peel common dictionary layer from inputs.

This should only be used for debugging purposes.

``native_debug_disable_common_sub_expressions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, disables the optimization in expression evaluation to reuse cached results for common sub-expressions.

This should only be used for debugging purposes.

``native_debug_disable_expression_with_memoization``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, disables the optimization in expression evaluation to reuse cached results between subsequent
input batches that are dictionary encoded and have the same alphabet(underlying flat vector).

This should only be used for debugging purposes.

``native_debug_disable_expression_with_lazy_inputs``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, disables the optimization in expression evaluation to delay loading of lazy inputs unless required.

This should only be used for debugging purposes.

``native_selective_nimble_reader_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Temporary flag to control whether selective Nimble reader should be used in this
query or not.  

``native_join_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Enable join spilling on native engine.

``native_join_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Native Execution only. Specifies the maximum memory, in bytes, that a hash join operation can use before starting to spill to disk.
A value of ``0`` indicates no limit, permitting the join operation to use unlimited memory resources, which might affect overall system performance.

``native_join_spiller_partition_bits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``2``

Native Execution only. Specifies the number of bits (N)
used to calculate the spilling partition number for hash join and RowNumber operations.
The partition number is determined as ``2`` raised to the power of N, defining how data is partitioned during the spill process.

``native_max_spill_file_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Specifies the maximum allowed spill file size in bytes. If set to ``0``, there is no limit on the spill file size,
allowing spill files to grow as large as necessary based on available disk space.
Use ``native_max_spill_file_size`` to manage disk space usage during operations that require spilling to disk.

``native_max_spill_level``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``4``

Native Execution only. The maximum allowed spilling level for hash join build.
``0`` is the initial spilling level, ``-1`` means unlimited.

``native_order_by_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Native Execution only. Specifies the maximum memory, in bytes, that the `ORDER BY` operation can utilize before starting to spill data to disk.
If set to ``0``, there is no limit on memory usage, potentially leading to large memory allocations for sorting operations.
Use this threshold to manage memory usage more efficiently during `ORDER BY` operations.

``native_row_number_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Enable row number spilling on native engine.

``native_simplified_expression_evaluation_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Native Execution only. Enable simplified path in expression evaluation.

``native_expression_max_array_size_in_reduce``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``100000``

Native Execution only. The `reduce <https://prestodb.io/docs/current/functions/array.html#reduce-array-T-initialState-S-inputFunction-S-T-S-outputFunction-S-R-R>`_ 
function will throw an error if it encounters an array of size greater than this value.

``native_spill_compression_codec``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``none``

Native Execution only. Specifies the compression CODEC used to compress spilled data.
Supported compression CODECs are: ZLIB, SNAPPY, LZO, ZSTD, LZ4, and GZIP.
Setting this property to ``none`` disables compression.

``native_spill_file_create_config``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``""``

Native Execution only. Specifies the configuration parameters used to create spill files.
These parameters are provided to the underlying file system, allowing for customizable spill file creation based on the requirements of the environment.
The format and options of these parameters are determined by the capabilities of the underlying file system
and may include settings such as file location, size limits, and file system-specific optimizations.

``native_spill_write_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``1048576``

Native Execution only. The maximum size in bytes to buffer the serialized spill data before writing to disk for IO efficiency.
If set to ``0``, buffering is disabled.

``native_topn_row_number_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Enable topN row number spilling on native engine.

``native_window_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Enable window spilling on native engine.

``native_writer_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Enable writer spilling on native engine.

``native_writer_flush_threshold_bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``100663296``

Minimum memory footprint size required to reclaim memory from a file writer by flushing its buffered data to disk.
Default is 96MB.

``native_max_output_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``33554432``

The maximum size in bytes for the task's buffered output. The buffer is shared among all drivers. Default is 32MB.

``native_max_page_partitioning_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``33554432``

The maximum bytes to buffer per PartitionedOutput operator to avoid creating tiny SerializedPages.
For PartitionedOutputNode::Kind::kPartitioned, PartitionedOutput operator would buffer up to that number of
bytes / number of destinations for each destination before producing a SerializedPage. Default is 32MB.

``native_max_local_exchange_partition_count``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``4294967296``

Maximum number of partitions created by a local exchange.
Affects concurrency for pipelines containing LocalPartitionNode.


``native_spill_prefixsort_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enable the prefix sort or fallback to std::sort in spill. The prefix sort is
faster than std::sort but requires the memory to build normalized prefix
keys, which might have potential risk of running out of server memory.

``native_prefixsort_normalized_key_max_bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``128``

Maximum number of bytes to use for the normalized key in prefix-sort.
Use ``0`` to disable prefix-sort.

``native_prefixsort_min_rows``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``130``

Minimum number of rows to use prefix-sort.
The default value has been derived using micro-benchmarking.