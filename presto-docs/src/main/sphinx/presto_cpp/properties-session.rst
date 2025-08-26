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

``native_debug_memory_pool_name_regex``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``""``

Native Execution only. Regular expression pattern to match memory pool names for allocation callsite tracking.
Matched pools will also perform leak checks at destruction. Empty string disables tracking.

This should only be used for debugging purposes.

``native_debug_memory_pool_warn_threshold_bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``0``

Native Execution only. Warning threshold for memory pool allocations. Logs callsites when exceeded.
Requires allocation tracking to be enabled with ``native_debug_memory_pool_name_regex``.
Accepts B/KB/MB/GB units. Set to 0B to disable.

This should only be used for debugging purposes.

``native_execution_type_rewrite_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

When set to ``true``:
  - Custom type names are peeled in the coordinator. Only the actual base type is preserved.
  - ``CAST(col AS EnumType<T>)`` is rewritten as ``CAST(col AS <T>)``.
  - ``ENUM_KEY(EnumType<T>)`` is rewritten as ``ELEMENT_AT(MAP(<T>, VARCHAR))``.

This property can only be enabled with native execution.

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``100000``

Native Execution only. The `reduce <https://prestodb.io/docs/current/functions/array.html#reduce-array-T-initialState-S-inputFunction-S-T-S-outputFunction-S-R-R>`_
function will throw an error if it encounters an array of size greater than this value.

``native_expression_max_compiled_regexes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``100``

Native Execution only. Controls maximum number of compiled regular expression patterns per
regular expression function instance per thread of execution.

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

``native_op_trace_directory_create_config``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``""``

Native Execution only. Config used to create operator trace directory. This config is provided
to underlying file system and the config is free form. The form should be defined by the
underlying file system.

``native_query_trace_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enable query tracing. After enabled, trace data will be generated with query execution, and
can be used by TraceReplayer. It needs to be used together with native_query_trace_node_id,
native_query_trace_max_bytes, native_query_trace_fragment_id, and native_query_trace_shard_id
to match the task to be traced.


``native_query_trace_dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``""``

The location to store the trace files.

``native_query_trace_node_id``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``""``

The plan node id whose input data will be traced.

``native_query_trace_max_bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

The max trace bytes limit. Tracing is disabled if zero.

``native_query_trace_fragment_id``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``.*``

The fragment id to be traced. If not specified, all fragments will be matched.

``native_query_trace_shard_id``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``.*``

The shard id to be traced. If not specified, all shards will be matched.

``native_scaled_writer_rebalance_max_memory_usage_ratio``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``double``
* **Minimum value:** ``0``
* **Maximum value:** ``1``
* **Default value:** ``0.7``

The max ratio of a query used memory to its max capacity, and the scale
writer exchange stops scaling writer processing if the query's current
memory usage exceeds this ratio. The value is in the range of (0, 1].

``native_scaled_writer_max_partitions_per_writer``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``128``

The max number of logical table partitions that can be assigned to a
single table writer thread. The logical table partition is used by local
exchange writer for writer scaling, and multiple physical table
partitions can be mapped to the same logical table partition based on the
hash value of calculated partitioned ids.

``native_scaled_writer_min_partition_processed_bytes_rebalance_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``134217728``

Minimum amount of data processed by a logical table partition to trigger
writer scaling if it is detected as overloaded by scale writer exchange.

``native_scaled_writer_min_processed_bytes_rebalance_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``268435456``

Minimum amount of data processed by all the logical table partitions to
trigger skewed partition rebalancing by scale writer exchange.

``native_table_scan_scaled_processing_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, enables scaling the table scan concurrency on each worker.

``native_table_scan_scale_up_memory_usage_ratio``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``double``
* **Minimum value:** ``0``
* **Maximum value:** ``1``
* **Default value:** ``0.7``

Controls the ratio of available memory that can be used for scaling up table scans.
A higher value allows more memory to be allocated for scaling up table scans,
while a lower value limits the amount of memory used.

``native_streaming_aggregation_min_output_batch_rows``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

In streaming aggregation, wait until there are enough output rows
to produce a batch of the size specified by this property. If set to ``0``, then
``Operator::outputBatchRows`` is used as the minimum number of output batch rows.

``native_request_data_sizes_max_wait_sec``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``10``

Maximum wait time for exchange long poll requests in seconds.

``native_query_memory_reclaimer_priority``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``2147483647``

Priority of the query in the memory pool reclaimer. Lower value means higher priority.
This is used in global arbitration victim selection.

``native_max_num_splits_listened_to``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Maximum number of splits to listen to by the SplitListener per table scan node per
native worker.

``native_index_lookup_join_max_prefetch_batches``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Specifies the max number of input batches to prefetch to do index lookup ahead.
If it is zero, then process one input batch at a time.

``native_index_lookup_join_split_output``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

If this is true, then the index join operator might split output for each input
batch based on the output batch size control. Otherwise, it tries to produce a
single output for each input batch.

``native_unnest_split_output``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

If this is true, then the unnest operator might split output for each input
batch based on the output batch size control. Otherwise, it produces a single
output for each input batch.
