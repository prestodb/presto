===================
Presto C++ Features
===================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Endpoints
---------

HTTP endpoints related to tasks are registered to Proxygen in
`TaskResource.cpp`. Important endpoints implemented include:

* POST: v1/task: This processes a `TaskUpdateRequest`
* GET: v1/task: This returns a serialized `TaskInfo` (used for comprehensive
  metrics, may be reported less frequently)
* GET: v1/task/status: This returns
  a serialized `TaskStatus` (used for query progress tracking, must be reported
  frequently)

Other HTTP endpoints include:

* POST: v1/memory: Reports memory, but no assignments are adjusted unlike in Java workers
* GET: v1/info/metrics: Returns worker level metrics in Prometheus Data format. Refer section `Worker Metrics Collection <#worker-metrics-collection>`_ for more info. Here is a sample Metrics data returned by this API.

   .. code-block:: text

      # TYPE presto_cpp_num_http_request counter
      presto_cpp_num_http_request{cluster="testing",worker=""} 0
      # TYPE presto_cpp_num_http_request_error counter
      presto_cpp_num_http_request_error{cluster="testing",worker=""} 0
      # TYPE presto_cpp_memory_pushback_count counter
      presto_cpp_memory_pushback_count{cluster="testing",worker=""} 0
      # TYPE velox_driver_yield_count counter
      velox_driver_yield_count{cluster="testing",worker=""} 0
      # TYPE velox_cache_shrink_count counter
      velox_cache_shrink_count{cluster="testing",worker=""} 0
      # TYPE velox_memory_cache_num_stale_entries counter
      velox_memory_cache_num_stale_entries{cluster="testing",worker=""} 0
      # TYPE velox_arbitrator_requests_count counter
      velox_arbitrator_requests_count{cluster="testing",worker=""} 0


* GET: v1/info: Returns basic information about the worker. Here is an example:

  .. code-block:: text

   {"coordinator":false,"environment":"testing","nodeVersion":{"version":"testversion"},"starting":false,"uptime":"49.00s"}

* GET: v1/status: Returns memory pool information.

The request/response flow of Presto C++ is identical to Java workers. The tasks or new splits are registered via `TaskUpdateRequest`. Resource utilization and query progress are sent to the coordinator via task endpoints.

Remote Function Execution
-------------------------

Presto C++ supports remote execution of scalar functions. This feature is
useful for cases when the function code is not written in C++, or if for
security or flexibility reasons, the function code cannot be linked to the same
executable as the main engine.

Remote function signatures need to be provided using a JSON file, following
the format implemented by `JsonFileBasedFunctionNamespaceManager
<https://github.com/prestodb/presto/blob/master/presto-function-namespace-managers/src/main/java/com/facebook/presto/functionNamespace/json/JsonFileBasedFunctionNamespaceManager.java>`_.
The following properties allow the configuration of remote function execution:

``remote-function-server.signature.files.directory.path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``""``

The local filesystem path where JSON files containing remote function
signatures are located. If not empty, the Presto native worker will
recursively search, open, parse, and register function definitions from
these JSON files.

``remote-function-server.catalog-name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``""``

The catalog name to be added as a prefix to the function names registered
in Velox. The function name pattern registered is
``catalog.schema.function_name``, where ``catalog`` is defined by this
parameter, and ``schema`` and ``function_name`` are read from the input
JSON file.

If empty, the function is registered as ``schema.function_name``.

``remote-function-server.serde``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``"presto_page"``

The serialization/deserialization method to use when communicating with
the remote function server. Supported values are ``presto_page`` or
``spark_unsafe_row``.

``remote-function-server.thrift.address``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``""``

The location (ip address or hostname) that hosts the remote function
server, if any remote functions were registered using
``remote-function-server.signature.files.directory.path``.
If not specified, falls back to the loopback interface (``::1``)

``remote-function-server.thrift.port``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

The port that hosts the remote function server. If not specified and remote
functions are trying to be registered, an exception is thrown.

``remote-function-server.thrift.uds-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``""``

The UDS (unix domain socket) path to communicate with a local remote
function server. If specified, takes precedence over
``remote-function-server.thrift.address`` and
``remote-function-server.thrift.port``.

JWT authentication support
--------------------------

C++ based Presto supports JWT authentication for internal communication.
For details on the generally supported parameters visit `JWT <../security/internal-communication.html#jwt>`_.

There is also an additional parameter:

``internal-communication.jwt.expiration-seconds``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``integer``
* **Default value:** ``300``

There is a time period between creating the JWT on the client
and verification by the server.
If the time period is less than or equal to the parameter value, the request
is valid.
If the time period exceeds the parameter value, the request is rejected as
authentication failure (HTTP 401).

Async Data Cache and Prefetching
--------------------------------

``connector.num-io-threads-hw-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``double``
* **Default value:** ``1.0``
* **Presto on Spark default value:** ``0.0``

Size of IO executor for connectors to do preload/prefetch.  Prefetch is
disabled if ``connector.num-io-threads-hw-multiplier`` is set to zero.

``async-data-cache-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``bool``
* **Default value:** ``true``
* **Presto on Spark default value:** ``false``

Whether async data cache is enabled.

``async-cache-ssd-gb``
^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``integer``
* **Default value:** ``0``

Size of the SSD cache when async data cache is enabled.

``enable-old-task-cleanup``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``bool``
* **Default value:** ``true``
* **Presto on Spark default value:** ``false``

Enable periodic clean up of old tasks. The default value is ``true`` for Presto C++.
For Presto on Spark this property defaults to ``false``, as zombie or stuck tasks
are handled by Spark by speculative execution.

``old-task-cleanup-ms``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``integer``
* **Default value:** ``60000``

Duration after which a task should be considered as old and will be eligible
for cleanup. Only applicable when ``enable-old-task-cleanup`` is ``true``.
Old task is defined as a PrestoTask which has not received heartbeat for at least
``old-task-cleanup-ms``, or is not running and has an end time more than
``old-task-cleanup-ms`` ago.

Worker metrics collection
-------------------------

Users can enable collection of worker level metrics by setting the property:

``runtime-metrics-collection-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* **Type:** ``boolean``
* **Default value:** ``false``

  When true, the default behavior is a no-op. There is a prior setup that must be done before enabling this flag. To enable
  metrics collection in Prometheus Data Format refer `here <https://github.com/prestodb/presto/tree/master/presto-native-execution#build-prestissimo>`_.

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
