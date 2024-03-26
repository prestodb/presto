====================
Prestissimo Features
====================

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

* POST: v1/memory
  * Reports memory, but no assignments are adjusted unlike in Java workers.
* GET: v1/info
* GET: v1/status

The request/response flow of Prestissimo is identical to Java workers. The
tasks or new splits are registered via `TaskUpdateRequest`. Resource
utilization and query progress are sent to the coordinator via task endpoints.


Remote Function Execution
-------------------------

Prestissimo supports remote execution of scalar functions. This feature is
useful for cases when the function code is not written in C++, or if for
security or flexibility reasons the function code cannot be linked to the same
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

Prestissimo supports JWT authentication for internal communication.
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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``bool``
* **Default value:** ``true``
* **Presto on Spark default value:** ``false``

Enable periodic clean up of old tasks. This is ``true`` for Prestissimo,
however for Presto on Spark this defaults to ``false`` as zombie/stuck tasks
are handled by spark via speculative execution.

``old-task-cleanup-ms``
^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``integer``
* **Default value:** ``60000``

Duration after which a task should be considered as old and will be eligible
for cleanup. Only applicable when ``enable-old-task-cleanup`` is ``true``.
Old task is defined as a PrestoTask which has not received heartbeat for at least
``old-task-cleanup-ms``, or is not running and has an end time more than
``old-task-cleanup-ms`` ago.


Session Properties
------------------

Defines all system session properties supported by native worker to ensure
that they are the source of truth and to differentiate them from Java based
session properties.

The following are the native session properties kept as the source of truth in prestissimo.

``driver_cpu_time_slice_limit_ms``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``1000``

Native Execution only. The cpu time slice limit in ms that a driver thread.
If not ``zero``, can continuously run without yielding.
If it is ``zero``,then there is no limit.

``legacy_timestamp``
^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Use legacy TIME & TIMESTAMP semantics.
Warning: this will be removed.

``native_aggregation_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Native Execution only. The max memory that a final aggregation can use before spilling.
If it is ``0``, then there is no limit.

``native_debug_validate_output_from_operators``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

If set to ``true``, then during execution of tasks, the output vectors of every operator are validated for consistency.
This is an expensive check so should only be used for debugging.
It can help debug issues where malformed vector cause failures or crashes by helping identify which operator is generating them.

``native_join_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Native Execution only. Enable join spilling on native engine.

``native_join_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Native Execution only. The max memory that hash join can use before spilling.
If it is ``0``, then there is no limit.

``native_join_spiller_partition_bits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``2``

Native Execution only. The number of bits (N) used to calculate the spilling partition number for hash join and RowNumber: ``2`` ^ N.

``native_max_spill_file_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

The max allowed spill file size. If it is ``0``, then there is no limit.

``native_max_spill_level``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``4``

Native Execution only. The maximum allowed spilling level for hash join build.
0 is the initial spilling level, -1 means unlimited.

``native_order_by_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``0``

Native Execution only. The max memory that order by can use before spilling.
If it is ``0``, then there is no limit.

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

Native Execution only. The compression algorithm type to compress the spilled data.
Supported compression codecs are: ZLIB, SNAPPY, LZO, ZSTD, LZ4 and GZIP. ``none`` means no compression.

``native_spill_file_create_config``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``varchar``
* **Default value:** ``""``

Native Execution only. Config used to create spill files.
This config is provided to underlying file system and the config is free form.
The form should be defined by the underlying file system.

``native_spill_write_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``bigint``
* **Default value:** ``1048576``

Native Execution only. The maximum size in bytes to buffer the serialized spill data before writing to disk for IO efficiency.
If set to zero, buffering is disabled.

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
