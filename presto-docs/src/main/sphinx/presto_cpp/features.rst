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

LinuxMemoryChecker
------------------

The LinuxMemoryChecker extends from PeriodicMemoryChecker and periodically checks 
memory usage using memory calculation from inactive_anon + active_anon in the memory stat 
file from Linux cgroups V1 or V2. The LinuxMemoryChecker is used for Linux systems only.

The LinuxMemoryChecker can be enabled by setting the CMake flag ``PRESTO_MEMORY_CHECKER_TYPE=LINUX_MEMORY_CHECKER``. 

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

``query-data-cache-enabled-default``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``bool``
* **Default value:** ``true``

If ``true``, SSD cache is enabled by default and is disabled only if
``node_selection_strategy`` is present and set to ``NO_PREFERENCE``.
Otherwise, SSD cache is disabled by default and is enabled if
``node_selection_strategy`` is present and set to ``SOFT_AFFINITY``.

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