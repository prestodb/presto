===========================
Presto Native - Prestissimo
===========================

Prestissimo is a C++ drop-in replacement for Presto workers based on the Velox
library. It implements the same RESTful endpoints as Java workers using the
Proxygen C++ HTTP framework. Since communication with the Java coordinator and
across workers is only done using the REST endpoints, Prestissimo does not use
JNI and it is commonly deployed in such a way as to avoid having a JVM process
on worker nodes.

Prestissimo's codebase is located at `presto-native-execution
<https://github.com/prestodb/presto/tree/master/presto-native-execution>`_.


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

Whether async data cache is enabled.  Setting ``async-data-cache-enabled``
to ``false`` disables split prefetching in table scan.

``async-cache-ssd-gb``
^^^^^^^^^^^^^^^^^^^^^^

* **Type** ``integer``
* **Default value:** ``0``

Size of the SSD cache when async data cache is enabled.  Must be zero if
``async-data-cache-enabled`` is ``false``.

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
