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
