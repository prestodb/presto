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
* GET: v1/info/metrics: Returns worker level metrics in Prometheus Data format. See `Worker Metrics Collection`_ for more information. Here is a sample Metrics data returned by this API.

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

The request/response flow of Presto C++ is identical to Java workers. The tasks or new splits are registered through `TaskUpdateRequest`. Resource utilization and query progress are sent to the coordinator through task endpoints.

* GET: /v1/operation/server/clearCache?type=memory: It clears the memory cache on worker node. Here is an example:

  .. code-block:: shell

   curl -X GET "http://localhost:7777/v1/operation/server/clearCache?type=memory"

   Cleared memory cache

* GET: /v1/operation/server/clearCache?type=ssd: It clears the ssd cache on worker node. Here is an example:

  .. code-block:: shell

   curl -X GET "http://localhost:7777/v1/operation/server/clearCache?type=ssd"

   Cleared ssd cache

* GET: /v1/operation/server/writeSsd: It writes data from memory cache to the ssd cache on worker node. Here is an example:

  .. code-block:: shell

   curl -X GET "http://localhost:7777/v1/operation/server/writeSsd"

   Succeeded write ssd cache

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

REST-Based Remote Functions
---------------------------

In addition to Thrift-based remote function execution, Presto C++ also supports
REST-based remote functions that communicate with HTTP/REST protocol. This provides
an alternative approach to remote function execution that may be easier to
implement and integrate with existing REST-based services.

REST functions use HTTP POST requests to invoke remote functions and receive
results. The communication uses serialized data in either Presto page format
or Spark unsafe row format for efficient data transfer.

Configuration
^^^^^^^^^^^^^

To enable REST-based remote functions, configure the following property:

``remote-function-server.rest.url``
"""""""""""""""""""""""""""""""""""

* **Type:** ``string``
* **Default value:** ``""``

The base URL of the REST server that hosts remote functions. When specified,
the Presto C++ worker will invoke functions using HTTP REST endpoints at this
server. The URL should include the protocol and host (for example,
``http://localhost:8080`` or ``https://remote-function-server.example.com``).

If empty, REST-based remote functions are disabled.

The REST function server must implement the following endpoint pattern:
``<base_url>/v1/functions/<schema>/<function>/<function_id>/<version>``

For example, if the base URL is ``http://localhost:8080`` and you have a
function ``my_schema.my_function``, the endpoint would be:
``http://localhost:8080/v1/functions/my_schema/my_function/...``

``remote-function-server.serde``
""""""""""""""""""""""""""""""""

* **Type:** ``string``
* **Default value:** ``"presto_page"``

This property (shared with Thrift-based remote functions) determines the
serialization format for data sent to and received from the REST server.

Supported values:

* ``presto_page``: Uses Presto's native page serialization format
* ``spark_unsafe_row``: Uses Spark's unsafe row serialization format

Setup and Usage
^^^^^^^^^^^^^^^

To use REST-based remote functions in your Presto C++ cluster:

1. **Deploy a REST Function Server**: Implement a REST service that conforms to the
   `REST Function Server API specification <https://github.com/prestodb/presto/blob/master/presto-openapi/src/main/resources/rest_function_server.yaml>`_.
   The server must implement endpoints for function discovery, management, and execution.

   Key requirements:

   * Implement ``GET /v1/functions`` to list available functions
   * Implement ``POST /v1/functions/{schema}/{functionName}/{functionId}/{version}`` for function execution
   * Accept serialized input data with appropriate Content-Type:

     * ``Content-Type: application/X-presto-pages`` for Presto page format
     * ``Content-Type: application/X-spark-unsafe-row`` for Spark unsafe row format

   * Return serialized results with the same Content-Type as the request

2. **Configure the Presto C++ Worker**: Add the following to your worker's
   configuration file (for example, ``config.properties``):

   .. code-block:: properties

      remote-function-server.rest.url=http://your-function-server:8080
      remote-function-server.serde=presto_page

3. **Register Functions**: Functions are registered when the coordinator sends
   function metadata to the worker during query execution. The function
   signatures and metadata are managed by the coordinator's function namespace
   manager.

4. **Use Functions in Queries**: Once configured, remote functions can be used
   in SQL queries like any other function:

   .. code-block:: sql

      SELECT catalog.schema.remote_function(column1, column2)
      FROM your_table;

REST Function Server API Specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The REST function server must implement the API specification defined in
`rest_function_server.yaml <https://github.com/prestodb/presto/blob/master/presto-openapi/src/main/resources/rest_function_server.yaml>`_.

A sample implementation using Presto Java functions is available in
`FunctionServer.java <https://github.com/prestodb/presto/blob/master/presto-function-server/src/main/java/com/facebook/presto/server/FunctionServer.java>`_.

The key endpoints include:

**Function Discovery:**

* ``GET /v1/functions`` - List all available functions
* ``GET /v1/functions/{schema}`` - List functions in a specific schema
* ``GET /v1/functions/{schema}/{functionName}`` - Get specific function metadata

**Function Management:**

* ``POST /v1/functions/{schema}/{functionName}`` - Create a new function
* ``PUT /v1/functions/{schema}/{functionName}/{functionId}`` - Update an existing function
* ``DELETE /v1/functions/{schema}/{functionName}/{functionId}`` - Delete a function

**Function Execution:**

* ``POST /v1/functions/{schema}/{functionName}/{functionId}/{version}`` - Execute a function

  * **Request Headers**:

    * ``Content-Type: application/X-presto-pages`` (for Presto page format)
    * ``Content-Type: application/X-spark-unsafe-row`` (for Spark unsafe row format)

  * **Request Body**: Serialized input vectors in the configured format (Presto page or Spark unsafe row)
  * **Response Headers**: Same ``Content-Type`` as request
  * **Response Body**: Serialized output vectors in the same format
  * **Response Status**: ``200 OK`` on success, appropriate error codes on failure

The function execution endpoint is responsible for:

1. Deserializing the input data from the request body
2. Executing the function logic with the provided inputs
3. Serializing the results
4. Returning the serialized results in the response

For complete API details, request/response schemas, and examples, refer to the
`OpenAPI specification <https://github.com/prestodb/presto/blob/master/presto-openapi/src/main/resources/rest_function_server.yaml>`_.

Secure Communication with the REST Function Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Presto C++ workers support two complementary mechanisms for authenticating with the REST
Function Server: mutual TLS (mTLS) for certificate-based identity verification, and JWT
for token-based request authentication. These can be used individually or together for
maximum security.

mTLS (Mutual TLS)
"""""""""""""""""

When mTLS is enabled, both the worker (as HTTP client) and the Function Server present
X.509 certificates during the TLS handshake. The worker verifies the Function Server's
certificate against a trusted CA, and the Function Server verifies the worker's client
certificate against the same CA. This provides strong mutual identity verification at the
transport layer.

The following properties are set in the native worker's ``config.properties``:

``https-cert-path``
'''''''''''''''''''

* **Type:** ``string``
* **Default value:** ``""``

Path to the PEM-encoded TLS certificate file used by the native worker when it acts as an
HTTPS server (for coordinator→worker communication).

``https-key-path``
''''''''''''''''''

* **Type:** ``string``
* **Default value:** ``""``

Path to the PEM-encoded private key corresponding to the certificate at ``https-cert-path``.

``https-client-cert-key-path``
''''''''''''''''''''''''''''''

* **Type:** ``string``
* **Default value:** ``""``

Path to a combined PEM file containing the worker's client certificate and private key.
This is presented to the Function Server during the mTLS handshake so the server can
authenticate the worker. The file must contain both the ``CERTIFICATE`` and ``PRIVATE KEY``
PEM blocks concatenated together.

``https-client-ca-file``
''''''''''''''''''''''''

* **Type:** ``string``
* **Default value:** ``""``

Path to the PEM-encoded CA certificate used to verify the Function Server's TLS certificate
during the mTLS handshake. Must be the CA that signed the Function Server's certificate.

JWT
"""

JWT adds a second layer of authentication at the HTTP request level. The worker signs every
request to the Function Server with a short-lived JWT token derived from a shared secret. The
Function Server validates the token on receipt, rejecting requests with an invalid or expired
token (HTTP 401). This is the same JWT mechanism used for coordinator↔worker internal
communication; the same ``internal-communication.*`` properties apply here.

The following properties are set in **both** the native worker's ``config.properties`` and
the Function Server's ``config.properties``:

``internal-communication.jwt.enabled``
''''''''''''''''''''''''''''''''''''''

* **Type:** ``boolean``
* **Default value:** ``false``

Set to ``true`` to enable JWT authentication on all outbound requests from the worker to the
Function Server and to require the Function Server to validate them.

``internal-communication.shared-secret``
''''''''''''''''''''''''''''''''''''''''

* **Type:** ``string``
* **Default value:** ``""``

The shared secret used to sign and verify JWT tokens. Must be identical on the worker,
coordinator, and Function Server. Keep this value secret and rotate it periodically.

Combined mTLS + JWT example
"""""""""""""""""""""""""""

The following snippet shows a complete secure configuration for a native worker that connects
to a Function Server using both mTLS and JWT:

.. code-block:: properties

   # Function Server URL — must use https:// when mTLS is active
   remote-function-server.rest.url=https://function-server.example.com:9443

   # Worker TLS server identity (presented during coordinator→worker TLS)
   http-server.https.enabled=true
   http-server.https.port=7443
   https-cert-path=/etc/presto/certs/worker.crt
   https-key-path=/etc/presto/certs/worker.key

   # Worker client identity presented to the Function Server during mTLS
   https-client-cert-key-path=/etc/presto/certs/worker-combined.pem

   # CA used to verify the Function Server's certificate
   https-client-ca-file=/etc/presto/certs/ca.crt

   # JWT — sign outbound requests to the Function Server
   internal-communication.jwt.enabled=true
   internal-communication.shared-secret=<your-shared-secret>

.. note::

   The certificate passed via ``https-client-cert-key-path`` is a combined PEM file
   containing both the ``CERTIFICATE`` and ``PRIVATE KEY`` blocks. The Function Server's
   truststore must trust the CA that signed this certificate for the mTLS handshake to
   succeed.

The Function Server must be configured with a matching HTTPS keystore, truststore, and the
same ``internal-communication.shared-secret``. See
`FunctionServer.java <https://github.com/prestodb/presto/blob/master/presto-function-server/src/main/java/com/facebook/presto/server/FunctionServer.java>`_
for the Java-based reference implementation.

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

.. _async_data_caching_and_prefetching:

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
  metrics collection in Prometheus Data Format see `Worker Metrics Collection <https://github.com/prestodb/presto/tree/master/presto-native-execution#worker-metrics-collection>`_.

  When enabled and Presto C++ workers interact with the S3 filesystem, additional runtime metrics are collected.
  For a detailed list of these metrics, see `S3 FileSystem <https://facebookincubator.github.io/velox/monitoring/metrics.html#s3-filesystem>`_.

  For comprehensive documentation of all available runtime metrics, see :doc:`metrics`.
