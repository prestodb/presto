====================
Connector Federation
====================

About FlightShim
----------------

FlightShim is used to support connector federation on a Presto C++ cluster.
It contains an Apache Arrow Flight server that can load Java-based Presto
connectors not otherwise available to a Presto C++ worker. The C++ worker
has a built-in Arrow Federation connector to register federated connectors
and will then forward table read requests to FlightShim after receiving split
information from the coordinator.

Installing FlightShim
---------------------

Download the Presto FlightShim tarball, :maven_download:`flight-shim`, and unpack it.
The tarball will contain a single top-level directory,
|presto_flight_shim_release|, which we will call the *installation* directory.

FlightShim needs a *data* directory for storing logs and other files.
We recommend creating a data directory outside of the installation directory,
which allows it to be preserved when upgrading Presto.

Configuring FlightShim
----------------------

Create an ``etc`` directory inside the installation directory.
Similar to the installation of Presto, this will hold the following configuration:

* JVM Configuration: command line options for the Java Virtual Machine
* Configuration Properties: configuration for the Presto FlightShim server
* Catalog Properties: configuration for :doc:`/connector` (data sources).
  The available catalog configuration properties for a connector are described
  in the respective connector documentation.

.. _jvm_configuration:

JVM Configuration
^^^^^^^^^^^^^^^^^

The JVM configuration file, ``etc/jvm.config``, contains a list of command line
options used for launching the Java Virtual Machine.

The following provides an example of ``etc/jvm.config``.

.. code-block:: none

    -ea
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+UseGCOverheadLimit
    -XX:+ExplicitGCInvokesConcurrent
    -Xmx12G
    --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED

.. _configuration_properties:

Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^

The properties file, ``etc/flightshim.properties``, contains the configuration
for the Presto FlightShim server.

The following provides an example of ``etc/flightshim.properties``.

.. code-block:: none

    flight-shim.server=localhost
    flight-shim.server.port=9999
    flight-shim.server-ssl-certificate-file=/path/to/server.crt
    flight-shim.server-ssl-key-file=/path/to/server.key

.. _authentication:

Authentication
--------------

The FlightShim server supports a secure TLS gRPC channel by specifying a
cert-key pair ``flight-shim.server-ssl-certificate-file`` and
``flight-shim.server-ssl-key-file`` in the properties. To enable mTLS,
include the client cert file with ``client-ssl-certificate-file``. SSL is
enabled by default, to disable SSL for an unsecure channel (for debugging
purposes only) add the config ``flight-shim.server-ssl-enabled=false``.

.. _worker_catalog_properties:

Worker Catalog Properties
-------------------------
The catalog files used by the coordinator and FlightShim are the normal properties
used by Presto. However, the worker catalogs have different configuration that is
used to communicate requests to the FlightShim server.

The following provides an example of a worker PostgresSQL catalog ``etc/catalog/postgres.properties``

.. code-block:: none

    connector.name=arrow-federation
    protocol-connector.id=postgresql
    arrow-flight.server=localhost
    arrow-flight.server.port=9999
    arrow-flight.server-ssl-enabled=true
    arrow-flight.server.verify=false

.. _running_flightshim:

Running FlightShim
------------------

The installation directory contains the launcher script in ``bin/launcher``.
FlightShim can be started as a a daemon by running the following:

.. code-block:: none

    bin/launcher --config etc/flightshim.properties start

Alternatively, it can be run in the foreground, with the logs and other
output being written to stdout/stderr (both streams should be captured
if using a supervision system like daemontools):

.. code-block:: none

    bin/launcher --config etc/flightshim.properties run
