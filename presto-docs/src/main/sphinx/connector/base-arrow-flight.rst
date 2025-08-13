======================
Arrow Flight Connector
======================
This connector allows querying multiple data sources that are supported by an Arrow Flight server. Flight supports parallel transfers, allowing data to be streamed to or from a cluster of servers simultaneously. Official documentation for Arrow Flight can be found at `Arrow Flight RPC <https://arrow.apache.org/docs/format/Flight.html>`_.

Getting Started with presto-base-arrow-flight module: Essential Abstract Methods for Developers
-----------------------------------------------------------------------------------------------

To create a plugin extending the presto-base-arrow-flight module, you need to implement certain abstract methods that are specific to your use case. Below are the required classes and their purposes:

- ``BaseArrowFlightClientHandler.java``
  This class contains the core functionality for the Arrow Flight module. A plugin extending base-arrow-module should extend this abstract class and implement the following methods.

  - ``getCallOptions`` This method should return an array of credential call options to authenticate to the Flight server.
  - ``getFlightDescriptorForSchema`` This method should return the flight descriptor to fetch Arrow Schema for the table.
  - ``listSchemaNames`` This method should return the list of schemas in the catalog.
  - ``listTables`` This method should return the list of tables in the catalog.
  - ``getFlightDescriptorForTableScan`` This method should return the flight descriptor for fetching data from the table.

- ``ArrowPlugin.java``
  Register your connector name by extending the ArrowPlugin class.
- ``ArrowBlockBuilder.java`` (optional override to customize data types)
  This class builds Presto blocks from Arrow vectors. Extend this class if needed and override ``getPrestoTypeFromArrowField`` method, if any customizations are needed for the conversion of Arrow vector to Presto type. A binding for this class should be created in the ``Module`` for the plugin.

A reference implementation of the presto-base-arrow-flight module is provided in the test folder, containing a Flight server and a connector implementation.
The testing Flight server in ``com.facebook.plugin.arrow.testingServer``, starts a local server and initializes an H2 database to fetch data from. The server defines ``TestingArrowFlightRequest`` and ``TestingArrowFlightResponse`` used for commands in the Flight calls, and the ``TestingArrowProducer`` handles the calls including actions for ``listSchemaNames`` and ``listTables``.
The testing Flight connector in ``com.facebook.plugin.arrow.testingConnector``, implements the above classes to connect with the testing Flight server to use as a data source for test queries.


Configuration
-------------
Create a catalog file
in ``etc/catalog`` named, for example, ``arrowmariadb.properties``, to
mount the Flight connector as the ``arrowmariadb`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:


.. code-block:: none


        connector.name=<connector_name> 
        arrow-flight.server=<server_endpoint>
        arrow-flight.server.port=<server_port>



Add other properties that are required for your Flight server to connect.

========================================== ==============================================================
Property Name                               Description
========================================== ==============================================================
``arrow-flight.server``                     Endpoint of the Flight server
``arrow-flight.server.port``                Flight server port
``arrow-flight.server-ssl-certificate``     Path to SSL certificate of Flight server
``arrow-flight.client-ssl-certificate``     Path to SSL certificate that Flight clients will use for mTLS authentication with the Flight server
``arrow-flight.client-ssl-key``             Path to SSL key that Flight clients will use for mTLS authentication with the Flight server
``arrow-flight.server.verify``              To verify server
``arrow-flight.server-ssl-enabled``         Port is ssl enabled
========================================== ==============================================================

Mutual TLS (mTLS) Support
-------------------------


To connect the Presto client to an Arrow Flight server with mutual TLS (mTLS) enabled, you must configure the client to present a valid certificate and key that the server can validate. This enhances security by ensuring both the client and server authenticate each other.

To enable mTLS, the following properties must be configured:

- ``arrow-flight.server-ssl-enabled=true``: Explicitly enables TLS for the connection.
- ``arrow-flight.server-ssl-certificate``: Path to the server's SSL certificate.
- ``arrow-flight.client-ssl-certificate``: Path to the client's SSL certificate.
- ``arrow-flight.client-ssl-key``: Path to the client's SSL private key.

These properties must be used alongside the existing SSL configurations for the server, such as ``arrow-flight.server-ssl-certificate`` and ``arrow-flight.server-ssl-enabled=true``. Make sure the server is configured to trust the client certificates (typically via a shared CA).

Below is an example code snippet to configure the Arrow Flight server with mTLS:

.. code-block:: java

   File certChainFile = new File("src/test/resources/certs/server.crt");
   File privateKeyFile = new File("src/test/resources/certs/server.key");
   File caCertFile = new File("src/test/resources/certs/ca.crt");

   server = FlightServer.builder(allocator, location, new TestingArrowProducer(allocator))
           .useTls(certChainFile, privateKeyFile)
           .useMTlsClientVerification(caCertFile)
           .build();

   server.start();

Querying Arrow-Flight
---------------------

The Flight connector provides schema for each supported *database*.
Example for MariaDB is shown below.
To see the available schemas, run ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM arrowmariadb;

To view the tables in the MariaDB database named ``user``,
run ``SHOW TABLES``::

    SHOW TABLES FROM arrowmariadb.user;

To see a list of the columns in the ``admin`` table in the ``user`` database,
use either of the following commands::

    DESCRIBE arrowmariadb.user.admin;
    SHOW COLUMNS FROM arrowmariadb.user.admin;

Finally, you can access the ``admin`` table in the ``user`` database::

    SELECT * FROM arrowmariadb.user.admin;

If you used a different name for your catalog properties file, use
that catalog name instead of ``arrowmariadb`` in the above examples.


Flight Connector Limitations
----------------------------

* SELECT and DESCRIBE queries are supported. Implementing modules can add support for additional features.

* The Flight connector can query against only those datasources which are supported by the Flight server.

* The Flight server must be running for the Flight connector to work.

Presto C++ Support
------------------

Presto C++ must be built to enable Arrow Flight connector support. See `Arrow Flight Connector <https://github.com/prestodb/presto/blob/master/presto-native-execution/README.md#arrow-flight-connector>`_.