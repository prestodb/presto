
======================
Arrow-flight Connector
======================
This connector allows querying multiple data sources that are supported by an Arrow Flight server.
Apache Arrow enhances performance and efficiency in data-intensive applications through its columnar memory layout, zero-copy reads, vectorized execution, cross-language interoperability, rich data type support, and optimization for modern hardware. These features collectively reduce overhead, improve data processing speeds, and facilitate seamless data exchange between different systems and languages.

Getting Started with base-arrow-module: Essential Abstract Methods for Developers
--------------
To utilize the base-arrow-module, you need to implement certain abstract methods that are specific to your use case. Below are the required classes and their purposes:

* ``ArrowFlightClientHandler.java``
  This class is responsible for initializing the Flight client and retrieving Flight information from the Flight server. To authenticate the Flight server, you must implement the abstract method ``getCallOptions`` in ArrowFlightClientHandler, which returns the ``CredentialCallOption`` specific to your Flight server.

* ``ArrowAbstractFlightRequest.java``
  Implement this class to define the request data, including the data source type, connection properties, the number of partitions and other data required to interact with database.

* ``ArrowAbstractMetadata.java``
  To retrieve metadata (schema and table information), implement the abstract methods in the ArrowAbstractMetadata class.

* ``ArrowAbstractSplitManager.java``
  Extend the ArrowAbstractSplitManager class to implement the Arrow flight request, defining the Arrow split.

* ``ArrowPlugin.java``
  Register your connector name by extending the ArrowPlugin class.

Configuration
-------------
To configure the Arrow connector, create a catalog file
in ``etc/catalog`` named, for example, ``arrowmariadb.properties``, to
mount the Arrow-flight connector as the ``arrowmariadb`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:


.. code-block:: none


        connector.name=<CONNECTOR_NAME> 

        arrow-flight.server=<SERVER_ENDPOINT>
        arrow-flight.server.port=<SERVER_PORT>

        data-source.name=<DATASOURCE_NAME>
        data-source.host=<HOST>
        data-source.database=<DATABASE_NAME>
        data-source.username=<USERNAME>
        data-source.password=<PASSWORD>
        data-source.port=<PORT>
        data-source.ssl=<TRUE/FALSE>

        Add other properties that are required for your Flight server to connect.

========================================== ==============================================================
Property Name                               Description
========================================== ==============================================================
``arrow-flight.server``                     Endpoint of arrow-flight server
``arrow-flight.server.port``                Flight server port
``data-source.name``                        Datasource name
``data-source.host``                        Hostname of the database
``data-source.database``                    Database name
``data-source.username``                    Username of database
``data-source.password``                    Password of database
``data-source.port``                        Database port
``data-source.ssl``                         Enable SSL for datasource True/False
``arrow-flight.server-ssl-certificate``     Pass ssl certificate
``arrow-flight.server.verify``              To verify server
``arrow-flight.server-ssl-enabled``         Port is ssl enabled
========================================== ==============================================================

Querying Arrow-Flight
---------------------

The Arrow-Flight connector provides schema for each supported *databases*.
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


Arrow-Flight Connector Limitations
---------------------------------

* SELECT and DESCRIBE queries are supported by this connector template. Implementing modules can add support for additional features.

* Arrow-connector can query against only those datasources which are supported by Flight server.

* The customer should have the flight server running for the arrow-connector to work.
