
======================
Arrow Flight Connector
======================
This connector allows querying multiple data sources that are supported by an Arrow Flight server. Flight supports parallel transfers, allowing data to be streamed to or from a cluster of servers simultaneously. Official documentation for Arrow Flight can be found at https://arrow.apache.org/docs/format/Flight.html

Getting Started with base-arrow-module: Essential Abstract Methods for Developers
---------------------------------------------------------------------------------
To create a plugin extending the base-arrow-module, you need to implement certain abstract methods that are specific to your use case. Below are the required classes and their purposes:

* ``AbstractArrowFlightClientHandler.java``
  This class is responsible for initializing the Flight client and retrieving Flight information from the Flight server. To authenticate the Flight server, you must extend this class and implement the abstract method ``getCallOptions``, which returns a list of ``CredentialCallOption`` specific to your Flight server.

* ``AbstractArrowMetadata.java``
  To retrieve metadata (schema and table information), implement the abstract methods in the ArrowAbstractMetadata class.

* ``AbstractArrowSplitManager.java``
  Extend the ArrowAbstractSplitManager class to implement ``getFlightDescriptor`` using which splits will be created.

* ``ArrowPlugin.java``
  Register your connector name by extending the ArrowPlugin class.

* ``ArrowBlockBuilder.java``
  This class builds Presto blocks from Arrow vectors. Extend this class if needed and override ``getPrestoTypeFromArrowField`` method, if any customizations are needed for the conversion of Arrow vector to Presto type. A binding for this class should be created in the ``Module`` for the plugin.


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
``arrow-flight.server``                     Endpoint of Arrow Flight server
``arrow-flight.server.port``                Flight server port
``arrow-flight.server-ssl-certificate``     Pass ssl certificate
``arrow-flight.server.verify``              To verify server
``arrow-flight.server-ssl-enabled``         Port is ssl enabled
========================================== ==============================================================

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

* SELECT and DESCRIBE queries are supported by this connector template. Implementing modules can add support for additional features.

* Flight connector can query against only those datasources which are supported by the Flight server.

* The user should have the Flight server running for the Flight connector to work.
