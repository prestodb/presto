
======================
Arrow-flight Connector
======================
This connector allows querying multiple datasources that supported by Arrow-flight-server.
Apache Arrow enhances performance and efficiency in data-intensive applications through its columnar memory layout, zero-copy reads, vectorized execution, cross-language interoperability, rich data type support, and optimization for modern hardware. These features collectively reduce overhead, improve data processing speeds, and facilitate seamless data exchange between different systems and languages.

Configuration
-------------
To configure the Arrow connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``arrowmariadb.properties``, to
mount the Arrow-flight connector as the ``arrowmariadb`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:


.. code-block:: none


        connector.name=<CONNECTOR_NAME> 

        arrow-flight.server=<SERVER_ENDPOINT>
        arrow-flight.server.port=<SERVER_PORT>
        arrow-flight.apikey=<API_KEY>
        arrow-flight.cloud.token-url=<URL>

        data-source.name=<DATASOURCE_NAME>
        data-source.host=<HOST>
        data-source.database=<DATABASE_NAME>
        data-source.username=<USERNAME>
        data-source.password=<PASSWORD>
        data-source.port=<PORT>
        data-source.ssl=<TRUE/FALSE>

========================================== ==============================================================
Property Name                               Description
========================================== ==============================================================
``arrow-flight.server``                     Endpoint of arrow-flight server
``arrow-flight.server.port``                Flight server port
``arrow-flight.apikey``                     API_KEY
``arrow-flight.cloud.token-url``            Cloud host
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
