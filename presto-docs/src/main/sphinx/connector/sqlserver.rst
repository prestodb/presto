====================
SQL Server Connector
====================

The SQL Server connector allows querying and creating tables in an
external SQL Server database. This can be used to join data between
different systems like SQL Server and Hive, or between two different
SQL Server instances.

Configuration
-------------

To configure the SQL Server connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``sqlserver.properties``, to
mount the SQL Server connector as the ``sqlserver`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=sqlserver
    connection-url=jdbc:sqlserver://[serverName[\instanceName][:portNumber]]
    connection-user=root
    connection-password=secret


Connection security
-------------------

The JDBC driver and connector automatically use Transport Layer Security (TLS) encryption and certificate validation. This requires a suitable TLS certificate configured on your SQL Server database host.

.. note::

   Starting from release 0.292, the default value of ``encrypt`` has changed from ``false`` to ``true``.

To disable encryption in the connection string, use the ``encrypt`` property:

.. code-block:: none

    connection-url=jdbc:sqlserver://<host>:<port>;databaseName=<databaseName>;encrypt=false;

Other SSL configuration properties that can be configured using the ``connection-url``:

SSL Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
================================================== ==================================================================== ===========
Property Name                                      Description                                                          Default
================================================== ==================================================================== ===========
``trustServerCertificate``                         Indicates that the server certificate is not trusted                 ``false``
                                                   automatically and a truststore is required for 
                                                   SSL certificate verification.

``trustStoreType``                                 File format of the truststore file, for example ``JKS`` or ``PEM``.

``hostNameInCertificate``                          Specifies the expected CN (Common Name) in the SSL certificate 
                                                   from the server.

``trustStore``                                     The path to the truststore file.

``trustStorePassword``                             The password for the truststore.
================================================== ==================================================================== ===========

A connection string using a truststore would be similar to the following example:

.. code-block:: none

    connection-url=jdbc:sqlserver://<host>:<port>;databaseName=<databaseName>;encrypt=true;trustServerCertificate=false;trustStoreType=PEM;hostNameInCertificate=hostname;trustStore=path/to/truststore.pem;trustStorePassword=password

Authentication
^^^^^^^^^^^^^^
+--------------------------+-----------------------------------------------------------------+------------------+
| **Property**             | **Description**                                                 | **Default Value**|
+--------------------------+-----------------------------------------------------------------+------------------+
| ``integratedSecurity``   | Enables Windows Authentication for SQL Server.                  | ``false``        |
|                          |                                                                 |                  |
|                          | - Set to ``true`` with ``authenticationScheme=JavaKerberos``    |                  |
|                          |   to indicate that Kerberos credentials are used by SQL Server. |                  |
|                          | - Set to ``true`` with ``authenticationScheme=NTLM`` to         |                  |
|                          |   indicate that NTLM credentials are used by SQL Server.        |                  |
+--------------------------+-----------------------------------------------------------------+------------------+
| ``authentication``       | Specifies the authentication method to use for connection.      | ``NotSpecified`` |
|                          | Possible values:                                                |                  |
|                          |                                                                 |                  |
|                          | - ``NotSpecified``                                              |                  |
|                          | - ``SqlPassword``                                               |                  |
|                          | - ``ActiveDirectoryPassword``                                   |                  |
|                          | - ``ActiveDirectoryIntegrated``                                 |                  |
|                          | - ``ActiveDirectoryManagedIdentity``                            |                  |
|                          | - ``ActiveDirectoryMSI``                                        |                  |
|                          | - ``ActiveDirectoryInteractive``                                |                  |
|                          | - ``ActiveDirectoryServicePrincipal``                           |                  |
+--------------------------+-----------------------------------------------------------------+------------------+

Below is a sample connection URL with the NTLM authentication:

.. code-block:: none

    connection-url=jdbc:sqlserver://<host>:<port>;databaseName=<databaseName>;encrypt=true;trustServerCertificate=false;integratedSecurity=true;authenticationScheme=NTLM;


Refer to `setting the connection properties <https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver16>`_ from the Microsoft official documentation.


Multiple SQL Server Databases or Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The SQL Server connector can only access a single database within
a SQL Server server. Thus, if you have multiple SQL Server databases,
or want to connect to multiple instances of the SQL Server, you must configure
multiple catalogs, one for each instance.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For example,
if you name the property file ``sales.properties``, Presto will create a
catalog named ``sales`` using the configured connector.

General Configuration Properties
--------------------------------

================================================== ==================================================================== ===========
Property Name                                      Description                                                          Default
================================================== ==================================================================== ===========
``user-credential-name``                           Name of the ``extraCredentials`` property whose value is the JDBC
                                                   driver's user name. See ``extraCredentials`` in
                                                   :ref:`Parameter Reference <jdbc-parameter-reference>`.

``password-credential-name``                       Name of the ``extraCredentials`` property whose value is the JDBC
                                                   driver's user password. See ``extraCredentials`` in
                                                   :ref:`Parameter Reference <jdbc-parameter-reference>`.

``case-insensitive-name-matching``                 Match dataset and table names case-insensitively.                    ``false``

``case-insensitive-name-matching.cache-ttl``       Duration for which remote dataset and table names will be
                                                   cached. Set to ``0ms`` to disable the cache.                         ``1m``

``list-schemas-ignored-schemas``                   List of schemas to ignore when listing schemas.                      ``information_schema``
================================================== ==================================================================== ===========

Querying SQL Server
-------------------

The SQL Server connector provides access to all schemas visible to the specified user in the configured database.
For the following examples, assume the SQL Server catalog is ``sqlserver``.

You can see the available schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM sqlserver;

If you have a schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM sqlserver.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE sqlserver.web.clicks;
    SHOW COLUMNS FROM sqlserver.web.clicks;

Finally, you can query the ``clicks`` table in the ``web`` schema::

    SELECT * FROM sqlserver.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``sqlserver`` in the above examples.

SQL Server Connector Limitations
--------------------------------

Presto supports connecting to SQL Server 2016, SQL Server 2014, SQL Server 2012
and Azure SQL Database.

Presto supports the following SQL Server data types.
The following table shows the mappings between SQL Server and Presto data types.

============================= ============================
SQL Server Type               Presto Type
============================= ============================
``bigint``                    ``bigint``
``smallint``                  ``smallint``
``int``                       ``integer``
``float``                     ``double``
``char(n)``                   ``char(n)``
``varchar(n)``                ``varchar(n)``
``date``                      ``date``
============================= ============================

Complete list of `SQL Server data types
<https://msdn.microsoft.com/en-us/library/ms187752.aspx>`_.

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-table` (:doc:`/sql/create-table-as` is supported)
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
