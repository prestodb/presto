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
