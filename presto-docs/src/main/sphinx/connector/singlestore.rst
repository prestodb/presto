=====================
SingleStore Connector
=====================

The SingleStore connector allows querying and creating tables in an external
SingleStore database. This can be used to join data between different
systems like SingleStore and Hive, or between two different SingleStore instances.

Configuration
-------------

To configure the SingleStore connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``singlestore.properties``, to
mount the SingleStore connector as the ``singlestore`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

        connector.name=singlestore
        connection-url=jdbc:singlestore://localhost:3306
        connection-user=root
        connection-password=LbRootPass1

The ``connection-url`` defines the connection information and parameters to pass
to the SingleStore JDBC driver. The supported parameters for the URL are
available in the `SingleStore Connection String Parameters
<https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters>`_.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user.

An optional ``list-schemas-ignored-schemas`` config can be set to ignore certain schemas
when listing schemas. The default value for this is ``information_schema,memsql``.

Multiple SingleStore Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
SingleStore servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.


Procedures
----------

Use the :doc:`/sql/call` statement to perform data manipulation or administrative tasks. Procedures are available in the ``system`` schema of the catalog.

Execute Procedure
^^^^^^^^^^^^^^^^^

Underlying datasources may support some operation or SQL syntax which is not supported by Presto, either at the parser level or at the connector level.
Trying to run such SQL statements in Presto can result in errors during parsing or analysing. For example, SingleStore supports creating AUTO_INCREMENT
primary keys which is not supported in Presto. Running this procedure enables users to do a SQL passthrough to the underlying database, and Presto just acts
as a middle man for passing the statement.

The following arguments are available:

============= ========== =============== =======================================================================
Argument Name Required   Type            Description
============= ========== =============== =======================================================================
``QUERY``     Yes        string          SQL statement to run
============= ========== =============== =======================================================================

Examples:

* Create a table with AUTO_INCREMENT primary key::

    CALL singlestore.system.execute('create table schema1.table1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, a int)')

    CALL singlestore.system.execute(QUERY => 'create table schema1.table1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, a int)')


Querying SingleStore
--------------------

The SingleStore connector provides a schema for every SingleStore *database*.
You can see the available SingleStore databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM singlestore;

If you have a SingleStore database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM singlestore.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE singlestore.web.clicks;
    SHOW COLUMNS FROM singlestore.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM singlestore.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``singlestore`` in the above examples.

SingleStore Connector Limitations
---------------------------------

For :doc:`/sql/create-table` statement, the default table type is ``columnstore``.
The table type can be configured by setting the ``default_table_type`` engine variable, see 
`Creating a Columnstore Table <https://docs.singlestore.com/cloud/create-a-database/creating-a-columnstore-table/>`_.

SingleStore to PrestoDB type mapping
------------------------------------

Map of SingleStore types to the relevant PrestoDB types:

.. list-table:: SingleStore to PrestoDB type mapping
  :widths: 50, 50
  :header-rows: 1

  * - SingleStore type
    - PrestoDB type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``FLOAT``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL``
    - ``DECIMAL``
  * - ``LARGETEXT``
    - ``VARCHAR (unbounded)``
  * - ``VARCHAR(len)``
    - ``VARCHAR(len) len < 21845``
  * - ``CHAR(len)``
    - ``CHAR(len)``
  * - ``MEDIUMTEXT``
    - ``VARCHAR(len) 21845 <= len < 5592405``
  * - ``LARGETEXT``
    - ``VARCHAR(len) 5592405 <= len < 1431655765``
  * - ``MEDIUMBLOB``
    - ``VARBINARY``
  * - ``UUID``
    - ``UUID``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME``
  * - ``DATETIME``
    - ``TIMESTAMP``

No other types are supported.

The following SQL statements are not supported:

* :doc:`/sql/alter-schema`
* :doc:`/sql/analyze`
* :doc:`/sql/create-role`
* :doc:`/sql/create-schema`
* :doc:`/sql/create-view`
* :doc:`/sql/delete`
* :doc:`/sql/drop-role`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
* :doc:`/sql/set-role`
