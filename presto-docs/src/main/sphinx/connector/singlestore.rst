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

Multiple SingleStore Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
SingleStore servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

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
