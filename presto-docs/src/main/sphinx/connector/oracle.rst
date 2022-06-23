================
Oracle Connector
================

The Oracle connector allows querying and creating tables in an
external Oracle database. This can be used to join data between
different systems like Oracle and Hive, or between two different
Oracle instances.

Configuration
-------------

To configure the Oracle connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``oracle.properties``, to
mount the Oracle connector as the ``oracle`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=oracle
    # The correct syntax of the connection-url varies by Oracle version and
    # configuration. The following example URL connects to an Oracle SID named
    # "orcl".
    connection-url=jdbc:oracle:thin:@example.net:1521:orcl
    connection-user=root
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the JDBC driver. The Oracle connector uses the Oracle JDBC Thin driver,
and the syntax of the URL may be different depending on your Oracle
configuration. For example, the connection URL is different if you are
connecting to an Oracle SID or an Oracle service name. See the `Oracle
Database JDBC driver documentation
<https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-088B1600-C6C2-4F19-A020-2DAF8FE1F1C3>`_
for more information.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user.

Multiple Oracle Servers
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Oracle servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Querying Oracle
---------------

The Oracle connector provides a schema for every Oracle *database*.
You can see the available Oracle databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM oracle;

If you have a Oracle database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM oracle.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE oracle.web.clicks;
    SHOW COLUMNS FROM oracle.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM oracle.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``oracle`` in the above examples.

Oracle Connector Limitations
----------------------------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
