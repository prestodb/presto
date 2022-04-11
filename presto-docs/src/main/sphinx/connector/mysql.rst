===============
MySQL Connector
===============

The MySQL connector allows querying and creating tables in an external
MySQL database. This can be used to join data between different
systems like MySQL and Hive, or between two different MySQL instances.

Configuration
-------------

To configure the MySQL connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``mysql.properties``, to
mount the MySQL connector as the ``mysql`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=mysql
    connection-url=jdbc:mysql://example.net:3306
    connection-user=root
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the MySQL JDBC driver. The supported parameters for the URL are
available in the `MySQL Developer Guide
<https://dev.mysql.com/doc/connector-j/8.0/en/>`_.

For example, the following ``connection-url`` allows you to
configure the JDBC driver to interpret time values based on UTC as a timezone on
the server, and serves as a `workaround for a known issue
<https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-usagenotes-known-issues-limitations.html>`_.

.. code-block:: text

    connection-url=jdbc:mysql://example.net:3306?serverTimezone=UTC

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user.

Multiple MySQL Servers
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
MySQL servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Querying MySQL
--------------

The MySQL connector provides a schema for every MySQL *database*.
You can see the available MySQL databases by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM mysql;

If you have a MySQL database named ``web``, you can view the tables
in this database by running ``SHOW TABLES``::

    SHOW TABLES FROM mysql.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE mysql.web.clicks;
    SHOW COLUMNS FROM mysql.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` database::

    SELECT * FROM mysql.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``mysql`` in the above examples.

Performance
-----------

The connector includes a number of performance improvements, detailed in the
following sections.

.. _mysql-table-statistics:

Table statistics
^^^^^^^^^^^^^^^^

The MySQL connector can use :doc:`table and column statistics
</optimizer/statistics>` for :doc:`cost based optimizations
</optimizer/cost-based-optimizations>`, to improve query processing performance
based on the actual data in the data source.

The statistics are collected by MySQL and retrieved by the connector.

The table-level statistics are based on MySQL's ``INFORMATION_SCHEMA.TABLES``
table. The column-level statistics are based on MySQL's index statistics
``INFORMATION_SCHEMA.STATISTICS`` table. The connector can return column-level
statistics only when the column is the first column in some index.

MySQL database can automatically update its table and index statistics. In some
cases, you may want to force statistics update, for example after creating new
index, or after changing data in the table. You can do that by executing the
following statement in MySQL Database.

.. code-block:: text

    ANALYZE TABLE table_name;

.. note::

    MySQL and Presto may use statistics information in different ways. For this
    reason, the accuracy of table and column statistics returned by the MySQL
    connector might be lower than than that of others connectors.

**Improving statistics accuracy**

You can improve statistics accuracy with histogram statistics (available since
MySQL 8.0). To create histogram statistics execute the following statement in
MySQL Database.

.. code-block:: text

    ANALYZE TABLE table_name UPDATE HISTOGRAM ON column_name1, column_name2, ...;

Refer to MySQL documentation for information about options, limitations
and additional considerations.

MySQL Connector Limitations
---------------------------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-table` (:doc:`/sql/create-table-as` is supported)
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`