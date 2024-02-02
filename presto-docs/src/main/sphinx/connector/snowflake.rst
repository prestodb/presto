===============
Snowflake Connector
===============

The Snowflake connector allows querying and creating tables in an external
Snowflake database. This can be used to join data between different
systems like Snowflake and Hive, or between two different Snowflake accounts.

Configuration
-------------

To configure the Snowflake connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``snowflake.properties``.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=snowflake
    connection-url=jdbc:snowflake://<account_identifier>.snowflakecomputing.com/?db={dbname}
    connection-user=root
    connection-password=secret

The ``connection-url`` defines the connection information and parameters to pass
to the Snowflake JDBC driver.

.. code-block:: text

    connection-url=jdbc:snowflake://<account_identifier>.snowflakecomputing.com/?db={dbname}

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user.

Multiple Snowflake Databases or Accounts
^^^^^^^^^^^^^^^^^^^^^^

The Snowflake connector can only access a single database within a Snowflake account.
If you have multiple Snowflake databases, or want to connect to multiple
Snowflake accounts, you must configure multiple instances of the Snowflake connector.
To add another catalog, add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

General Configuration Properties
---------------------------------

================================================== ==================================================================== ===========
Property Name                                      Description                                                          Default
================================================== ==================================================================== ===========
``user-credential-name``                           Name of the ``extraCredentials`` property whose value is the JDBC
                                                   driver's user name. See ``extraCredentials`` in `Parameter Reference
                                                   <https://prestodb.io/docs/current/installation/jdbc.html
                                                   #parameter-reference>`_.

``password-credential-name``                       Name of the ``extraCredentials`` property whose value is the JDBC
                                                   driver's user password. See ``extraCredentials`` in `Parameter
                                                   Reference <https://prestodb.io/docs/current/installation/jdbc.html
                                                   #parameter-reference>`_.

``case-insensitive-name-matching``                 Match dataset and table names case-insensitively.                    ``false``

``case-insensitive-name-matching.cache-ttl``       Duration for which remote dataset and table names will be
                                                   cached. Set to ``0ms`` to disable the cache.                         ``1m``
================================================== ==================================================================== ===========

SSL Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^

By default, connections to Snowflake use SSL.

Querying Snowflake
--------------

The Snowflake connector provides a schema for every Snowflake *database*.
You can see the available Snowflake schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM snowflake;

If you have a Snowflake schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM snowflake.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` schema
using either of the following::

    DESCRIBE snowflake.web.clicks;
    SHOW COLUMNS FROM snowflake.web.clicks;

You can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM snowflake.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``snowflake`` in the above examples.

Snowflake Connector Limitations
---------------------------

The following SQL statements are not supported:

* :doc:`/sql/create-schema`
* :doc:`/sql/alter-schema`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
* :doc:`/sql/create-role`
* :doc:`/sql/create-view`
* :doc:`/sql/drop-schema`
* :doc:`/sql/drop-view`
* :doc:`/sql/truncate`
* :doc:`/sql/update`
* :doc:`/sql/delete`
