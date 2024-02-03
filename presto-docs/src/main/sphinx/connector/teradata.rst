===============
Teradata Connector
===============

The Teradata connector allows querying and creating tables in an external Teradata database.

Configuration
-------------

To configure the Teradata connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``teradata.properties``.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=teradata
    connection-url=jdbc:teradata://{host}/DATABASE={database},DBS_PORT=1025
    connection-user={username}
    connection-password={password}

The ``connection-url`` defines the connection information and parameters to pass
to the Teradata JDBC driver.

.. code-block:: text

    connection-url=jdbc:teradata://{host}/DATABASE={database},DBS_PORT=1025

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user.

Multiple Teradata Databases or Accounts
^^^^^^^^^^^^^^^^^^^^^^

The Teradata connector can only access a single database within a Teradata account.
If you have multiple Teradata databases, or want to connect to multiple
Teradata accounts, you must configure multiple instances of the Teradata connector.
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

If TLS is configured with a globally-trusted certificate installed on the Teradata database,
TLS can be enabled between the cluster and the database by appending a parameter to the
JDBC connection string set in the ``connection-url`` catalog configuration property.

For example, use the parameters ``SSLMODE`` and ``ENCRYPTDATA`` to secure the connection with TLS.

* ``SSLMODE`` values can be ``DISABLE``, ``ALLOW``, ``PREFER``, ``REQUIRE``, ``VERIFY-CA``, and ``VERIFY-FULL``.
* ``ENCRYPTDATA`` values can be ``ON`` or ``OFF``.

You can set the ``SSLMODE`` and ``ENCRYPTDATA`` parameter in the catalog configuration file by appending it to the ``connection-url`` configuration property:

.. code-block:: none

    connection-url=jdbc:teradata://<host>/DATABASE=<database>,DBS_PORT=443,SSLMODE=VERIFY-CA,SSLCA=<PEM_encoded_trusted_certificates>,ENCRYPTDATA=ON

For a non-SSL configuration you can use ``SSLMODE=DISABLE`` and ``ENCRYPTDATA=OFF`` in ``connection-url``.

For more information on TLS configuration options, see the `Teradata JDBC Security Documentation <https://docs.teradata.com/r/Enterprise_IntelliFlex_Lake_VMware/Teradata-Call-Level-Interface-Version-2-Reference-for-Workstation-Attached-Systems-17.20/CLI-Files-and-Setup/CLI-Environment-Variables/SSLMODE-SSLCA-SSLCAPATH-SSLCRC>`_.

Querying Teradata
--------------

The Teradata connector provides a schema for every Teradata *database*.
You can see the available Teradata schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM teradata;

If you have a Teradata schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM teradata.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` schema
using either of the following::

    DESCRIBE teradata.web.clicks;
    SHOW COLUMNS FROM teradata.web.clicks;

You can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM teradata.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``teradata`` in the above examples.

Teradata Connector Limitations
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
