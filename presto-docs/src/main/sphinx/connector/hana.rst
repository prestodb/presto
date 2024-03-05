====================
HANA Connector
====================

The HANA connector allows querying and creating tables in an
external HANA database. This can be used to join data between
different systems like HANA and Hive, or between two different
HANA instances.

Configuration
-------------

To configure the HANA connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``hana.properties``, to
mount the HANA connector as the ``hana`` catalog.

Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=hana
    connection-url=jdbc:sap://[serverName[\instanceName][:portNumber]]
    connection-user=root
    connection-password=secret

Multiple HANA Databases or Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The HANA connector can only access a single database within
a HANA server. If you have multiple HANA databases,
or want to connect to multiple HANA instances, you must configure
multiple catalogs, one for each instance.

To add another catalog, add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For example,
if you name the property file ``sales.properties``, Presto will create a
catalog named ``sales`` using the configured connector.

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

Querying HANA
-------------------

The HANA connector provides access to all schemas visible to the specified user in the configured database.
For the following examples, assume the HANA catalog is ``hana``.

You can see the available schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM hana;

If you have a schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM hana.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE hana.web.clicks;
    SHOW COLUMNS FROM hana.web.clicks;

Finally, you can query the ``clicks`` table in the ``web`` schema::

    SELECT * FROM hana.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``hana`` in the above examples.

HANA Connector Limitations
--------------------------------

The following SQL statements are not supported:

* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`