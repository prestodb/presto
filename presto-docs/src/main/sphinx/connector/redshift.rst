==================
Redshift Connector
==================

The Redshift connector allows querying and creating tables in an
external Amazon Redshift cluster. This can be used to join data between
different systems like Redshift and Hive, or between two different
Redshift clusters.

Configuration
-------------

To configure the Redshift connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``redshift.properties``, to
mount the Redshift connector as the ``redshift`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=redshift
    connection-url=jdbc:redshift://example.net:5439/database
    connection-user=root
    connection-password=secret

Multiple Redshift Databases or Clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Redshift connector can only access a single database within
a Redshift cluster. Thus, if you have multiple Redshift databases,
or want to connect to multiple Redshift clusters, you must configure
multiple instances of the Redshift connector.

To add another catalog, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For example,
if you name the property file ``sales.properties``, Presto will create a
catalog named ``sales`` using the configured connector.

General Configuration Properties
---------------------------------

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

``case-sensitive-name-matching``                   Enable case sensitive identifier support for schema and table        ``false``
                                                   names for the connector. When disabled, names are matched
                                                   case-insensitively using lowercase normalization.

``enable-datasource-managed-views``                Delegate view resolution to Redshift instead of allowing Presto      ``false``
                                                   to analyze view definitions. Useful for views containing
                                                   Redshift-specific SQL syntax or functions. See
                                                   :ref:`redshift-datasource-managed-views` for details.
================================================== ==================================================================== ===========


Procedures
----------

Use the :doc:`/sql/call` statement to perform data manipulation or administrative tasks. Procedures are available in the ``system`` schema of the catalog.

Execute Procedure
^^^^^^^^^^^^^^^^^

Underlying datasources may support some operation or SQL syntax which is not supported by Presto, either at the parser level or at the connector level.
Trying to run such SQL statements in Presto can result in errors during parsing or analysing. For example, Redshift supports creating auto generated
primary keys which is not supported in Presto. Running this procedure enables users to do a SQL passthrough to the underlying database, and Presto just acts
as a middle man for passing the statement.

The following arguments are available:

============= ========== =============== =======================================================================
Argument Name Required   Type            Description
============= ========== =============== =======================================================================
``QUERY``     Yes        string          SQL statement to run
============= ========== =============== =======================================================================

Examples:

* Create a table with auto generated primary key::

    CALL redshift.system.execute('create table schema1.table1 (id INT IDENTITY(1, 1), a int)')

    CALL redshift.system.execute(QUERY => 'create table schema1.table1 (id INT IDENTITY(1, 1), a int)')



Querying Redshift
-----------------

The Redshift connector provides a schema for every Redshift schema.
You can see the available Redshift schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM redshift;

If you have a Redshift schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM redshift.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE redshift.web.clicks;
    SHOW COLUMNS FROM redshift.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM redshift.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``redshift`` in the above examples.

View Support
------------

The Redshift connector supports creating, querying, displaying, and dropping Redshift views.

CREATE VIEW

.. code-block:: sql

    CREATE VIEW redshift.schema.view AS
    SELECT col FROM redshift.schema.table;

SHOW CREATE VIEW

.. code-block:: sql

    SHOW CREATE VIEW redshift.schema.view;

DROP VIEW

.. code-block:: sql

    DROP VIEW redshift.schema.view;

Views created through the connector can be queried using standard Presto SQL.

.. _redshift-datasource-managed-views:

Datasource-managed views
^^^^^^^^^^^^^^^^^^^^^^^^

By default, the Redshift connector retrieves Redshift view definitions and allows Presto to analyze and resolve the underlying view SQL.

Some Redshift views may contain Redshift-specific functions or syntax that cannot be analyzed by Presto.
In these cases, view analysis can be disabled and view resolution delegated to Redshift.

Typical use cases include views that contain Redshift-specific functions or SQL syntax that cannot be analyzed by Presto.

To enable datasource-managed views:

.. code-block:: none

    enable-datasource-managed-views=true


Default value:

.. code-block:: none

    enable-datasource-managed-views=false

When datasource-managed views are enabled:

* Presto does not analyze Redshift view definitions.
* View resolution is delegated to Redshift.
* Queries against views continue to be executed by Redshift.

Limitations
^^^^^^^^^^^
* ``SHOW CREATE VIEW`` and ``DROP VIEW`` are not supported when datasource-managed views are enabled.
* Views may reference only objects within the same Redshift catalog. Cross-catalog references are not supported.

Redshift Connector Limitations
------------------------------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-table` (:doc:`/sql/create-table-as` is supported)
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
