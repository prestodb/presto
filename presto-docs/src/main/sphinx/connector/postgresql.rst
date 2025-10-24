====================
PostgreSQL Connector
====================

The PostgreSQL connector allows querying and creating tables in an
external PostgreSQL database. This can be used to join data between
different systems like PostgreSQL and Hive, or between two different
PostgreSQL instances.

Configuration
-------------

To configure the PostgreSQL connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``postgresql.properties``, to
mount the PostgreSQL connector as the ``postgresql`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=postgresql
    connection-url=jdbc:postgresql://example.net:5432/database
    connection-user=root
    connection-password=secret

Multiple PostgreSQL Databases or Servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The PostgreSQL connector can only access a single database within
a PostgreSQL server. Thus, if you have multiple PostgreSQL databases,
or want to connect to multiple PostgreSQL servers, you must configure
multiple instances of the PostgreSQL connector.

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
================================================== ==================================================================== ===========

Querying PostgreSQL
-------------------

The PostgreSQL connector provides a schema for every PostgreSQL schema.
You can see the available PostgreSQL schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM postgresql;

If you have a PostgreSQL schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM postgresql.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE postgresql.web.clicks;
    SHOW COLUMNS FROM postgresql.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM postgresql.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``postgresql`` in the above examples.

Type mapping
------------

PrestoDB and PostgreSQL each support types that the other does not. When reading from or writing to PostgreSQL, Presto converts
the data types from PostgreSQL to equivalent Presto data types, and from Presto to equivalent PostgreSQL data types.

PostgreSQL to PrestoDB type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps PostgreSQL types to the corresponding PrestoDB types:

.. list-table:: PostgreSQL to PrestoDB type mapping
  :widths: 70, 70
  :header-rows: 1

  * - PostgreSQL type
    - PrestoDB type
  * - ``BIT``
    - ``BOOLEAN``
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``SMALLINT``
    - ``SMALLINT``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``BIGINT``
    - ``BIGINT``
  * - ``DOUBLE PRECISION``
    - ``DOUBLE``
  * - ``REAL``
    - ``REAL``
  * - ``NUMERIC(p, s)``
    - ``DECIMAL(p, s)``
  * - ``CHAR(n)``
    - ``CHAR(n)``
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
  * - ``ENUM``
    - ``VARCHAR``
  * - ``BYTEA``
    - ``VARBINARY``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME``
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
  * - ``TIMESTAMPTZ``
    - ``TIMESTAMP``
  * - ``MONEY``
    - ``DOUBLE``
  * - ``UUID``
    - ``UUID``
  * - ``JSON``
    - ``JSON``
  * - ``JSONB``
    - ``JSON``
  * - ``GEOMETRY``
    - ``VARCHAR``
  * - ``GEOGRAPHY``
    - ``VARCHAR``

No other types are supported.

PrestoDB to PostgreSQL type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps PrestoDB types to the corresponding PostgreSQL types:

.. list-table:: PrestoDB to PostgreSQL type mapping
  :widths: 50, 50
  :header-rows: 1

  * - PrestoDB type
    - PostgreSQL type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``SMALLINT``
    - ``SMALLINT``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``BIGINT``
    - ``BIGINT``
  * - ``DOUBLE``
    - ``DOUBLE PRECISION``
  * - ``DECIMAL(p, s)``
    - ``NUMERIC(p, s)``
  * - ``CHAR(n)``
    - ``CHAR(n)``
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
  * - ``VARBINARY``
    - ``BYTEA``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME``
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
  * - ``UUID``
    - ``UUID``

No other types are supported.

Tables with Unsupported Columns
-------------------------------

If you query a PostgreSQL table with the Presto connector, and the table either has no supported columns or contains
only unsupported data types, Presto returns an error similar to the following example:

``Query 20231120_102910_00004_35dqb failed: Table 'public.unsupported_type_table' has no supported columns (all 1 columns are not supported).``


SQL Support
-----------

The PostgreSQL connector allows querying and creating tables. Here are some examples of the SQL operations supported:

ALTER TABLE
^^^^^^^^^^^

.. code-block:: sql

    ALTER TABLE postgresql.public.sample_table ADD COLUMN new_col INT;
    ALTER TABLE postgresql.public.sample_table DROP COLUMN new_col;
    ALTER TABLE postgresql.public.sample_table RENAME COLUMN is_active TO is_enabled;
    ALTER TABLE postgresql.public.sample_table RENAME TO renamed_table;

.. note:: Presto does not support altering the data type of a column directly with the ALTER TABLE command.

 .. code-block:: sql

   ALTER TABLE postgresql.public.users ALTER COLUMN age TYPE BIGINT;

 returns an error similar to the following:

 ``Query 20240322_091317_00007_syzb3 failed: line 1:19: mismatched input 'ALTER'. Expecting: 'FUNCTION', 'SCHEMA', 'TABLE'``

CREATE TABLE
^^^^^^^^^^^^

Create a new table named ``test_integer`` in the ``public`` schema:

.. code-block:: sql

    CREATE TABLE postgresql.public.test_integer ( id INTEGER );

.. note:: Presto does not enforce primary key constraints. For example, the following statement

 .. code-block:: sql

  CREATE TABLE users (
          id INT PRIMARY KEY,
          name VARCHAR,
          email VARCHAR
      );

 returns an error similar to the following:

 ``Query 20240322_095447_00010_syzb3 failed: line 2:19: mismatched input 'PRIMARY'. Expecting: ')', ','``

CREATE TABLE AS SELECT
^^^^^^^^^^^^^^^^^^^^^^

Create a new table ``new_table`` from an existing table ``renamed_table``:

.. code-block:: sql

    CREATE TABLE postgresql.public.new_table AS SELECT * FROM postgresql.public.renamed_table;

DESCRIBE
^^^^^^^^

To see a list of the columns in the ``clicks`` table in the ``web`` database,
use either of the following:

.. code-block:: sql

    DESCRIBE postgresql.web.clicks;
    SHOW COLUMNS FROM postgresql.web.clicks;

INSERT INTO
^^^^^^^^^^^

Insert data into the ``renamed_table`` table:

.. code-block:: sql

    INSERT INTO postgresql.public.renamed_table (id, name) VALUES (1, 'Test');

SELECT
^^^^^^

.. code-block:: sql

    SELECT * FROM postgresql.public.test_integer;

SHOW SCHEMAS
^^^^^^^^^^^^

To see the available PostgreSQL schemas:

.. code-block:: sql

    SHOW SCHEMAS FROM postgresql;

SHOW TABLES
^^^^^^^^^^^

To view the tables in a PostgreSQL schema named ``web``:

.. code-block:: sql

    SHOW TABLES FROM postgresql.web;

TRUNCATE
^^^^^^^^

Delete all of the data from the table ``renamed_table`` without dropping the table:

.. code-block:: sql

    TRUNCATE TABLE postgresql.public.renamed_table;

PostgreSQL Connector Limitations
--------------------------------

The following SQL statements are not supported:

* :doc:`/sql/create-schema`
* :doc:`/sql/create-view`
* :doc:`/sql/delete`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-role-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/update`
