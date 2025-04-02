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

``case-sensitive-name-matching``                   Enable case sensitive identifier support for schema and table        ``false``
                                                   names for the connector. When disabled, names are matched
                                                   case-insensitively using lowercase normalization.
================================================== ==================================================================== ===========

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

Type mapping
------------

PrestoDB and MySQL each support types that the other does not. When reading from or writing to MySQL, Presto converts
the data types from MySQL to equivalent Presto data types, and from Presto to equivalent MySQL data types.
Refer to the following sections for type mapping in each direction.

MySQL to PrestoDB type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps MySQL types to the corresponding PrestoDB types:

.. list-table:: MySQL to PrestoDB type mapping
  :widths: 50, 50
  :header-rows: 1

  * - MySQL type
    - PrestoDB type
  * - ``BIT``
    - ``BOOLEAN``
  * - ``BOOLEAN``
    - ``TINYINT``
  * - ``TINYINT``
    - ``TINYINT``
  * - ``TINYINT UNSIGNED``
    - ``TINYINT``
  * - ``SMALLINT``
    - ``SMALLINT``
  * - ``SMALLINT UNSIGNED``
    - ``SMALLINT``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``INTEGER UNSIGNED``
    - ``INTEGER``
  * - ``BIGINT``
    - ``BIGINT``
  * - ``BIGINT UNSIGNED``
    - ``BIGINT``
  * - ``DOUBLE PRECISION``
    - ``DOUBLE``
  * - ``FLOAT``
    - ``REAL``
  * - ``REAL``
    - ``DOUBLE``
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
  * - ``CHAR(n)``
    - ``CHAR(n)``
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
  * - ``TINYTEXT``
    - ``VARCHAR(255)``
  * - ``TEXT``
    - ``VARCHAR(65535)``
  * - ``MEDIUMTEXT``
    - ``VARCHAR(16777215)``
  * - ``LONGTEXT``
    - ``VARCHAR``
  * - ``ENUM(n)``
    - ``CHAR(n)``
  * - ``BINARY``, ``VARBINARY``, ``TINYBLOB``, ``BLOB``, ``MEDIUMBLOB``, ``LONGBLOB``
    - ``VARBINARY``
  * - ``JSON``
    - ``CHAR(n)``
  * - ``DATE``
    - ``DATE``
  * - ``TIME(n)``
    - ``TIME``
  * - ``DATETIME(n)``
    - ``DATETIME``
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP``

No other types are supported.

PrestoDB to MySQL type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps PrestoDB types to the corresponding MySQL types:

.. list-table:: PrestoDB to MySQL type mapping
  :widths: 50, 50
  :header-rows: 1

  * - PrestoDB type
    - MySQL type
  * - ``BOOLEAN``
    - ``TINYINT``
  * - ``TINYINT``
    - ``TINYINT``
  * - ``SMALLINT``
    - ``SMALLINT``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``BIGINT``
    - ``BIGINT``
  * - ``REAL``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE PRECISION``
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
  * - ``CHAR(n)``
    - ``CHAR(n)``
  * - ``VARCHAR(n)``
    - ``TINYTEXT``, ``MEDIUMTEXT``
  * - ``VARCHAR``
    - ``LONGTEXT``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME``
  * - ``TIMESTAMP``
    - ``DATETIME``
  * - ``VARBINARY``
    - ``MEDIUMBLOB``

No other types are supported.

SQL Support
-----------

The MySQL connector allows querying and creating MySQL tables. Here are some examples of the SQL operations supported:

ALTER TABLE
^^^^^^^^^^^

.. code-block:: sql

    ALTER TABLE mysql.web.page_views ADD COLUMN zipcode VARCHAR;
    ALTER TABLE mysql.web.page_views RENAME COLUMN zipcode TO location;
    ALTER TABLE mysql.web.page_views DROP COLUMN location;

CREATE TABLE
^^^^^^^^^^^^

Create a new MySQL table named ``page_views`` in the ``web`` schema:

.. code-block:: sql

    CREATE TABLE mysql.web.page_views (
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    );

.. note:: Presto does not enforce primary key constraints. For example, the following statement

 .. code-block:: sql

  CREATE TABLE users (
          id INT PRIMARY KEY,
          name TEXT,
          email TEXT
      );

 returns an error similar to the following:

 ``Query 20240322_095447_00010_syzb3 failed: line 2:19: mismatched input 'PRIMARY'. Expecting: ')', ','``

CREATE TABLE AS SELECT
^^^^^^^^^^^^^^^^^^^^^^

Create a new table ``page_views_new`` from an existing table ``page_views``:

.. code-block:: sql

    CREATE TABLE mysql.web.page_views_new AS SELECT * FROM mysql.web.page_views;

.. note:: Advanced SQL features such as data compression are not supported in the ``CREATE TABLE AS SELECT`` statement.

 .. code-block:: sql

   CREATE TABLE compressed_employees AS SELECT * FROM employees WITH (compression = 'Zlib');

 returns an error similar to the following:

 ``Query 20240321_103408_00015_kbd43 failed: line 1:67: mismatched input '('. Expecting: 'DATA', 'NO'``

INSERT INTO
^^^^^^^^^^^

Insert data into the ``page_views`` table:

.. code-block:: sql

    INSERT INTO mysql.web.page_views VALUES(1, 'https://example.com', current_date, 'country');

SELECT
^^^^^^

.. code-block:: sql

    SELECT * FROM mysql.web.page_views;

TRUNCATE
^^^^^^^^

Delete all of the data from the table ``page_views`` without dropping the table:

.. code-block:: sql

    TRUNCATE TABLE mysql.web.page_views;

MySQL Connector Limitations
---------------------------

The following SQL statements are not supported:

* :doc:`/sql/alter-table`
* :doc:`/sql/analyze`
* :doc:`/sql/create-schema`
* :doc:`/sql/create-view`
* :doc:`/sql/delete`
* :doc:`/sql/drop-schema`
* :doc:`/sql/drop-table`
* :doc:`/sql/drop-view`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-role-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/update`
