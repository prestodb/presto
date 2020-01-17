==============
Kudu Connector
==============

The Kudu connector allows querying, inserting and deleting data in `Apache Kudu`_

.. _Apache Kudu: https://kudu.apache.org/


.. contents::
    :local:
    :backlinks: none
    :depth: 1


Compatibility
-------------

Connector is compatible with all Apache Kudu versions starting from 1.0.

If the connector uses features that are not available on the target server, an error will be returned.
Apache Kudu 1.8.0 is currently used for testing.


Configuration
-------------

To configure the Kudu connector, create a catalog properties file
``etc/catalog/kudu.properties`` with the following contents,
replacing the properties as appropriate:

  .. code-block:: none

       connector.name=kudu

       ## List of Kudu master addresses, at least one is needed (comma separated)
       ## Supported formats: example.com, example.com:7051, 192.0.2.1, 192.0.2.1:7051,
       ##                    [2001:db8::1], [2001:db8::1]:7051, 2001:db8::1
       kudu.client.master-addresses=localhost

       ## Kudu does not support schemas, but the connector can emulate them optionally.
       ## By default, this feature is disabled, and all tables belong to the default schema.
       ## For more details see connector documentation.
       #kudu.schema-emulation.enabled=false

       ## Prefix to use for schema emulation (only relevant if `kudu.schema-emulation.enabled=true`)
       ## The standard prefix is `presto::`. Empty prefix is also supported.
       ## For more details see connector documentation.
       #kudu.schema-emulation.prefix=

       #######################
       ### Advanced Kudu Java client configuration
       #######################

       ## Default timeout used for administrative operations (e.g. createTable, deleteTable, etc.)
       #kudu.client.default-admin-operation-timeout = 30s

       ## Default timeout used for user operations
       #kudu.client.default-operation-timeout = 30s

       ## Default timeout to use when waiting on data from a socket
       #kudu.client.default-socket-read-timeout = 10s

       ## Disable Kudu client's collection of statistics.
       #kudu.client.disable-statistics = false


Querying Data
-------------

Apache Kudu does not support schemas, i.e. namespaces for tables.
The connector can optionally emulate schemas by table naming conventions.

Default behaviour (without schema emulation)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The emulation of schemas is disabled by default.
In this case all Kudu tables are part of the ``default`` schema.

For example, a Kudu table named ``orders`` can be queried in Presto
with ``SELECT * FROM kudu.default.orders`` or simple with ``SELECT * FROM orders``
if catalog and schema are set to ``kudu`` and ``default`` respectively.

Table names can contain any characters in Kudu. In this case, use double quotes.
E.g. To query a Kudu table named ``special.table!`` use ``SELECT * FROM kudu.default."special.table!"``.


Example
^^^^^^^

-  Create a users table in the default schema with

  .. code:: sql

    CREATE TABLE kudu.default.users (
      user_id int WITH (primary_key = true),
      first_name varchar,
      last_name varchar
    ) WITH (
      partition_by_hash_columns = ARRAY['user_id'],
      partition_by_hash_buckets = 2
    );

On creating a Kudu table you must/can specify addition information about
the primary key, encoding, and compression of columns and hash or range
partitioning. Details see in section
`Create Table`_.

-  The table can be described using

  .. code:: sql

    DESCRIBE kudu.default.users;

You should get something like

::

       Column   |  Type   |                      Extra                      | Comment
    ------------+---------+-------------------------------------------------+---------
     user_id    | integer | primary_key, encoding=auto, compression=default |
     first_name | varchar | nullable, encoding=auto, compression=default    |
     last_name  | varchar | nullable, encoding=auto, compression=default    |
    (3 rows)


-  Insert some data with

  .. code:: sql

    INSERT INTO kudu.default.users VALUES (1, 'Donald', 'Duck'), (2, 'Mickey', 'Mouse');

-  Select the inserted data

  .. code:: sql

    SELECT * FROM kudu.default.users;


Behaviour With Schema Emulation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If schema emulation has been enabled in the connector properties, i.e. ``etc/catalog/kudu.properties``,
tables are mapped to schemas depending on some conventions.

- With ``kudu.schema-emulation.enabled=true`` and ``kudu.schema-emulation.prefix=``,
  the mapping works like:

  +----------------------------+---------------------------------+
  | Kudu Table Name            | Presto Qualified Name           |
  +============================+=================================+
  | ``orders``                 | ``kudu.default.orders``         |
  +----------------------------+---------------------------------+
  | ``part1.part2``            | ``kudu.part1.part2``            |
  +----------------------------+---------------------------------+
  | ``x.y.z``                  | ``kudu.x."y.z"``                |
  +----------------------------+---------------------------------+

  As schemas are not directly supported by Kudu, a special table named
  ``$schemas`` is created for managing the schemas.


- With ``kudu.schema-emulation.enabled=true`` and ``kudu.schema-emulation.prefix=presto::``,
  the mapping works like:

  +----------------------------+---------------------------------+
  | Kudu Table Name            | Presto Qualified Name           |
  +============================+=================================+
  | ``orders``                 | ``kudu.default.orders``         |
  +----------------------------+---------------------------------+
  | ``part1.part2``            | ``kudu.default."part1.part2"``  |
  +----------------------------+---------------------------------+
  | ``x.y.z``                  | ``kudu.default."x.y.z"``        |
  +----------------------------+---------------------------------+
  | ``presto::part1.part2``    | ``kudu.part1.part2``            |
  +----------------------------+---------------------------------+
  | ``presto:x.y.z``           | ``kudu.x."y.z"``                |
  +----------------------------+---------------------------------+

  As schemas are not directly supported by Kudu, a special table named
  ``presto::$schemas`` is created for managing the schemas.

Data Type Mapping
-----------------

The data types of Presto and Kudu are mapped as far as possible:

+-----------------------+-----------------------+-----------------------+
| Presto Data Type      | Kudu Data Type        | Comment               |
+=======================+=======================+=======================+
| ``BOOLEAN``           | ``BOOL``              |                       |
+-----------------------+-----------------------+-----------------------+
| ``TINYINT``           | ``INT8``              |                       |
+-----------------------+-----------------------+-----------------------+
| ``SMALLINT``          | ``INT16``             |                       |
+-----------------------+-----------------------+-----------------------+
| ``INTEGER``           | ``INT32``             |                       |
+-----------------------+-----------------------+-----------------------+
| ``BIGINT``            | ``INT64``             |                       |
+-----------------------+-----------------------+-----------------------+
| ``REAL``              | ``FLOAT``             |                       |
+-----------------------+-----------------------+-----------------------+
| ``DOUBLE``            | ``DOUBLE``            |                       |
+-----------------------+-----------------------+-----------------------+
| ``VARCHAR``           | ``STRING``            | see [1]_              |
+-----------------------+-----------------------+-----------------------+
| ``VARBINARY``         | ``BINARY``            | see [1]_              |
+-----------------------+-----------------------+-----------------------+
| ``TIMESTAMP``         | ``UNIXTIME_MICROS``   | µs resolution in Kudu |
|                       |                       | column is reduced to  |
|                       |                       | ms resolution         |
+-----------------------+-----------------------+-----------------------+
| ``DECIMAL``           | ``DECIMAL``           | only supported for    |
|                       |                       | Kudu server >= 1.7.0  |
+-----------------------+-----------------------+-----------------------+
| ``CHAR``              | -                     | not supported         |
+-----------------------+-----------------------+-----------------------+
| ``DATE``              | -                     | not supported [2]_    |
+-----------------------+-----------------------+-----------------------+
| ``TIME``              | -                     | not supported         |
+-----------------------+-----------------------+-----------------------+
| ``JSON``              | -                     | not supported         |
+-----------------------+-----------------------+-----------------------+
| ``TIME WITH           | -                     | not supported         |
| TIMEZONE``            |                       |                       |
+-----------------------+-----------------------+-----------------------+
| ``TIMESTAMP WITH TIME | -                     | not supported         |
| ZONE``                |                       |                       |
+-----------------------+-----------------------+-----------------------+
| ``INTERVAL YEAR TO MO | -                     | not supported         |
| NTH``                 |                       |                       |
+-----------------------+-----------------------+-----------------------+
| ``INTERVAL DAY TO SEC | -                     | not supported         |
| OND``                 |                       |                       |
+-----------------------+-----------------------+-----------------------+
| ``ARRAY``             | -                     | not supported         |
+-----------------------+-----------------------+-----------------------+
| ``MAP``               | -                     | not supported         |
+-----------------------+-----------------------+-----------------------+
| ``IPADDRESS``         | -                     | not supported         |
+-----------------------+-----------------------+-----------------------+


.. [1] On performing ``CREATE TABLE ... AS ...`` from a Presto table to Kudu,
   the optional maximum length is lost

.. [2] On performing ``CREATE TABLE ... AS ...`` from a Presto table to Kudu,
   a ``DATE`` column is converted to ``STRING``


Supported Presto SQL statements
-------------------------------

+------------------------------------------+-------------------------------+
| Presto SQL statement                     | Comment                       |
+==========================================+===============================+
| ``SELECT``                               |                               |
+------------------------------------------+-------------------------------+
| ``INSERT INTO ... VALUES``               | Behaves like ``upsert``       |
+------------------------------------------+-------------------------------+
| ``INSERT INTO ... SELECT ...``           | Behaves like ``upsert``       |
+------------------------------------------+-------------------------------+
| ``DELETE``                               |                               |
+------------------------------------------+-------------------------------+
| ``CREATE SCHEMA``                        | Only allowed, if schema       |
|                                          | emulation is enabled          |
+------------------------------------------+-------------------------------+
| ``DROP SCHEMA``                          | Only allowed, if schema       |
|                                          | emulation is enabled          |
+------------------------------------------+-------------------------------+
| ``CREATE TABLE``                         | See `Create Table`_           |
+------------------------------------------+-------------------------------+
| ``CREATE TABLE ... AS``                  |                               |
+------------------------------------------+-------------------------------+
| ``DROP TABLE``                           |                               |
+------------------------------------------+-------------------------------+
| ``ALTER TABLE ... RENAME TO ...``        |                               |
+------------------------------------------+-------------------------------+
| ``ALTER TABLE ... RENAME COLUMN ...``    | Only allowed, if not part of  |
|                                          | primary key                   |
+------------------------------------------+-------------------------------+
| ``ALTER TABLE ... ADD COLUMN ...``       | See `Add Column`_             |
+------------------------------------------+-------------------------------+
| ``ALTER TABLE ... DROP COLUMN ...``      | Only allowed, if not part of  |
|                                          | primary key                   |
+------------------------------------------+-------------------------------+
| ``SHOW SCHEMAS``                         |                               |
+------------------------------------------+-------------------------------+
| ``SHOW TABLES``                          |                               |
+------------------------------------------+-------------------------------+
| ``SHOW CREATE TABLE``                    |                               |
+------------------------------------------+-------------------------------+
| ``SHOW COLUMNS FROM``                    |                               |
+------------------------------------------+-------------------------------+
| ``DESCRIBE``                             | Same as ``SHOW COLUMNS FROM`` |
+------------------------------------------+-------------------------------+
| ``CALL kudu.system.add_range_partition`` | Adds range partition to a     |
|                                          | table. See `Managing range    |
|                                          | partitions`_                  |
+------------------------------------------+-------------------------------+
| ``CALL kudu.system.drop_range_partition``| Drops a range partition       |
|                                          | from a table. See `Managing   |
|                                          | range partitions`_            |
+------------------------------------------+-------------------------------+

``ALTER SCHEMA ... RENAME TO ...`` is not supported.


Create Table
------------

On creating a Kudu Table you need to provide the columns and their types, of
course, but Kudu needs information about partitioning and optionally
for column encoding and compression.

Simple Example:

  .. code:: sql

    CREATE TABLE user_events (
      user_id int WITH (primary_key = true),
      event_name varchar WITH (primary_key = true),
      message varchar,
      details varchar WITH (nullable = true, encoding = 'plain')
    ) WITH (
      partition_by_hash_columns = ARRAY['user_id'],
      partition_by_hash_buckets = 5,
      number_of_replicas = 3
    );

The primary key consists of ``user_id`` and ``event_name``, the table is partitioned into
five partitions by hash values of the column ``user_id``, and the ``number_of_replicas`` is
explicitly set to 3.

The primary key columns must always be the first columns of the column list.
All columns used in partitions must be part of the primary key.

The table property ``number_of_replicas`` is optional. It defines the
number of tablet replicas and must be an odd number. If it is not specified,
the default replication factor from the Kudu master configuration is used.

Kudu supports two different kinds of partitioning: hash and range partitioning.
Hash partitioning distributes rows by hash value into one of many buckets.
Range partitions distributes rows using a totally-ordered range partition key.
The concrete range partitions must be created explicitly.
Kudu also supports multi-level partitioning. A table must have at least one
partitioning (either hash or range). It can have at most one range partitioning,
but multiple hash partitioning 'levels'.

For more details see `Partitioning Design`_.


Column Properties
~~~~~~~~~~~~~~~~~

Besides column name and type, you can specify some more properties of a column.

+----------------------+---------------+---------------------------------------------------------+
| Column property name | Type          | Description                                             |
+======================+===============+=========================================================+
| ``primary_key``      | ``BOOLEAN``   | If ``true``, the column belongs to primary key columns. |
|                      |               | The Kudu primary key enforces a uniqueness constraint.  |
|                      |               | Inserting a second row with the same primary key        |
|                      |               | results in updating the existing row ('UPSERT').        |
|                      |               | See also `Primary Key Design`_ in the Kudu              |
|                      |               | documentation.                                          |
+----------------------+---------------+---------------------------------------------------------+
| ``nullable``         | ``BOOLEAN``   | If ``true``, the value can be null. Primary key         |
|                      |               | columns must not be nullable.                           |
+----------------------+---------------+---------------------------------------------------------+
| ``encoding``         | ``VARCHAR``   | The column encoding can help to save storage space and  |
|                      |               | to improve query performance. Kudu uses an auto         |
|                      |               | encoding depending on the column type if not specified. |
|                      |               | Valid values are:                                       |
|                      |               | ``'auto'``, ``'plain'``, ``'bitshuffle'``,              |
|                      |               | ``'runlength'``, ``'prefix'``, ``'dictionary'``,        |
|                      |               | ``'group_varint'``.                                     |
|                      |               | See also `Column encoding`_ in the Kudu documentation.  |
+----------------------+---------------+---------------------------------------------------------+
| ``compression``      | ``VARCHAR``   | The encoded column values can be compressed. Kudu uses  |
|                      |               | a default compression if not specified.                 |
|                      |               | Valid values are:                                       |
|                      |               | ``'default'``, ``'no'``, ``'lz4'``, ``'snappy'``,       |
|                      |               | ``'zlib'``.                                             |
|                      |               | See also `Column compression`_ in the Kudu              |
|                      |               | documentation.                                          |
+----------------------+---------------+---------------------------------------------------------+

.. _`Primary Key Design`: http://kudu.apache.org/docs/schema_design.html#primary-keys
.. _`Column encoding`: https://kudu.apache.org/docs/schema_design.html#encoding
.. _`Column compression`: https://kudu.apache.org/docs/schema_design.html#compression


Example
^^^^^^^

  .. code:: sql

    CREATE TABLE mytable (
      name varchar WITH (primary_key = true, encoding = 'dictionary', compression = 'snappy'),
      index bigint WITH (nullable = true, encoding = 'runlength', compression = 'lz4'),
      comment varchar WITH (nullable = true, encoding = 'plain', compression = 'default'),
       ...
    ) WITH (...);



Partitioning Design
~~~~~~~~~~~~~~~~~~~

A table must have at least one partitioning (either hash or range).
It can have at most one range partitioning, but multiple hash partitioning 'levels'.
For more details see Apache Kudu documentation: `Partitioning`_

If you create a Kudu table in Presto, the partitioning design is given by
several table properties.

.. _Partitioning: https://kudu.apache.org/docs/schema_design.html#partitioning


Hash partitioning
^^^^^^^^^^^^^^^^^

You can provide the first hash partition group with two table properties:

The ``partition_by_hash_columns`` defines the column(s) belonging to the
partition group and ``partition_by_hash_buckets`` the number of partitions to
split the hash values range into. All partition columns must be part of the
primary key.


Example:

  .. code:: sql

    CREATE TABLE mytable (
      col1 varchar WITH (primary_key=true),
      col2 varchar WITH (primary_key=true),
      ...
    ) WITH (
      partition_by_hash_columns = ARRAY['col1', 'col2'],
      partition_by_hash_buckets = 4
    )


This defines a hash partitioning with the columns ``col1`` and ``col2``
distributed over 4 partitions.

To define two separate hash partition groups use also the second pair
of table properties named ``partition_by_second_hash_columns`` and
``partition_by_second_hash_buckets``.

Example:

  .. code:: sql

    CREATE TABLE mytable (
      col1 varchar WITH (primary_key=true),
      col2 varchar WITH (primary_key=true),
      ...
    ) WITH (
      partition_by_hash_columns = ARRAY['col1'],
      partition_by_hash_buckets = 2,
      partition_by_second_hash_columns = ARRAY['col2'],
      partition_by_second_hash_buckets = 3
    )

This defines a two-level hash partitioning with the first hash partition group
over the column ``col1`` distributed over 2 buckets and the second
hash partition group over the column ``col2`` distributed over 3 buckets.
As a result you have table with 2 x 3 = 6 partitions.


Range partitioning
^^^^^^^^^^^^^^^^^^

You can provide at most one range partitioning in Apache Kudu. The columns
are defined with the table property ``partition_by_range_columns``.
The ranges themselves are given either in the
table property ``range_partitions`` on creating the table.
Or alternatively, the procedures ``kudu.system.add_range_partition`` and
``kudu.system.drop_range_partition`` can be used to manage range
partitions for existing tables. For both ways see below for more
details.

Example:

  .. code:: sql

    CREATE TABLE events (
      rack varchar WITH (primary_key=true),
      machine varchar WITH (primary_key=true),
      event_time timestamp WITH (primary_key=true),
      ...
    ) WITH (
      partition_by_hash_columns = ARRAY['rack'],
      partition_by_hash_buckets = 2,
      partition_by_second_hash_columns = ARRAY['machine'],
      partition_by_second_hash_buckets = 3,
      partition_by_range_columns = ARRAY['event_time'],
      range_partitions = '[{"lower": null, "upper": "2018-01-01T00:00:00"}, {"lower": "2018-01-01T00:00:00", "upper": null}]'
    )

This defines a tree-level partitioning with two hash partition groups and
one range partitioning on the ``event_time`` column.
Two range partitions are created with a split at “2018-01-01T00:00:00”.


Table property ``range_partitions``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the ``range_partitions`` table property you specify the concrete
range partitions to be created. The range partition definition itself
must be given in the table property ``partition_design`` separately.

Example:

  .. code:: sql

    CREATE TABLE events (
      serialno varchar WITH (primary_key = true),
      event_time timestamp WITH (primary_key = true),
      message varchar
    ) WITH (
      partition_by_hash_columns = ARRAY['serialno'],
      partition_by_hash_buckets = 4,
      partition_by_range_columns = ARRAY['event_time'],
      range_partitions = '[{"lower": null, "upper": "2017-01-01T00:00:00"},
                           {"lower": "2017-01-01T00:00:00", "upper": "2017-07-01T00:00:00"},
                           {"lower": "2017-07-01T00:00:00", "upper": "2018-01-01T00:00:00"}]'
    );

This creates a table with a hash partition on column ``serialno`` with 4
buckets and range partitioning on column ``event_time``. Additionally
three range partitions are created:

    1. for all event_times before the year 2017 (lower bound = ``null`` means it is unbound)
    2. for the first half of the year 2017
    3. for the second half the year 2017

This means any try to add rows with ``event_time`` of year 2018 or greater will fail, as no partition is defined.
The next section shows how to define a new range partition for an existing table.

Managing range partitions
^^^^^^^^^^^^^^^^^^^^^^^^^

For existing tables, there are procedures to add and drop a range
partition.

- adding a range partition

  .. code:: sql

    CALL kudu.system.add_range_partition(<schema>, <table>, <range_partition_as_json_string>),

- dropping a range partition

  .. code:: sql

    CALL kudu.system.drop_range_partition(<schema>, <table>, <range_partition_as_json_string>)

  - ``<schema>``: schema of the table

  - ``<table>``: table names

  - ``<range_partition_as_json_string>``: lower and upper bound of the
    range partition as json string in the form
    ``'{"lower": <value>, "upper": <value>}'``, or if the range partition
    has multiple columns:
    ``'{"lower": [<value_col1>,...], "upper": [<value_col1>,...]}'``. The
    concrete literal for lower and upper bound values are depending on
    the column types.

    Examples:

    +-------------------------------+----------------------------------------------+
    | Presto Data Type              | JSON string example                          |
    +===============================+==============================================+
    | ``BIGINT``                    | ``‘{“lower”: 0, “upper”: 1000000}’``         |
    +-------------------------------+----------------------------------------------+
    | ``SMALLINT``                  | ``‘{“lower”: 10, “upper”: null}’``           |
    +-------------------------------+----------------------------------------------+
    | ``VARCHAR``                   | ``‘{“lower”: “A”, “upper”: “M”}’``           |
    +-------------------------------+----------------------------------------------+
    | ``TIMESTAMP``                 | ``‘{“lower”: “2018-02-01T00:00:00.000”,      |
    |                               | “upper”: “2018-02-01T12:00:00.000”}’``       |
    +-------------------------------+----------------------------------------------+
    | ``BOOLEAN``                   | ``‘{“lower”: false, “upper”: true}’``        |
    +-------------------------------+----------------------------------------------+
    | ``VARBINARY``                 | values encoded as base64 strings             |
    +-------------------------------+----------------------------------------------+

    To specified an unbounded bound, use the value ``null``.

Example:

  .. code:: sql

    CALL kudu.system.add_range_partition('myschema', 'events', '{"lower": "2018-01-01", "upper": "2018-06-01"}')

This would add a range partition for a table ``events`` in the schema
``myschema`` with the lower bound ``2018-01-01`` (more exactly
``2018-01-01T00:00:00.000``) and the upper bound ``2018-07-01``.

Use the sql statement ``SHOW CREATE TABLE`` to query the existing
range partitions (they are shown in the table property
``range_partitions``).

Add Column
----------

Adding a column to an existing table uses the SQL statement ``ALTER TABLE ... ADD COLUMN ...``.
You can specify the same column properties as on creating a table.

Example:

  .. code:: sql

    ALTER TABLE mytable ADD COLUMN extraInfo varchar WITH (nullable = true, encoding = 'plain')

See also `Column Properties`_.


Known limitations
-----------------

-  Only lower case table and column names in Kudu are supported
-  Using a secured Kudu cluster has not been tested.
