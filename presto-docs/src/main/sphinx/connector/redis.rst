===============
Redis Connector
===============

The Redis connector allows querying of live data stored in Redis. This can be
used to join data between different systems like Redis and Hive.

Each Redis key/value pair is presented as a single row in Presto. Rows can be
broken down into cells by using table definition files.

Only Redis string and hash value types are supported; sets and zsets cannot be
queried from Presto.

The connector requires Redis 2.8.0 or later.

Configuration
-------------

To configure the Redis connector, create a catalog properties file
``etc/catalog/redis.properties`` with the following content,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=redis
    redis.table-names=schema1.table1,schema1.table2
    redis.nodes=host:port

Multiple Redis Servers
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Redis servers, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``).

Configuration Properties
------------------------

The following configuration properties are available:

=================================   ==============================================================
Property Name                       Description
=================================   ==============================================================
``redis.table-names``               List of all tables provided by the catalog
``redis.default-schema``            Default schema name for tables
``redis.nodes``                     Location of the Redis server
``redis.scan-count``                Redis parameter for scanning of the keys
``redis.key-prefix-schema-table``   Redis keys have schema-name:table-name prefix
``redis.key-delimiter``             Delimiter separating schema_name and table_name if redis.key-prefix-schema-table is used
``redis.table-description-dir``     Directory containing table description files
``redis.hide-internal-columns``     Controls whether internal columns are part of the table schema or not
``redis.database-index``            Redis database index
``redis.password``                  Redis server password
=================================   ==============================================================

``redis.table-names``
^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and will be put into the default schema
(see below) or qualified with a schema name (``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) may
exist. If no table description file exists, the
table will only contain internal columns (see below).

This property is required; there is no default and at least one table must be
defined.

``redis.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema which will contain all tables that were defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``redis.nodes``
^^^^^^^^^^^^^^^

The ``hostname:port`` pair for the Redis server.

This property is required; there is no default.

Redis clusters are not supported.

``redis.scan-count``
^^^^^^^^^^^^^^^^^^^^

The internal COUNT parameter for Redis SCAN command when connector is using
SCAN to find keys for the data. This parameter can be used to tune performance
of the Redis connector.

This property is optional; the default is ``100``.

``redis.key-prefix-schema-table``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If true, only keys prefixed with the ``schema-name:table-name`` are be scanned
for a table, and all other keys will be filtered out.  If false, all keys are
scanned.

This property is optional; the default is ``false``.

``redis.key-delimiter``
^^^^^^^^^^^^^^^^^^^^^^^

The character used for separating ``schema-name`` and ``table-name`` when
``redis.key-prefix-schema-table`` is ``true``

This property is optional; the default is ``:``.

``redis.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

References a folder within Presto deployment that holds one or more JSON
files (must end with ``.json``) which contain table description files.

This property is optional; the default is ``etc/redis``.

``redis.hide-internal-columns``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the data columns defined in a table description file, the
connector maintains a number of additional columns for each table. If
these columns are hidden, they can still be used in queries but do not
show up in ``DESCRIBE <table-name>`` or ``SELECT *``.

This property is optional; the default is ``true``.

``redis.database-index``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Redis database to query.

This property is optional; the default is ``0``.

``redis.password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The password for password-protected Redis server.

This property is optional; the default is ``null``.


Internal Columns
----------------

For each defined table, the connector maintains the following columns:

======================= ========= =============================
Column name             Type      Description
======================= ========= =============================
``_key``                VARCHAR   Redis key.
``_value``              VARCHAR   Redis value corresponding to the key.
``_key_length``         BIGINT    Number of bytes in the key.
``_value_length``       BIGINT    Number of bytes in the value.
``_key_corrupt``        BOOLEAN   True if the decoder could not decode the key for this row. When true, data columns mapped from the key should be treated as invalid.
``_value_corrupt``      BOOLEAN   True if the decoder could not decode the message for this row. When true, data columns mapped from the value should be treated as invalid.
======================= ========= =============================

For tables without a table definition file, the ``_key_corrupt`` and
``_value_corrupt`` columns will always be ``false``.

Table Definition Files
----------------------

With the Redis connector it's possible to further reduce Redis key/value pairs into
granular cells provided the key/value sting follow a particular format. This process
will define new columns that can be further queried from Presto.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary but must end in ``.json``.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "key": {
            "dataFormat": ...,
            "fields": [
                ...
            ]
        },
        "value": {
            "dataFormat": ...,
            "fields": [
                ...
           ]
        }
    }

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``tableName``   required  string         Presto table name defined by this file.
``schemaName``  optional  string         Schema which will contain the table. If omitted, the default schema name is used.
``key``         optional  JSON object    Field definitions for data columns mapped to the value key.
``value``       optional  JSON object    Field definitions for data columns mapped to the value itself.
=============== ========= ============== =============================

Please refer to the `Kafka connector`_ page for the description of the ``dataFormat`` as well as various available decoders.

In addition to the above Kafka types, the Redis connector supports ``hash`` type for the ``value`` field which represent data stored in the Redis hash.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "value": {
            "dataFormat": "hash",
            "fields": [
                ...
           ]
        }
    }

.. _Kafka connector: ./kafka.html
