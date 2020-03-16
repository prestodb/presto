===============
Kafka Connector
===============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

This connector allows the use of Apache Kafka topics as tables in Presto.
Each message is presented as a row in Presto.

Topics can be live: rows will appear as data arrives and disappear as
messages get dropped. This can result in strange behavior if accessing the
same table multiple times in a single query (e.g., performing a self join).

.. note::

    Apache Kafka 2.3.1+ is supported.

Configuration
-------------

To configure the Kafka connector, create a catalog properties file
``etc/catalog/kafka.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=kafka
    kafka.table-names=table1,table2
    kafka.nodes=host1:port,host2:port

Multiple Kafka Clusters
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Kafka clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Configuration Properties
------------------------

The following configuration properties are available:

=================================== ==============================================================
Property Name                       Description
=================================== ==============================================================
``kafka.table-names``               List of all tables provided by the catalog
``kafka.default-schema``            Default schema name for tables
``kafka.nodes``                     List of nodes in the Kafka cluster
``kafka.connect-timeout``           Timeout for connecting to the Kafka cluster
``kafka.max-poll-records``          Maximum number of records per poll
``kafka.max-partition-fetch-bytes`` Maximum number of bytes from one partition per poll
``kafka.table-description-dir``     Directory containing topic description files
``kafka.hide-internal-columns``     Controls whether internal columns are part of the table schema or not
=================================== ==============================================================

``kafka.table-names``
^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and will be put into the default schema
(see below) or qualified with a schema name (``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) may
exist. If no table description file exists, the table name is used as the
topic name on Kafka and no data columns are mapped into the table. The
table will still contain all internal columns (see below).

This property is required; there is no default and at least one table must be defined.

``kafka.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema which will contain all tables that were defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``kafka.nodes``
^^^^^^^^^^^^^^^

A comma separated list of ``hostname:port`` pairs for the Kafka data nodes.

This property is required; there is no default and at least one node must be defined.

.. note::

    Presto must still be able to connect to all nodes of the cluster
    even if only a subset is specified here as messages may be
    located only on a specific node.

``kafka.connect-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^

Timeout for connecting to a data node. A busy Kafka cluster may take quite
some time before accepting a connection; when seeing failed queries due to
timeouts, increasing this value is a good strategy.

This property is optional; the default is 10 seconds (``10s``).

``kafka.max-poll-records``
^^^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of records per poll() from Kafka.

This property is optional; the default is ``500``.

kafka.max-partition-fetch-bytes``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum number of bytes from one partition per poll

This property is optional; the default is ``1MB``.

``kafka.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

References a folder within Presto deployment that holds one or more JSON
files (must end with ``.json``) which contain table description files.

This property is optional; the default is ``etc/kafka``.

``kafka.hide-internal-columns``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the data columns defined in a table description file, the
connector maintains a number of additional columns for each table. If
these columns are hidden, they can still be used in queries but do not
show up in ``DESCRIBE <table-name>`` or ``SELECT *``.

This property is optional; the default is ``true``.

Internal Columns
----------------

For each defined table, the connector maintains the following columns:

======================= ========= =============================
Column name             Type      Description
======================= ========= =============================
``_partition_id``       BIGINT    ID of the Kafka partition which contains this row.
``_partition_offset``   BIGINT    Offset within the Kafka partition for this row.
``_message_corrupt``    BOOLEAN   True if the decoder could not decode the message for this row. When true, data columns mapped from the message should be treated as invalid.
``_message``            VARCHAR   Message bytes as an UTF-8 encoded string. This is only useful for a text topic.
``_message_length``     BIGINT    Number of bytes in the message.
``_key_corrupt``        BOOLEAN   True if the key decoder could not decode the key for this row. When true, data columns mapped from the key should be treated as invalid.
``_key``                VARCHAR   Key bytes as an UTF-8 encoded string. This is only useful for textual keys.
``_key_length``         BIGINT    Number of bytes in the key.
======================= ========= =============================

For tables without a table definition file, the ``_key_corrupt`` and
``_message_corrupt`` columns will always be ``false``.

Table Definition Files
----------------------

Kafka maintains topics only as byte messages and leaves it to producers
and consumers to define how a message should be interpreted. For Presto,
this data must be mapped into columns to allow queries against the data.

.. note::

    For textual topics that contain JSON data, it is entirely possible to not
    use any table definition files, but instead use the Presto
    :doc:`/functions/json` to parse the ``_message`` column which contains
    the bytes mapped into an UTF-8 string. This is, however, pretty
    cumbersome and makes it difficult to write SQL queries.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary but must end in ``.json``.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "topicName": ...,
        "key": {
            "dataFormat": ...,
            "fields": [
                ...
            ]
        },
        "message": {
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
``topicName``   required  string         Kafka topic that is mapped.
``key``         optional  JSON object    Field definitions for data columns mapped to the message key.
``message``     optional  JSON object    Field definitions for data columns mapped to the message itself.
=============== ========= ============== =============================

Key and Message in Kafka
------------------------

Starting with Kafka 0.8, each message in a topic can have an optional key.
A table definition file contains sections for both key and message to map
the data onto table columns.

Each of the ``key`` and ``message`` fields in the table definition is a
JSON object that must contain two fields:

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``dataFormat``  required  string         Selects the decoder for this group of fields.
``fields``      required  JSON array     A list of field definitions. Each field definition creates a new column in the Presto table.
=============== ========= ============== =============================

Each field definition is a JSON object:

.. code-block:: none

    {
        "name": ...,
        "type": ...,
        "dataFormat": ...,
        "mapping": ...,
        "formatHint": ...,
        "hidden": ...,
        "comment": ...
    }

=============== ========= ========= =============================
Field           Required  Type      Description
=============== ========= ========= =============================
``name``        required  string    Name of the column in the Presto table.
``type``        required  string    Presto type of the column.
``dataFormat``  optional  string    Selects the column decoder for this field. Defaults to the default decoder for this row data format and column type.
``dataSchema``  optional  string    The path or URL where the Avro schema resides. Used only for Avro decoder.
``mapping``     optional  string    Mapping information for the column. This is decoder specific, see below.
``formatHint``  optional  string    Sets a column specific format hint to the column decoder.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
``comment``     optional  string    Adds a column comment which is shown with ``DESCRIBE <table name>``.
=============== ========= ========= =============================

There is no limit on field descriptions for either key or message.

Row Decoding
------------

For key and message, a decoder is used to map message and key data onto table columns.

The Kafka connector contains the following decoders:

* ``raw`` - Kafka message is not interpreted, ranges of raw message bytes are mapped to table columns
* ``csv`` - Kafka message is interpreted as comma separated message, and fields are mapped to table columns
* ``json`` - Kafka message is parsed as JSON and JSON fields are mapped to table columns
* ``avro`` - Kafka message is parsed based on an Avro schema and Avro fields are mapped to table columns

.. note::

    If no table definition file exists for a table, the ``dummy`` decoder is used,
    which does not expose any columns.

``raw`` Decoder
^^^^^^^^^^^^^^^

The raw decoder supports reading of raw (byte based) values from Kafka message
or key and converting it into Presto columns.

For fields, the following attributes are supported:

* ``dataFormat`` - selects the width of the data type converted
* ``type`` - Presto data type (see table below for list of supported data types)
* ``mapping`` - ``<start>[:<end>]``; start and end position of bytes to convert (optional)

The ``dataFormat`` attribute selects the number of bytes converted.
If absent, ``BYTE`` is assumed. All values are signed.

Supported values are:

* ``BYTE`` - one byte
* ``SHORT`` - two bytes (big-endian)
* ``INT`` - four bytes (big-endian)
* ``LONG`` - eight bytes (big-endian)
* ``FLOAT`` - four bytes (IEEE 754 format)
* ``DOUBLE`` - eight bytes (IEEE 754 format)

The ``type`` attribute defines the Presto data type on which the value is mapped.

Depending on Presto type assigned to column different values of dataFormat can be used:

===================================== =======================================
Presto data type                      Allowed ``dataFormat`` values
===================================== =======================================
``BIGINT``                            ``BYTE``, ``SHORT``, ``INT``, ``LONG``
``INTEGER``                           ``BYTE``, ``SHORT``, ``INT``
``SMALLINT``                          ``BYTE``, ``SHORT``
``TINYINT``                           ``BYTE``
``DOUBLE``                            ``DOUBLE``, ``FLOAT``
``BOOLEAN``                           ``BYTE``, ``SHORT``, ``INT``, ``LONG``
``VARCHAR`` / ``VARCHAR(x)``          ``BYTE``
===================================== =======================================

The ``mapping`` attribute specifies the range of the bytes in a key or
message used for decoding. It can be one or two numbers separated by a colon (``<start>[:<end>]``).

If only a start position is given:

 * For fixed width types the column will use the appropriate number of bytes for the specified ``dateFormat`` (see above).
 * When ``VARCHAR`` value is decoded all bytes from start position till the end of the message will be used.

If start and end position are given, then:

 * For fixed width types the size must be equal to number of bytes used by specified ``dataFormat``.
 * For ``VARCHAR`` all bytes between start (inclusive) and end (exclusive) are used.

If no ``mapping`` attribute is specified it is equivalent to setting start position to 0 and leaving end position undefined.

Decoding scheme of numeric data types (``BIGINT``, ``INTEGER``, ``SMALLINT``, ``TINYINT``, ``DOUBLE``) is straightforward.
A sequence of bytes is read from input message and decoded according to either:

 * big-endian encoding (for integer types)
 * IEEE 754 format for (for ``DOUBLE``).

Length of decoded byte sequence is implied by the ``dataFormat``.

For ``VARCHAR`` data type a sequence of bytes is interpreted according to UTF-8 encoding.

``csv`` Decoder
^^^^^^^^^^^^^^^

The CSV decoder converts the bytes representing a message or key into a
string using UTF-8 encoding and then interprets the result as a CSV
(comma-separated value) line.

For fields, the ``type`` and ``mapping`` attributes must be defined:

* ``type`` - Presto data type (see table below for list of supported data types)
* ``mapping`` - the index of the field in the CSV record

``dataFormat`` and ``formatHint`` are not supported and must be omitted.

Table below lists supported Presto types which can be used in ``type`` and decoding scheme:

+-------------------------------------+--------------------------------------------------------------------------------+
| Presto data type                    | Decoding rules                                                                 |
+=====================================+================================================================================+
| | ``BIGINT``                        | Decoded using Java ``Long.parseLong()``                                        |
| | ``INTEGER``                       |                                                                                |
| | ``SMALLINT``                      |                                                                                |
| | ``TINYINT``                       |                                                                                |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``DOUBLE``                          | Decoded using Java ``Double.parseDouble()``                                    |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``BOOLEAN``                         | "true" character sequence maps to ``true``;                                    |
|                                     | Other character sequences map to ``false``                                     |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``VARCHAR`` / ``VARCHAR(x)``        | Used as is                                                                     |
+-------------------------------------+--------------------------------------------------------------------------------+


``json`` Decoder
^^^^^^^^^^^^^^^^

The JSON decoder converts the bytes representing a message or key into a
JSON according to :rfc:`4627`. Note that the message or key *MUST* convert
into a JSON object, not an array or simple type.

For fields, the following attributes are supported:

* ``type`` - Presto type of column.
* ``dataFormat`` - Field decoder to be used for column.
* ``mapping`` - slash-separated list of field names to select a field from the JSON object
* ``formatHint`` - only for ``custom-date-time``, see below

The JSON decoder supports multiple field decoders, with ``_default`` being
used for standard table columns and a number of decoders for date and time
based types.

Table below lists Presto data types which can be used as in ``type`` and matching field decoders
which can be specified via ``dataFormat`` attribute

+-------------------------------------+--------------------------------------------------------------------------------+
| Presto data type                    | Allowed ``dataFormat`` values                                                  |
+=====================================+================================================================================+
| | ``BIGINT``                        | Default field decoder (omitted ``dataFormat`` attribute)                       |
| | ``INTEGER``                       |                                                                                |
| | ``SMALLINT``                      |                                                                                |
| | ``TINYINT``                       |                                                                                |
| | ``DOUBLE``                        |                                                                                |
| | ``BOOLEAN``                       |                                                                                |
| | ``VARCHAR``                       |                                                                                |
| | ``VARCHAR(x)``                    |                                                                                |
+-------------------------------------+--------------------------------------------------------------------------------+
| | ``TIMESTAMP``                     | ``custom-date-time``, ``iso8601``, ``rfc2822``,                                |
| | ``TIMESTAMP WITH TIME ZONE``      | ``milliseconds-since-epoch``, ``seconds-since-epoch``                          |
| | ``TIME``                          |                                                                                |
| | ``TIME WITH TIME ZONE``           |                                                                                |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``DATE``                            | ``custom-date-time``, ``iso8601``, ``rfc2822``,                                |
+-------------------------------------+--------------------------------------------------------------------------------+


Default Field decoder
^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the standard field decoder supporting all the Presto physical data
types. A field value will be coerced by JSON conversion rules into
boolean, long, double or string values. For non-date/time based columns,
this decoder should be used.

Date and Time Decoders
^^^^^^^^^^^^^^^^^^^^^^

To convert values from JSON objects into Presto ``DATE``, ``TIME``, ``TIME WITH TIME ZONE`,
``TIMESTAMP`` or ``TIMESTAMP WITH TIME ZONE`` columns, special decoders must be selected using the
``dataFormat`` attribute of a field definition.

* ``iso8601`` - text based, parses a text field as an ISO 8601 timestamp.
* ``rfc2822`` - text based, parses a text field as an :rfc:`2822` timestamp.
* ``custom-date-time`` - text based, parses a text field according to Joda format pattern
                         specified via ``formatHint`` attribute. Format pattern should conform
                         to https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html.
* ``milliseconds-since-epoch`` - number based, interprets a text or number as number of milliseconds since the epoch.
* ``seconds-since-epoch`` - number based, interprets a text or number as number of milliseconds since the epoch.

For ``TIMESTAMP WITH TIME ZONE`` and ``TIME WITH TIME ZONE`` data types, if timezone information is present in decoded value, it will
be used in Presto value. Otherwise result time zone will be set to ``UTC``.

``avro`` Decoder
^^^^^^^^^^^^^^^^

The Avro decoder converts the bytes representing a message or key in
Avro format based on a schema. The message must have the Avro schema embedded.
Presto does not support schema-less Avro decoding.

For key/message, using ``avro`` decoder, the ``dataSchema`` must be defined.
This should point to the location of a valid Avro schema file of the message which needs to be decoded. This location can be a remote web server
(e.g.: ``dataSchema: 'http://example.org/schema/avro_data.avsc'``) or local file system(e.g.: ``dataSchema: '/usr/local/schema/avro_data.avsc'``).
The decoder will fail if this location is not accessible from the Presto coordinator node.

For fields, the following attributes are supported:

* ``name`` - Name of the column in the Presto table.
* ``type`` - Presto type of column.
* ``mapping`` - slash-separated list of field names to select a field from the Avro schema. If field specified in ``mapping`` does not exist in the original Avro schema then a read operation will return NULL.

Table below lists supported Presto types which can be used in ``type`` for the equivalent Avro field type/s.

===================================== =======================================
Presto data type                      Allowed Avro data type
===================================== =======================================
``BIGINT``                            ``INT``, ``LONG``
``DOUBLE``                            ``DOUBLE``, ``FLOAT``
``BOOLEAN``                           ``BOOLEAN``
``VARCHAR`` / ``VARCHAR(x)``          ``STRING``
``VARBINARY``                         ``FIXED``, ``BYTES``
``ARRAY``                             ``ARRAY``
``MAP``                               ``MAP``
===================================== =======================================

Avro schema evolution
#####################

The Avro decoder supports schema evolution feature with backward compatibility. With backward compatibility,
a newer schema can be used to read Avro data created with an older schema. Any change in the Avro schema must also be
reflected in Presto's topic definition file. Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

* Column added in new schema:
  Data created with an older schema will produce a *default* value when table is using the new schema.

* Column removed in new schema:
  Data created with an older schema will no longer output the data from the column that was removed.

* Column is renamed in the new schema:
  This is equivalent to removing the column and adding a new one, and data created with an older schema
  will produce a *default* value when table is using the new schema.

* Changing type of column in the new schema:
  If the type coercion is supported by Avro, then the conversion happens. An error is thrown for incompatible types.
