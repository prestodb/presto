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
segments get dropped. This can result in strange behavior if accessing the
same table multiple times in a single query (e.g., performing a self join).

.. note::

    Apache Kafka 0.8+ is supported although it is highly recommend to use 0.8.1 or later.

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

=============================== ==============================================================
Property Name                   Description
=============================== ==============================================================
``kafka.table-names``           List of all tables provided by the catalog
``kafka.default-schema``        Default schema name for tables
``kafka.nodes``                 List of nodes in the Kafka cluster
``kafka.connect-timeout``       Timeout for connecting to the Kafka cluster
``kafka.buffer-size``           Kafka read buffer size
``kafka.table-description-dir`` Directory containing topic description files
``kafka.hide-internal-columns`` Controls whether internal columns are part of the table schema or not
=============================== ==============================================================

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
    even if only a subset is specified here as segment files may be
    located only on a specific node.

``kafka.connect-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^

Timeout for connecting to a data node. A busy Kafka cluster may take quite
some time before accepting a connection; when seeing failed queries due to
timeouts, increasing this value is a good strategy.

This property is optional; the default is 10 seconds (``10s``).

``kafka.buffer-size``
^^^^^^^^^^^^^^^^^^^^^

Size of the internal data buffer for reading data from Kafka. The data
buffer must be able to hold at least one message and ideally can hold many
messages. There is one data buffer allocated per worker and data node.

This property is optional; the default is ``64kb``.

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
``_segment_start``      BIGINT    Lowest offset in the segment (inclusive) which contains this row. This offset is partition specific.
``_segment_end``        BIGINT    Highest offset in the segment (exclusive) which contains this row. The offset is partition specific. This is the same value as ``_segment_start`` of the next segment (if it exists).
``_segment_count``      BIGINT    Running count for the current row within the segment. For an uncompacted topic, ``_segment_start + _segment_count`` is equal to ``_partition_offset``.
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
``dataFormat``  optional  string    Selects the column decoder for this field. Default to the default decoder for this row data format and column type.
``mapping``     optional  string    Mapping information for the column. This is decoder specific, see below.
``formatHint``  optional  string    Sets a column specifc format hint to the column decoder.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
``comment``     optional  string    Add a column comment which is shown with ``DESCRIBE <table name>``.
=============== ========= ========= =============================

There is no limit on field descriptions for either key or message.

Row Decoding
------------

For key and message, a decoder is used to map data onto columns. If no
table definition file exists for a table, the ``dummy`` decoder is used.

The Kafka connector contains the following decoders:

* ``raw`` - do not convert the row data, use as raw bytes
* ``csv`` - interpret the value as CSV
* ``json`` - convert the value to a JSON object

The main purpose of the decoders is to select the appropriate field
decoders to interpret the message or key data.

Presto supports only four physical data types onto which the Presto types
are mapped: boolean, long, double and a sequence of bytes which is treated
as a string.

``raw`` Decoder
^^^^^^^^^^^^^^^

The raw decoder supports reading of raw (byte based) values from a message
or key and converting it into Presto columns.

For fields, the following attributes are supported:

* ``type`` - all Presto primitive data types are supported
* ``dataFormat`` - only ``_default`` supported (optional)
* ``mapping`` - selects the width of the data type converted
* ``formatHint`` - ``<start>[:<end>]``; start and end position of bytes to convert (optional)

The ``mapping`` column selects the number of bytes converted.
If absent, ``BYTE`` is assumed. All values are signed.

Supported values are:

* ``BYTE`` - one byte
* ``SHORT`` - two bytes
* ``INT`` - four bytes
* ``LONG`` - eight bytes
* ``FLOAT`` - four bytes (IEEE 754 format)
* ``DOUBLE`` - eight bytes (IEEE 754 format)

The ``type`` column defines the Presto data type on which the value is mapped.

* boolean based types require a mapping to ``BYTE``, ``SHORT``, ``INT`` or ``LONG``.
  Any other type will throw a conversion error.
  A value of ``0`` returns false, everything else true.
* long based types require a mapping to ``BYTE``, ``SHORT``, ``INT`` or ``LONG``.
  Any other type will throw a conversion error.
* double based types require a mapping to ``FLOAT`` or ``DOUBLE``.
  Any other type will throw a conversion error.
* string based types require a mapping to ``BYTE``.
  Any other type will throw a conversion error.

The ``formatHint`` field specifies the position of the bytes in a key or
message. It can be one or two numbers separated by a colon (``<start>[:<end>]``).
If only a start position is given, the column will use the appropriate
number of bytes for the type (see above). string based types (``VARCHAR``)
will use all bytes to the end of the message. If start and end position is
given, then for fixed with types the size must be at least the size of the
type. For string based types, all bytes between start (inclusive) and end
(exclusive) are used.

``csv`` Decoder
^^^^^^^^^^^^^^^

.. note:: The CSV decoder is of beta quality and should be used with caution.

The CSV decoder converts the bytes representing a message or key into a
string using UTF-8 encoding and then interprets the result as a CSV
(comma-separated value) line.

For fields, the following attributes are supported:

* ``type`` - all Presto primitive data types are supported
* ``dataFormat`` - only ``_default`` supported (optional)
* ``mapping`` - field index used for the column (required)
* ``formatHint`` - not supported, ignored

* boolean based types return ``true`` if the field value is the string "true" (case insensitive), ``false`` otherwise.
* long and double based types parse the field value according to Java long and double parse rules.
* string types use the field as-is (text using UTF-8 encoding)

``json`` Decoder
^^^^^^^^^^^^^^^^

The JSON decoder converts the bytes representing a message or key into a
JSON according to :rfc:`4627`. Note that the message or key *MUST* convert
into a JSON object, not an array or simple type.

For fields, the following attributes are supported:

* ``type`` - all Presto primitive data types are supported
* ``dataFormat`` - ``_default``, ``custom-date-time``, ``iso8601``, ``rfc2822``,
  ``milliseconds-since-epoch``, ``seconds-since-epoch``. If missing, ``_default`` is used.
* ``mapping`` - slash-separated list of field names to select a field from the JSON object
* ``formatHint`` - only for ``custom-date-time``, see below

The JSON decoder supports multiple field decoders, with ``_default`` being
used for standard table columns and a number of decoders for date and time
based types.

``_default`` Field decoder
^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the standard field decoder supporting all the Presto physical data
types. A field value will be coerced by JSON conversion rules into
boolean, long, double or string values. For non-date/time based columns,
this decoder should be used.

Date and Time Decoders
^^^^^^^^^^^^^^^^^^^^^^

To convert values from JSON objects into Presto ``DATE``, ``TIME`` or
``TIMESTAMP`` columns, special decoders can be selected using the
``dataFormat`` attribute of a field definition.

Text Decoders
"""""""""""""

* ``iso8601`` - text based, parses a text field as an ISO 8601 timestamp.
* ``rfc2822`` - text based, parses a text field as an :rfc:`2822` timestamp.
* ``custom-date-time`` - text based, a formatting hint is required which is parsed as a Joda-Time formatting string.

===================== ========================================================= =========================================================
Presto Type           JSON Text                                                 JSON Long
===================== ========================================================= =========================================================
string type           as-is                                                     parse according to format type, return millis since epoch
long-based type       parse according to format type, return millis since epoch return as millis since epoch
===================== ========================================================= =========================================================

Number Decoders
"""""""""""""""

* ``milliseconds-since-epoch`` - number based, interprets a text or number as number of milliseconds since the epoch.
* ``seconds-since-epoch`` - number based, interprets a text or number as number of milliseconds since the epoch.

===================== ========================================================= =========================================================
Presto Type           JSON Text                                                 JSON Long
===================== ========================================================= =========================================================
string type           parse as long, format as ISO8601                          format as ISO8601
long-based type       parse as long, return millis since epoch                  return millis since epoch
===================== ========================================================= =========================================================
