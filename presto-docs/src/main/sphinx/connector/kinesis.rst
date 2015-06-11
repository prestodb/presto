=================
Kinesis Connector
=================

Kinesis is Amazon’s fully managed cloud-based service for real-time processing
of large, distributed data streams.

Analogous to Kafka connector, this connector allows the use of Kinesis streams as tables in Presto,
such that each data-blob in kinesis stream is presented as a row in Presto.
Streams can be live: rows will appear as data is pushed into the stream,
and disappear as they are dropped once their time expires. (A message is held up for 24 hours by kinesis streams).
 This can result in strange behavior if accessing the same table multiple times in a single query. (eg. performing self join)

.. note::

    This connector is Read-Only connector. It can only fetch data from kinesis streams, but can not create streams or push data into the already existing streams.

 Configuration
 -------------
 
 To configure the Kinesis Connector, create a catalog properties file
 ``etc/catalog/kinesis.properties`` with the following contents,
 replacing the properties as appropriate.
 
 .. code-block:: none

    connector.name=kinesis
    kinesis.table-names=table1,table2
    kinesis.access-key=<amazon-access-key>
    kinesis.secret-key=<amazon-secret-key>

Configuration Properties
------------------------

The following configuration properties are available :

=================================== =========================================================================
Property Name                       Description
=================================== =========================================================================
``kinesis.table-names``             List of all the tables provided by the catalog.
``kinesis.default-schema``          Default schema name for tables.
``kinesis.table-description-dir``   Directory containing table description files.
``kinesis.access-key``              Access key to aws account.
``kinesis.secret-key``              Secret key to aws account.
``kinesis.hide-internal-columns``   Controls whether internal columns are part of the table schema or not.
``kinesis.aws-region``              Aws region to be used to read kinesis stream from.
``kinesis.batch-size``              Maximum number of records to return. Maximum Limit 10000.
``kinesis.fetch-attempts``          Attempts to be made to fetch data from kinesis streams.
``kinesis.sleep-time``              Time till which thread sleep waiting to make next attempt to fetch data.
=================================== =========================================================================

``kinesis.table-names``
^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and will be put into the default schema
(see below) or qualified with a scheme name (``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) may exist.
If no table description file exists, the table name is used as the stream
name on Kinesis and no data columns are mapped into the table. The table will
contain all internal columns (see below).

This property is required; there is no defualt and at least one table must be
defined.

``kinesis.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema which will contain all tables that were defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``kinesis.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

References a folder within Presto deployment that holds one or more JSON
files (must end wiht ``.json``) which contain table description files.

This property is optional; the default is ``etc/kinesis``.

``kinesis.access-key``
^^^^^^^^^^^^^^^^^^^^^^

Defines the access key ID for AWS root account or IAM roles, which is
used to sign programmatic requests to AWS Kinesis.

This property is required; it must be defined to access Kinesis streams.

``kinesis.secret-key``
^^^^^^^^^^^^^^^^^^^^^^

Defines the secret key for AWS root account or IAM roles, which together
with Access Key ID, is used to sign programmatic requests to AWS Kinesis.

This property is required; it must be defined to access Kinesis streams.

``kinesis.aws-region``
^^^^^^^^^^^^^^^^^^^^^^

Defines AWS Kinesis regional endpoint. Selecting appropriate region may
reduce latency in fetching data.

This field is optional; The default region is ``us-east-1`` referring to
end point 'kinesis.us-east-1.amazonaws.com'.

Amazon Kinesis Regions
----------------------

For each Amazon Kinesis account, following availabe regions can be used:

======================= =========================== =========================================
Region                  Region Name                 Endpoint
======================= =========================== =========================================
``us-east-1``           US East (N. Virginia)       kinesis.us-east-1.amazonaws.com
``us-west-1``           US West (N. California)     kinesis.us-west-1.amazonaws.com
``us-west-2``           US West (Oregon)            kinesis.us-west-2.amazonaws.com
``eu-west-1``           EU (Ireland)                kinesis.eu-west-1.amazonaws.com
``eu-central-1``        EU (Frankfurt)              kinesis.eu-central-1.amazonaws.com
``ap-southeast-1``      Asia Pacific (Singapore)    kinesis.ap-southeast-1.amazonaws.com
``ap-southeast-2``      Asia Pacific (Sydney)       kinesis.ap-southeast-2.amazonaws.com
``ap-northeast-1``      Asia Pacific (Tokyo)        kinesis.ap-northeast-1.amazonaws.com
======================= =========================== ==========================================

``kinesis.batch-size``
^^^^^^^^^^^^^^^^^^^^^^

Defines maximum number of records to return in one request to Kinesis Streams. Maximum Limit is
10000 records. If a value greater than 10000 is specified, will throw ``InvalidArgumentException``.

This field is optional; the default value is ``10000``.

``kinesis.fetch-attempts``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines number of failed attempts to be made to fetch data from Kinesis Streams. If first attempt returns
non empty records, then no further attempts are made.

.. note::

    It has been found that sometimes ``GetRecordResult`` returns empty records, when shard is not empty. That is why multiple attempts need to be made.

This field is optional; the default value is ``3``.

``kinesis.sleep-time``
^^^^^^^^^^^^^^^^^^^^^^

Defines the milliseconds for which thread needs to sleep between get-record-attempts made
to fetch data.

This field is optional; the defaul value is ``1000`` milliseconds.

``kinesis.hide-internal-columns``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the data columns defined in a table description file, the
connector maintains a number of additional columns for each table. If
these columns are hidden, they can still be used in queries but do not
show up in ``DESCRIBE <table-name>`` or ``SELECT *``.

This property is optional; the default is ``true``.

Internal Columns
----------------

For each defined table, the connector maintains the following columns:

======================= ========= ===========================================================
Column name             Type      Description
======================= ========= ===========================================================
``_shard_id``           VARCHAR   ID of the Kinesis stream shard which contains this row.
``_shard_sequence_id``  VARCHAR   Sequence id within the Kinesis shard for this row.
``_segment_start``      VARCHAR   Lowest sequence id in the segment (inclusive) which contains this row. This sequence id is shrard specific.
``_segment_end``        VARCHAR   Highest sequence id in the segment (exclusive) which contains this row. The sequence id is shard specific. If stream is open, then this is not defined.
``_segment_count``      BIGINT    Running count of for the current row within the segment.
``_message_valid``      BOOLEAN   True if the decoder could decode the message successfully for this row. When false, data columns mapped from the message should be treated as invalid.
``_message``            VARCHAR   Message bytes as an UTF-8 encoded string.
``_message_length``     BIGINT    Number of bytes in the message.
``_partition_key``      VARCHAR   Partition Key bytes as an UTF-8 encoded string.
======================= ========= ============================================================

For tables without a table definition file, the ``_message_valid`` column will
always be ``true``.

Table Definition Files
----------------------

Kinesis streams stores data as stream of bytes and leaves it to produceres and
consumers to define how a message should be interpreted. For Presto, this data
must be mapped into columns to allow queries against the data.

..note::

    For textual topics that contain JSON data, it is entirely possible to not
    use any table definition files, but instead use the Presto
    :doc:`/functions/json` to parse the ``_message`` column which contains
    the bytes mapped into an UTF-8 string. This is, however, pretty
    cumbersome and makes it difficult to write SQL queries.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary but must end in ``.json``.

.. code-block:: json

    {
        "tableName": ...,
        "schemaName": ...,
        "streamName": ...,
        "message": {
            "dataFormat": ...,
            "field": [
                ...
            ]
        }
    }

=============== ========= ============== ========================================================================
Field           Required  Type           Description
=============== ========= ============== ========================================================================
``tableName``   required  string         Presto table name defined by this file.
``schemaName``  optional  string         Schema which will contain the table. If omitted, the default schema name is used.
``streamName``  required  string         Kinesis Stream that is mapped.
``message``     optional  JSON object    Field definitions for data columns mapped to the message itself.
=============== ========= ============== =========================================================================

Every message in Kinesis stream can be decoded using the table definition file.
The json object message in table definition file contains two fields:

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``dataFormat``  required  string         Selects the decoder for this group of fields.
``fields``      required  JSON array     A list of field definitions. Each field definition creates a new column in the Presto table.
=============== ========= ============== =============================

Each field definition is a JSON object:

.. code-block:: json

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
``formatHint``  optional  string    Sets a column specific format hint to the column decoder.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
``comment``     optional  string    Add a column comment which is shown with ``DESCRIBE <table name>``.
=============== ========= ========= =============================

There is no limit on field descriptions for message.

Please refer to `Kafka Connector` _ page for further information about different decoders available.

.. _Kafka connector: ./kafka.html