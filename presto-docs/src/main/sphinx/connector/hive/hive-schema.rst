=====================
Hive Connector Schema
=====================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Schema Evolution
----------------

Hive allows the partitions in a table to have a different schema than the
table. This occurs when the column types of a table are changed after
partitions already exist (that use the original column types). The Hive
connector supports this by allowing the same conversions as Hive:

* ``varchar`` to and from ``tinyint``, ``smallint``, ``integer`` and ``bigint``
* ``real`` to ``double``
* Widening conversions for integers, such as ``tinyint`` to ``smallint``

In addition to the conversions above, the Hive connector does also support the following conversions when working with Parquet file format:

* ``integer`` to ``bigint``, ``real`` and ``double``
* ``bigint`` to ``real`` and ``double``

Any conversion failure will result in null, which is the same behavior
as Hive. For example, converting the string ``foo`` to a number,
or converting the string ``1234`` to a ``tinyint`` (which has a
maximum value of ``127``).

Avro Schema Evolution
---------------------

Presto supports querying and manipulating Hive tables with Avro storage format which has the schema set
based on an Avro schema file/literal. It is also possible to create tables in Presto which infers the schema
from a valid Avro schema file located locally or remotely in HDFS/Web server.

To specify that Avro schema should be used for interpreting table's data one must use ``avro_schema_url`` table property.
The schema can be placed remotely in
HDFS (e.g. ``avro_schema_url = 'hdfs://user/avro/schema/avro_data.avsc'``),
S3 (e.g. ``avro_schema_url = 's3n:///schema_bucket/schema/avro_data.avsc'``),
a web server (e.g. ``avro_schema_url = 'http://example.org/schema/avro_data.avsc'``)
as well as the local file system. This url where the schema is located, must be accessible from the
Hive metastore and Presto coordinator/worker nodes.

The table created in Presto using ``avro_schema_url`` behaves the same way as a Hive table with ``avro.schema.url`` or ``avro.schema.literal`` set.

Example::

   CREATE TABLE hive.avro.avro_data (
      id bigint
    )
   WITH (
      format = 'AVRO',
      avro_schema_url = '/usr/local/avro_data.avsc'
   )

The columns listed in the DDL (``id`` in the above example) will be ignored if ``avro_schema_url`` is specified.
The table schema will match the schema in the Avro schema file. Before any read operation, the Avro schema is
accessed so query result reflects any changes in schema. Thus Presto takes advantage of Avro's backward compatibility abilities.

If the schema of the table changes in the Avro schema file, the new schema can still be used to read old data.
Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

* Column added in new schema:
  Data created with an older schema will produce a *default* value when table is using the new schema.

* Column removed in new schema:
  Data created with an older schema will no longer output the data from the column that was removed.

* Column is renamed in the new schema:
  This is equivalent to removing the column and adding a new one, and data created with an older schema
  will produce a *default* value when table is using the new schema.

* Changing type of column in the new schema:
  If the type coercion is supported by Avro or the Hive connector, then the conversion happens.
  An error is thrown for incompatible types.

Limitations
^^^^^^^^^^^

The following operations are not supported when ``avro_schema_url`` is set:

* ``CREATE TABLE AS`` is not supported.
* Using partitioning(``partitioned_by``) or bucketing(``bucketed_by``) columns are not supported in ``CREATE TABLE``.
* ``ALTER TABLE`` commands modifying columns are not supported.