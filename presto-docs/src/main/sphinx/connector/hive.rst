==============
Hive Connector
==============

.. toctree::
    :maxdepth: 1

    hive/hive-filetypes
    hive/hive-metastore
    hive/hive-configuration
    hive/hive-security
    hive/hive-table-statistics
    hive/hive-schema
    hive/hive-procedures
    hive/hive-hidden-columns
    hive/hive-examples
    hive/hive-limitations

Overview
--------

The Hive connector allows querying data stored in a Hive
data warehouse. Hive is a combination of three components:

* Data files in varying formats that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database such as MySQL and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Parquet Writer Version
----------------------

Presto supports Parquet writer versions V1 and V2 for the Hive catalog.
It can be toggled using the session property ``parquet_writer_version`` and the config property ``hive.parquet.writer.version``.
Valid values for these properties are ``PARQUET_1_0`` and ``PARQUET_2_0``. Default is ``PARQUET_1_0``.