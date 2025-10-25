========================
Hive Connector Metastore
========================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Hive Metastore
--------------

The Hive Metastore is a central metadata repository that the Hive connector uses to access table definitions, partition information, 
and other structural details about your Hive tables.

The Hive Metastore:

* Stores metadata about tables, columns, partitions, and storage locations
* Enables schema-on-read functionality
* Supports multiple metastore backends (Apache Hive Metastore Service, AWS Glue)

See the `Metastore <https://hive.apache.org/development/desingdocs/design/#metastore>`_ design documentation for more details.


Additional Resources for Metastore Configuration
------------------------------------------------

* :ref:`connector/hive/hive-configuration:Metastore Configuration Properties` 
* :ref:`connector/hive/hive-procedures:How to invalidate the metastore cache` 
* :ref:`installation/deployment:File-Based Metastore`
* :doc:`/connector/hive/hive-security`
* :ref:`connector/hive/hive-configuration:AWS Glue Catalog Configuration Properties`

Additional authentication-related configuration properties are covered in
:ref:`connector/hive/hive-security:Hive Metastore Thrift Service Authentication` and
:ref:`connector/hive/hive-security:HDFS Authentication`.

File-Based Metastore
--------------------

For testing or development purposes, this connector can be configured to use a local 
filesystem directory as a Hive Metastore. See :ref:`installation/deployment:File-Based Metastore`.  
