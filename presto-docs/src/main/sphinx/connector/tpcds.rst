===============
TPCDS Connector
===============

The TPCDS connector provides a set of schemas to support the TPC
Benchmark™ DS (TPC-DS). TPC-DS is a database benchmark used to measure the
performance of complex decision support databases.

This connector can also be used to test the capabilities and query
syntax of Presto without configuring access to an external data
source. When you query a TPCDS schema, the connector generates the
data on the fly using a deterministic algorithm.

Configuration
-------------

To configure the TPCDS connector, create a catalog properties file
``etc/catalog/tpcds.properties`` with the following contents:

.. code-block:: none

    connector.name=tpcds

TPCDS Schemas
-------------

The TPCDS connector supplies several schemas::

    SHOW SCHEMAS FROM tpcds;

.. code-block:: none

           Schema
    --------------------
     information_schema
     sf1
     sf10
     sf100
     sf1000
     sf10000
     sf100000
     sf300
     sf3000
     sf30000
    (10 rows)

Ignore the standard schema ``information_schema`` which exists in every
catalog and is not directly provided by the TPCDS connector.

Every TPCDS schema provides the same set of tables. Some tables are
identical in all schemas. The *scale factor* of the tables in a particular
schema is determined from the schema name. For example, the schema
``sf1`` corresponds to scale factor ``1`` and the schema ``sf300``
corresponds to scale factor ``300``. Every unit in the scale factor
corresponds to a gigabyte of data. For example, for scale factor ```300```,
a total of ```300`` gigabytes will be generated.
