==============
TPCH Connector
==============

The TPCH connector provides a set of schemas to support the TPC
Benchmark™ H (TPC-H). TPC-H is a database benchmark used to measure the
performance of highly-complex decision support databases.

This connector can also be used to test the capabilities and query
syntax of Presto without configuring access to an external data
source. When you query a TPCH schema, the connector generates the
data on the fly using a deterministic algorithm.

Configuration
-------------

To configure the TPCH connector, create a catalog properties file
``etc/catalog/tpch.properties`` with the following contents:

.. code-block:: none

    connector.name=tpch

TPCH Schemas
------------

The TPCH connector supplies several schemas::

    SHOW SCHEMAS FROM tpch;

.. code-block:: none

           Schema
    --------------------
     information_schema
     sf1
     sf100
     sf1000
     sf10000
     sf100000
     sf300
     sf3000
     sf30000
     tiny
    (11 rows)

Ignore the standard schema ``information_schema`` which exists in every
catalog and is not directly provided by the TPCH connector.

Every TPCH schema provides the same set of tables. Some tables are
identical in all schemas. Other tables vary based on the *scale factor*
which is determined based on the schema name. For example, the schema
``sf1`` corresponds to scale factor ``1`` and the schema ``sf300``
corresponds to scale factor ``300``. The TPCH connector provides an
infinite number of schemas for any scale factor, not just the few common
ones listed by ``SHOW SCHEMAS``. The ``tiny`` schema is an alias for scale
factor ``0.01``, which is a very small data set useful for testing.

General Configuration Properties
---------------------------------

================================================== ========================================================================= ==============================
Property Name                                      Description                                                               Default
================================================== ========================================================================= ==============================
``tpch.splits-per-node``                           Number of data splits generated per Presto worker node when querying      Number of available processors
                                                   data from the TPCH connector.

``tpch.column-naming``                             This property defines the format for generating column names in TPCH
                                                   tables. According to the TPC-H specification, each column is given a
                                                   prefix based on its table, such as ``l_`` for the ``lineitem`` table.
                                                   By default, the TPCH connector simplifies column names by excluding
                                                   these prefixes.The available values are ``SIMPLIFIED`` and ``STANDARD``.  ``SIMPLIFIED``
================================================== ========================================================================= ==============================
