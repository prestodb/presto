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
    (10 rows)

Ignore the standard schema ``information_schema`` which exists in every
catalog and is not directly provided by the TPCH connector.

Every TPCH schema provides the same set of tables. Some tables are
identical in all schemas. Other tables vary based on the *scale factor*
which is determined based on the schema name. For example, the schema
``sf1`` corresponds to scale factor ``1`` and the schema ``sf300``
corresponds to scale factor ``300``. The scale factor represents the approximate size,
in bytes, of the entire set of tables when stored uncompressed. For example,
``sf1`` implies that writing all tables to disk uncompressed would require approximately 1GB.
The TPCH connector provides an infinite number of schemas
for any scale factor which includes floating-point values,
not just the few common ones listed by ``SHOW SCHEMAS``.
The ``tiny`` schema is an alias for scale factor ``0.01``,
which is a very small data set useful for testing.

For more information, review the `TPCH Specification document <https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf>`_.
In section 1.2 of the document, the TPCH schemas are provided.

Schema Scale Factors and Corresponding Table Row Counts
-------------------------------------------------------
Example query to return row counts from schema ``sf1`` and table ``customer``:

.. code-block:: sql

    SELECT COUNT(*) FROM tpch.sf1.customer;

=============== ========== ========== =========== ============ ============= ============= ============ ============= =============
Schema          ``tiny``   ``sf1``    ``sf100``   ``sf1000``   ``sf10000``   ``sf100000``  ``sf300``    ``sf3000``    ``sf30000``
Table Name
=============== ========== ========== =========== ============ ============= ============= ============ ============= =============
``customer``    1.5K       150K       15M         150M         1.5B          15B           45M          450M          4.5B
``lineitem``    60K        6M         600M        6B           60B           600B          1.8B         18B           180B
``nation``      25         25         25          25           25            25            25           25            25
``orders``      15K        1.5M       150M        1.5B         15B           150B          450M         4.5B          45B
``part``        2K         200K       20M         200M         2B            20B           60M          600M          6B
``partsupp``    8K         800K       80M         800M         8B            80B           240M         2.4B          24B
``region``      5          5          5           5            5             5             5            5             5
``supplier``    100        10K        1M          10M          100M          1B            3M           30M           300M
=============== ========== ========== =========== ============ ============= ============= ============ ============= =============

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
