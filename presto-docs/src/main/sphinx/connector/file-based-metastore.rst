====================
File-Based Metastore
====================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
^^^^^^^^

For testing or developing purposes, Presto can be configured to use a local 
filesystem directory as a Hive Metastore. 

The file-based metastore works only with the following connectors: 

* :doc:`/connector/deltalake`
* :doc:`/connector/hive`
* :doc:`/connector/hudi`
* :doc:`/connector/iceberg`

Configuring a File-Based Metastore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. In ``etc/catalog/``, find the catalog properties file for the supported 
   connector. 

2. In the catalog properties file, set the following properties:

.. code-block:: none

    hive.metastore=file
    hive.metastore.catalog.dir=file:///<catalog-dir>

Replace ``<catalog-dir>`` in the example with the path to a directory on an 
accessible filesystem.

Using a File-Based Warehouse
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For this example, assume the Hive connector is being used, and the properties 
in the Hive connector catalog file are set to the following:

.. code-block:: none

    connector.name=hive
    hive.metastore=file
    hive.metastore.catalog.dir=file:///data/hive_data/

Create a schema

.. code-block:: none

    CREATE SCHEMA hive.warehouse;

This query creates a directory ``warehouse`` in the directory set for 
``hive.metastore.catalog.dir``, so the path to the new directory is 
``/data/hive_data/warehouse``.

Create a table with any connector-supported file formats. For example, if the 
Hive connector is being configured: 

.. code-block:: none

    CREATE TABLE hive.warehouse.orders_csv("order_name" varchar, "quantity" varchar) WITH (format = 'CSV');
    CREATE TABLE hive.warehouse.orders_parquet("order_name" varchar, "quantity" int) WITH (format = 'PARQUET');

These queries create folders as ``/data/hive_data/warehouse/orders_csv`` and 
``/data/hive_data/warehouse/orders_parquet``. Users can insert and query 
from these tables.
