====================
Delta Lake Connector
====================

Overview
--------

This connector allows reading `Delta Lake <https://delta.io/>`_
tables in Presto. The connector uses the
`Delta Kernel API <https://docs.delta.io/latest/delta-kernel.html>`_
provided by Delta Lake project to read the table metadata.

Configuration
-------------

To configure the Delta Lake connector, create a catalog properties file
``etc/catalog/delta.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=delta
    hive.metastore.uri=hostname:port

File-Based Metastore
^^^^^^^^^^^^^^^^^^^^

For testing or development purposes, this connector can be configured to use a local 
filesystem directory as a Hive Metastore. See :ref:`installation/deployment:File-Based Metastore`.  

Configuration Properties
------------------------

The following configuration properties are available:

=============================================== ========================================================= ============
Property Name                                   Description                                               Default
=============================================== ========================================================= ============
``hive.metastore.uri``                          The URI(s) of the Hive metastore where Delta Lake tables  ``null``
                                                are registered. The connector relies on the Hive
                                                metastore to find the location of Delta Lake tables.
                                                From the Delta Log at given location, schema and data
                                                file list of the table is found.

``hive.metastore.catalog.name``                 Specifies the catalog name to be passed to the metastore.

``delta.parquet-dereference-pushdown-enabled``  Enable pushing nested column dereferences into            ``true``
                                                table scan so that only the required fields
                                                selected in a ``struct`` data type column are selected.
                                                In order for this option to work, also set
                                                ``experimental.pushdown-dereference-enabled`` to
                                                ``true``.
``delta.case-sensitive-partitions-enabled``     Allows matching the names of partitioned columns in a     ``true``
                                                case-sensitive manner.
=============================================== ========================================================= ============

Delta Lake connector reuses many of the modules existing in Hive connector.
Modules for connectivity and security such as S3, Azure Data Lake, Glue metastore etc.
So the configurations for these modules is same those available in Hive connector documentation.

Querying Delta Lake Tables
--------------------------
Example query

.. code-block:: sql

    SELECT * FROM sales.apac.sales_data LIMIT 200;

In the above query

* ``sales`` refers to the Delta Lake catalog.
* ``apac`` refers to the database in Hive metastore.
* ``sales_data`` refers to the Delta Lake table registered in the ``apac`` database.

If the table is not registered in Hive metastore, it can be registered using the following DDL
command.

.. note::

    To register a table in Hive metastore, full schema of the table is not required in DDL
    as the Delta Lake connector gets the schema from the metadata located at the Delta Lake
    table location. To get around no columns error in Hive metastore, provide a dummy column
    as schema of the Delta table being registered.

Examples
--------

Create a new Delta table named ``sales_data_new`` in the ``apac`` schema that has Delta Lake
table location in an S3 bucket named ``db-sa-datasets`` using Delta Lake connector:

.. code-block:: sql

    CREATE TABLE sales.apac.sales_data_new (dummyColumn INT)
    WITH (external_location = 's3://db-sa-datasets/presto/sales_data_new');

To register a partition Delta table in Hive metastore, use the ``CREATE TABLE`` same as above.
Only ``external_location`` is required in the properties, no need to specify ``partitioned_by`` in
``CREATE TABLE``

Another option is querying the table directly using the table location as table name.

.. code-block:: sql

    SELECT * FROM sales."$path$"."s3://db-sa-datasets/presto/sales_data" LIMIT 200;

In the above query the schema ``$path$`` indicates the table name is a path.
Table name given as `s3://db-sa-datasets/presto/sales_date` is a path where the
Delta Lake table is located. The path based option allows users to query a
Delta table without registering it in the Hive metastore.

To query a specific snapshot of the Delta Lake table use the snapshot identifier
as suffix to the table name.

.. code-block:: sql

    SELECT * FROM sales.apac."sales_data@v4" LIMIT 200;

Above query reads data from snapshot version ``4`` of the table ``sales.apac.sales_data``.

To query the snapshot of the Delta Lake table as of particular time, specify the timestamp
as suffix to the table name.

.. code-block:: sql

    SELECT * FROM sales.apac."sales_data@t2021-11-18 09:45" LIMIT 200;

Above query reads data from the latest snapshot as of timestamp ``2021-11-18 09:45:00``
in the table ``sales.apac.sales_data``.

.. code-block:: sql

    DROP TABLE sales.apac.sales_data_new;

Above query drops the external table ``sales.apac.sales_data_new``. This only drops the
metadata for the table. The referenced data directory is not deleted.

Delta Lake to PrestoDB type mapping
-----------------------------------

Map of Delta Lake types to the relevant PrestoDB types:

.. list-table:: Delta Lake to PrestoDB type mapping
  :widths: 50, 50
  :header-rows: 1

  * - Delta Lake type
    - PrestoDB type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``SMALLINT``
    - ``SMALLINT`` 
  * - ``TINYINT``
    - ``TINYINT``
  * - ``INT``
    - ``INTEGER``
  * - ``LONG``
    - ``BIGINT``
  * - ``FLOAT``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL``
    - ``DECIMAL``
  * - ``STRING``
    - ``VARCHAR``
  * - ``BINARY``
    - ``VARBINARY``
  * - ``DATE``
    - ``DATE``
  * - ``TIMESTAMP_NTZ``
    - ``TIMESTAMP``
  * - ``TIMESTAMP``
    - ``TIMESTAMP WITH TIME ZONE``
  * - ``ARRAY``
    - ``ARRAY``
  * - ``MAP``
    - ``MAP``
  * - ``STRUCT``
    - ``ROW``
