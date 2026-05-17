==================================
SQL Statement Support by Connector
==================================

This page provides a comprehensive overview of which SQL statements are supported by each connector in Presto.
The table below shows the compatibility matrix between connectors and SQL statements.

.. note::
    This table is maintained manually and should be reviewed at each release for updates.
    Support may vary based on connector configuration and the underlying data source capabilities.

Compatibility Matrix
====================

The following table shows SQL statement support across all Presto connectors:

* **✓** = Fully supported
* **✗** = Not supported
* **⚠** = Partially supported or has limitations

.. list-table:: SQL Statement Support by Connector
  :header-rows: 1
  :stub-columns: 1
  :widths: 20 8 8 8 8 8 8 8 8 8 8 8 8

  * - Connector
    - ALTER SCHEMA
    - ALTER TABLE
    - ALTER VIEW
    - ANALYZE
    - CALL
    - COMMIT
    - CREATE ROLE
    - CREATE SCHEMA
    - CREATE TABLE
    - CREATE TABLE AS
    - CREATE VIEW
    - CREATE MATERIALIZED VIEW
  * - Accumulo
    - ✗
    - ⚠
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Base Arrow Flight
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - BigQuery
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Black Hole
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Cassandra
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - ClickHouse
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - Delta Lake
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
  * - Druid
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Elasticsearch
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Google Sheets
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - HANA
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Hive
    - ✓
    - ✓
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
  * - Hudi
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Iceberg
    - ✓
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
  * - JMX
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Kafka
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Kudu
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Lance
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Lark Sheets
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Local File
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Memory
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - MongoDB
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - MySQL
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - Oracle
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - Pinot
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - PostgreSQL
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - Prometheus
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Redis
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Redshift
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - ScyllaDB
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - SingleStore
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - SQL Server
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - System
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Thrift
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - TPCDS
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - TPCH
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗

.. list-table:: SQL Statement Support by Connector (continued)
  :header-rows: 1
  :stub-columns: 1
  :widths: 20 8 8 8 8 8 8 8 8 8 8 8 8

  * - Connector
    - DELETE
    - DROP SCHEMA
    - DROP TABLE
    - DROP VIEW
    - DROP MATERIALIZED VIEW
    - GRANT
    - INSERT
    - MERGE
    - REFRESH MATERIALIZED VIEW
    - ROLLBACK
    - TRUNCATE
    - UPDATE
  * - Accumulo
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Base Arrow Flight
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - BigQuery
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Black Hole
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Cassandra
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - ClickHouse
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Delta Lake
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Druid
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Elasticsearch
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Google Sheets
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - HANA
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Hive
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
  * - Hudi
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Iceberg
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
    - ✓
  * - JMX
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Kafka
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Kudu
    - ✓
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Lance
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Lark Sheets
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Local File
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Memory
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - MongoDB
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - MySQL
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Oracle
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Pinot
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - PostgreSQL
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Prometheus
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Redis
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Redshift
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - ScyllaDB
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - SingleStore
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - SQL Server
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - System
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Thrift
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - TPCDS
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - TPCH
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗

Notes and Limitations
=====================

Accumulo
--------

* ``ALTER TABLE``: Adding columns via SQL is not supported; must use external tools
* ``DELETE``: Not yet implemented
* ``INSERT``: Effectively acts as UPSERT when row ID already exists

Hive
----

* Supports most DDL and DML operations
* ``DELETE``: Only supported for transactional tables
* ``UPDATE``: Not supported
* ``MERGE``: Not supported

Iceberg
-------

* Most comprehensive SQL support among connectors
* Full support for transactions (COMMIT/ROLLBACK)
* Supports advanced features like MERGE and UPDATE
* Branch and tag operations supported via ALTER TABLE

Kudu
----

* ``INSERT``: Behaves like UPSERT
* ``ALTER TABLE``: Column operations have restrictions for primary key columns
* ``DELETE``: Fully supported

Memory
------

* Temporary storage connector
* Memory not released immediately after DROP TABLE
* Limited to CREATE TABLE, INSERT, and DROP TABLE operations

MongoDB
-------

* ``ALTER TABLE``: Supported for ADD/DROP/RENAME COLUMN and RENAME TABLE
* Schema changes require manual updates to ``_schema`` collection
* ``CREATE TABLE``: Automatically creates schema entries

MySQL / PostgreSQL
------------------

* ``ALTER TABLE``: Supported for ADD/DROP/RENAME COLUMN and RENAME TABLE
* ``CREATE TABLE``: Fully supported with constraints
* ``INSERT``: Fully supported
* Read-mostly connectors with limited write support

For detailed information about each connector's capabilities and limitations, please refer to the individual connector documentation pages.

See Also
========

* :doc:`../connector` - Complete list of available connectors
* :doc:`../sql` - SQL statement syntax reference
* Individual connector documentation for specific capabilities and configuration
