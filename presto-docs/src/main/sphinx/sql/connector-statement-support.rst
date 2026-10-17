==================================
SQL Statement Support by Connector
==================================

This page provides a comprehensive overview of which SQL statements are supported by each connector in Presto.
The table below shows the compatibility matrix between connectors and SQL statements.

.. note::
    Support may vary based on connector configuration and the underlying data source capabilities.

Compatibility Matrix
====================

The following tables show SQL statement support across all Presto connectors:

* :ref:`sql-alter-schema-to-commit`
* :ref:`sql-create-role-to-create-materialized-view`
* :ref:`sql-delete-to-grant`
* :ref:`sql-insert-to-update`

* **✓** = Fully supported
* **✗** = Not supported
* **⚠** = Partially supported. See :ref:`connector-statement-support-limitations`.

.. _sql-alter-schema-to-commit:

ALTER SCHEMA to COMMIT
----------------------

.. list-table:: SQL Statement Support: ALTER SCHEMA to COMMIT
  :header-rows: 1
  :stub-columns: 1
  :widths: 20 11 11 11 14 11 11 11

  * - Connector
    - ALTER SCHEMA
    - ALTER TABLE
    - ALTER VIEW
    - ALTER MATERIALIZED VIEW
    - ANALYZE
    - CALL
    - COMMIT
  * - Accumulo
    - ✗
    - ⚠
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
  * - BigQuery
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
  * - Cassandra
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
  * - Delta Lake
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
  * - Elasticsearch
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
  * - HANA
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Hive
    - ✓
    - ✓
    - ✗
    - ✗
    - ✓
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
  * - Iceberg
    - ✓
    - ✓
    - ✗
    - ✓
    - ✗
    - ✗
    - ✓
  * - JMX
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
  * - Kudu
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
  * - Local File
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
  * - MongoDB
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
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
  * - Oracle
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
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
  * - PostgreSQL
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
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
  * - Redis
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
  * - ScyllaDB
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
  * - SQL Server
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
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
  * - Thrift
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
  * - TPCH
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗

.. _sql-create-role-to-create-materialized-view:

CREATE ROLE to CREATE MATERIALIZED VIEW
---------------------------------------

.. list-table:: SQL Statement Support: CREATE ROLE to CREATE MATERIALIZED VIEW
  :header-rows: 1
  :stub-columns: 1
  :widths: 20 13 13 13 13 13 13

  * - Connector
    - CREATE ROLE
    - CREATE SCHEMA
    - CREATE TABLE
    - CREATE TABLE AS
    - CREATE VIEW
    - CREATE MATERIALIZED VIEW
  * - Accumulo
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
  * - BigQuery
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
  * - ClickHouse
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - Delta Lake
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
  * - Elasticsearch
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
  * - HANA
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Hive
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
  * - Iceberg
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
  * - Kafka
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Kudu
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - Lance
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
  * - Local File
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
    - ✓
    - ✗
    - ✗
  * - MongoDB
    - ✗
    - ✗
    - ✓
    - ✓
    - ✗
    - ✗
  * - MySQL
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - Oracle
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
  * - PostgreSQL
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
  * - Redis
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Redshift
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
  * - SingleStore
    - ✗
    - ✓
    - ✓
    - ✓
    - ✗
    - ✗
  * - SQL Server
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
  * - Thrift
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
  * - TPCH
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗

.. _sql-delete-to-grant:

DELETE to GRANT
---------------

.. list-table:: SQL Statement Support: DELETE to GRANT
  :header-rows: 1
  :stub-columns: 1
  :widths: 20 13 13 13 13 13 13

  * - Connector
    - DELETE
    - DROP SCHEMA
    - DROP TABLE
    - DROP VIEW
    - DROP MATERIALIZED VIEW
    - GRANT
  * - Accumulo
    - ✗
    - ✗
    - ✓
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
  * - BigQuery
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
  * - Cassandra
    - ✗
    - ✗
    - ✓
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
  * - Delta Lake
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
  * - Elasticsearch
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
  * - HANA
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
  * - Hive
    - ✓
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
  * - Iceberg
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
  * - Kafka
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
  * - Lance
    - ✗
    - ✗
    - ✓
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
  * - Local File
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
  * - MongoDB
    - ✗
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
  * - Oracle
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
  * - Pinot
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
  * - Prometheus
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
  * - Redshift
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
  * - ScyllaDB
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
  * - SQL Server
    - ✗
    - ✗
    - ✓
    - ✗
    - ✗
    - ✗
  * - System
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
  * - TPCDS
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

.. _sql-insert-to-update:

INSERT to UPDATE
----------------

.. list-table:: SQL Statement Support: INSERT to UPDATE
  :header-rows: 1
  :stub-columns: 1
  :widths: 20 13 13 13 13 13 13

  * - Connector
    - INSERT
    - MERGE
    - REFRESH MATERIALIZED VIEW
    - ROLLBACK
    - TRUNCATE
    - UPDATE
  * - Accumulo
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
  * - BigQuery
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Black Hole
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Cassandra
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - ClickHouse
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Delta Lake
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
  * - Elasticsearch
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
  * - HANA
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Hive
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
  * - Iceberg
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
  * - Kafka
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Kudu
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Lance
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
  * - Local File
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Memory
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - MongoDB
    - ✓
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - MySQL
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - Oracle
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
  * - PostgreSQL
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
  * - Redis
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
  * - Redshift
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
  * - SingleStore
    - ✓
    - ✗
    - ✗
    - ✗
    - ✓
    - ✗
  * - SQL Server
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
  * - Thrift
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
  * - TPCH
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗
    - ✗

.. _connector-statement-support-limitations:

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
* ``ALTER TABLE``: Supports changing column default values (write-default) via ``ALTER COLUMN SET DEFAULT`` on Iceberg format version 3+ tables
* ``ALTER MATERIALIZED VIEW``: Supports updating properties (such as ``stale_read_behavior``, ``staleness_window``, and ``refresh_type``) in place

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
