==================
DuckLake Connector
==================

Overview
--------

`DuckLake <https://ducklake.select/>`_ is an open lakehouse format from the
DuckDB project. Table metadata (schemas, tables, columns, snapshots, data and
delete file lists, and statistics) is stored in ordinary tables of a SQL
catalog database, and table data is stored as Parquet files.

The DuckLake connector reads DuckLake tables whose catalog database is
PostgreSQL. Every snapshot is catalog-wide: there is a single snapshot id
sequence for the whole catalog, not one per table.

This connector is read-only. Every DDL and DML statement fails with an error
stating that the connector is read-only.

Configuration
--------------

To configure the DuckLake connector, create a catalog properties file
``etc/catalog/ducklake.properties`` with the following contents, replacing
the properties as appropriate:

.. code-block:: none

    connector.name=ducklake
    ducklake.catalog.type=POSTGRESQL
    ducklake.catalog.connection-url=jdbc:postgresql://catalog-host:5432/ducklake
    ducklake.catalog.connection-user=ducklake
    ducklake.catalog.connection-password=secret

Data file paths recorded in the catalog are resolved against the table's
storage path as recorded by the writer. Local paths, ``s3://``, ``gs://``,
``abfs://``, and ``hdfs://`` paths all go through the Hive filesystem layer,
so the same filesystem configuration properties used by the Iceberg
connector apply here, including S3 credentials and
``hive.config.resources``. See :doc:`/connector/hive` and the
:ref:`Amazon S3 <connector/hive:Amazon S3 Configuration>` configuration for
the Hive connector.

Configuration Properties
-------------------------

The following configuration properties are available:

========================================== ============================================================= =========== =================== ===================
Property Name                              Description                                                   Default     Presto Java Support Presto C++ Support
========================================== ============================================================= =========== =================== ===================
``ducklake.catalog.type``                  Kind of catalog database. Only ``POSTGRESQL`` is supported.   required    Yes                 No
``ducklake.catalog.connection-url``        JDBC URL of the catalog database.                             required    Yes                 No
``ducklake.catalog.connection-user``       User for the catalog database.                                none        Yes                 No
``ducklake.catalog.connection-password``   Password for the catalog database.                            none        Yes                 No
``ducklake.catalog.schema``                Database schema that holds the ``ducklake_*`` metadata        ``public``  Yes                 No
                                           tables.
``ducklake.minimum-assigned-split-weight`` Lower bound on the weight assigned to a split, as in the      ``0.05``    Yes                 No
                                           Iceberg connector.
========================================== ============================================================= =========== =================== ===================

.. note::

    Presto C++ (native) workers are not supported by the DuckLake connector.
    See `Limitations`_.

Session Properties
-------------------

The following session properties can be set per-query using ``SET SESSION``:

.. code-block:: sql

    SET SESSION ducklake.minimum_assigned_split_weight = 0.1;

========================================== ================================================================== =======================================================
Property Name                              Description                                                        Default
========================================== ================================================================== =======================================================
``minimum_assigned_split_weight``          Session override for                                               Value of the
                                            ``ducklake.minimum-assigned-split-weight``.                        ``ducklake.minimum-assigned-split-weight``
                                                                                                                configuration property.
``cache_enabled``                          Enable the Hive file system cache for                              Value of the Hive connector's cache
                                            DuckLake data files.                                               configuration property (``false`` by
                                                                                                                default).
========================================== ================================================================== =======================================================

The Parquet reader session properties of the Hive connector also apply to
the DuckLake connector: ``parquet_batch_read_optimization_enabled``,
``parquet_max_read_block_size``, and
``parquet_batch_reader_verification_enabled``. See :doc:`/connector/hive`
for details on these properties.

Reading Tables
--------------

Schemas and tables are read from the catalog as of its latest snapshot.
``SHOW SCHEMAS``, ``SHOW TABLES``, ``DESCRIBE``, and ``SELECT`` are
supported.

The following hidden columns are available in ``SELECT``:

* ``$path`` : Path of the data file containing the row. ``NULL`` for rows
  stored inline in the catalog.
* ``$row_id`` : DuckLake's stable row id.
* ``$row_position`` : Position of the row within its data file.

Partition pruning (identity, and year, month, day, and hour transforms) and
per-file min/max statistics pruning are applied based on ``WHERE``
predicates. Filtering of the rows themselves is performed by Presto.

Table statistics (row count, per-column null fraction, data size, and
min/max ranges for numeric and date columns) are exposed to the cost-based
optimizer and can be viewed with ``SHOW STATS``. Distinct-value counts are
not provided. Rows stored inline in the catalog are not counted in the row
count estimate.

Time Travel
-----------

The DuckLake connector supports time travel using both ``FOR SYSTEM_VERSION``
and ``FOR SYSTEM_TIME``:

.. code-block:: sql

    SELECT * FROM ducklake.sales.orders FOR SYSTEM_VERSION AS OF 42;
    SELECT * FROM ducklake.sales.orders FOR SYSTEM_VERSION BEFORE 42;
    SELECT * FROM ducklake.sales.orders
        FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-09 21:21:45.128 UTC';
    SELECT * FROM ducklake.sales.orders
        FOR SYSTEM_TIME BEFORE TIMESTAMP '2026-09-09 21:21:45.128 UTC';

``FOR SYSTEM_VERSION`` takes a DuckLake snapshot id (``BIGINT``).
``FOR SYSTEM_TIME AS OF`` resolves to the latest snapshot whose commit time
is at or before the given timestamp; the ``BEFORE`` variants resolve to a
snapshot strictly earlier than the given version or timestamp. Because
snapshot ids are catalog-wide, the same snapshot id names the same moment in
time for every table in the catalog.

Querying an unknown snapshot id fails with ``DuckLake snapshot <id> does not
exist``. Querying a timestamp older than the first snapshot fails with
``No DuckLake snapshot exists at or before <timestamp>``. Querying a table
or schema that had not been created yet at the requested snapshot fails with
``Table <schema>.<table> does not exist at DuckLake snapshot <id>``.
Positional deletes, compacted files, and inline rows all honor the
requested snapshot.

.. note::

    Presto resolves the column list of a versioned table reference against
    the table's *current* schema. This is engine behavior shared with the
    Iceberg connector: time travel pins which rows and values are returned,
    not which column names exist. A column added after the requested
    snapshot reads as its default value for old rows, and a column renamed
    since the requested snapshot must be referred to by its current name.
    A default value the connector cannot represent (a nested type, or a
    literal it cannot parse) fails the query with an error naming the
    column, rather than reading back as ``NULL``.

``$snapshots`` Table
^^^^^^^^^^^^^^^^^^^^

Every DuckLake catalog snapshot is listed in the ``$snapshots`` system
table:

.. code-block:: sql

    SELECT * FROM ducklake.sales."orders$snapshots";

===================== ================================= ===============================================
Column                Type                              Notes
===================== ================================= ===============================================
``snapshot_id``       ``BIGINT``
``snapshot_time``     ``TIMESTAMP WITH TIME ZONE``
``schema_version``    ``BIGINT``
``author``            ``VARCHAR``
``commit_message``    ``VARCHAR``
``changes``           ``VARCHAR``                        DuckLake's change summary, for example
                                                          ``inserted_into_table:23``
===================== ================================= ===============================================

Data Types
----------

The following table shows the mapping between DuckLake and Presto data
types:

======================================== =========================================
DuckLake                                 Presto
======================================== =========================================
``boolean``                              ``BOOLEAN``
``int8``                                 ``TINYINT``
``int16``                                ``SMALLINT``
``int32``                                ``INTEGER``
``int64``                                ``BIGINT``
``uint8``                                ``SMALLINT`` (widened)
``uint16``                               ``INTEGER`` (widened)
``uint32``                               ``BIGINT`` (widened)
``float32``                              ``REAL``
``float64``                              ``DOUBLE``
``decimal(P,S)``                         ``DECIMAL(P,S)``
``date``                                 ``DATE``
``time``                                 ``TIME``, microseconds truncated to
                                          milliseconds
``timestamp``, ``timestamp_s``,          ``TIMESTAMP``, sub-millisecond precision
``timestamp_ms``, ``timestamp_ns``       truncated
``timestamptz``                          ``TIMESTAMP WITH TIME ZONE``
``varchar``                              ``VARCHAR``
``json``                                 ``JSON``
``uuid``                                 ``UUID``
``blob``                                 ``VARBINARY``
``list``                                 ``ARRAY``
``struct``                               ``ROW``
``map``                                  ``MAP``
======================================== =========================================

.. note::

    ``uint64``, ``int128``, ``uint128``, ``timetz``, ``interval``,
    ``variant``, and ``geometry`` columns are unsupported. Querying a table
    with a column of one of these types fails with an error naming the
    column: the whole table fails rather than hiding the unsupported
    column, so query results never silently differ from those returned by
    other DuckLake clients.

Limitations
-----------

* The connector is read-only. The following SQL statements are not
  supported:

  * :doc:`/sql/create-table`
  * :doc:`/sql/create-table-as`
  * :doc:`/sql/insert`
  * :doc:`/sql/delete`
  * :doc:`/sql/update`
  * :doc:`/sql/alter-table`
  * :doc:`/sql/drop-table`
  * :doc:`/sql/create-schema`
  * :doc:`/sql/drop-schema`
  * :doc:`/sql/create-view`
  * :doc:`/sql/analyze`

* Only PostgreSQL catalog databases are supported. DuckDB-file, SQLite, and
  MySQL catalogs are not supported.
* Presto C++ (native) workers are not supported.
* Encrypted data files are not supported and fail with an error.
* Data files imported with name mapping (``mapping_id``) are not supported
  and fail with an error.
* DuckLake views are not exposed.
* Deletion vectors stored in Puffin files are not supported.
* ``variant`` and ``geometry`` columns are unsupported.
* Reading inline data of nested types (``list``, ``struct``, ``map``) fails
  with an unsupported-feature error.
* Rows stored inline in the catalog are not counted in table statistics.
* Time travel does not version the column list of a table; see `Time
  Travel`_.
* A table containing a column of an unsupported type is omitted from bulk
  column listings such as ``information_schema.columns`` (with a warning
  logged on the coordinator), while querying the table directly fails with
  an error naming the column.
