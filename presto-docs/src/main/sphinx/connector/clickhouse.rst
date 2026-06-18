====================
ClickHouse connector
====================

The ClickHouse connector allows querying tables in an external
`ClickHouse <https://clickhouse.tech/>`_ server. This can be used to
query data in the databases on that server, or combine it with other data
from different catalogs accessing ClickHouse or any other supported data source.

Requirements
------------

To connect to a ClickHouse server, you need:

* ClickHouse version 20.8 or higher.
* Network access from the Presto coordinator and workers to the ClickHouse
  server. Port 8123 is the default port.

Configuration
-------------

The connector can query a ClickHouse server. Create a catalog properties file
that specifies the ClickHouse connector by setting the ``connector.name`` to
``clickhouse``.

For example, to access a server as ``clickhouse``, create the file
``etc/catalog/clickhouse.properties``. Replace the connection properties as
appropriate for your setup:

.. code-block:: none

    connector.name=clickhouse
    clickhouse.connection-url=jdbc:clickhouse://host1:8123/
    clickhouse.connection-user=default
    clickhouse.connection-password=secret

Multiple ClickHouse servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have multiple ClickHouse servers you need to configure one
catalog for each server. To add another catalog:

* Add another properties file to ``etc/catalog``
* Save it with a different name that ends in ``.properties``

For example, if you name the property file ``clickhouse.properties``, Prestodb uses the
configured connector to create a catalog named ``clickhouse``.

General configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following table describes general catalog configuration properties for the connector:

========================================= ================ ==============================================================================================================
Property Name                             Default Value    Description
========================================= ================ ==============================================================================================================
``clickhouse.map-string-as-varchar``      false             When creating a table, support the clickhouse data type String.

``clickhouse.allow-drop-table``           false             Allow delete table operation.

``case-sensitive-name-matching``          false             Enable case sensitive identifier support for schema, table, and column names for the connector.
                                                            When disabled, names are matched case-insensitively using lowercase normalization.
                                                            Defaults to ``false``.

========================================= ================ ==============================================================================================================


Querying ClickHouse
-------------------

The ClickHouse connector provides a schema for every ClickHouse *database*.
run ``SHOW SCHEMAS``  to see the available ClickHouse databases::

    SHOW SCHEMAS FROM clickhouse;

If you have a ClickHouse database named ``tpch``, run ``SHOW TABLES`` to view the
tables in this database::

    SHOW TABLES FROM clickhouse.tpch;

Run ``DESCRIBE`` or ``SHOW COLUMNS`` to list the columns in the ``cks`` table
in the ``tpch`` databases::

    DESCRIBE clickhouse.tpch.cks;
    SHOW COLUMNS FROM clickhouse.tpch.cks;

Run ``SELECT`` to access the ``cks`` table in the ``tpch`` database::

    SELECT * FROM clickhouse.tpch.cks;

.. note::

    If you used a different name for your catalog properties file, use
    that catalog name instead of ``clickhouse`` in the above examples.

PrestoDB to ClickHouse Type Mapping
-----------------------------------

========================================== ========================= =================================================================================
**PrestoDB Type**                          **ClickHouse Type**       **Notes**
========================================== ========================= =================================================================================
BOOLEAN                                    UInt8                     ClickHouse uses UInt8 as boolean, restricted values to 0 and 1.
TINYINT                                    Int8
SMALLINT                                   Int16
INTEGER                                    Int32
BIGINT                                     Int64
REAL                                       Float32
DOUBLE                                     Float64
DECIMAL                                    Decimal(precision, scale) The precision and scale are dynamic based on the PrestoDB type.
CHAR / VARCHAR                             String                    The String type replaces VARCHAR, BLOB, CLOB, and related types from other DBMSs.
VARBINARY                                  String
DATE                                       Date
TIMESTAMP                                  DateTime64(3)             Timestamp with 3 digits of millisecond precision.
========================================== ========================= =================================================================================

Table properties
----------------

Table property usage example::

    CREATE TABLE default.prestodb_ck (
      id int NOT NULL,
      birthday DATE NOT NULL,
      name VARCHAR,
      age BIGINT,
      logdate DATE NOT NULL
    )
    WITH (
      engine = 'MergeTree',
      order_by = ARRAY['id', 'birthday'],
      partition_by = ARRAY['toYYYYMM(logdate)'],
      primary_key = ARRAY['id'],
      sample_by = 'id'
    );

The following are supported ClickHouse table properties from `<https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/>`_

=========================== ================ ==============================================================================================================
Property Name               Default Value    Description
=========================== ================ ==============================================================================================================
``engine``                  ``Log``          Name and parameters of the engine.

``order_by``                (none)           Array of columns or expressions to concatenate to create the sorting key. Required if ``engine`` is ``MergeTree``.

``partition_by``            (none)           Array of columns or expressions to use as nested partition keys. Optional.

``primary_key``             (none)           Array of columns or expressions to concatenate to create the primary key. Optional.

``sample_by``               (none)           An expression to use for `sampling <https://clickhouse.tech/docs/en/sql-reference/statements/select/sample/>`_.
                                             Optional.

=========================== ================ ==============================================================================================================

Currently the connector only supports ``Log`` and ``MergeTree`` table engines
in create table statement. ``ReplicatedMergeTree`` engine is not yet supported.

Pushdown
--------

The connector supports pushdown for a number of operations:

* :ref:`!limit-pushdown`

.. _clickhouse-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
a ClickHouse catalog. In addition to the globally available and
read operation statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/truncate`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`
