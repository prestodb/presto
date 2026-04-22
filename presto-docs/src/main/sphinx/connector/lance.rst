===============
Lance Connector
===============

Overview
--------

The Lance connector allows querying and writing data stored in
`Lance <https://lance.org/>`_ format from Presto. Lance is a columnar
data format optimized for machine learning workloads and fast random access.

The connector uses the Lance Java SDK to read and write Lance datasets.
Each Lance dataset is organized into **fragments**, and the connector maps each fragment to a
Presto split for parallel processing across workers.

Configuration
-------------

To configure the Lance connector, create a catalog properties file
``etc/catalog/lance.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=lance
    lance.root-url=/path/to/lance/data

Configuration Properties
------------------------

The following configuration properties are available:

=============================== ============================================================= ===============
Property Name                   Description                                                   Default
=============================== ============================================================= ===============
``lance.impl``                  Namespace implementation: ``dir``                              ``dir``
``lance.root-url``              Root storage path for Lance datasets.                          ``""``
``lance.single-level-ns``       When ``true``, uses a single-level namespace with a            ``true``
                                virtual ``default`` schema.
``lance.read-batch-size``       Number of rows per Arrow batch during reads.                   ``8192``
``lance.max-rows-per-file``     Maximum number of rows per Lance data file.                    ``1000000``
``lance.max-rows-per-group``    Maximum number of rows per row group.                          ``100000``
``lance.write-batch-size``      Number of rows to batch before writing to Arrow.               ``10000``
=============================== ============================================================= ===============

``lance.impl``
^^^^^^^^^^^^^^

Namespace implementation to use. The default ``dir`` uses a directory-based
table store where each table is a ``<name>.lance`` directory under the root.

``lance.root-url``
^^^^^^^^^^^^^^^^^^

Root storage path for Lance datasets. All tables are stored as subdirectories
named ``<table_name>.lance`` under this path. For example, if ``lance.root-url``
is set to ``/data/lance``, a table named ``my_table`` is stored at
``/data/lance/my_table.lance``.

``lance.single-level-ns``
^^^^^^^^^^^^^^^^^^^^^^^^^

When set to ``true`` (the default), the connector exposes a single ``default``
schema that maps directly to the root directory. All tables are accessed as
``lance.default.<table_name>``.

``lance.read-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^

Controls the number of rows read per Arrow batch from Lance. Larger values may
improve read throughput at the cost of higher memory usage. The default is
``8192``.

``lance.max-rows-per-file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum number of rows per Lance data file. The default is ``1000000``.

.. note::

    This property is reserved for future use and is not yet wired into the
    write path.

``lance.max-rows-per-group``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum number of rows per row group within a Lance data file. The default is
``100000``.

.. note::

    This property is reserved for future use and is not yet wired into the
    write path.

``lance.write-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Number of rows to batch before converting to Arrow format during writes. The
default is ``10000``.

.. note::

    This property is reserved for future use and is not yet wired into the
    write path.

Data Types
----------

The following table lists the supported data type mappings between Lance
(Arrow) types and Presto types:

================= =============== ======================================
Lance (Arrow)     Presto          Notes
================= =============== ======================================
``Bool``          ``BOOLEAN``
``Int(8)``        ``TINYINT``
``Int(16)``       ``SMALLINT``
``Int(32)``       ``INTEGER``
``Int(64)``       ``BIGINT``
``Float(SINGLE)`` ``REAL``
``Float(DOUBLE)`` ``DOUBLE``
``Utf8``          ``VARCHAR``
``LargeUtf8``     ``VARCHAR``
``Binary``        ``VARBINARY``
``LargeBinary``   ``VARBINARY``
``Date(DAY)``     ``DATE``
``Timestamp``     ``TIMESTAMP``   Microsecond precision; reads support
                                  both with and without timezone
``List``          ``ARRAY``       Read only; element type mapped
                                  recursively
``FixedSizeList`` ``ARRAY``       Read only; element type mapped
                                  recursively
================= =============== ======================================

.. note::

    Arrow types not listed above are unsupported and will cause an error.

SQL Support
-----------

The Lance connector supports the following SQL operations.

CREATE TABLE
^^^^^^^^^^^^

Create a new Lance table:

.. code-block:: sql

    CREATE TABLE lance.default.my_table (
        id BIGINT,
        name VARCHAR,
        score DOUBLE
    );

CREATE TABLE AS
^^^^^^^^^^^^^^^

Create a Lance table from a query:

.. code-block:: sql

    CREATE TABLE lance.default.my_table AS
    SELECT * FROM tpch.tiny.nation;

INSERT INTO
^^^^^^^^^^^

Append data to an existing Lance table:

.. code-block:: sql

    INSERT INTO lance.default.my_table
    SELECT * FROM tpch.tiny.nation;

SELECT
^^^^^^

Query data from a Lance table:

.. code-block:: sql

    SELECT * FROM lance.default.my_table;

Column projection is pushed down to Lance, so queries that select a subset
of columns only read those columns from disk:

.. code-block:: sql

    SELECT id, name FROM lance.default.my_table;

Predicate Pushdown
^^^^^^^^^^^^^^^^^^

The Lance connector supports predicate pushdown for ``WHERE`` clauses.
Filter predicates are converted to Lance SQL filter strings and evaluated
natively by Lance during scan, reducing data read from disk.

.. code-block:: sql

    SELECT * FROM lance.default.my_table
    WHERE age > 30 AND status = 'active';

The following predicate types are pushed down:

- **Equality**: ``col = value``
- **Comparisons**: ``col > value``, ``col >= value``, ``col < value``, ``col <= value``
- **IN lists**: ``col IN (v1, v2, v3)``
- **NULL checks**: ``col IS NULL``
- **Range**: ``col >= low AND col < high``
- **Multiple columns**: predicates on different columns are combined with ``AND``

The following data types are supported in pushed-down predicates:

========================= ======================================
Presto Type               Lance Filter Literal
========================= ======================================
``BOOLEAN``               ``true`` / ``false``
``TINYINT``               Integer literal
``SMALLINT``              Integer literal
``INTEGER``               Integer literal
``BIGINT``                Integer literal
``REAL``                  Float literal
``DOUBLE``                Double literal
``VARCHAR``               ``'string'`` (single quotes escaped)
``DATE``                  ``date '2024-01-15'``
``TIMESTAMP``             ``timestamp '2024-01-15 10:30:00.000000'``
========================= ======================================

.. note::

    Predicates on unsupported types such as ``VARBINARY``, ``ARRAY``, and
    complex predicates with more than 100 ranges per column are not pushed
    down. In these cases, Presto evaluates the filter at the executor level.
    Correctness is always guaranteed because Presto re-evaluates all
    predicates regardless of pushdown.

DROP TABLE
^^^^^^^^^^

Drop a Lance table and delete all its data:

.. code-block:: sql

    DROP TABLE lance.default.my_table;

SHOW TABLES
^^^^^^^^^^^

List all tables in the catalog:

.. code-block:: sql

    SHOW TABLES FROM lance.default;

DESCRIBE
^^^^^^^^

Show the columns and types of a Lance table:

.. code-block:: sql

    DESCRIBE lance.default.my_table;

Session Properties
------------------

The following session properties can be set per-query using ``SET SESSION``:

.. code-block:: sql

    SET SESSION lance.filter_pushdown_enabled = false;

================================================= ================================================================ =========
Property Name                                     Description                                                      Default
================================================= ================================================================ =========
``filter_pushdown_enabled``                        Enable SQL predicate pushdown to the Lance scanner. Disable     ``true``
                                                   to fall back to executor-level filtering only.
================================================= ================================================================ =========

Limitations
-----------

* Only a single schema (``default``) is supported when ``lance.single-level-ns``
  is ``true``.
* The following SQL statements are not supported:

  * :doc:`/sql/alter-table`
  * :doc:`/sql/delete`
  * :doc:`/sql/update`

* Predicate pushdown is not supported for ``VARBINARY`` and ``ARRAY`` types.
  Only column projection is pushed down for these types.
* ``ARRAY`` types are supported for reads but cannot be written.
* Only local filesystem paths are supported in the current ``dir`` implementation.
* Data written by one Presto cluster is not visible to another cluster until the
  write transaction commits.
