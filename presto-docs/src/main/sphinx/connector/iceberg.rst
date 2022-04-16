=================
Iceberg Connector
=================

Overview
--------

The Iceberg connector allows querying data stored in Iceberg tables.

.. note::

    It is recommended to use Iceberg 0.9.0 or later.

Configuration
-------------

To configure the Iceberg connector, create a catalog properties file
``etc/catalog/iceberg.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=iceberg
    hive.metastore.uri=hostname:port
    iceberg.catalog.type=hive

Configuration Properties
------------------------

The following configuration properties are available:

====================================== ===================================================
Property Name                          Description
====================================== ===================================================
``hive.metastore.uri``                 The URI(s) of the Hive metastore.

``iceberg.file-format``                The storage file format for Iceberg tables.

``iceberg.compression-codec``          The compression codec to use when writing files.

``iceberg.catalog.type``               The catalog type for Iceberg tables.

``iceberg.catalog.warehouse``          The catalog warehouse root path for Iceberg tables.

``iceberg.catalog.cached-catalog-num`` The number of Iceberg catalogs to cache.

``iceberg.hadoop.config.resources``    The path(s) for Hadoop configuration resources.
====================================== ===================================================

``hive.metastore.uri``
^^^^^^^^^^^^^^^^^^^^^^

The URI(s) of the Hive metastore to connect to using the Thrift protocol.
If multiple URIs are provided, the first URI is used by default and the
rest of the URIs are fallback metastores. This property is required.
Example: ``thrift://192.0.2.3:9083`` or ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``iceberg.file-format``
^^^^^^^^^^^^^^^^^^^^^^^

The storage file format for Iceberg tables. The available values are
``PARQUET`` and ``ORC``.

The default is ``PARQUET``.

``iceberg.compression-codec``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The compression codec to use when writing files. The available values are
``NONE``, ``SNAPPY``, ``GZIP``, ``LZ4``, and ``ZSTD``.

The default is ``GZIP``.

``iceberg.catalog.type``
^^^^^^^^^^^^^^^^^^^^^^^^

The catalog type for Iceberg tables. The available values are ``hive``
and ``hadoop``, corresponding to the catalogs in the Iceberg.

The default is ``hive``.

``iceberg.catalog.warehouse``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The catalog warehouse root path for Iceberg tables. Example:
``hdfs://nn:8020/warehouse/path``.

This property is required if the ``iceberg.catalog.type`` is ``hadoop``.
Otherwise, it will be ignored.

``iceberg.catalog.cached-catalog-num``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The number of Iceberg catalogs to cache.

The default is ``10``. This property is required if the ``iceberg.catalog.type``
is ``hadoop``. Otherwise, it will be ignored.

``iceberg.hadoop.config.resources``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path(s) for Hadoop configuration resources. Example:
``/etc/hadoop/conf/core-site.xml``.

This property is required if the ``iceberg.catalog.type`` is ``hadoop``.
Otherwise, it will be ignored.

Schema Evolution
------------------------

Iceberg and Presto Iceberg connector supports in-place table evolution, aka
schema evolution, such as adding, dropping, and renaming columns. With schema
evolution, users can evolve a table schema with SQL after enabling the Presto
Iceberg connector.

Example Queries
^^^^^^^^^^^^^^^

Let's create an Iceberg table named `ctas_nation`, created from the TPCH `nation`
table. The table has four columns: `nationkey`, `name`, `regionkey`, and `comment`.

.. code-block:: sql

    USE iceberg.tpch;
    CREATE TABLE IF NOT EXISTS ctas_nation AS (SELECT * FROM nation);
    DESCRIBE ctas_nation;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     nationkey | bigint  |       |
     name      | varchar |       |
     regionkey | bigint  |       |
     comment   | varchar |       |
    (4 rows)

We can simply add a new column to the Iceberg table by using the `ALTER TABLE`
statement. The following query adds a new column named `zipcode` to the table.

.. code-block:: sql

    ALTER TABLE ctas_nation ADD COLUMN zipcode VARCHAR;
    DESCRIBE ctas_nation;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     nationkey | bigint  |       |
     name      | varchar |       |
     regionkey | bigint  |       |
     comment   | varchar |       |
     zipcode   | varchar |       |
    (5 rows)

We can also rename the new column to `location`:

.. code-block:: sql

    ALTER TABLE ctas_nation RENAME COLUMN zipcode TO location;
    DESCRIBE ctas_nation;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     nationkey | bigint  |       |
     name      | varchar |       |
     regionkey | bigint  |       |
     comment   | varchar |       |
     location  | varchar |       |
    (5 rows)

Finally, we can delete the new column. The table columns will be restored to the
original state.

.. code-block:: sql

    ALTER TABLE ctas_nation DROP COLUMN location;
    DESCRIBE ctas_nation;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     nationkey | bigint  |       |
     name      | varchar |       |
     regionkey | bigint  |       |
     comment   | varchar |       |
    (4 rows)


Time Travel
------------------------

Iceberg and Presto Iceberg connector supports time travel via table snapshots
identified by unique snapshot IDs. The snapshot IDs are stored in the `$snapshots`
metadata table. We can rollback the state of a table to a previous snapshot ID.

Example Queries
^^^^^^^^^^^^^^^

Similar to the example queries in the `Schema Evolution`, let's create an Iceberg
table named `ctas_nation`, created from the TPCH `nation` table.

.. code-block:: sql

    USE iceberg.tpch;
    CREATE TABLE IF NOT EXISTS ctas_nation AS (SELECT * FROM nation);
    DESCRIBE ctas_nation;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     nationkey | bigint  |       |
     name      | varchar |       |
     regionkey | bigint  |       |
     comment   | varchar |       |
    (4 rows)

We can find snapshot IDs of the Iceberg table from the `$snapshots` metadata table.

.. code-block:: sql

    SELECT snapshot_id FROM iceberg.tpch."ctas_nation$snapshots" ORDER BY committed_at;

.. code-block:: text

         snapshot_id
    ---------------------
     5837462824399906536
    (1 row)

For now, as we've just created the table, there's only one snapshot ID. Let's
insert one row into the table and see the change of the snapshot IDs.

.. code-block:: sql

    INSERT INTO ctas_nation VALUES(25, 'new country', 1, 'comment');
    SELECT snapshot_id FROM iceberg.tpch."ctas_nation$snapshots" ORDER BY committed_at;

.. code-block:: text

         snapshot_id
    ---------------------
     5837462824399906536
     5140039250977437531
    (2 rows)

Now there's a new snapshot (`5140039250977437531`) created as a new row is
inserted into the table. The new row can be verified by running

.. code-block:: sql

    SELECT * FROM ctas_nation WHERE name = 'new country';

.. code-block:: text

     nationkey |    name     | regionkey | comment
    -----------+-------------+-----------+---------
            25 | new country |         1 | comment
    (1 row)

With the time travel feature, we can rollback to the previous state without the
new row by calling `iceberg.system.rollback_to_snapshot`:

.. code-block:: sql

    CALL iceberg.system.rollback_to_snapshot('tpch', 'ctas_nation', 5837462824399906536);

Now if we check the table again, we'll find the inserted new row no longer
exists as we've rollbacked to the previous state.

.. code-block:: sql

    SELECT * FROM ctas_nation WHERE name = 'new country';

.. code-block:: text

     nationkey | name | regionkey | comment
    -----------+------+-----------+---------
    (0 rows)
