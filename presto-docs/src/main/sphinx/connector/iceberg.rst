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

Hive Metastore catalog
^^^^^^^^^^^^^^^^^^^^^^

Iceberg connector supports the same configuration for
`HMS <https://prestodb.io/docs/current/connector/hive.html#metastore-configuration-properties>`_
as Hive connector.

.. code-block:: none

    connector.name=iceberg
    hive.metastore.uri=hostname:port
    iceberg.catalog.type=hive

Glue catalog
^^^^^^^^^^^^

Iceberg connector supports the same configuration for
`Glue <https://prestodb.io/docs/current/connector/hive.html#aws-glue-catalog-configuration-properties>`_
as Hive connector.

.. code-block:: none

    connector.name=iceberg
    hive.metastore=glue
    iceberg.catalog.type=hive

Nessie catalog
^^^^^^^^^^^^^^

In order to use a Nessie catalog, ensure to configure the catalog type with
``iceberg.catalog.type=nessie`` and provide further details with the following
properties:

==================================================== ============================================================
Property Name                                        Description
==================================================== ============================================================
``iceberg.nessie.ref``                               The branch/tag to use for Nessie, defaults to ``main``.

``iceberg.nessie.uri``                               Nessie API endpoint URI (required).
                                                     Example: ``https://localhost:19120/api/v1``

``iceberg.nessie.auth.type``                         The authentication type to use.
                                                     Available values are ``BASIC`` or ``BEARER``.
                                                     Example: ``BEARER``

``iceberg.nessie.auth.basic.username``               The username to use with ``BASIC`` authentication.
                                                     Example: ``test_user``

``iceberg.nessie.auth.basic.password``               The password to use with ``BASIC`` authentication.
                                                     Example: ``my$ecretPass``

``iceberg.nessie.auth.bearer.token``                 The token to use with ``BEARER`` authentication.
                                                     Example: ``SXVLUXUhIExFQ0tFUiEK``

``iceberg.nessie.read-timeout-ms``                   The read timeout in milliseconds for requests
                                                     to the Nessie server.
                                                     Example: ``5000``

``iceberg.nessie.connect-timeout-ms``                The connection timeout in milliseconds for connection
                                                     requests to the Nessie server.
                                                     Example: ``10000``

``iceberg.nessie.compression-enabled``               Configuration of whether compression should be enabled or
                                                     not for requests to the Nessie server, defaults to ``true``.

``iceberg.nessie.client-builder-impl``               Configuration of the custom ClientBuilder implementation
                                                     class to be used.

==================================================== ============================================================

.. code-block:: none

    connector.name=iceberg
    iceberg.catalog.type=nessie
    iceberg.catalog.warehouse=/tmp
    iceberg.nessie.uri=https://localhost:19120/api/v1

Configuration Properties
------------------------

.. note::

    The Iceberg connector supports configuration options for
    `Amazon S3 <https://prestodb.io/docs/current/connector/hive.html##amazon-s3-configuration>`_
    as Hive connector.

The following configuration properties are available:

========================================= =====================================================
Property Name                             Description
========================================= =====================================================
``hive.metastore.uri``                    The URI(s) of the Hive metastore.

``iceberg.file-format``                   The storage file format for Iceberg tables.

``iceberg.compression-codec``             The compression codec to use when writing files.

``iceberg.catalog.type``                  The catalog type for Iceberg tables.

``iceberg.catalog.warehouse``             The catalog warehouse root path for Iceberg tables.

``iceberg.catalog.cached-catalog-num``    The number of Iceberg catalogs to cache.

``iceberg.hadoop.config.resources``       The path(s) for Hadoop configuration resources.

``iceberg.max-partitions-per-writer``     The maximum number of partitions handled per writer.

``iceberg.minimum-assigned-split-weight`` A decimal value in the range (0, 1] used as a minimum
                                          for weights assigned to each split.
========================================= =====================================================

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

The catalog type for Iceberg tables. The available values are ``hive``/``hadoop``/``nessie``,
 corresponding to the catalogs in Iceberg.

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

``iceberg.max-partitions-per-writer``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Maximum number of partitions handled per writer.

The default is 100.

``iceberg.minimum-assigned-split-weight``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A decimal value in the range (0, 1] used as a minimum for weights assigned to each split.
A low value may improve performance on tables with small files. A higher value may improve
performance for queries with highly skewed aggregations or joins.

The default is 0.05.

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

Table Properties
------------------------

Table properties set metadata for the underlying tables. This is key for
CREATE TABLE/CREATE TABLE AS statements. Table properties are passed to the
connector using a WITH clause:

.. code-block:: sql

    CREATE TABLE tablename
    WITH (
        property_name = property_value,
        ...
    )

The following table properties are available:

========================================= ===============================================================
Property Name                             Description
========================================= ===============================================================
``format``                                 Optionally specifies the format of table data files,
                                           either ``PARQUET`` or ``ORC``. Defaults to ``PARQUET``.

``partitioning``                           Optionally specifies table partitioning. If a table
                                           is partitioned by columns ``c1`` and ``c2``, the partitioning
                                           property is ``partitioning = ARRAY['c1', 'c2']``.

``location``                               Optionally specifies the file system location URI for
                                           the table.

``format_version``                         Optionally specifies the format version of the Iceberg
                                           specification to use for new tables, either ``1`` or ``2``.
                                           Defaults to ``1``.
========================================= ===============================================================

The table definition below specifies format ``ORC``, partitioning by columns ``c1`` and ``c2``,
and a file system location of ``s3://test_bucket/test_schema/test_table``:

.. code-block:: sql

    CREATE TABLE test_table (
        c1 bigint,
        c2 varchar,
        c3 double
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['c1', 'c2'],
        location = 's3://test_bucket/test_schema/test_table')
    )


Extra Hidden Metadata Tables
----------------------------

The Iceberg connector exposes extra hidden metadata tables. You can query these
as a part of SQL query by appending name with the table.

* ``$properties`` : General properties of the given table
.. code-block:: sql

    SELECT * FROM "ctas_nation$properties";

.. code-block:: text

             key           |  value
     ----------------------+---------
      write.format.default | PARQUET

* ``$history`` : History of table state changes
.. code-block:: sql

    SELECT * FROM "ctas_nation$history";

.. code-block:: text

               made_current_at            |     snapshot_id     | parent_id | is_current_ancestor
    --------------------------------------+---------------------+-----------+---------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | true

* ``$snapshots`` : Details about the table snapshots, see the details `here <https://iceberg.apache.org/spec/#snapshots>`_.
.. code-block:: sql

    SELECT * FROM "ctas_nation$snapshots";

.. code-block:: text

                 committed_at             |     snapshot_id     | parent_id | operation |                                                  manifest_list                                           |                                                                                 summary
    --------------------------------------+---------------------+-----------+-----------+----------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | append    | s3://my-bucket/ctas_nation/metadata/snap-7606232158543069775-1-395a2cad-b244-409b-b030-cc44949e5a4e.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=25, total-position-deletes=0, added-files-size=1648, total-delete-files=0, total-files-size=1648, total-records=25, total-data-files=1}

* ``$manifests`` : Details about the manifests of different table snapshots, see the details `here <https://iceberg.apache.org/spec/#manifests>`_.
.. code-block:: sql

    SELECT * FROM "ctas_nation$manifests";

.. code-block:: text

                                               path                                  | length | partition_spec_id |  added_snapshot_id  | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions
    ---------------------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+-----------
    s3://my-bucket/ctas_nation/metadata/395a2cad-b244-409b-b030-cc44949e5a4e-m0.avro |   5957 |                 0 | 7606232158543069775 |                      1 |                         0 |                    0     |    []


* ``$partitions`` : Detailed partition information of the table
.. code-block:: sql

    SELECT * FROM "ctas_nation$partitions";

.. code-block:: text

     row_count | file_count | total_size |           nationkey           |                   name                   |          regionkey           |                          comment
    -----------+------------+------------+-------------------------------+------------------------------------------+------------------------------+------------------------------------------------------------
        25     |          1 |       1648 | {min=0, max=24, null_count=0} | {min=ALGERIA, max=VIETNAM, null_count=0} | {min=0, max=4, null_count=0} | {min= haggle. careful, max=y final packaget, null_count=0}


* ``$files`` : Overview of data files in current snapshot of the table
.. code-block:: sql

    SELECT * FROM "ctas_nation$files";

.. code-block:: text

     content |                                      file_path                               | file_format | record_count | file_size_in_bytes |        column_sizes         |       value_counts       |  null_value_counts   | nan_value_counts |          lower_bounds                     |             upper_bounds                   | key_metadata | split_offsets | equality_ids
    ---------+------------------------------------------------------------------------------+-------------+--------------+--------------------+-----------------------------+--------------------------+----------------------+------------------+-------------------------------------------+--------------------------------------------+--------------+---------------+-------------
       0     | s3://my-bucket/ctas_nation/data/9f889274-6f74-4d28-8164-275eef99f660.parquet | PARQUET     |           25 |               1648 | {1=52, 2=222, 3=105, 4=757} | {1=25, 2=25, 3=25, 4=25} | {1=0, 2=0, 3=0, 4=0} |  NULL            | {1=0, 2=ALGERIA, 3=0, 4= haggle. careful} | {1=24, 2=VIETNAM, 3=4, 4=y final packaget} | NULL         | NULL          | NULL

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
