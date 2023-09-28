=================
Iceberg Connector
=================

Overview
--------

The Iceberg connector allows querying data stored in Iceberg tables.

Metastores
-----------
Iceberg tables store most of the metadata in the metadata files, along with the data on the
filesystem, but it still requires a central place to find the current location of the
current metadata pointer for a table. This central place is called the ``Iceberg Catalog``.
The Presto Iceberg connector supports different types of Iceberg Catalogs : ``Hive Metastore``,
``GLUE``, ``NESSIE``, and ``HADOOP``.

To configure the Iceberg connector, create a catalog properties file
``etc/catalog/iceberg.properties``. To define the catalog type, ``iceberg.catalog.type`` property
is required along with the following contents, with the property values replaced as follows:

Hive Metastore catalog
^^^^^^^^^^^^^^^^^^^^^^

The Iceberg connector supports the same configuration for
`HMS <https://prestodb.io/docs/current/connector/hive.html#metastore-configuration-properties>`_
as a Hive connector.

.. code-block:: none

    connector.name=iceberg
    hive.metastore.uri=hostname:port
    iceberg.catalog.type=hive

Glue catalog
^^^^^^^^^^^^

The Iceberg connector supports the same configuration for
`Glue <https://prestodb.io/docs/current/connector/hive.html#aws-glue-catalog-configuration-properties>`_
as a Hive connector.

.. code-block:: none

    connector.name=iceberg
    hive.metastore=glue
    iceberg.catalog.type=hive

Nessie catalog
^^^^^^^^^^^^^^

To use a Nessie catalog, configure the catalog type as
``iceberg.catalog.type=nessie``

.. code-block:: none

    connector.name=iceberg
    iceberg.catalog.type=nessie
    iceberg.catalog.warehouse=/tmp
    iceberg.nessie.uri=https://localhost:19120/api/v1

Additional supported properties for the Nessie catalog:

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

``iceberg.nessie.connect-timeout-ms``                The connection timeout in milliseconds for the connection
                                                     requests to the Nessie server.
                                                     Example: ``10000``

``iceberg.nessie.compression-enabled``               Configuration of whether compression should be enabled or
                                                     not for requests to the Nessie server, defaults to ``true``.

``iceberg.nessie.client-builder-impl``               Configuration of the custom ClientBuilder implementation
                                                     class to be used.

==================================================== ============================================================

Setting Up Nessie With Docker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To set up a Nessie instance locally using the Docker image, see `Setting up Nessie <https://projectnessie.org/try/docker/>`_. Once the Docker instance is up and running, you should see logs similar to the following example:

.. code-block:: none

    2023-09-05 13:11:37,905 INFO  [io.quarkus] (main) nessie-quarkus 0.69.0 on JVM (powered by Quarkus 3.2.4.Final) started in 1.921s. Listening on: http://0.0.0.0:19120
    2023-09-05 13:11:37,906 INFO  [io.quarkus] (main) Profile prod activated.
    2023-09-05 13:11:37,906 INFO  [io.quarkus] (main) Installed features: [agroal, amazon-dynamodb, cassandra-client, cdi, google-cloud-bigtable, hibernate-validator, jdbc-postgresql, logging-sentry, micrometer, mongodb-client, narayana-jta, oidc, opentelemetry, reactive-routes, resteasy, resteasy-jackson, security, security-properties-file, smallrye-context-propagation, smallrye-health, smallrye-openapi, swagger-ui, vertx]


If log messages related to Nessie's OpenTelemetry collector appear similar to the following example, you can disable OpenTelemetry using the configuration option ``quarkus.otel.sdk.disabled=true``.

.. code-block:: none

    2023-08-27 11:10:02,492 INFO  [io.qua.htt.access-log] (executor-thread-1) 172.17.0.1 - - [27/Aug/2023:11:10:02 +0000] "GET /api/v1/config HTTP/1.1" 200 62
    2023-08-27 11:10:05,007 SEVERE [io.ope.exp.int.grp.OkHttpGrpcExporter] (OkHttp http://localhost:4317/...) Failed to export spans. The request could not be executed. Full error message: Failed to connect to localhost/127.0.0.1:4317

For example, start the Docker image using the following command:
``docker run -p 19120:19120 -e QUARKUS_OTEL_SDK_DISABLED=true ghcr.io/projectnessie/nessie``

For more information about this configuration option and other related options, see the `OpenTelemetry Configuration Reference <https://quarkus.io/guides/opentelemetry#quarkus-opentelemetry_quarkus.otel.sdk.disabled>`_.

For more information about troubleshooting OpenTelemetry traces, see `Troubleshooting traces <https://projectnessie.org/try/configuration/#troubleshooting-traces>`_.

If an error similar to the following example is displayed, this is probably because you are interacting with an http server, and not https server. You need to set ``iceberg.nessie.uri`` to ``http://localhost:19120/api/v1``.

.. code-block:: none

    Caused by: javax.net.ssl.SSLException: Unsupported or unrecognized SSL message
    	at sun.security.ssl.SSLSocketInputRecord.handleUnknownRecord(SSLSocketInputRecord.java:448)
    	at sun.security.ssl.SSLSocketInputRecord.decode(SSLSocketInputRecord.java:174)
    	at sun.security.ssl.SSLTransport.decode(SSLTransport.java:111)
    	at sun.security.ssl.SSLSocketImpl.decode(SSLSocketImpl.java:1320)
    	at sun.security.ssl.SSLSocketImpl.readHandshakeRecord(SSLSocketImpl.java:1233)
    	at sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:417)
    	at sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:389)
    	at sun.net.www.protocol.https.HttpsClient.afterConnect(HttpsClient.java:558)
    	at sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection.connect(AbstractDelegateHttpsURLConnection.java:201)
    	at sun.net.www.protocol.https.HttpsURLConnectionImpl.connect(HttpsURLConnectionImpl.java:167)
    	at org.projectnessie.client.http.impl.jdk8.UrlConnectionRequest.executeRequest(UrlConnectionRequest.java:71)
    	... 42 more


Hadoop catalog
^^^^^^^^^^^^^^

Iceberg connector supports Hadoop catalog

.. code-block:: none

    connector.name=iceberg
    iceberg.catalog.type=hadoop
    iceberg.catalog.warehouse=hdfs://hostname:port

Configuration Properties
------------------------

.. note::

    The Iceberg connector supports configuration options for
    `Amazon S3 <https://prestodb.io/docs/current/connector/hive.html##amazon-s3-configuration>`_
    as a Hive connector.

The following configuration properties are available:

================================================== ============================================================= ============
Property Name                                      Description                                                   Default
================================================== ============================================================= ============
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default, and the rest of the URIs are
                                                   fallback metastores.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``
                                                   This property is required if the
                                                   ``iceberg.catalog.type`` is ``hive``. Otherwise, it will
                                                   be ignored.

``iceberg.file-format``                            The storage file format for Iceberg tables. The available     ``ORC``
                                                   values are ``PARQUET`` and ``ORC``.

``iceberg.compression-codec``                      The compression codec to use when writing files. The          ``GZIP``
                                                   available values are ``NONE``, ``SNAPPY``, ``GZIP``,
                                                   ``LZ4``, and ``ZSTD``.

``iceberg.catalog.type``                           The catalog type for Iceberg tables. The available values     ``hive``
                                                   are ``hive``, ``hadoop``, and ``nessie`` corresponding to
                                                   the catalogs in Iceberg.

``iceberg.catalog.warehouse``                      The catalog warehouse root path for Iceberg tables.
                                                   ``Example: hdfs://nn:8020/warehouse/path``
                                                   This property is required if the ``iceberg.catalog.type`` is
                                                   ``hadoop``. Otherwise, it will be ignored.

``iceberg.catalog.cached-catalog-num``             The number of Iceberg catalogs to cache. This property is     ``10``
                                                   required if the ``iceberg.catalog.type`` is ``hadoop``.
                                                   Otherwise, it will be ignored.

``iceberg.hadoop.config.resources``                The path(s) for Hadoop configuration resources.
                                                   ``Example: /etc/hadoop/conf/core-site.xml.`` This property
                                                   is required if the iceberg.catalog.type is hadoop. Otherwise,
                                                   it will be ignored.

``iceberg.max-partitions-per-writer``              The Maximum number of partitions handled per writer.          ``100``

``iceberg.minimum-assigned-split-weight``          A decimal value in the range (0, 1] is used as a minimum      ``0.05``
                                                   for weights assigned to each split. A low value may improve
                                                   performance on tables with small files. A higher value may
                                                   improve performance for queries with highly skewed
                                                   aggregations or joins.

``iceberg.enable-merge-on-read-mode``              Enable reading base tables that use merge-on-read for          ``false``
                                                   updates. The Iceberg connector currently does not read
                                                   delete lists, which means any updates will not be
                                                   reflected in the table.
================================================== ============================================================= ============

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

The following table properties are available, which are specific to the Presto Iceberg connector:

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
as a part of a SQL query by appending name to the table.

``$properties`` Table
^^^^^^^^^^^^^^^^^^^^^
* ``$properties`` : General properties of the given table
.. code-block:: sql

    SELECT * FROM "ctas_nation$properties";

.. code-block:: text

             key           |  value
     ----------------------+---------
      write.format.default | PARQUET

``$history`` Table
^^^^^^^^^^^^^^^^^^
* ``$history`` : History of table state changes
.. code-block:: sql

    SELECT * FROM "ctas_nation$history";

.. code-block:: text

               made_current_at            |     snapshot_id     | parent_id | is_current_ancestor
    --------------------------------------+---------------------+-----------+---------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | true

``$snapshots`` Table
^^^^^^^^^^^^^^^^^^^^
* ``$snapshots`` : Details about the table snapshots, see the details `here <https://iceberg.apache.org/spec/#snapshots>`_.
.. code-block:: sql

    SELECT * FROM "ctas_nation$snapshots";

.. code-block:: text

                 committed_at             |     snapshot_id     | parent_id | operation |                                                  manifest_list                                           |                                                                                 summary
    --------------------------------------+---------------------+-----------+-----------+----------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | append    | s3://my-bucket/ctas_nation/metadata/snap-7606232158543069775-1-395a2cad-b244-409b-b030-cc44949e5a4e.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=25, total-position-deletes=0, added-files-size=1648, total-delete-files=0, total-files-size=1648, total-records=25, total-data-files=1}

``$manifests`` Table
^^^^^^^^^^^^^^^^^^^^
* ``$manifests`` : Details about the manifests of different table snapshots, see the details `here <https://iceberg.apache.org/spec/#manifests>`_.
.. code-block:: sql

    SELECT * FROM "ctas_nation$manifests";

.. code-block:: text

                                               path                                  | length | partition_spec_id |  added_snapshot_id  | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions
    ---------------------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+-----------
    s3://my-bucket/ctas_nation/metadata/395a2cad-b244-409b-b030-cc44949e5a4e-m0.avro |   5957 |                 0 | 7606232158543069775 |                      1 |                         0 |                    0     |    []

``$partitions`` Table
^^^^^^^^^^^^^^^^^^^^^
* ``$partitions`` : Detailed partition information for the table
.. code-block:: sql

    SELECT * FROM "ctas_nation$partitions";

.. code-block:: text

     row_count | file_count | total_size |           nationkey           |                   name                   |          regionkey           |                          comment
    -----------+------------+------------+-------------------------------+------------------------------------------+------------------------------+------------------------------------------------------------
        25     |          1 |       1648 | {min=0, max=24, null_count=0} | {min=ALGERIA, max=VIETNAM, null_count=0} | {min=0, max=4, null_count=0} | {min= haggle. careful, max=y final packaget, null_count=0}

``$files`` Table
^^^^^^^^^^^^^^^^
* ``$files`` : Overview of data files in the current snapshot of the table
.. code-block:: sql

    SELECT * FROM "ctas_nation$files";

.. code-block:: text

     content |                                      file_path                               | file_format | record_count | file_size_in_bytes |        column_sizes         |       value_counts       |  null_value_counts   | nan_value_counts |          lower_bounds                     |             upper_bounds                   | key_metadata | split_offsets | equality_ids
    ---------+------------------------------------------------------------------------------+-------------+--------------+--------------------+-----------------------------+--------------------------+----------------------+------------------+-------------------------------------------+--------------------------------------------+--------------+---------------+-------------
       0     | s3://my-bucket/ctas_nation/data/9f889274-6f74-4d28-8164-275eef99f660.parquet | PARQUET     |           25 |               1648 | {1=52, 2=222, 3=105, 4=757} | {1=25, 2=25, 3=25, 4=25} | {1=0, 2=0, 3=0, 4=0} |  NULL            | {1=0, 2=ALGERIA, 3=0, 4= haggle. careful} | {1=24, 2=VIETNAM, 3=4, 4=y final packaget} | NULL         | NULL          | NULL

SQL Support
-----------

The Iceberg connector supports querying and manipulating Iceberg tables and schemas
(databases). Here are some examples of the SQL operations supported by Presto :

CREATE SCHEMA
^^^^^^^^^^^^^^

Create a new Iceberg schema named ``web`` that will store tables in an
S3 bucket named ``my-bucket``::

    CREATE SCHEMA iceberg.web
    WITH (location = 's3://my-bucket/')

CREATE TABLE
^^^^^^^^^^^^^

Create a new Iceberg table named ``page_views`` in the ``web`` schema
that is stored using the ORC file format, partitioned by ``ds`` and
``country``::

    CREATE TABLE iceberg.web.page_views (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioning = ARRAY['ds', 'country']
    )

Create an Iceberg table with Iceberg format version 2::

    CREATE TABLE iceberg.web.page_views_v2 (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioning = ARRAY['ds', 'country'],
      format_version = '2'
    )

Partition Column Transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Beyond selecting some particular columns for partitioning, you can use the ``transform`` functions and partition the table
by the transformed value of the column.

Available transforms in the Presto Iceberg connector include:

* ``Bucket`` (partitions data into a specified number of buckets using a hash function)
* ``Truncate`` (partitions the table based on the truncated value of the field and can specify the width of the truncated value)

Create an Iceberg table partitioned into 8 buckets of equal sized ranges::

    CREATE TABLE players (
        id int,
        name varchar,
        team varchar
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['bucket(team, 8)']
    );

Create an Iceberg table partitioned by the first letter of the team field::

    CREATE TABLE players (
        id int,
        name varchar,
        team varchar
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['truncate(team, 1)']
    );

.. note::

    ``Day``, ``Month``, ``Year``, ``Hour`` partition column transform functions are not supported in Presto Iceberg
    connector yet (:issue:`20570`).

INSERT INTO
^^^^^^^^^^^^

Insert data into the ``page_views`` table::

    INSERT INTO iceberg.web.page_views VALUES(TIMESTAMP '2023-08-12 03:04:05.321', 1, 'https://example.com', current_date, 'country');

CREATE TABLE AS SELECT
^^^^^^^^^^^^^^^^^^^^^^^

Create a new table ``page_views_new`` from an existing table ``page_views``::

    CREATE TABLE iceberg.web.page_views_new AS SELECT * FROM iceberg.web.page_views

SELECT
^^^^^^

SELECT table operations are supported for Iceberg format version 1 and version 2 in the connector::

    SELECT * FROM iceberg.web.page_views;

    SELECT * FROM iceberg.web.page_views_v2;

.. note::

    The ``SELECT`` operations on Iceberg Tables with format version 2 do not read the delete files and
    remove the deleted rows as of now (:issue:`20492`).

ALTER TABLE
^^^^^^^^^^^^

Alter table operations are supported in the connector::

     ALTER TABLE iceberg.web.page_views ADD COLUMN zipcode VARCHAR;

     ALTER TABLE iceberg.web.page_views RENAME COLUMN zipcode TO location;

     ALTER TABLE iceberg.web.page_views DROP COLUMN location;

TRUNCATE
^^^^^^^^

The iceberg connector can delete all of the data from tables without
dropping the table from the metadata catalog using ``TRUNCATE TABLE``.

.. code-block:: sql

    TRUNCATE TABLE nation;

.. code-block:: text

    TRUNCATE TABLE;

.. code-block:: sql

    SELECT * FROM nation;

.. code-block:: text

     nationkey | name | regionkey | comment
    -----------+------+-----------+---------
    (0 rows)

DROP TABLE
^^^^^^^^^^^

Drop the table ``page_views`` ::

    DROP TABLE iceberg.web.page_views

* Dropping an Iceberg table with Hive Metastore and Glue catalogs only removes metadata from metastore.
* Dropping an Iceberg table with Hadoop and Nessie catalogs removes all the data and metadata in the table.

DROP SCHEMA
^^^^^^^^^^^^

Drop a schema::

    DROP SCHEMA iceberg.web

Schema Evolution
-----------------

Iceberg and Presto Iceberg connector support in-place table evolution, aka
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

We can simply add a new column to the Iceberg table by using `ALTER TABLE`
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

We can also rename the new column to another name, `address`:

.. code-block:: sql

    ALTER TABLE ctas_nation RENAME COLUMN zipcode TO address;
    DESCRIBE ctas_nation;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     nationkey | bigint  |       |
     name      | varchar |       |
     regionkey | bigint  |       |
     comment   | varchar |       |
     address  | varchar |       |
    (5 rows)

Finally, we can delete the new column. The table columns will be restored to the
original state.

.. code-block:: sql

    ALTER TABLE ctas_nation DROP COLUMN address;
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
-----------

Iceberg and Presto Iceberg connector support time travel via table snapshots
identified by unique snapshot IDs. The snapshot IDs are stored in the ``$snapshots``
metadata table. We can rollback the state of a table to a previous snapshot ID.

Example Queries
^^^^^^^^^^^^^^^

Similar to the example queries in `Schema Evolution`, let's create an Iceberg
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

We can find snapshot IDs for the Iceberg table from the `$snapshots` metadata table.

.. code-block:: sql

    SELECT snapshot_id FROM iceberg.tpch."ctas_nation$snapshots" ORDER BY committed_at;

.. code-block:: text

         snapshot_id
    ---------------------
     5837462824399906536
    (1 row)

For now, as we've just created the table, there's only one snapshot ID. Let's
insert one row into the table and see the change in the snapshot IDs.

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

Now if we check the table again, we'll find that the newly inserted row no longer
exists as we've rolled back to the previous state.

.. code-block:: sql

    SELECT * FROM ctas_nation WHERE name = 'new country';

.. code-block:: text

     nationkey | name | regionkey | comment
    -----------+------+-----------+---------
    (0 rows)

Iceberg Connector Limitations
-----------------------------

* The ``SELECT`` operations on Iceberg Tables with format version 2 do not read the delete files
  and remove the deleted rows as of now (:issue:`20492`).
