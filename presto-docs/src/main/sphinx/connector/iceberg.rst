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
``iceberg.catalog.type=nessie``.

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

                                                     **Note:** Nessie BASIC authentication type is deprecated,
                                                     this will be removed in upcoming release

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

If an error similar to the following example is displayed, this is probably because you are interacting with an http server, and not an https server. You need to set ``iceberg.nessie.uri`` to ``http://localhost:19120/api/v1``.

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

To use a Hadoop catalog, configure the catalog type as
``iceberg.catalog.type=hadoop``

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

``iceberg.enable-merge-on-read-mode``              Enable reading base tables that use merge-on-read for          ``true``
                                                   updates.

``iceberg.delete-as-join-rewrite-enabled``         When enabled, equality delete row filtering is applied        ``true``
                                                   as a join with the data of the equality delete files.
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

Session Properties
-------------------

Session properties set behavior changes for queries executed within the given session.

============================================= ======================================================================
Property Name                                 Description
============================================= ======================================================================
``iceberg.delete_as_join_rewrite_enabled``    Overrides the behavior of the connector property
                                              ``iceberg.delete-as-join-rewrite-enabled`` in the current session.
============================================= ======================================================================

Caching Support
----------------

Manifest File Caching
^^^^^^^^^^^^^^^^^^^^^^

As of Iceberg version 1.1.0, Apache Iceberg provides a mechanism to cache the contents of Iceberg manifest files in memory. This feature helps
to reduce repeated reads of small Iceberg manifest files from remote storage.

.. note::

    Currently, manifest file caching is supported for Hadoop and Nessie catalogs in the Presto Iceberg connector.

The following configuration properties are available:

====================================================   =============================================================   ============
Property Name                                          Description                                                     Default
====================================================   =============================================================   ============
``iceberg.io.manifest.cache-enabled``                  Enable or disable the manifest caching feature. This feature    ``false``
                                                       is only available if ``iceberg.catalog.type`` is ``hadoop``
                                                       or ``nessie``.

``iceberg.io-impl``                                    Custom FileIO implementation to use in a catalog. It must       ``org.apache.iceberg.hadoop.HadoopFileIO``
                                                       be set to enable manifest caching.

``iceberg.io.manifest.cache.max-total-bytes``          Maximum size of cache size in bytes.                            ``104857600``

``iceberg.io.manifest.cache.expiration-interval-ms``   Maximum time duration in milliseconds for which an entry        ``60000``
                                                       stays in the manifest cache.

``iceberg.io.manifest.cache.max-content-length``       Maximum length of a manifest file to be considered for          ``8388608``
                                                       caching in bytes. Manifest files with a length exceeding
                                                       this size will not be cached.
====================================================   =============================================================   ============

Alluxio Data Cache
^^^^^^^^^^^^^^^^^^

A Presto worker caches remote storage data in its original form (compressed and possibly encrypted) on local SSD upon read.

The following configuration properties are required to set in the Iceberg catalog file (catalog/iceberg.properties):

.. code-block:: none

    cache.enabled=true
    cache.base-directory=file:///mnt/flash/data
    cache.type=ALLUXIO
    cache.alluxio.max-cache-size=1600GB
    hive.node-selection-strategy=SOFT_AFFINITY

JMX queries to get the metrics and verify the cache usage::

    SELECT * FROM jmx.current."com.facebook.alluxio:name=client.cachehitrate,type=gauges";

    SELECT * FROM jmx.current."com.facebook.alluxio:name=client.cachebytesreadcache,type=meters";

    SHOW TABLES FROM jmx.current like '%alluxio%';

File And Stripe Footer Cache
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Caches open file descriptors and stripe or file footer information in leaf worker memory. These pieces of data are mostly frequently accessed when reading files.

The following configuration properties are required to set in the Iceberg catalog file (catalog/iceberg.properties):

.. code-block:: none

    # scheduling
    hive.node-selection-strategy=SOFT_AFFINITY

    # orc
    iceberg.orc.file-tail-cache-enabled=true
    iceberg.orc.file-tail-cache-size=100MB
    iceberg.orc.file-tail-cache-ttl-since-last-access=6h
    iceberg.orc.stripe-metadata-cache-enabled=true
    iceberg.orc.stripe-footer-cache-size=100MB
    iceberg.orc.stripe-footer-cache-ttl-since-last-access=6h
    iceberg.orc.stripe-stream-cache-size=300MB
    iceberg.orc.stripe-stream-cache-ttl-since-last-access=6h

    # parquet
    iceberg.parquet.metadata-cache-enabled=true
    iceberg.parquet.metadata-cache-size=100MB
    iceberg.parquet.metadata-cache-ttl-since-last-access=6h

JMX queries to get the metrics and verify the cache usage::

    SELECT * FROM jmx.current."com.facebook.presto.hive:name=iceberg_parquetmetadata,type=cachestatsmbean";

Metastore Versioned Cache
^^^^^^^^^^^^^^^^^^^^^^^^^

Metastore cache only caches schema and table names. Other metadata would be fetched from the filesystem.

.. note::

    Metastore Versioned Cache would be applicable only for Hive Catalog in the Presto Iceberg connector.

.. code-block:: none

    hive.metastore-cache-ttl=2d
    hive.metastore-refresh-interval=3d
    hive.metastore-cache-maximum-size=10000000

Extra Hidden Metadata Columns
----------------------------

The Iceberg connector exposes extra hidden metadata columns. You can query these
as part of a SQL query by including them in your SELECT statement.

``$path`` column
^^^^^^^^^^^^^^^^
* ``$path``: Full file system path name of the file for this row
.. code-block:: sql

    SELECT "$path", regionkey FROM "ctas_nation";

.. code-block:: text

             $path                    |  regionkey
     ---------------------------------+-----------
      /full/path/to/file/file.parquet | 2

``$data_sequence_number`` column
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* ``$data_sequence_number``: The Iceberg data sequence number in which this row was added
.. code-block:: sql

    SELECT "$data_sequence_number", regionkey FROM "ctas_nation";

.. code-block:: text

             $data_sequence_number     |  regionkey
     ----------------------------------+------------
                  2                    | 3

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
* ``$snapshots`` : Details about the table snapshots. For more information see `Snapshots <https://iceberg.apache.org/spec/#snapshots>`_ in the Iceberg Table Spec.
.. code-block:: sql

    SELECT * FROM "ctas_nation$snapshots";

.. code-block:: text

                 committed_at             |     snapshot_id     | parent_id | operation |                                                  manifest_list                                           |                                                                                 summary
    --------------------------------------+---------------------+-----------+-----------+----------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | append    | s3://my-bucket/ctas_nation/metadata/snap-7606232158543069775-1-395a2cad-b244-409b-b030-cc44949e5a4e.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=25, total-position-deletes=0, added-files-size=1648, total-delete-files=0, total-files-size=1648, total-records=25, total-data-files=1}

``$manifests`` Table
^^^^^^^^^^^^^^^^^^^^
* ``$manifests`` : Details about the manifests of different table snapshots. For more information see `Manifests <https://iceberg.apache.org/spec/#manifests>`_ in the Iceberg Table Spec.
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

``$changelog`` Table
^^^^^^^^^^^^^^^^^^^^

This table lets you view which row-level changes have occurred to the table in a
particular order over time. The ``$changelog`` table represents the history of
changes to the table, while also making the data available to process through a
query.

The result of a changelog query always returns a static schema with four
columns:

1. ``operation``: (``VARCHAR``) indicating whether the row was inserted,
   updated, or deleted.
2. ``ordinal``: (``int``) A number indicating a relative order that a particular
   change needs to be applied to the table relative to all other changes.
3. ``snapshotid``: (``bigint``) Represents the snapshot a row-level
   change was made in.
4. ``rowdata``: (``row(T)``) which includes the data for the particular row. The
   inner values of this type match the schema of the parent table.

The changelog table can be queried with the following name format:

.. code-block:: sql

    ... FROM "<table>[@<begin snapshot ID>]$changelog[@<end snapshot ID>]"

- ``<table>`` is the name of the table.
- ``<begin snapshot ID>`` is the snapshot of the table you want to begin viewing
  changes from. This parameter is optional. If absent, the oldest available
  snapshot is used.
- ``<end snapshot ID>`` is the last snapshot for which you want to view changes.
  This parameter is optional. If absent, the most current snapshot of the
  table is used.

One use for the ``$changelog`` table would be to find when a record was inserted
or removed from the table. To accomplish this, the  ``$changelog`` table can be
used in conjunction with the ``$snapshots`` table. First, choose a snapshot ID
from the ``$snapshots`` table to choose the starting point.

.. code-block:: sql

    SELECT * FROM "orders$snapshots";

.. code-block:: text

                    committed_at                 |     snapshot_id     |      parent_id      | operation |                                                                                       manifest_list                                                                                        |                                                                                                              summary
    ---------------------------------------------+---------------------+---------------------+-----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     2023-09-26 08:45:20.930 America/Los_Angeles | 2423571386296047175 | NULL                | append    | file:/var/folders/g_/6_hxl7r16qdddw7956j_r88h0000gn/T/PrestoTest8140889264166671718/catalog/tpch/ctas_orders/metadata/snap-2423571386296047175-1-3f288b1c-95a9-406b-9e17-9cfe31a11b48.avro | {changed-partition-count=1, added-data-files=4, total-equality-deletes=0, added-records=100, total-position-deletes=0, added-files-size=9580, total-delete-files=0, total-files-size=9580, total-records=100, total-data-files=4}
     2023-09-26 08:45:36.942 America/Los_Angeles | 8702997868627997320 | 2423571386296047175 | append    | file:/var/folders/g_/6_hxl7r16qdddw7956j_r88h0000gn/T/PrestoTest8140889264166671718/catalog/tpch/ctas_orders/metadata/snap-8702997868627997320-1-a2e1c714-7eed-4e2c-b144-dae4147ebaa4.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=1687, total-delete-files=0, total-files-size=11267, total-records=101, total-data-files=5}
     2023-09-26 08:45:39.866 America/Los_Angeles | 7615903782581283889 | 8702997868627997320 | append    | file:/var/folders/g_/6_hxl7r16qdddw7956j_r88h0000gn/T/PrestoTest8140889264166671718/catalog/tpch/ctas_orders/metadata/snap-7615903782581283889-1-d94c2114-fd22-4de2-9ab5-c0b5bf67282f.avro | {changed-partition-count=1, added-data-files=3, total-equality-deletes=0, added-records=3, total-position-deletes=0, added-files-size=4845, total-delete-files=0, total-files-size=16112, total-records=104, total-data-files=8}
     2023-09-26 08:45:48.404 America/Los_Angeles |  677209275408372885 | 7615903782581283889 | append    | file:/var/folders/g_/6_hxl7r16qdddw7956j_r88h0000gn/T/PrestoTest8140889264166671718/catalog/tpch/ctas_orders/metadata/snap-677209275408372885-1-ad69e208-1440-459b-93e8-48e61f961758.avro  | {changed-partition-count=1, added-data-files=3, total-equality-deletes=0, added-records=5, total-position-deletes=0, added-files-size=4669, total-delete-files=0, total-files-size=20781, total-records=109, total-data-files=11}

Now that we know the snapshots available to query in the changelog, we can see
what changes were made to the table since it was created. Specifically, this
example uses the earliest snapshot ID: ``2423571386296047175``

.. code-block:: sql

    SELECT * FROM "ctas_orders@2423571386296047175$changelog" ORDER BY ordinal;

.. code-block:: text
    
     operation | ordinal |     snapshotid      |                                                                                                                   rowdata
    -----------+---------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     INSERT    |       0 | 8702997868627997320 | {orderkey=37504, custkey=1291, orderstatus=O, totalprice=165509.83, orderdate=1996-03-04, orderpriority=5-LOW, clerk=Clerk#000000871, shippriority=0, comment=c theodolites alongside of the fluffily bold requests haggle quickly against }
     INSERT    |       1 | 7615903782581283889 | {orderkey=12001, custkey=739, orderstatus=F, totalprice=138635.75, orderdate=1994-07-07, orderpriority=2-HIGH, clerk=Clerk#000000863, shippriority=0, comment=old, even theodolites. regular, special theodolites use furio}
     INSERT    |       1 | 7615903782581283889 | {orderkey=17989, custkey=364, orderstatus=F, totalprice=133669.05, orderdate=1994-01-17, orderpriority=4-NOT SPECIFIED, clerk=Clerk#000000547, shippriority=0, comment=ously express excuses. even theodolit}
     INSERT    |       1 | 7615903782581283889 | {orderkey=37504, custkey=1291, orderstatus=O, totalprice=165509.83, orderdate=1996-03-04, orderpriority=5-LOW, clerk=Clerk#000000871, shippriority=0, comment=c theodolites alongside of the fluffily bold requests haggle quickly against }
     INSERT    |       2 |  677209275408372885 | {orderkey=17991, custkey=92, orderstatus=O, totalprice=20732.51, orderdate=1998-07-09, orderpriority=4-NOT SPECIFIED, clerk=Clerk#000000636, shippriority=0, comment= the quickly express accounts. iron}
     INSERT    |       2 |  677209275408372885 | {orderkey=17989, custkey=364, orderstatus=F, totalprice=133669.05, orderdate=1994-01-17, orderpriority=4-NOT SPECIFIED, clerk=Clerk#000000547, shippriority=0, comment=ously express excuses. even theodolit}
     INSERT    |       2 |  677209275408372885 | {orderkey=17990, custkey=458, orderstatus=O, totalprice=218031.58, orderdate=1998-03-18, orderpriority=3-MEDIUM, clerk=Clerk#000000340, shippriority=0, comment=ounts wake final foxe}
     INSERT    |       2 |  677209275408372885 | {orderkey=18016, custkey=403, orderstatus=O, totalprice=174070.99, orderdate=1996-03-19, orderpriority=1-URGENT, clerk=Clerk#000000629, shippriority=0, comment=ly. quickly ironic excuses are furiously. carefully ironic pack}
     INSERT    |       2 |  677209275408372885 | {orderkey=18017, custkey=958, orderstatus=F, totalprice=203091.02, orderdate=1993-03-26, orderpriority=1-URGENT, clerk=Clerk#000000830, shippriority=0, comment=sleep quickly bold requests. slyly pending pinto beans haggle in pla}


SQL Support
-----------

The Iceberg connector supports querying and manipulating Iceberg tables and schemas
(databases). Here are some examples of the SQL operations supported by Presto:

CREATE SCHEMA
^^^^^^^^^^^^^^

Create a new Iceberg schema named ``web`` that stores tables in an
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
* ``Identity`` (partitions data using unmodified source value)
* ``Year`` (partitions data using integer value by extracting a date or timestamp year, as years from 1970)
* ``Month`` (partitions data using integer value by extracting a date or timestamp month, as months from 1970-01-01)
* ``Day`` (partitions data using integer value by extracting a date or timestamp day, as days from 1970-01-01)
* ``Hour`` (partitions data using integer value by extracting a timestamp hour, as hours from 1970-01-01 00:00:00)

Create an Iceberg table partitioned into 8 buckets of equal size ranges::

    CREATE TABLE players (
        id int,
        name varchar,
        team varchar
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['bucket(team, 8)']
    );

Create an Iceberg table partitioned by the first letter of the ``team`` field::

    CREATE TABLE players (
        id int,
        name varchar,
        team varchar
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['truncate(team, 1)']
    );

Create an Iceberg table partitioned by ``ds``::

    CREATE TABLE players (
        id int,
        name varchar,
        team varchar,
        ds date
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['year(ds)']
    );

Create an Iceberg table partitioned by ``ts``::

    CREATE TABLE players (
        id int,
        name varchar,
        team varchar,
        ts timestamp
    )
    WITH (
        format = 'ORC',
        partitioning = ARRAY['hour(ts)']
    );

CREATE VIEW
^^^^^^^^^^^^

The Iceberg connector supports creating views in Hive and Glue metastores.
To create a view named ``view_page_views`` for the ``iceberg.web.page_views`` table created in the `CREATE TABLE`_ example::

    CREATE VIEW iceberg.web.view_page_views AS SELECT user_id, country FROM iceberg.web.page_views;

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

Table with delete files
~~~~~~~~~~~~~~~~~~~~~~~

Iceberg V2 tables support row-level deletion. For more information see
`Row-level deletes <https://iceberg.apache.org/spec/#row-level-deletes>`_ in the Iceberg Table Spec.
Presto supports reading delete files, including Position Delete Files and Equality Delete Files.
When reading, Presto merges these delete files to read the latest results.

ALTER TABLE
^^^^^^^^^^^^

Alter table operations are supported in the Iceberg connector::

     ALTER TABLE iceberg.web.page_views ADD COLUMN zipcode VARCHAR;

     ALTER TABLE iceberg.web.page_views RENAME COLUMN zipcode TO location;

     ALTER TABLE iceberg.web.page_views DROP COLUMN location;

To add a new column as a partition column, identify the transform functions for the column.
The table is partitioned by the transformed value of the column::

     ALTER TABLE iceberg.web.page_views ADD COLUMN zipcode VARCHAR WITH (partitioning = 'identity');

     ALTER TABLE iceberg.web.page_views ADD COLUMN location VARCHAR WITH (partitioning = 'truncate(2)');

     ALTER TABLE iceberg.web.page_views ADD COLUMN location VARCHAR WITH (partitioning = 'bucket(8)');

     ALTER TABLE iceberg.web.page_views ADD COLUMN dt date WITH (partitioning = 'year');

     ALTER TABLE iceberg.web.page_views ADD COLUMN ts timestamp WITH (partitioning = 'month');

     ALTER TABLE iceberg.web.page_views ADD COLUMN dt date WITH (partitioning = 'day');

     ALTER TABLE iceberg.web.page_views ADD COLUMN ts timestamp WITH (partitioning = 'hour');

TRUNCATE
^^^^^^^^

The Iceberg connector can delete all of the data from tables without
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

DELETE
^^^^^^^^

The Iceberg connector can delete data in one or more entire partitions from tables by using ``DELETE FROM``. For example, to delete from the table ``lineitem``::

     DELETE FROM lineitem;

     DELETE FROM lineitem WHERE linenumber = 1;

     DELETE FROM lineitem WHERE linenumber not in (1, 3, 5, 7) and linestatus in ('O', 'F');

.. note::

    Columns in the filter must all be identity transformed partition columns of the target table.

    Filtered columns only support comparison operators, such as EQUALS, LESS THAN, or LESS THAN EQUALS.

    Deletes must only occur on the latest snapshot.

DROP TABLE
^^^^^^^^^^^

Drop the table ``page_views`` ::

    DROP TABLE iceberg.web.page_views

* Dropping an Iceberg table with Hive Metastore and Glue catalogs only removes metadata from metastore.
* Dropping an Iceberg table with Hadoop and Nessie catalogs removes all the data and metadata in the table.

DROP VIEW
^^^^^^^^^^

Drop the view ``view_page_views``::

    DROP VIEW iceberg.web.view_page_views;

DROP SCHEMA
^^^^^^^^^^^^

Drop the schema ``iceberg.web``::

    DROP SCHEMA iceberg.web

Register table
^^^^^^^^^^^^

Iceberg tables for which table data and metadata already exist in the
file system can be registered with the catalog using the ``register_table``
procedure on the catalog's ``system`` schema by supplying the target schema,
desired table name, and the location of the table metadata::

    CALL iceberg.system.register_table('schema_name', 'table_name', 'hdfs://localhost:9000/path/to/iceberg/table/metadata/dir')

.. note::

    If multiple metadata files of the same version exist at the specified
    location, the most recently modified one will be used.

A metadata file can optionally be included as an argument to ``register_table``
in the case where a specific metadata file contains the targeted table state::

    CALL iceberg.system.register_table('schema_name', 'table_name', 'hdfs://localhost:9000/path/to/iceberg/table/metadata/dir', '00000-35a08aed-f4b0-4010-95d2-9d73ef4be01c.metadata.json')

.. note::

    When registering a table with the Hive metastore, the user calling the
    procedure will be set as the owner of the table and will have ``SELECT``,
    ``INSERT``, ``UPDATE``, and ``DELETE`` privileges for that table. These
    privileges can be altered using the ``GRANT`` and ``REVOKE`` commands.

.. note::

    When using the Hive catalog, attempts to read registered Iceberg tables
    using the Hive connector will fail.

Unregister table
^^^^^^^^^^^^

Iceberg tables can be unregistered from the catalog using the ``unregister_table``
procedure on the catalog's ``system`` schema::

    CALL iceberg.system.unregister_table('schema_name', 'table_name')

.. note::

    Table data and metadata will remain in the filesystem after a call to
    ``unregister_table`` only when using the Hive catalog. This is similar to
    the behavior listed above for the ``DROP TABLE`` command.

Schema Evolution
-----------------

Iceberg and Presto Iceberg connector support in-place table evolution, also known as
schema evolution, such as adding, dropping, and renaming columns. With schema
evolution, users can evolve a table schema with SQL after enabling the Presto
Iceberg connector.

Parquet Writer Version
----------------------

Presto now supports Parquet writer versions V1 and V2 for the Iceberg catalog.
It can be toggled using the session property ``parquet_writer_version`` and the config property ``hive.parquet.writer.version``.
Valid values for these properties are ``PARQUET_1_0`` and ``PARQUET_2_0``. Default is ``PARQUET_2_0``.

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
metadata table. You can rollback the state of a table to a previous snapshot ID.
It also supports time travel query using VERSION (SYSTEM_VERSION) and TIMESTAMP (SYSTEM_TIME) options.

Example Queries
^^^^^^^^^^^^^^^

Similar to the example queries in `SCHEMA EVOLUTION`_, create an Iceberg
table named `ctas_nation` from the TPCH `nation` table::


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

Time Travel using VERSION (SYSTEM_VERSION) and TIMESTAMP (SYSTEM_TIME)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the Iceberg connector to access the historical data of a table.
You can see how the table looked like at a certain point in time,
even if the data has changed or been deleted since then.

.. code-block:: sql

    // snapshot ID 5300424205832769799
    INSERT INTO ctas_nation VALUES(10, 'united states', 1, 'comment');

    // snapshot ID 6891257133877048303
    INSERT INTO ctas_nation VALUES(20, 'canada', 2, 'comment');

    // snapshot ID 705548372863208787
    INSERT INTO ctas_nation VALUES(30, 'mexico', 3, 'comment');

    // snapshot ID for first record
    SELECT * FROM ctas_nation FOR VERSION AS OF 5300424205832769799;

    // snapshot ID for first record using SYSTEM_VERSION
    SELECT * FROM ctas_nation FOR SYSTEM_VERSION AS OF 5300424205832769799;

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
    (1 row)

In above example, SYSTEM_VERSION can be used as an alias for VERSION.

You can access the historical data of a table using FOR TIMESTAMP AS OF TIMESTAMP.
The query returns the table’s state using the table snapshot that is closest to the specified timestamp.
In this example, SYSTEM_TIME can be used as an alias for TIMESTAMP.

.. code-block:: sql

    // In following query, timestamp string is matching with second inserted record.
    SELECT * FROM ctas_nation FOR TIMESTAMP AS OF TIMESTAMP '2023-10-17 13:29:46.822 America/Los_Angeles';

    // Same example using SYSTEM_TIME as an alias for TIMESTAMP
    SELECT * FROM ctas_nation FOR SYSTEM_TIME AS OF TIMESTAMP '2023-10-17 13:29:46.822 America/Los_Angeles';

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
            20 | canada        |         2 | comment
    (2 rows)

The option following FOR TIMESTAMP AS OF can accept any expression that returns a timestamp with time zone value.
For example, `TIMESTAMP '2023-10-17 13:29:46.822 America/Los_Angeles'` is a constant string for the expression.
In the following query, the expression CURRENT_TIMESTAMP returns the current timestamp with time zone value.

.. code-block:: sql

    SELECT * FROM ctas_nation FOR TIMESTAMP AS OF CURRENT_TIMESTAMP;

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
            20 | canada        |         2 | comment
            30 | mexico        |         3 | comment
    (3 rows)
