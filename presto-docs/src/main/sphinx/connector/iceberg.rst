=================
Iceberg Connector
=================

Overview
--------

The Iceberg connector allows querying data stored in Iceberg tables.

Metastores
----------
Iceberg tables store most of the metadata in the metadata files, along with the data on the
filesystem, but it still requires a central place to find the current location of the
current metadata pointer for a table. This central place is called the ``Iceberg Catalog``.
The Presto Iceberg connector supports different types of Iceberg Catalogs : ``HIVE``,
``NESSIE``, ``REST``, and ``HADOOP``.

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

File-Based Metastore
^^^^^^^^^^^^^^^^^^^^

For testing or development purposes, this connector can be configured to use a local 
filesystem directory as a Hive Metastore. See :ref:`installation/deployment:File-Based Metastore`.  

Glue catalog
^^^^^^^^^^^^

The Iceberg connector supports the same configuration for
`Glue <https://prestodb.io/docs/current/connector/hive.html#aws-glue-catalog-configuration-properties>`_
as a Hive connector.

.. code-block:: none

    connector.name=iceberg
    hive.metastore=glue
    iceberg.catalog.type=hive

There are additional configurations available when using the Iceberg connector configured with Hive
or Glue catalogs.

======================================================== ============================================================= ============
Property Name                                            Description                                                   Default
======================================================== ============================================================= ============
``hive.metastore.uri``                                   The URI(s) of the Hive metastore to connect to using the
                                                         Thrift protocol. If multiple URIs are provided, the first
                                                         URI is used by default, and the rest of the URIs are
                                                         fallback metastores.

                                                         Example: ``thrift://192.0.2.3:9083`` or
                                                         ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``.

                                                         This property is required if the
                                                         ``iceberg.catalog.type`` is ``hive`` and ``hive.metastore``
                                                         is ``thrift``.

``hive.metastore.catalog.name``                          Specifies the catalog name to be passed to the metastore.

``iceberg.hive-statistics-merge-strategy``               Comma separated list of statistics to use from the
                                                         Hive Metastore to override Iceberg table statistics.
                                                         The available values are ``NUMBER_OF_DISTINCT_VALUES``
                                                         and ``TOTAL_SIZE_IN_BYTES``.

                                                         **Note**: Only valid when the Iceberg connector is
                                                         configured with Hive.

``iceberg.hive.table-refresh.backoff-min-sleep-time``    The minimum amount of time to sleep between retries when      100ms
                                                         refreshing table metadata.

``iceberg.hive.table-refresh.backoff-max-sleep-time``    The maximum amount of time to sleep between retries when      5s
                                                         refreshing table metadata.

``iceberg.hive.table-refresh.max-retry-time``            The maximum amount of time to take across all retries before  1min
                                                         failing a table metadata refresh operation.

``iceberg.hive.table-refresh.retries``                   The number of times to retry after errors when refreshing     20
                                                         table metadata using the Hive metastore.

``iceberg.hive.table-refresh.backoff-scale-factor``      The multiple used to scale subsequent wait time between       4.0
                                                         retries.

``iceberg.engine.hive.lock-enabled``                     Whether to use locks to ensure atomicity of commits.          true
                                                         This will turn off locks but is overridden at a table level
                                                         with the table configuration ``engine.hive.lock-enabled``.
======================================================== ============================================================= ============

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

REST catalog
^^^^^^^^^^^^

To use a REST catalog, configure the catalog type as
``iceberg.catalog.type=rest``. A minimal configuration includes:

.. code-block:: none

    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest.uri=https://localhost:8181

Additional supported properties for the REST catalog:

==================================================== ============================================================
Property Name                                        Description
==================================================== ============================================================
``iceberg.rest.uri``                                 REST API endpoint URI (required).
                                                     Example: ``https://localhost:8181``

``iceberg.rest.auth.type``                           The authentication type to use.
                                                     Available values are ``NONE`` or ``OAUTH2`` (default: ``NONE``).
                                                     ``OAUTH2`` requires either a credential or token.

``iceberg.rest.auth.oauth2.uri``                     OAUTH2 server endpoint URI.
                                                     Example: ``https://localhost:9191``

``iceberg.rest.auth.oauth2.credential``              The credential to use for OAUTH2 authentication.
                                                     Example: ``key:secret``

``iceberg.rest.auth.oauth2.token``                   The Bearer token to use for OAUTH2 authentication.
                                                     Example: ``SXVLUXUhIExFQ0tFUiEK``

``iceberg.rest.auth.oauth2.scope``                   The scope to use for OAUTH2 authentication.
                                                     This property is only applicable when using
                                                     ``iceberg.rest.auth.oauth2.credential``.
                                                     Example: ``PRINCIPAL_ROLE:ALL``

``iceberg.rest.nested.namespace.enabled``            In REST Catalogs, tables are grouped into namespaces, that can be
                                                     nested. But if a large number of recursive namespaces result in
                                                     lower performance, querying nested namespaces can be disabled.
                                                     Defaults to ``true``.

``iceberg.rest.session.type``                        The session type to use when communicating with the REST catalog.
                                                     Available values are ``NONE`` or ``USER`` (default: ``NONE``).

``iceberg.catalog.warehouse``                        A catalog warehouse root path for Iceberg tables (optional).
                                                     Example: ``s3://warehouse/``

==================================================== ============================================================

Hadoop catalog
^^^^^^^^^^^^^^

To use a Hadoop catalog, configure the catalog type as
``iceberg.catalog.type=hadoop``. A minimal configuration includes:

.. code-block:: none

    connector.name=iceberg
    iceberg.catalog.type=hadoop
    iceberg.catalog.warehouse=hdfs://hostname:port

Hadoop catalog configuration properties:

======================================================= ============================================================= ============
Property Name                                           Description                                                   Default
======================================================= ============================================================= ============
``iceberg.catalog.warehouse``                           The catalog warehouse root path for Iceberg tables.

                                                        The Hadoop catalog requires a file system that supports
                                                        an atomic rename operation, such as HDFS, to maintain
                                                        metadata files in order to implement an atomic transaction
                                                        commit.

                                                        Example: ``hdfs://nn:8020/warehouse/path``

                                                        Do not set ``iceberg.catalog.warehouse`` to a path in object
                                                        stores or local file systems in the production environment.

                                                        This property is required if the ``iceberg.catalog.type`` is
                                                        ``hadoop``. Otherwise, it will be ignored.

``iceberg.catalog.hadoop.warehouse.datadir``            The catalog warehouse root data path for Iceberg tables.
                                                        It is only supported with the Hadoop catalog.

                                                        Example: ``s3://iceberg_bucket/warehouse``.

                                                        This optional property can be set to a path in object
                                                        stores or HDFS.
                                                        If set, all tables in this Hadoop catalog default to saving
                                                        their data and delete files in the specified root
                                                        data directory.

``iceberg.catalog.cached-catalog-num``                  The number of Iceberg catalogs to cache. This property is     ``10``
                                                        required if the ``iceberg.catalog.type`` is ``hadoop``.
                                                        Otherwise, it will be ignored.
======================================================= ============================================================= ============

Configure the `Amazon S3 <https://prestodb.io/docs/current/connector/hive.html#amazon-s3-configuration>`_
properties to specify a S3 location as the warehouse data directory for the Hadoop catalog. This way,
the data and delete files of Iceberg tables are stored in S3. An example configuration includes:

.. code-block:: none

    connector.name=iceberg
    iceberg.catalog.type=hadoop
    iceberg.catalog.warehouse=hdfs://nn:8020/warehouse/path
    iceberg.catalog.hadoop.warehouse.datadir=s3://iceberg_bucket/warehouse

    hive.s3.use-instance-credentials=false
    hive.s3.aws-access-key=accesskey
    hive.s3.aws-secret-key=secretkey
    hive.s3.endpoint=http://192.168.0.103:9878
    hive.s3.path-style-access=true

Presto C++ Support
^^^^^^^^^^^^^^^^^^

``HIVE``, ``NESSIE``, ``REST``, and ``HADOOP`` Iceberg catalogs are supported in Presto C++.

Configuration Properties
------------------------

.. note::

    The Iceberg connector supports configuration options for
    `Amazon S3 <https://prestodb.io/docs/current/connector/hive.html#amazon-s3-configuration>`_
    as a Hive connector.

The following configuration properties are available for all catalog types:

======================================================= ============================================================= ================================== =================== =============================================
Property Name                                           Description                                                   Default                            Presto Java Support Presto C++ Support
======================================================= ============================================================= ================================== =================== =============================================
``iceberg.catalog.type``                                The catalog type for Iceberg tables. The available values     ``HIVE``                           Yes                 Yes, only needed on coordinator
                                                        are ``HIVE``, ``HADOOP``, and ``NESSIE`` and ``REST``.

``iceberg.hadoop.config.resources``                     The path(s) for Hadoop configuration resources.                                                  Yes                 Yes, only needed on coordinator

                                                        Example: ``/etc/hadoop/conf/core-site.xml.`` This property
                                                        is required if the iceberg.catalog.type is ``hadoop``.
                                                        Otherwise, it will be ignored.

``iceberg.file-format``                                 The storage file format for Iceberg tables. The available     ``PARQUET``                        Yes                 No, write is not supported yet
                                                        values are ``PARQUET`` and ``ORC``.

``iceberg.compression-codec``                           The compression codec to use when writing files. The          ``GZIP``                           Yes                 No, write is not supported yet
                                                        available values are ``NONE``, ``SNAPPY``, ``GZIP``,
                                                        ``LZ4``, and ``ZSTD``.

``iceberg.max-partitions-per-writer``                   The maximum number of partitions handled per writer.          ``100``                            Yes                 No, write is not supported yet

``iceberg.minimum-assigned-split-weight``               A decimal value in the range (0, 1] is used as a minimum      ``0.05``                           Yes                 Yes
                                                        for weights assigned to each split. A low value may improve
                                                        performance on tables with small files. A higher value may
                                                        improve performance for queries with highly skewed
                                                        aggregations or joins.

``iceberg.enable-merge-on-read-mode``                   Enable reading base tables that use merge-on-read for         ``true``                           Yes                 Yes, only needed on coordinator
                                                        updates.

``iceberg.delete-as-join-rewrite-enabled``              When enabled, equality delete row filtering is applied        ``true``                           Yes                 No, Equality delete read is not supported
                                                        as a join with the data of the equality delete files.
                                                        Deprecated: This property is deprecated and will be removed
                                                        in a future release. Use the
                                                        ``iceberg.delete-as-join-rewrite-max-delete-columns``
                                                        configuration property instead.

``iceberg.delete-as-join-rewrite-max-delete-columns``   When set to a number greater than 0, this property enables    ``400``                            Yes                 No, Equality delete read is not supported
                                                        equality delete row filtering as a join with the data of the
                                                        equality delete files. The value of this property is the
                                                        maximum number of columns that can be used in the equality
                                                        delete files. If the number of columns in the equality delete
                                                        files exceeds this value, then the optimization is not
                                                        applied and the equality delete files are applied directly to
                                                        each row in the data files.

                                                        This property is only applicable when
                                                        ``iceberg.delete-as-join-rewrite-enabled`` is set to
                                                        ``true``.

``iceberg.enable-parquet-dereference-pushdown``         Enable parquet dereference pushdown.                          ``true``                           Yes                 No

``iceberg.statistic-snapshot-record-difference-weight`` The amount that the difference in total record count matters                                     Yes                 Yes, only needed on coordinator
                                                        when calculating the closest snapshot when picking
                                                        statistics. A value of 1 means a single record is equivalent
                                                        to 1 millisecond of time difference.

``iceberg.pushdown-filter-enabled``                     Experimental: Enable filter pushdown for Iceberg. This is     ``false``                          No                  Yes
                                                        only supported with Presto C++.

``iceberg.rows-for-metadata-optimization-threshold``    The maximum number of partitions in an Iceberg table to       ``1000``                           Yes                 Yes
                                                        allow optimizing queries of that table using metadata. If
                                                        an Iceberg table has more partitions than this threshold,
                                                        metadata optimization is skipped.

                                                        Set to ``0`` to disable metadata optimization.

``iceberg.split-manager-threads``                       Number of threads to use for generating Iceberg splits.       ``Number of available processors`` Yes                 Yes, only needed on coordinator

``iceberg.metadata-previous-versions-max``              The maximum number of old metadata files to keep in           ``100``                            Yes                 No, write is not supported yet
                                                        current metadata log.

``iceberg.metadata-delete-after-commit``                Set to ``true`` to delete the oldest metadata files after     ``false``                          Yes                 No, write is not supported yet
                                                        each commit.

``iceberg.metrics-max-inferred-column``                 The maximum number of columns for which metrics               ``100``                            Yes                 No, write is not supported yet
                                                        are collected.
``iceberg.max-statistics-file-cache-size``              Maximum size in bytes that should be consumed by the          ``256MB``                          Yes                 Yes, only needed on coordinator
                                                        statistics file cache.
======================================================= ============================================================= ================================== =================== =============================================

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

========================================================   ===============================================================   ===================== =================== =============================================
Property Name                                              Description                                                       Default               Presto Java Support Presto C++ Support
========================================================   ===============================================================   ===================== =================== =============================================
``commit.retry.num-retries``                               Determines the number of attempts for committing the metadata     ``4``                 Yes                 No, write is not supported yet
                                                           in case of concurrent upsert requests, before failing.

``format-version``                                         Optionally specifies the format version of the Iceberg            ``2``                 Yes                 No, write is not supported yet
                                                           specification to use for new tables, either ``1`` or ``2``.

``location``                                               Optionally specifies the file system location URI for                                   Yes                 Yes
                                                           the table.

``partitioning``                                           Optionally specifies table partitioning. If a table                                     Yes                 Yes
                                                           is partitioned by columns ``c1`` and ``c2``, the partitioning
                                                           property is ``partitioning = ARRAY['c1', 'c2']``.

``read.split.target-size``                                 The target size for an individual split when generating splits    ``134217728`` (128MB) Yes                 Yes
                                                           for a table scan. Generated splits may still be larger or
                                                           smaller than this value. Must be specified in bytes.

``write.data.path``                                        Optionally specifies the file system location URI for                                   Yes                 No, write is not supported yet
                                                           storing the data and delete files of the table. This only
                                                           applies to files written after this property is set. Files
                                                           previously written aren't relocated to reflect this
                                                           parameter.

``write.delete.mode``                                      Optionally specifies the write delete mode of the Iceberg         ``merge-on-read``     Yes                 No, write is not supported yet
                                                           specification to use for new tables, either ``copy-on-write``
                                                           or ``merge-on-read``.

``write.format.default``                                   Optionally specifies the format of table data files,              ``PARQUET``           Yes                 No, write is not supported yet
                                                           either ``PARQUET`` or ``ORC``.

``write.metadata.previous-versions-max``                   Optionally specifies the max number of old metadata files to      ``100``               Yes                 No, write is not supported yet
                                                           keep in current metadata log.

``write.metadata.delete-after-commit.enabled``             Set to ``true`` to delete the oldest metadata file after          ``false``             Yes                 No, write is not supported yet
                                                           each commit.

``write.metadata.metrics.max-inferred-column-defaults``    Optionally specifies the maximum number of columns for which      ``100``               Yes                 No, write is not supported yet
                                                           metrics are collected.

``write.update.mode``                                      Optionally specifies the write delete mode of the Iceberg         ``merge-on-read``     Yes                 No, write is not supported yet
                                                           specification to use for new tables, either ``copy-on-write``
                                                           or ``merge-on-read``.
========================================================   ===============================================================   ===================== =================== =============================================

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

Deprecated Table Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some table properties have been deprecated or removed. The following table lists the deprecated
properties and their replacements. Update queries to use the new property names as soon as
possible. They will be removed in a future version.

=======================================   =======================================================
Deprecated Property Name                  New Property Name
=======================================   =======================================================
``format``                                ``write.format.default``
``format_version``                        ``format-version``
``commit_retries``                        ``commit.retry.num-retries``
``delete_mode``                           ``write.delete.mode``
``metadata_previous_versions_max``        ``write.metadata.previous-versions-max``
``metadata_delete_after_commit``          ``write.metadata.delete-after-commit.enabled``
``metrics_max_inferred_column``           ``write.metadata.metrics.max-inferred-column-defaults``
=======================================   =======================================================

Session Properties
------------------

Session properties set behavior changes for queries executed within the given session.

===================================================== ======================================================================= =================== =============================================
Property Name                                         Description                                                             Presto Java Support Presto C++ Support
===================================================== ======================================================================= =================== =============================================
``iceberg.delete_as_join_rewrite_enabled``            Overrides the behavior of the connector property                        Yes                 No, Equality delete read is not supported
                                                      ``iceberg.delete-as-join-rewrite-enabled`` in the current session.

                                                      Deprecated: This property is deprecated and will be removed.  Use
                                                      ``iceberg.delete_as_join_rewrite_max_delete_columns`` instead.
``iceberg.delete_as_join_rewrite_max_delete_columns`` Overrides the behavior of the connector property                        Yes                 No, Equality delete read is not supported
                                                      ``iceberg.delete-as-join-rewrite-max-delete-columns`` in the
                                                      current session.
``iceberg.hive_statistics_merge_strategy``            Overrides the behavior of the connector property                        Yes                 Yes
                                                      ``iceberg.hive-statistics-merge-strategy`` in the current session.
``iceberg.rows_for_metadata_optimization_threshold``  Overrides the behavior of the connector property                        Yes                 Yes
                                                      ``iceberg.rows-for-metadata-optimization-threshold`` in the current
                                                      session.
``iceberg.target_split_size_bytes``                   Overrides the target split size for all tables in a query in bytes.     Yes                 Yes
                                                      Set to 0 to use the value in each Iceberg table's
                                                      ``read.split.target-size`` property.
``iceberg.affinity_scheduling_file_section_size``     When the ``node_selection_strategy`` or                                 Yes                 Yes
                                                      ``hive.node-selection-strategy`` property is set to ``SOFT_AFFINITY``,
                                                      this configuration property will change the size of a file chunk that
                                                      is hashed to a particular node when determining the which worker to
                                                      assign a split to. Splits which read data from the same file within
                                                      the same chunk will hash to the same node. A smaller chunk size will
                                                      result in a higher probability splits being distributed evenly across
                                                      the cluster, but reduce locality.
``iceberg.parquet_dereference_pushdown_enabled``      Overrides the behavior of the connector property                        Yes                 No
                                                      ``iceberg.enable-parquet-dereference-pushdown`` in the current session.
===================================================== ======================================================================= =================== =============================================

Caching Support
---------------

Statistics File Caching
^^^^^^^^^^^^^^^^^^^^^^^

Support for Puffin-based statistics caching. It is enabled by default.

JMX query to get the metrics and verify the cache usage::

    SELECT * FROM jmx.current."com.facebook.presto.iceberg.statistics:name=iceberg,type=statisticsfilecache";

Manifest File Caching
^^^^^^^^^^^^^^^^^^^^^

As of Iceberg version 1.1.0, Apache Iceberg provides a mechanism to cache the contents of Iceberg manifest files in memory. This feature helps
to reduce repeated reads of small Iceberg manifest files from remote storage.

The following configuration properties are available:

====================================================   =============================================================   ============
Property Name                                          Description                                                     Default
====================================================   =============================================================   ============
``iceberg.io.manifest.cache-enabled``                  Enable or disable the manifest caching feature.                 ``true``

``iceberg.io-impl``                                    Custom FileIO implementation to use in a catalog. It must       ``org.apache.iceberg.hadoop.HadoopFileIO``
                                                       be set to enable manifest caching. This is only needed for
                                                       Hadoop, Nessie and REST catalogs.

``iceberg.io.manifest.cache.max-total-bytes``          Maximum size of cache size in bytes.                            ``104857600``

``iceberg.io.manifest.cache.expiration-interval-ms``   Maximum time duration in milliseconds for which an entry        ``60000``
                                                       stays in the manifest cache. Set to 0 to disable entry
                                                       expiration.

``iceberg.io.manifest.cache.max-content-length``       Maximum length of a manifest file to be considered for          ``8388608``
                                                       caching in bytes. Manifest files with a length exceeding
                                                       this size will not be cached.
====================================================   =============================================================   ============

JMX query to get the metrics and verify the cache usage::

    SELECT * FROM jmx.current."com.facebook.presto.iceberg:name=iceberg,type=manifestfilecache";

.. note::

    Manifest file cache statistics are only available through the JMX connector when the Iceberg connector is configured with a HIVE catalog type.

Presto C++ Support
~~~~~~~~~~~~~~~~~~

Manifest file caching is supported in Presto C++.

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

Presto C++ Support
~~~~~~~~~~~~~~~~~~

Alluxio data caching is applicable for Presto Java. Async data cache is supported in Presto C++. See :ref:`async_data_caching_and_prefetching`.

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

Presto C++ Support
~~~~~~~~~~~~~~~~~~

File and stripe footer cache is not applicable for Presto C++.

Metastore Cache
^^^^^^^^^^^^^^^

Iceberg Connector does not support Metastore Caching.

Extra Hidden Metadata Columns
-----------------------------

The Iceberg connector exposes extra hidden metadata columns. You can query these
as part of a SQL query by including them in your SELECT statement.

``$path`` column
^^^^^^^^^^^^^^^^
The full file system path name of the file for this row.

.. code-block:: sql

    SELECT "$path", regionkey FROM "ctas_nation";

.. code-block:: text

             $path                    |  regionkey
     ---------------------------------+-----------
      /full/path/to/file/file.parquet | 2

``$data_sequence_number`` column
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The Iceberg data sequence number in which this row was added.

.. code-block:: sql

    SELECT "$data_sequence_number", regionkey FROM "ctas_nation";

.. code-block:: text

             $data_sequence_number     |  regionkey
     ----------------------------------+------------
                  2                    | 3

``$deleted`` column
^^^^^^^^^^^^^^^^^^^
Whether this row is a deleted row. When this column is used, deleted rows
from delete files will be marked as ``true`` instead of being filtered out of the results.

.. code-block:: sql

    DELETE FROM "ctas_nation" WHERE regionkey = 0;

    SELECT "$deleted", regionkey FROM "ctas_nation";

.. code-block:: text

     $deleted | regionkey
    ----------+-----------
     true     |         0
     false    |         1

``$delete_file_path`` column
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The path of the delete file corresponding to a deleted row, or NULL if the row was not deleted.
When this column is used, deleted rows will not be filtered out of the results.

.. code-block:: sql

    DELETE FROM "ctas_nation" WHERE regionkey = 0;

    SELECT "$delete_file_path", regionkey FROM "ctas_nation";

.. code-block:: text

                                     $delete_file_path                                 | regionkey
    -----------------------------------------------------------------------------------+-----------
     file:/path/to/table/data/delete_file_d8510b3e-510a-4fc2-b2b2-e59ead7fd386.parquet |         0
     NULL                                                                              |         1

Presto C++ Support
^^^^^^^^^^^^^^^^^^

All above metadata columns are supported in Presto C++.

Extra Hidden Metadata Tables
----------------------------

The Iceberg connector exposes extra hidden metadata tables. You can query these
as a part of a SQL query by appending name to the table.

``$properties`` Table
^^^^^^^^^^^^^^^^^^^^^
General properties of the given table.

.. code-block:: sql

    SELECT * FROM "ctas_nation$properties";

.. code-block:: text

             key           |  value
     ----------------------+---------
      write.format.default | PARQUET

``$history`` Table
^^^^^^^^^^^^^^^^^^
History of table state changes.

.. code-block:: sql

    SELECT * FROM "ctas_nation$history";

.. code-block:: text

               made_current_at            |     snapshot_id     | parent_id | is_current_ancestor
    --------------------------------------+---------------------+-----------+---------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | true

``$snapshots`` Table
^^^^^^^^^^^^^^^^^^^^
Details about the table snapshots. For more information see `Snapshots <https://iceberg.apache.org/spec/#snapshots>`_ in the Iceberg Table Spec.

.. code-block:: sql

    SELECT * FROM "ctas_nation$snapshots";

.. code-block:: text

                 committed_at             |     snapshot_id     | parent_id | operation |                                                  manifest_list                                           |                                                                                 summary
    --------------------------------------+---------------------+-----------+-----------+----------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    2022-11-25 20:56:31.784 Asia/Kolkata  | 7606232158543069775 | NULL      | append    | s3://my-bucket/ctas_nation/metadata/snap-7606232158543069775-1-395a2cad-b244-409b-b030-cc44949e5a4e.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=25, total-position-deletes=0, added-files-size=1648, total-delete-files=0, total-files-size=1648, total-records=25, total-data-files=1}

``$manifests`` Table
^^^^^^^^^^^^^^^^^^^^
Details about the manifests of different table snapshots. For more information see `Manifests <https://iceberg.apache.org/spec/#manifests>`_ in the Iceberg Table Spec.

.. code-block:: sql

    SELECT * FROM "ctas_nation$manifests";

.. code-block:: text

                                               path                                  | length | partition_spec_id |  added_snapshot_id  | added_data_files_count | existing_data_files_count | deleted_data_files_count | partitions
    ---------------------------------------------------------------------------------+--------+-------------------+---------------------+------------------------+---------------------------+--------------------------+-----------
    s3://my-bucket/ctas_nation/metadata/395a2cad-b244-409b-b030-cc44949e5a4e-m0.avro |   5957 |                 0 | 7606232158543069775 |                      1 |                         0 |                    0     |    []

``$partitions`` Table
^^^^^^^^^^^^^^^^^^^^^
Detailed partition information for the table.

.. code-block:: sql

    SELECT * FROM "ctas_nation$partitions";

.. code-block:: text

     row_count | file_count | total_size |           nationkey           |                   name                   |          regionkey           |                          comment
    -----------+------------+------------+-------------------------------+------------------------------------------+------------------------------+------------------------------------------------------------
        25     |          1 |       1648 | {min=0, max=24, null_count=0} | {min=ALGERIA, max=VIETNAM, null_count=0} | {min=0, max=4, null_count=0} | {min= haggle. careful, max=y final packaget, null_count=0}

``$files`` Table
^^^^^^^^^^^^^^^^
Overview of data files in the current snapshot of the table.

.. code-block:: sql

    SELECT * FROM "ctas_nation$files";

.. code-block:: text

     content |                                      file_path                               | file_format | record_count | file_size_in_bytes |        column_sizes         |       value_counts       |  null_value_counts   | nan_value_counts |          lower_bounds                     |             upper_bounds                   | key_metadata | split_offsets | equality_ids
    ---------+------------------------------------------------------------------------------+-------------+--------------+--------------------+-----------------------------+--------------------------+----------------------+------------------+-------------------------------------------+--------------------------------------------+--------------+---------------+-------------
       0     | s3://my-bucket/ctas_nation/data/9f889274-6f74-4d28-8164-275eef99f660.parquet | PARQUET     |           25 |               1648 | {1=52, 2=222, 3=105, 4=757} | {1=25, 2=25, 3=25, 4=25} | {1=0, 2=0, 3=0, 4=0} |  NULL            | {1=0, 2=ALGERIA, 3=0, 4= haggle. careful} | {1=24, 2=VIETNAM, 3=4, 4=y final packaget} | NULL         | NULL          | NULL

``$changelog`` Table
^^^^^^^^^^^^^^^^^^^^

This table lets you view which row-level changes have occurred to the table in a
particular order over time. The ``$changelog`` table presents the history of
changes to the table and makes the data available to process through a
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

``$refs`` Table
^^^^^^^^^^^^^^^^^^^^
Details about Iceberg references including branches and tags. For more information see `Branching and Tagging <https://iceberg.apache.org/docs/nightly/branching/>`_.

.. code-block:: sql

    SELECT * FROM "ctas_nation$refs";

.. code-block:: text

        name     |  type  |     snapshot_id     | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms
     ------------+--------+---------------------+-------------------------+-----------------------+------------------------
      main       | BRANCH | 3074797416068623476 | NULL                    | NULL                  | NULL
      testBranch | BRANCH | 3374797416068698476 | NULL                    | NULL                  | NULL
      testTag    | TAG    | 4686954189838128572 | 10                      | NULL                  | NULL

Presto C++ Support
^^^^^^^^^^^^^^^^^^

All above metadata tables, except `$changelog`, are supported in Presto C++.

Procedures
----------

Use the :doc:`/sql/call` statement to perform data manipulation or administrative tasks. Procedures are available in the ``system`` schema of the catalog.

Register Table
^^^^^^^^^^^^^^

Iceberg tables for which table data and metadata already exist in the
file system can be registered with the catalog. Use the ``register_table``
procedure on the catalog's ``system`` schema to register a table which
already exists but does not known by the catalog.

The following arguments are available:


===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to register

``table_name``        Yes        string          Name of the table to register

``metadata_location`` Yes        string          The location of the table metadata which is to be registered

``metadata_file``                string          An optionally specified metadata file which is to be registered
===================== ========== =============== =======================================================================

Examples:

* Register a table through supplying the target schema, desired table name, and the location of the table metadata::

    CALL iceberg.system.register_table('schema_name', 'table_name', 'hdfs://localhost:9000/path/to/iceberg/table/metadata/dir')

    CALL iceberg.system.register_table(table_name => 'table_name', schema => 'schema_name', metadata_location => 'hdfs://localhost:9000/path/to/iceberg/table/metadata/dir')

  .. note::

    If multiple metadata files of the same version exist at the specified location,
    the most recently modified one is used.

* Register a table through additionally supplying a specific metadata file::

    CALL iceberg.system.register_table('schema_name', 'table_name', 'hdfs://localhost:9000/path/to/iceberg/table/metadata/dir', '00000-35a08aed-f4b0-4010-95d2-9d73ef4be01c.metadata.json')

    CALL iceberg.system.register_table(table_name => 'table_name', schema => 'schema_name', metadata_location => 'hdfs://localhost:9000/path/to/iceberg/table/metadata/dir', metadata_file => '00000-35a08aed-f4b0-4010-95d2-9d73ef4be01c.metadata.json')

  .. note::

    The Iceberg REST catalog may not support table register depending on the
    type of the backing catalog.

  .. note::

    When registering a table with the Hive metastore, the user calling the procedure 
    is set as the owner of the table and has ``SELECT``, ``INSERT``, ``UPDATE``, and 
    ``DELETE`` privileges for that table. These privileges can be altered using the 
    ``GRANT`` and ``REVOKE`` commands.

  .. note::

    When using the Hive catalog, attempts to read registered Iceberg tables
    using the Hive connector will fail.

Unregister Table
^^^^^^^^^^^^^^^^

Iceberg tables can be unregistered from the catalog using the ``unregister_table``
procedure on the catalog's ``system`` schema.

The following arguments are available:

===================== ========== =============== ===================================
Argument Name         Required   Type            Description
===================== ========== =============== ===================================
``schema``            Yes        string          Schema of the table to unregister

``table_name``        Yes        string          Name of the table to unregister
===================== ========== =============== ===================================

Examples::

    CALL iceberg.system.unregister_table('schema_name', 'table_name')

    CALL iceberg.system.unregister_table(table_name => 'table_name', schema => 'schema_name')

.. note::

    Table data and metadata remain in the filesystem after a call to
    ``unregister_table`` only when using the Hive catalog. This is similar to
    the behavior listed for the `DROP TABLE <#id1>`_ command.


Rollback to Snapshot
^^^^^^^^^^^^^^^^^^^^

Rollback a table to a specific snapshot ID. Iceberg can rollback to a specific snapshot ID by using the ``rollback_to_snapshot`` procedure on Iceberg's ``system`` schema::

    CALL iceberg.system.rollback_to_snapshot('schema_name', 'table_name', snapshot_id);

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes         string          Schema of the table to update

``table_name``        Yes         string          Name of the table to update

``snapshot_id``       Yes         long            Snapshot ID to rollback to
===================== ========== =============== =======================================================================

Rollback to Timestamp
^^^^^^^^^^^^^^^^^^^^^

Rollback a table to a given point in time. Iceberg can rollback to a specific point in time by using the ``rollback_to_timestamp`` procedure on Iceberg's ``system`` schema.

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to update

``table_name``        Yes        string          Name of the table to update

``timestamp``         Yes        timestamp       Timestamp to rollback to
===================== ========== =============== =======================================================================

Example::

    CALL iceberg.system.rollback_to_timestamp('schema_name', 'table_name', TIMESTAMP '1995-04-26 00:00:00.000');

Set Current Snapshot
^^^^^^^^^^^^^^^^^^^^

Set a ``snapshot_id`` or ``ref`` as the current snapshot for a table.

.. note::

    Use either ``snapshot_id`` or ``ref``, but do not use both in the same procedure.

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to update

``table_name``        Yes        string          Name of the table to update

``snapshot_id``                  long            Snapshot ID to set as current

``ref``                          string          Snapshot Reference (branch or tag) to set as current
===================== ========== =============== =======================================================================

Examples:

* Set current table snapshot ID for the given table to 10000 ::

    CALL iceberg.system.set_current_snapshot('schema_name', 'table_name', 10000);

* Set current table snapshot ID for the given table to snapshot ID of branch1 ::

    CALL iceberg.system.set_current_snapshot(schema => 'schema_name', table_name => 'table_name', ref => 'branch1');

Expire Snapshots
^^^^^^^^^^^^^^^^

Each DML (Data Manipulation Language) action in Iceberg produces a new snapshot while keeping the old data and metadata for snapshot isolation and time travel. Use `expire_snapshots` to remove older snapshots and their files.

This procedure removes old snapshots and their corresponding files, and never removes files which are required by a non-expired snapshot.

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to update

``table_name``        Yes        string          Name of the table to update

``older_than``                   timestamp       Timestamp before which snapshots will be removed (Default: 5 days ago)

``retain_last``                  int             Number of ancestor snapshots to preserve regardless of older_than
                                                 (defaults to 1)

``snapshot_ids``                 array of long   Array of snapshot IDs to expire
===================== ========== =============== =======================================================================

Examples:

* Remove snapshots older than a specific day and time, but retain the last 10 snapshots::

    CALL iceberg.system.expire_snapshots('schema_name', 'table_name', TIMESTAMP '2023-08-31 00:00:00.000', 10);

* Remove snapshots with snapshot ID 10001 and 10002 (note that these snapshot IDs should not be the current snapshot)::

    CALL iceberg.system.expire_snapshots(schema => 'schema_name', table_name => 'table_name', snapshot_ids => ARRAY[10001, 10002]);

Remove Orphan Files
^^^^^^^^^^^^^^^^^^^

Use to remove files which are not referenced in any metadata files of an Iceberg table.

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to clean

``table_name``        Yes        string          Name of the table to clean

``older_than``                   timestamp       Remove orphan files created before this timestamp (Default: 3 days ago)
===================== ========== =============== =======================================================================

Examples:

* Remove any files which are not known to the table `db.sample` and older than specified timestamp::

    CALL iceberg.system.remove_orphan_files('db', 'sample', TIMESTAMP '2023-08-31 00:00:00.000');

* Remove any files which are not known to the table `db.sample` and created 3 days ago (by default)::

    CALL iceberg.system.remove_orphan_files(schema => 'db', table_name => 'sample');

Fast Forward Branch
^^^^^^^^^^^^^^^^^^^

This procedure advances the current snapshot of the specified branch to a more recent snapshot from another branch without replaying any intermediate snapshots.
``branch`` can be fast-forwarded up to the ``to`` snapshot if ``branch`` is an ancestor of ``to``.

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         required   type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to update

``table_name``        Yes        string          Name of the table to update

``branch``            Yes        string          The branch you want to fast-forward

``to``                Yes        string          The branch you want to fast-forward to
===================== ========== =============== =======================================================================

Examples:

* Fast-forward the ``dev`` branch to the latest snapshot of the ``main`` branch: ::

    CALL iceberg.system.fast_forward('schema_name', 'table_name', 'dev', 'main');

* Given the branch named ``branch1`` does not exist yet, create a new branch named ``branch1``  and set it's current snapshot equal to the latest snapshot of the ``main`` branch: ::

    CALL iceberg.system.fast_forward('schema_name', 'table_name', 'branch1', 'main');

Statistics file cache invalidation procedure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Invalidate Statistics file cache: ::

    CALL <catalog-name>.system.invalidate_statistics_file_cache();

Manifest file cache invalidation procedure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Invalidate Manifest file cache: ::

    CALL <catalog-name>.system.invalidate_manifest_file_cache();

Set Table Property
^^^^^^^^^^^^^^^^^^

Set from the catalog using the ``set_table_property`` procedure on the catalog's ``system`` schema.

The following arguments are available:

===================== ========== =============== =======================================================================
Argument Name         Required   Type            Description
===================== ========== =============== =======================================================================
``schema``            Yes        string          Schema of the table to update

``table_name``        Yes        string          Name of the table to update

``key``               Yes        string          Name of the table property

``value``             Yes        string          Value for the table property
===================== ========== =============== =======================================================================

Examples:

* Set table property ``commit.retry.num-retries`` to ``10`` for a Iceberg table: ::

    CALL iceberg.system.set_table_property('schema_name', 'table_name', 'commit.retry.num-retries', '10');

Presto C++ Support
^^^^^^^^^^^^^^^^^^

All above procedures are supported in Presto C++.

SQL Support
-----------

============================== ============= ============ ============================================================================
SQL Operation                  Presto Java   Presto C++   Comments
============================== ============= ============ ============================================================================
``CREATE SCHEMA``              Yes           Yes

``CREATE TABLE``               Yes           Yes

``CREATE VIEW``                Yes           Yes

``INSERT INTO``                Yes           No

``CREATE TABLE AS SELECT``     Yes           No

``SELECT``                     Yes           Yes          Read is supported in Presto C++ including those with positional delete files.

``ALTER TABLE``                Yes           Yes

``ALTER VIEW``                 Yes           Yes

``TRUNCATE``                   Yes           Yes

``DELETE``                     Yes           No

``DROP TABLE``                 Yes           Yes

``DROP VIEW``                  Yes           Yes

``DROP SCHEMA``                Yes           Yes

``SHOW CREATE TABLE``          Yes           Yes

``SHOW COLUMNS``               Yes           Yes

``DESCRIBE``                   Yes           Yes

``UPDATE``                     Yes           No
============================== ============= ============ ============================================================================

The Iceberg connector supports querying and manipulating Iceberg tables and schemas
(databases). Here are some examples of the SQL operations supported by Presto:

CREATE SCHEMA
^^^^^^^^^^^^^

Create a new Iceberg schema named ``web`` that stores tables in an
S3 bucket named ``my-bucket``::

    CREATE SCHEMA iceberg.web
    WITH (location = 's3://my-bucket/')

CREATE TABLE
^^^^^^^^^^^^

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

Create an Iceberg table with Iceberg format version 2 and with commit_retries set to 5::

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
      format_version = '2',
      commit_retries = 5
    )

Partition Column Transform
~~~~~~~~~~~~~~~~~~~~~~~~~~
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

Types for partition transforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The list of supported types in Presto for each partition transform is as follows:

===================== =======================================================================
Transform Name        Source Types
===================== =======================================================================
``Identity``          ``boolean``, ``int``, ``bigint``, ``real``, ``double``, ``decimal``,
                      ``varchar``, ``varbinary``, ``date``, ``time``, ``timestamp``

``Bucket``            ``int``, ``bigint``, ``decimal``, ``varchar``, ``varbinary``, ``date``,
                      ``time``

``Truncate``          ``int``, ``bigint``, ``decimal``, ``varchar``, ``varbinary``

``Year``              ``date``, ``timestamp``

``Month``             ``date``, ``timestamp``

``Day``               ``date``, ``timestamp``

``Hour``              ``timestamp``
===================== =======================================================================

Presto C++ Support
~~~~~~~~~~~~~~~~~~

Reads from tables with partition column transforms is supported in Presto C++.

CREATE VIEW
^^^^^^^^^^^

The Iceberg connector supports creating views in Hive, Glue, REST, and Nessie catalogs.
To create a view named ``view_page_views`` for the ``iceberg.web.page_views`` table created in the `CREATE TABLE`_ example::

    CREATE VIEW iceberg.web.view_page_views AS SELECT user_id, country FROM iceberg.web.page_views;

.. note::

    The Iceberg REST catalog may not support view creation depending on the
    type of the backing catalog.

INSERT INTO
^^^^^^^^^^^

Insert data into the ``page_views`` table::

    INSERT INTO iceberg.web.page_views VALUES(TIMESTAMP '2023-08-12 03:04:05.321', 1, 'https://example.com', current_date, 'country');

CREATE TABLE AS SELECT
^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^

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

Use ``ARRAY[...]`` instead of a string to specify multiple partition transforms when adding a column. For example::

    ALTER TABLE iceberg.web.page_views ADD COLUMN location VARCHAR WITH (partitioning = ARRAY['truncate(2)', 'bucket(8)', 'identity']);

    ALTER TABLE iceberg.web.page_views ADD COLUMN dt date WITH (partitioning = ARRAY['year', 'bucket(16)', 'identity']);

Table properties can be modified for an Iceberg table using an ALTER TABLE SET PROPERTIES statement. Only `commit_retries` can be modified at present.
For example, to set `commit_retries` to 6 for the table `iceberg.web.page_views_v2`, use::

    ALTER TABLE iceberg.web.page_views_v2 SET PROPERTIES (commit_retries = 6);

ALTER VIEW
^^^^^^^^^^

Alter view operations to alter the name of an existing view to a new name is supported in the Iceberg connector.

.. code-block:: sql

    ALTER VIEW iceberg.web.page_views RENAME TO iceberg.web.page_new_views;

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
^^^^^^

The Iceberg connector can delete data from tables by using ``DELETE FROM``. For example, to delete from the table ``lineitem``::

     DELETE FROM lineitem;

     DELETE FROM lineitem WHERE linenumber = 1;

     DELETE FROM lineitem WHERE linenumber not in (1, 3, 5, 7) and linestatus in ('O', 'F');

.. note::

    Filtered columns only support comparison operators, such as EQUALS, LESS THAN, or LESS THAN EQUALS.

    Deletes must only occur on the latest snapshot.

    For V1 tables, the Iceberg connector can only delete data in one or more entire
    partitions. Columns in the filter must all be identity transformed partition
    columns of the target table.

DROP TABLE
^^^^^^^^^^

Drop the table ``page_views`` ::

    DROP TABLE iceberg.web.page_views

* Dropping an Iceberg table with Hive Metastore and Glue catalogs only removes metadata from metastore.
* Dropping an Iceberg table with Hadoop and Nessie catalogs removes all the data and metadata in the table.

DROP VIEW
^^^^^^^^^

Drop the view ``view_page_views``::

    DROP VIEW iceberg.web.view_page_views;

DROP SCHEMA
^^^^^^^^^^^

Drop the schema ``iceberg.web``::

    DROP SCHEMA iceberg.web

SHOW CREATE TABLE
^^^^^^^^^^^^^^^^^

Show the SQL statement that creates the specified Iceberg table by using ``SHOW CREATE TABLE``.

For example, ``SHOW CREATE TABLE`` from the partitioned Iceberg table ``customer``:

.. code-block:: sql

    SHOW CREATE TABLE customer;

.. code-block:: text

    CREATE TABLE iceberg.tpch_iceberg.customer (
        "custkey" bigint,
        "name" varchar,
        "address" varchar,
        "nationkey" bigint,
        "phone" varchar,
        "acctbal" double,
        "mktsegment" varchar,
        "comment" varchar
    )
    WITH (
        delete_mode = 'copy-on-write',
        format = 'PARQUET',
        format_version = '2',
        location = 's3a://tpch-iceberg/customer',
        partitioning = ARRAY['mktsegment']
    )
    (1 row)

``SHOW CREATE TABLE`` from the un-partitioned Iceberg table ``region``:

.. code-block:: sql

    SHOW CREATE TABLE region;

.. code-block:: text

    CREATE TABLE iceberg.tpch_iceberg.region (
        "regionkey" bigint,
        "name" varchar,
        "comment" varchar
    )
    WITH (
        delete_mode = 'copy-on-write',
        format = 'PARQUET',
        format_version = '2',
        location = 's3a://tpch-iceberg/region'
    )
    (1 row)

SHOW COLUMNS
^^^^^^^^^^^^

List the columns in table along with their data type and other attributes by using ``SHOW COLUMNS``.

For example, ``SHOW COLUMNS`` from the partitioned Iceberg table ``customer``:

.. code-block:: sql

    SHOW COLUMNS FROM customer;

.. code-block:: text

       Column   |  Type   |     Extra     | Comment
    ------------+---------+---------------+---------
     custkey    | bigint  |               |
     name       | varchar |               |
     address    | varchar |               |
     nationkey  | bigint  |               |
     phone      | varchar |               |
     acctbal    | double  |               |
     mktsegment | varchar | partition key |
     comment    | varchar |               |
     (8 rows)

``SHOW COLUMNS`` from the un-partitioned Iceberg table ``region``:

.. code-block:: sql

    SHOW COLUMNS FROM region;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     regionkey | bigint  |       |
     name      | varchar |       |
     comment   | varchar |       |
     (3 rows)

DESCRIBE
^^^^^^^^

List the columns in table along with their data type and other attributes by using ``DESCRIBE``.
``DESCRIBE`` is an alias for ``SHOW COLUMNS``.

For example, ``DESCRIBE`` from the partitioned Iceberg table ``customer``:

.. code-block:: sql

   DESCRIBE customer;

.. code-block:: text

       Column   |  Type   |     Extra     | Comment
    ------------+---------+---------------+---------
     custkey    | bigint  |               |
     name       | varchar |               |
     address    | varchar |               |
     nationkey  | bigint  |               |
     phone      | varchar |               |
     acctbal    | double  |               |
     mktsegment | varchar | partition key |
     comment    | varchar |               |
     (8 rows)

``DESCRIBE`` from the un-partitioned Iceberg table ``region``:

.. code-block:: sql

    DESCRIBE region;

.. code-block:: text

      Column   |  Type   | Extra | Comment
    -----------+---------+-------+---------
     regionkey | bigint  |       |
     name      | varchar |       |
     comment   | varchar |       |
     (3 rows)

UPDATE
^^^^^^

The Iceberg connector supports :doc:`../sql/update` operations on Iceberg
tables. Only some tables support updates. These tables must be at minimum format
version 2, and the ``write.update.mode`` must be set to `merge-on-read`.

.. code-block:: sql

    UPDATE region SET name = 'EU', comment = 'Europe' WHERE regionkey = 1;

.. code-block:: text

    UPDATE: 1 row

    Query 20250204_010341_00021_ymwi5, FINISHED, 2 nodes

The query returns an error if the table does not meet the requirements for
updates.

.. code-block:: text

    Query 20250204_010445_00022_ymwi5 failed: Iceberg table updates require at least format version 2 and update mode must be merge-on-read

Schema Evolution
----------------

Iceberg and Presto Iceberg connector support in-place table evolution, also known as
schema evolution, such as adding, dropping, and renaming columns. With schema
evolution, users can evolve a table schema with SQL after enabling the Presto
Iceberg connector.

Presto C++ Support
^^^^^^^^^^^^^^^^^^

Schema evolution is supported in Presto C++.

Parquet Writer Version
----------------------

Presto now supports Parquet writer versions V1 and V2 for the Iceberg catalog.
It can be toggled using the session property ``parquet_writer_version`` and the config property ``hive.parquet.writer.version``.
Valid values for these properties are ``PARQUET_1_0`` and ``PARQUET_2_0``. Default is ``PARQUET_1_0``.

Presto C++ Support
^^^^^^^^^^^^^^^^^^

 Reading Parquet data written with Parquet writer version V1 is supported in Presto C++.

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
It also supports time travel query using SYSTEM_VERSION (VERSION) and SYSTEM_TIME (TIMESTAMP) options.

Example Time Travel Queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similar to the example queries in `SCHEMA EVOLUTION`_, create an Iceberg
table named `ctas_nation` from the TPCH `nation` table:

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

.. code-block:: sql

    // snapshot ID for second record using BEFORE clause to retrieve previous state
    SELECT * FROM ctas_nation FOR SYSTEM_VERSION BEFORE 6891257133877048303;

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
    SELECT * FROM ctas_nation FOR TIMESTAMP AS OF TIMESTAMP '2023-10-17 13:29:46.822';

    // Same example using SYSTEM_TIME as an alias for TIMESTAMP
    SELECT * FROM ctas_nation FOR SYSTEM_TIME AS OF TIMESTAMP '2023-10-17 13:29:46.822 America/Los_Angeles';
    SELECT * FROM ctas_nation FOR SYSTEM_TIME AS OF TIMESTAMP '2023-10-17 13:29:46.822';

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
            20 | canada        |         2 | comment
    (2 rows)

.. note::

    Timestamp without timezone will be parsed and rendered in the session time zone. See `TIMESTAMP <https://prestodb.io/docs/current/language/types.html#timestamp>`_.

The option following FOR TIMESTAMP AS OF can accept any expression that returns a timestamp or timestamp with time zone value.
For example, `TIMESTAMP '2023-10-17 13:29:46.822 America/Los_Angeles'` and `TIMESTAMP '2023-10-17 13:29:46.822'` are both valid timestamps. The first specifies the timestamp within the timezone `America/Los_Angeles`. The second will use the timestamp based on the user's session timezone.
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

.. code-block:: sql

    // In following query, timestamp string is matching with second inserted record.
    // BEFORE clause returns first record which is less than timestamp of the second record.
    SELECT * FROM ctas_nation FOR TIMESTAMP BEFORE TIMESTAMP '2023-10-17 13:29:46.822 America/Los_Angeles';
    SELECT * FROM ctas_nation FOR TIMESTAMP BEFORE TIMESTAMP '2023-10-17 13:29:46.822';

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
    (1 row)

Querying branches and tags
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Iceberg supports branches and tags which are named references to snapshots.

Query Iceberg table by specifying the branch name:

.. code-block:: sql

    SELECT * FROM nation FOR SYSTEM_VERSION AS OF 'testBranch';

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
            20 | canada        |         2 | comment
            30 | mexico        |         3 | comment
    (3 rows)

Query Iceberg table by specifying the tag name:

.. code-block:: sql

    SELECT * FROM nation FOR SYSTEM_VERSION AS OF 'testTag';

.. code-block:: text

     nationkey |      name     | regionkey | comment
    -----------+---------------+-----------+---------
            10 | united states |         1 | comment
            20 | canada        |         2 | comment
    (3 rows)

Presto C++ Support
^^^^^^^^^^^^^^^^^^

Time travel queries are supported in Presto C++.

Type mapping
------------

PrestoDB and Iceberg have data types not supported by the other. When using Iceberg to read or write data, Presto changes
each Iceberg data type to the corresponding Presto data type, and from each Presto data type to the comparable Iceberg data type.
The following tables detail the specific type maps between PrestoDB and Iceberg.

Iceberg to PrestoDB type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Map of Iceberg types to the relevant PrestoDB types:

.. list-table:: Iceberg to PrestoDB type mapping
  :widths: 50, 50
  :header-rows: 1

  * - Iceberg type
    - PrestoDB type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INTEGER``
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
  * - ``BINARY``, ``FIXED``
    - ``VARBINARY``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME``
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
  * - ``TIMESTAMP TZ``
    - ``TIMESTAMP WITH TIME ZONE``
  * - ``UUID``
    - ``UUID``
  * - ``LIST``
    - ``ARRAY``
  * - ``MAP``
    - ``MAP``
  * - ``STRUCT``
    - ``ROW``


No other types are supported.

PrestoDB to Iceberg type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Map of PrestoDB types to the relevant Iceberg types:

.. list-table:: PrestoDB to Iceberg type mapping
  :widths: 50, 50
  :header-rows: 1

  * - PrestoDB type
    - Iceberg type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``BIGINT``
    - ``LONG``
  * - ``REAL``
    - ``FLOAT``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL``
    - ``DECIMAL``
  * - ``VARCHAR``
    - ``STRING``
  * - ``VARBINARY``
    - ``BINARY``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME``
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
  * - ``TIMESTAMP WITH TIME ZONE``
    - ``TIMESTAMP TZ``
  * - ``UUID``
    - ``UUID``
  * - ``ARRAY``
    - ``LIST``
  * - ``MAP``
    - ``MAP``
  * - ``ROW``
    - ``STRUCT``


No other types are supported.


Sorted Tables
-------------

The Iceberg connector supports the creation of sorted tables.
Data in the Iceberg table is sorted as each file is written.

Sorted Iceberg tables can decrease query execution time in many cases; but query times can also depend on the query shape and cluster configuration.
Sorting is particularly beneficial when the sorted columns have a
high cardinality and are used as a filter for selective reads.

Configure sort order with the ``sorted_by`` table property to specify an array of
one or more columns to use for sorting.
The following example creates the table with the ``sorted_by`` property, and sorts the file based
on the field ``join_date``. The default sort direction is ASC, with null values ordered as NULLS FIRST.

.. code-block:: text

    CREATE TABLE emp.employees.employee (
        emp_id BIGINT,
        emp_name VARCHAR,
        join_date DATE,
        country VARCHAR)
    WITH (
        sorted_by = ARRAY['join_date']
    )

Explicitly configure sort directions or null ordering using the following example::

    CREATE TABLE emp.employees.employee (
        emp_id BIGINT,
        emp_name VARCHAR,
        join_date DATE,
        country VARCHAR)
    WITH (
        sorted_by = ARRAY['join_date DESC NULLS FIRST', 'emp_id ASC NULLS LAST']
    )

Sorting can be combined with partitioning on the same column. For example::

    CREATE TABLE emp.employees.employee (
        emp_id BIGINT,
        emp_name VARCHAR,
        join_date DATE,
        country VARCHAR)
    WITH (
        partitioning = ARRAY['month(join_date)'],
        sorted_by = ARRAY['join_date']
    )

Sort order does not support transforms. The following transforms are not supported:

.. code-block:: text

    bucket(n, column)
    truncate(column, n)
    year(column)
    month(column)
    day(column)
    hour(column)

For example::

    CREATE TABLE emp.employees.employee (
        emp_id BIGINT,
        emp_name VARCHAR,
        join_date DATE,
        country VARCHAR)
    WITH (
        sorted_by = ARRAY['month(join_date)']
    )

If a user creates a table externally with non-identity sort columns and then inserts data, the following warning message will be shown.
``Iceberg table sort order has sort fields of <X>, <Y>, ... which are not currently supported by Presto``