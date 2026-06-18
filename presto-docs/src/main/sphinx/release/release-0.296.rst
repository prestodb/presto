=============
Release 0.296
=============

**Breaking Changes**
====================
* Replace default Iceberg compression codec from ``GZIP`` to ``ZSTD``. Existing tables are unaffected, but new tables will use ZSTD compression by default if ``iceberg.compression-codec`` is not set. `#26399 <https://github.com/prestodb/presto/pull/26399>`_
* Replace the ``String serializedCommitOutput`` argument with ``Optional<Object> commitOutput`` in the ``com.facebook.presto.spi.eventlistener.QueryInputMetadata`` and ``com.facebook.presto.spi.eventlistener.QueryOutputMetadata`` constructors. `#26331 <https://github.com/prestodb/presto/pull/26331>`_ 

**Highlights**
==============
* Add support for :doc:`Materialized Views </admin/materialized-views>`. `#26492 <https://github.com/prestodb/presto/pull/26492>`_
* Add support for the :doc:`/sql/merge` command in the Presto engine. `#26278 <https://github.com/prestodb/presto/pull/26278>`_
* Add support for distributed execution of procedures. `#26373 <https://github.com/prestodb/presto/pull/26373>`_
* Add HTTP/2 support for internal cluster communication with data compression. `#26439 <https://github.com/prestodb/presto/pull/26439>`_ `#26381 <https://github.com/prestodb/presto/pull/26381>`_
* Add support for basic insertion to Iceberg tables on C++ worker clusters. `#26338 <https://github.com/prestodb/presto/pull/26338>`_

**Details**
===========

General Changes
_______________
* Improve sort-merge join performance when one side of the join input is already sorted. `#26361 <https://github.com/prestodb/presto/pull/26361>`_
* Improve query performance for semi joins (used in ``IN`` and ``EXISTS`` subqueries) when join keys contain many null values. `#26251 <https://github.com/prestodb/presto/pull/26251>`_
* Improve connector optimizer to support queries involving multiple connectors. `#26246 <https://github.com/prestodb/presto/pull/26246>`_
* Add :func:`array_transpose` to return a transpose of an array. `#26470 <https://github.com/prestodb/presto/pull/26470>`_
* Add :ref:`admin/properties:\`\`cluster-tag\`\`` configuration property to assign a custom identifier to the cluster, which is displayed in the Web UI. `#26485 <https://github.com/prestodb/presto/pull/26485>`_
* Add a session property ``query_types_enabled_for_history_based_optimization`` to specify query types which will use HBO. See :doc:`/optimizer/history-based-optimization`. `#26183 <https://github.com/prestodb/presto/pull/26183>`_
* Add data compression support for HTTP/2 protocol. `#26381 <https://github.com/prestodb/presto/pull/26381>`_ `#26382 <https://github.com/prestodb/presto/pull/26382>`_
* Add :ref:`admin/properties:\`\`max-prefixes-count\`\`` configuration property to limit the number of catalog/schema/table scope prefixes generated when querying ``information_schema``, which can improve metadata query performance. `#25550 <https://github.com/prestodb/presto/pull/25550>`_
* Add detailed latency and failure count metrics for the system access control plugin. `#26116 <https://github.com/prestodb/presto/pull/26116>`_
* Add experimental support for sorted exchanges to improve sort-merge join performance. When enabled with the ``sorted_exchange_enabled`` session property or the ``optimizer.experimental.sorted-exchange-enabled`` configuration property, this optimization eliminates redundant sorting steps and reduces memory usage for distributed queries with sort-merge joins. This feature is disabled by default. `#26403 <https://github.com/prestodb/presto/pull/26403>`_
* Add HTTP/2 support for internal cluster communication. `#26439 <https://github.com/prestodb/presto/pull/26439>`_
* Add ``native_use_velox_geospatial_join`` session property to enable an optimized implementation for geospatial joins in native execution. This feature is enabled by default. `#26057 <https://github.com/prestodb/presto/pull/26057>`_
* Add support for the :doc:`/sql/merge` command in the Presto engine. `#26278 <https://github.com/prestodb/presto/pull/26278>`_
* Add ``enable-java-cluster-query-retry`` configuration property in ``router-scheduler.properties`` to retry queries on ``router-java-url`` when they fail on ``router-native-url``. `#25720 <https://github.com/prestodb/presto/pull/25720>`_
* Add :func:`array_to_map_int_keys` function. `#26681 <https://github.com/prestodb/presto/pull/26681>`_
* Add :func:`map_int_keys_to_array` function. `#26681 <https://github.com/prestodb/presto/pull/26681>`_
* Add :func:`t_cdf` and :func:`inverse_t_cdf` functions for Student's t-distribution calculations. `#26363 <https://github.com/prestodb/presto/pull/26363>`_
* Add support for distributed execution of procedures. `#26373 <https://github.com/prestodb/presto/pull/26373>`_
* Add support for :doc:`Materialized Views </admin/materialized-views>`. `#26492 <https://github.com/prestodb/presto/pull/26492>`_
* Update encoding of refresh token secret key from HMAC to AES. `#26487 <https://github.com/prestodb/presto/pull/26487>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix query errors when using mixed case column names with the Iceberg connector. `#26163 <https://github.com/prestodb/presto/pull/26163>`_
* Add ``native_max_partial_aggregation_memory`` session property to control memory limits for partial aggregation. `#26389 <https://github.com/prestodb/presto/pull/26389>`_
* Add :ref:`presto_cpp/properties-session:\`\`native_max_split_preload_per_driver\`\`` session property to configure the maximum number of splits to preload per driver. `#26591 <https://github.com/prestodb/presto/pull/26591>`_
* Add support for basic insertion to Iceberg tables. `#26338 <https://github.com/prestodb/presto/pull/26338>`_
* Add support for custom schemas in native sidecar function registry. `#26236 <https://github.com/prestodb/presto/pull/26236>`_
* Add support for the TPC-DS connector. `#24751 <https://github.com/prestodb/presto/pull/24751>`_
* Add support for REST API for remote functions. `#23568 <https://github.com/prestodb/presto/pull/23568>`_

Security Changes
________________
* Upgrade dagre-d3-es to 7.0.13 in response to `CVE-2025-57347 <https://github.com/advisories/GHSA-cc8p-78qf-8p7q>`_. `#26422 <https://github.com/prestodb/presto/pull/26422>`_
* Upgrade Netty to 4.1.128.Final to address `CVE-2025-59419 <https://github.com/advisories/GHSA-jq43-27x9-3v86>`_. `#26349 <https://github.com/prestodb/presto/pull/26349>`_
* Upgrade at.favre.lib:bcrypt version to 0.10.2 in response to `CVE-2020-15250 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-15250>`_. `#26463 <https://github.com/prestodb/presto/pull/26463>`_
* Upgrade calcite-core to 1.41.0 in response to `CVE-2025-48924 <https://github.com/advisories/GHSA-j288-q9x7-2f5v>`_. `#26248 <https://github.com/prestodb/presto/pull/26248>`_
* Upgrade io.grpc:grpc-netty-shaded from 1.70.0 to 1.75.0 to address `CVE-2025-55163 <https://nvd.nist.gov/vuln/detail/CVE-2025-55163>`_. `#26273 <https://github.com/prestodb/presto/pull/26273>`_
* Upgrade mssql-jdbc to 12.10.2.jre8 to address `CVE-2025-59250 <https://github.com/advisories/GHSA-m494-w24q-6f7w>`_. `#26534 <https://github.com/prestodb/presto/pull/26534>`_
* Upgrade org.apache.calcite to 1.38.0 in response to `CVE-2022-36944 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-36944>`_. `#26400 <https://github.com/prestodb/presto/pull/26400>`_
* Upgrade zookeeper to 3.9.4 to address `CVE-2025-58457 <https://github.com/advisories/GHSA-2hmj-97jw-28jh>`_. `#26180 <https://github.com/prestodb/presto/pull/26180>`_.

Arrow Flight Connector Changes
______________________________
* Add support for case-sensitive identifiers in Arrow. To enable, set ``case-sensitive-name-matching=true``. `#26176 <https://github.com/prestodb/presto/pull/26176>`_

Cassandra Connector Changes
___________________________
* Add support for case-sensitive identifiers in Cassandra. To enable, set ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#25690 <https://github.com/prestodb/presto/pull/25690>`_

Delta Connector Changes
_______________________
* Fix problem reading Delta Lake tables with spaces in location or partition values. `#26397 <https://github.com/prestodb/presto/pull/26397>`_

Druid Connector Changes
_______________________
* Fix Druid connector to use strict application/json content type. `#26200 <https://github.com/prestodb/presto/pull/26200>`_
* Add TLS support. `#26027 <https://github.com/prestodb/presto/pull/26027>`_
* Add support for case-sensitive identifiers in Druid. To enable, set ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#26038 <https://github.com/prestodb/presto/pull/26038>`_

Elasticsearch Connector Changes
_______________________________
* Add support for case-sensitive identifiers in Elasticsearch. To enable, set ``case-sensitive-name-matching=true`` in the catalog configuration. `#26352 <https://github.com/prestodb/presto/pull/26352>`_

Hive Connector Changes
______________________
* Add support for ``LZ4`` compression codec in ORC format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_
* Add support for ``ZSTD`` compression codec in Parquet format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_

Iceberg Connector Changes
_________________________
* Fix Bearer authentication with Nessie catalog. `#26512 <https://github.com/prestodb/presto/pull/26512>`_
* Fix ``SHOW STATS`` for Timestamp with Timezone columns. `#26305 <https://github.com/prestodb/presto/pull/26305>`_
* Fix reading decimal partition values when using native execution. `#26240 <https://github.com/prestodb/presto/pull/26240>`_
* Fix handling of ``TIME`` columns in Iceberg tables. `#26523 <https://github.com/prestodb/presto/pull/26523>`_
* Add support for ``LZ4`` compression codec in ORC format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_
* Add support for ``ZSTD`` compression codec in Parquet format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_
* Add support for ``engine.hive.lock-enabled`` property when creating or altering Iceberg tables. `#26234 <https://github.com/prestodb/presto/pull/26234>`_
* Add support to access Nessie with S3 using Iceberg REST catalog. `#26610 <https://github.com/prestodb/presto/pull/26610>`_
* Add support for :ref:`Materialized Views <connector/iceberg:Materialized Views>`. `#26603 <https://github.com/prestodb/presto/pull/26603>`_
* Replace default Iceberg compression codec from ``GZIP`` to ``ZSTD``. Existing tables are unaffected, but new tables will use ZSTD compression by default if ``iceberg.compression-codec`` is not set. `#26399 <https://github.com/prestodb/presto/pull/26399>`_

Kafka Connector Changes
_______________________
* Add support for case-sensitive identifiers in Kafka. To enable, set ``case-sensitive-name-matching=true`` in the catalog configuration. `#26023 <https://github.com/prestodb/presto/pull/26023>`_

Memory Connector Changes
________________________
* Add support for :doc:`Materialized Views </admin/materialized-views>`. `#26405 <https://github.com/prestodb/presto/pull/26405>`_

MongoDB Connector Changes
_________________________
* Add TLS/SSL support with automatic JKS and PEM certificate format detection. Configure using ``mongodb.tls.enabled``, ``mongodb.tls.keystore-path``, ``mongodb.tls.keystore-password``, ``mongodb.tls.truststore-path``, and ``mongodb.tls.truststore-password`` properties. `#25374 <https://github.com/prestodb/presto/pull/25374>`_
* Upgrade MongoDB Java Driver to 3.12.14. `#25374 <https://github.com/prestodb/presto/pull/25374>`_

MySQL Connector Changes
_______________________
* Fix timestamp handling when ``legacy_timestamp`` is disabled. Timestamp values are now correctly stored and retrieved as wall-clock times without timezone conversion. Previously, values were incorrectly converted using the JVM timezone, causing data corruption. `#26449 <https://github.com/prestodb/presto/pull/26449>`_

Oracle Connector Changes
________________________
* Add support for fetching table statistics from Oracle source tables. `#26120 <https://github.com/prestodb/presto/pull/26120>`_
* Add support for reading Oracle ``BLOB`` columns as ``VARBINARY``. `#25354 <https://github.com/prestodb/presto/pull/25354>`_

Pinot Connector Changes
_______________________
* Add support for case-sensitive identifiers in Pinot. To enable, set ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#26239 <https://github.com/prestodb/presto/pull/26239>`_
* Upgrade Pinot version to 1.3.0. `#25785 <https://github.com/prestodb/presto/pull/25785>`_

PostgreSQL Connector Changes
____________________________
* Fix timestamp handling when ``legacy_timestamp`` is disabled. Timestamp values are now correctly stored and retrieved as wall-clock times without timezone conversion. Previously, values were incorrectly converted using the JVM timezone, causing data corruption. `#26449 <https://github.com/prestodb/presto/pull/26449>`_

Redis Connector Changes
_______________________
* Add support for case-sensitive identifiers in Redis. To enable, set ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#26078 <https://github.com/prestodb/presto/pull/26078>`_

SingleStore Connector Changes
_____________________________
* Fix string type mapping to support ``VARCHAR(len)`` where len <= 21844. `#25476 <https://github.com/prestodb/presto/pull/25476>`_

SPI Changes
___________
* Add ``getCommitOutputForRead()`` and ``getCommitOutputForWrite()`` methods to ``ConnectorCommitHandle``, and deprecates the existing ``getSerializedCommitOutputForRead()`` and ``getSerializedCommitOutputForWrite()`` methods. `#26331 <https://github.com/prestodb/presto/pull/26331>`_
* Add new metric ``getTotalScheduledTime()`` to QueryStatistics SPI. This value is the sum of wall time across all threads of all tasks/stages of a query that were actually scheduled for execution. `#26279 <https://github.com/prestodb/presto/pull/26279>`_
* Replace the ``String serializedCommitOutput`` argument with ``Optional<Object> commitOutput`` in the ``com.facebook.presto.spi.eventlistener.QueryInputMetadata`` and ``com.facebook.presto.spi.eventlistener.QueryOutputMetadata`` constructors. `#26331 <https://github.com/prestodb/presto/pull/26331>`_

**Credits**
===========

Aditi Pandit, Adrian Carpente (Denodo), Alex Austin Chettiar, Amit Dutta, Anant Aneja, Andrew X, Andrii Rosa, Artem Selishchev, Auden Woolfson, Bryan Cutler, Chris Matzenbach, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Dilli-Babu-Godari, Dong Wang, Elbin Pallimalil, Gary Helmling, Ge Gao, Han Yan, HeidiHan0000, Jalpreet Singh Nanda, James Gill, Jay Feldblum, Jiaqi Zhang, Joe Abraham, Joe O'Hallaron, Karthikeyan, Ke, Kevin Tang, Li Zhou, LingBin, Maria Basmanova, Mariam AlMesfer, Namya Sehgal, Natasha Sehgal, Nidhin Varghese, Nikhil Collooru, Nivin C S, PRASHANT GOLASH, Pedro Pedreira, Ping Liu, Pramod Satya, Prashant Sharma, Pratyaksh Sharma, Rebecca Schlussel, Reetika Agrawal, RindsSchei225e, Sayari Mukherjee, Sergey Pershin, Shahad Shamsan, Shahim Sharafudeen, Shang Ma, Shrinidhi Joshi, Sreeni Viswanadha, Steve Burnett, Tal Galili, Timothy Meehan, Weitao Wan, XiaoDu, Xiaoxuan, Xin Zhang, Yihong Wang, Yolande Yan, Zac, Zoltán Arnold Nagy, abhinavmuk04, adheer-araokar, bibith4, dependabot[bot], ericyuliu, feilong-liu, inf, jkhaliqi, maniloya, mohsaka, nishithakbhaskaran, rkurniawati, shanhao-203, singcha, sumi-mathew, tanjialiang, vhsu14
