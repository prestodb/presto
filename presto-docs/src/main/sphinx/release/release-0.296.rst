=============
Release 0.296
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix Druid connector to use strict application/json content type. `#26200 <https://github.com/prestodb/presto/pull/26200>`_
* Improve  ``MergeJoinForSortedInputOptimizer`` to do sort merge join when one side of the input is sorted. `#26361 <https://github.com/prestodb/presto/pull/26361>`_
* Add :func:`array_transpose` to return a transpose of an array. `#26470 <https://github.com/prestodb/presto/pull/26470>`_
* Add a configurable `clusterTag` config flag, which is returned from the `/v1/cluster` endpoints and displayed in the UI. `#26485 <https://github.com/prestodb/presto/pull/26485>`_
* Add a new optimizer which performs null skew mitigation for applicable semi joins. `#26251 <https://github.com/prestodb/presto/pull/26251>`_
* Add a session property `query_types_enabled_for_history_based_optimization` to specify query types which will use HBO. `#26183 <https://github.com/prestodb/presto/pull/26183>`_
* Add compression support for http2 protocol on cpp worker. `#26382 <https://github.com/prestodb/presto/pull/26382>`_
* Add configuration property ``max-prefixes-count``. `#25550 <https://github.com/prestodb/presto/pull/25550>`_
* Add data compression support for http2 protocol. `#26381 <https://github.com/prestodb/presto/pull/26381>`_
* Add detailed latency and failure count metrics for the system access control plugin. `#26116 <https://github.com/prestodb/presto/pull/26116>`_
* Add experimental support for sorted exchanges to improve sort-merge join performance. When enabled via the `sorted_exchange_enabled` session property or `optimizer.experimental.sorted-exchange-enabled` configuration property, the query planner will push sort operations into exchange nodes, eliminating redundant sorting steps and reducing memory usage for distributed queries with sort-merge joins. This feature is disabled by default. `#26403 <https://github.com/prestodb/presto/pull/26403>`_
* Add http2 support for HTTP client. `#26439 <https://github.com/prestodb/presto/pull/26439>`_
* Add new feature to connector optimizer so that it can work for sub plans with multiple connectors. `#26246 <https://github.com/prestodb/presto/pull/26246>`_
* Add property ```native_use_velox_geospatial_join ```  which will use the new optimized velox::SpatialJoinNode for geo-spatial joins, but flip to an basic velox::NestedLoopJoinNode for cross-checking if false. Enable the ```native_use_velox_geospatial_join ``` flag as well. `#26057 <https://github.com/prestodb/presto/pull/26057>`_
* Add support for scaling the maximum number of splits to preload per driver. Native execution only. See :ref:`presto_cpp/properties-session:\`\`native_max_split_preload_per_driver\`\``. `#26591 <https://github.com/prestodb/presto/pull/26591>`_
* Add support for the MERGE command in the Presto engine. `#26278 <https://github.com/prestodb/presto/pull/26278>`_
* Add test suite for mixed-case support in PostgreSQL. `#26332 <https://github.com/prestodb/presto/pull/26332>`_
* Added `enable-java-cluster-query-retry` configuration in `router-scheduler.properties` to retry queries on `router-java-url` when they fail on `router-native-url`. `#25720 <https://github.com/prestodb/presto/pull/25720>`_
* Added array_to_map_int_keys function. `#26681 <https://github.com/prestodb/presto/pull/26681>`_
* Added map_int_keys_to_array. `#26681 <https://github.com/prestodb/presto/pull/26681>`_
* Replace the java standard base64 encoder with BaseEncoding from Guava. `#26557 <https://github.com/prestodb/presto/pull/26557>`_
* Change encoding of refresh token secret key to AES. `#26487 <https://github.com/prestodb/presto/pull/26487>`_
* Upgrade dagre-d3-es to 7.0.13 in response to `CVE-2025-57347 <https://github.com/advisories/GHSA-cc8p-78qf-8p7q>`_. `#26422 <https://github.com/prestodb/presto/pull/26422>`_
* Upgrade the procedure architecture to support distributed execution of procedures. `#26373 <https://github.com/prestodb/presto/pull/26373>`_

Prestissimo (native Execution) Changes
______________________________________
* Fix Prestissimo Iceberg connector mixed case column name query error. `#26163 <https://github.com/prestodb/presto/pull/26163>`_
* Add back session property native_max_partial_aggregation_memory for Presto C++. `#26389 <https://github.com/prestodb/presto/pull/26389>`_
* Add support for basic insertion to Iceberg tables. `#26338 <https://github.com/prestodb/presto/pull/26338>`_
* Add support for custom schemas in native sidecar function registry. `#26236 <https://github.com/prestodb/presto/pull/26236>`_
* Support TPC-DS connector in Presto C++. `#24751 <https://github.com/prestodb/presto/pull/24751>`_

Security Changes
________________
* Upgrade Netty to 4.1.128.Final to address `CVE-2025-59419 <https://github.com/advisories/GHSA-jq43-27x9-3v86>`_. `#26349 <https://github.com/prestodb/presto/pull/26349>`_
* Upgrade RoaringBitmap to 1.3.0. `#26238 <https://github.com/prestodb/presto/pull/26238>`_
* Upgrade at.favre.lib:bcrypt version to 0.10.2 in response to `CVE-2020-15250 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-15250>`_. `#26463 <https://github.com/prestodb/presto/pull/26463>`_
* Upgrade calcite-core to 1.41.0 in response to `CVE-2025-48924 <https://github.com/advisories/GHSA-j288-q9x7-2f5v>`_. `#26248 <https://github.com/prestodb/presto/pull/26248>`_
* Upgrade com.google.api:google-api-client version to 2.8.0 in response to the use of an outdated version. `#26063 <https://github.com/prestodb/presto/pull/26063>`_
* Upgrade io.dropwizard.metrics:metrics-core versio to 4.2.33 in response to the use of an outdated version. `#26199 <https://github.com/prestodb/presto/pull/26199>`_
* Upgrade io.grpc:grpc-netty-shaded from 1.70.0 to 1.75.0 to address `CVE-2025-55163 <https://nvd.nist.gov/vuln/detail/CVE-2025-55163>`_. `#26273 <https://github.com/prestodb/presto/pull/26273>`_
* Upgrade mssql-jdbc to 12.10.2.jre8 to address `CVE-2025-59250 <https://github.com/advisories/GHSA-m494-w24q-6f7w>`_. `#26534 <https://github.com/prestodb/presto/pull/26534>`_
* Upgrade org.anarres.lzo:lzo-hadoop version from 1.0.5 to 1.0.6. `#26294 <https://github.com/prestodb/presto/pull/26294>`_
* Upgrade org.apache.calcite to 1.38.0 in response to `CVE-2022-36944<https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-36944>`_. `#26400 <https://github.com/prestodb/presto/pull/26400>`_
* Upgrade sourceforge to version 0.9.16. `#26247 <https://github.com/prestodb/presto/pull/26247>`_
* Upgrade threetenbp  to 1.7.2 in response to the use of an outdated version. `#26132 <https://github.com/prestodb/presto/pull/26132>`_
* Upgrade zookeeper to 3.9.4 to address 'CVE-2025-58457  <https://github.com/advisories/GHSA-2hmj-97jw-28jh>' _. `#26180 <https://github.com/prestodb/presto/pull/26180>`_

Arrow Flight Connector Changes
______________________________
* Add support for case-sensitive identifiers in Arrow. To enable, set ``case-sensitive-name-matching=true``. `#26176 <https://github.com/prestodb/presto/pull/26176>`_

Cassandra Connector Changes
___________________________
* Add support for case-sensitive identifiers in Cassandra. It can be enabled by setting ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#25690 <https://github.com/prestodb/presto/pull/25690>`_

Delta Connector Changes
_______________________
* Fix problem reading Delta Lake tables with spaces in location or partition values. `#26397 <https://github.com/prestodb/presto/pull/26397>`_

Druid Connector Changes
_______________________
* Add TLS support. `#26027 <https://github.com/prestodb/presto/pull/26027>`_
* Add support for case-sensitive identifiers in Druid. It can be enabled by setting ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#26038 <https://github.com/prestodb/presto/pull/26038>`_

Elasticsearch Connector Changes
_______________________________
* Add mixed case support for Elasticsearch connector. `#26352 <https://github.com/prestodb/presto/pull/26352>`_

Hive Connector Changes
______________________
* Add support for ``LZ4`` compression codec in ORC format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_
* Add support for`` ZSTD`` compression codec in Parquet format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_

Iceberg Connector Changes
_________________________
* Fix Bearer authentication with Nessie catalog. `#26512 <https://github.com/prestodb/presto/pull/26512>`_
* Fix ``SHOW STATS`` for Timestamp with Timezone columns. `#26305 <https://github.com/prestodb/presto/pull/26305>`_
* Add more type conversion for decimal partition value. `#26240 <https://github.com/prestodb/presto/pull/26240>`_
* Add support for Materialized Views. `#26603 <https://github.com/prestodb/presto/pull/26603>`_
* Add support for ``LZ4`` compression codec in ORC format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_
* Add support for ``ZSTD`` compression codec in Parquet format. `#26346 <https://github.com/prestodb/presto/pull/26346>`_
* Add support for ``engine.hive.lock-enabled`` property when creating or altering iceberg tables. `#26234 <https://github.com/prestodb/presto/pull/26234>`_
* Add support to access Nessie with S3 using Iceberg REST catalog. `#26610 <https://github.com/prestodb/presto/pull/26610>`_
* Replace default iceberg compression codec from GZIP to ZSTD. `#26399 <https://github.com/prestodb/presto/pull/26399>`_
* Update iceberg time column catalog type from string to long. `#26523 <https://github.com/prestodb/presto/pull/26523>`_

MongoDB Connector Changes
_________________________
* Add TLS/SSL support with automatic JKS and PEM certificate format detection. Configure using ``mongodb.tls.enabled``, ``mongodb.tls.keystore-path``, ``mongodb.tls.keystore-password``, ``mongodb.tls.truststore-path``, and ``mongodb.tls.truststore-password`` properties. `#25374 <https://github.com/prestodb/presto/pull/25374>`_
* Upgrade MongoDB Java Driver to 3.12.14. `#25374 <https://github.com/prestodb/presto/pull/25374>`_

MySQL Connector Changes
_______________________
* Fix timestamp handling when ``legacy_timestamp`` is disabled. Timestamp values are now correctly stored and retrieved as wall-clock times without timezone conversion. Previously, values were incorrectly converted using the JVM timezone, causing data corruption. `#26449 <https://github.com/prestodb/presto/pull/26449>`_

Oracle Connector Changes
________________________
* Add : Implementation to fetch table stats from source tables. `#26120 <https://github.com/prestodb/presto/pull/26120>`_
* Added type mappings to internally convert BLOB types to VARBINARY, enabling read access without introducing first-class BLOB/CLOB support to Presto's type system. `#25354 <https://github.com/prestodb/presto/pull/25354>`_

Pinot Connector Changes
_______________________
* Add support for case-sensitive identifiers in Pinot. It can be enabled by setting ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#26239 <https://github.com/prestodb/presto/pull/26239>`_
* Upgrade Pinot version to 1.3.0. `#25785 <https://github.com/prestodb/presto/pull/25785>`_

Postgresql Connector Changes
____________________________
* Fix timestamp handling when ``legacy_timestamp`` is disabled. Timestamp values are now correctly stored and retrieved as wall-clock times without timezone conversion. Previously, values were incorrectly converted using the JVM timezone, causing data corruption. `#26449 <https://github.com/prestodb/presto/pull/26449>`_

Redis Connector Changes
_______________________
* Add support for case-sensitive identifiers in Redis. It can be enabled by setting ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#26078 <https://github.com/prestodb/presto/pull/26078>`_

SingleStore Connector Changes
_____________________________
* Improved string type mapping, now supports varchar(len) where len <= 21844. `#25476 <https://github.com/prestodb/presto/pull/25476>`_

SPI Changes
___________
* Adds ``getCommitOutputForRead()`` and ``getCommitOutputForWrite()`` methods to ``ConnectorCommitHandle``, and deprecates the existing ``getSerializedCommitOutputForRead()`` and ``getSerializedCommitOutputForWrite()`` methods. `#26331 <https://github.com/prestodb/presto/pull/26331>`_
* Adds new metric getTotalScheduledTime() to QueryStatistics SPI. This value is the sum of wall time across all threads of all tasks/stages of a query that were actually scheduled for execution. `#26279 <https://github.com/prestodb/presto/pull/26279>`_
* Replaces the ``String serializedCommitOutput`` argument with ``Optional<Object> commitOutput`` in the ``com.facebook.presto.spi.eventlistener.QueryInputMetadata`` and ``com.facebook.presto.spi.eventlistener.QueryOutputMetadata`` constructors. `#26331 <https://github.com/prestodb/presto/pull/26331>`_

**Credits**
===========

Aditi Pandit, Adrian Carpente (Denodo), Alex Austin Chettiar, Amit Dutta, Anant Aneja, Andrew X, Andrii Rosa, Artem Selishchev, Auden Woolfson, Bryan Cutler, Chris Matzenbach, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Dilli-Babu-Godari, Dong Wang, Elbin Pallimalil, Gary Helmling, Ge Gao, Han Yan, HeidiHan0000, Jalpreet Singh Nanda, James Gill, Jay Feldblum, Jiaqi Zhang, Joe Abraham, Joe O'Hallaron, Karthikeyan, Ke, Kevin Tang, Li Zhou, LingBin, Maria Basmanova, Mariam AlMesfer, Namya Sehgal, Natasha Sehgal, Nidhin Varghese, Nikhil Collooru, Nivin C S, PRASHANT GOLASH, Pedro Pedreira, Ping Liu, Pramod Satya, Prashant Sharma, Pratyaksh Sharma, Rebecca Schlussel, Reetika Agrawal, RindsSchei225e, Sayari Mukherjee, Sergey Pershin, Shahad Shamsan, Shahim Sharafudeen, Shang Ma, Shrinidhi Joshi, Sreeni Viswanadha, Steve Burnett, Tal Galili, Timothy Meehan, Weitao Wan, XiaoDu, Xiaoxuan, Xin Zhang, Yihong Wang, Yolande Yan, Zac, Zoltán Arnold Nagy, abhinavmuk04, adheer-araokar, bibith4, dependabot[bot], ericyuliu, feilong-liu, inf, jkhaliqi, maniloya, mohsaka, nishithakbhaskaran, rkurniawati, shanhao-203, singcha, sumi-mathew, tanjialiang, vhsu14
