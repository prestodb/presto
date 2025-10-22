=============
Release 0.295
=============

**Breaking Changes**
====================
* Add all inline SQL invoked functions into a new plugin ``presto-sql-invoked-functions-plugin``. The following functions were moved: ``replace_first``, ``trail``, ``key_sampling_percent``, ``no_values_match``, ``no_keys_match``, ``any_values_match``, ``any_keys_match``, ``all_keys_match``, ``map_remove_null_values``, ``map_top_n_values``, ``map_top_n_keys``, ``map_top_n``, ``map_key_exists``, ``map_keys_by_top_n_values``, ``map_normalize``, ``array_top_n``, ``remove_nulls``, ``array_sort_desc``, ``array_min_by``, ``array_max_by``, ``array_least_frequent``, ``array_has_duplicates``, ``array_duplicates``, ``array_frequency``, ``array_split_into_chunks``, ``array_average``, ``array_intersect``. See `#26025 <https://github.com/prestodb/presto/pull/26025>`_ and `presto-sql-helpers/README.md <https://github.com/prestodb/presto/blob/master/presto-sql-helpers/README.md>`_. `#25818 <https://github.com/prestodb/presto/pull/25818>`_
* Upgrade Presto to require Java 17. The Presto client and Presto-on-Spark remain Java 8-compatible. Presto now requires a Java 17 VM to run both coordinator and workers. `#24866 <https://github.com/prestodb/presto/pull/24866>`_

**Highlights**
==============
* Add OAuth2 support for WebUI and JDBC Presto Client. `#24443 <https://github.com/prestodb/presto/pull/24443>`_
* Add a new configuration property ``query.max-queued-time`` to specify maximum queued time for a query before killing it. This can be overridden by the ``query_max_queued_time`` session property. `#25589 <https://github.com/prestodb/presto/pull/25589>`_
* Add spatial join support for native execution. `#25823 <https://github.com/prestodb/presto/pull/25823>`_
* Add support for `mutual TLS (mTLS) authentication <https://prestodb.io/docs/current/connector/base-arrow-flight.html#mutual-tls-mtls-support>`_ in the Arrow Flight connector. `#25388 <https://github.com/prestodb/presto/pull/25388>`_
* Add support for `GEOMETRY <https://prestodb.io/docs/current/language/types.html#geospatial>`_ type in the PostgreSQL connector. `#25240 <https://github.com/prestodb/presto/pull/25240>`_
* Add documentation about the Presto :doc:`/develop/release-process` and :doc:`/admin/version-support`. `#25742 <https://github.com/prestodb/presto/pull/25742>`_
* Add support for configuring http2 server on worker for communication between coordinator and workers. To enable, set the configuration property ``http-server.http2.enabled`` to ``true``. `#25708 <https://github.com/prestodb/presto/pull/25708>`_
* Add support for cross-cluster query retry. Failed queries can be automatically retried on a backup cluster by providing the retry URL and expiration time as query parameters. `#25625 <https://github.com/prestodb/presto/pull/25625>`_

**Details**
===========

General Changes
_______________
* Fix `localtime` and `current_time` issues in legacy timestamp semantics. `#25985 <https://github.com/prestodb/presto/pull/25985>`_
* Fix a bug where ``map(varchar, json)`` does not canonicalize values. See :doc:`/functions/map`. `#24232 <https://github.com/prestodb/presto/pull/24232>`_
* Fix add exchange and add local exchange optimizers to simplify query plans with unique columns. `#25882 <https://github.com/prestodb/presto/pull/25882>`_
* Fix failure when preparing statements or creating views that contain a quoted reserved word as a table name. `#25528 <https://github.com/prestodb/presto/pull/25528>`_
* Fix weak cipher mode usage during spilling by switching to a stronger algorithm. `#25603 <https://github.com/prestodb/presto/pull/25603>`_
* Improve ``DELETE`` on columns with special characters in their names. `#25737 <https://github.com/prestodb/presto/pull/25737>`_
* Improve the protocol efficiency of the C++ worker by supporting thrift codec for connector-specific data. `#25595 <https://github.com/prestodb/presto/pull/25595>`_
* Improve the protocol efficiency of coordinator by supporting thrift codec for connector-specific data. `#25242 <https://github.com/prestodb/presto/pull/25242>`_
* Add Scale and Precision columns to :doc:`/sql/show-columns` to get the respective scale of the decimal value and precision of numerical values. A Length column is introduced to get the length of ``CHAR`` and ``VARCHAR`` fields. `#25351 <https://github.com/prestodb/presto/pull/25351>`_
* Add ``Cache-Control`` header with max-age to statement API responses. `#25433 <https://github.com/prestodb/presto/pull/25433>`_
* Add ``X-Presto-Retry-Query`` header to identify queries that are being retried on a backup cluster. `#25625 <https://github.com/prestodb/presto/pull/25625>`_
* Add ``presto-sql-helpers`` directory for inlined SQL invoked function plugins with plugin loading rules. `#26025 <https://github.com/prestodb/presto/pull/26025>`_
* Add a new plugin ``presto-native-sql-invoked-functions-plugin`` that contains all inline SQL functions, except those with overridden native implementations. `#25870 <https://github.com/prestodb/presto/pull/25870>`_
* Add ``max_serializable_object_size`` session property to change the maximum serializable object size at the coordinator. `#25616 <https://github.com/prestodb/presto/pull/25616>`_
* Add all inline SQL invoked functions into a new plugin ``presto-sql-invoked-functions-plugin``. The following functions were moved: ``replace_first``, ``trail``, ``key_sampling_percent``, ``no_values_match``, ``no_keys_match``, ``any_values_match``, ``any_keys_match``, ``all_keys_match``, ``map_remove_null_values``, ``map_top_n_values``, ``map_top_n_keys``, ``map_top_n``, ``map_key_exists``, ``map_keys_by_top_n_values``, ``map_normalize``, ``array_top_n``, ``remove_nulls``, ``array_sort_desc``, ``array_min_by``, ``array_max_by``, ``array_least_frequent``, ``array_has_duplicates``, ``array_duplicates``, ``array_frequency``, ``array_split_into_chunks``, ``array_average``, ``array_intersect``. See `#26025 <https://github.com/prestodb/presto/pull/26025>`_ and `presto-sql-helpers/README.md <https://github.com/prestodb/presto/blob/master/presto-sql-helpers/README.md>`_. `#25818 <https://github.com/prestodb/presto/pull/25818>`_
* Add ``array_sort(array, function)`` support for key-based sorting. See :doc:`/functions/array`. `#25851 <https://github.com/prestodb/presto/pull/25851>`_
* Add ``array_sort_desc(array, function)`` support for key-based sorting. See :doc:`/functions/array`.  `#25851 <https://github.com/prestodb/presto/pull/25851>`_
* Add OAuth2 support for WebUI and JDBC Presto Client. `#24443 <https://github.com/prestodb/presto/pull/24443>`_
* Add a new configuration property ``query.max-queued-time`` to specify maximum queued time for a query before killing it. This can be overridden by the ``query_max_queued_time`` session property. `#25589 <https://github.com/prestodb/presto/pull/25589>`_
* Add support for BuiltInFunctionKind enum parameter in BuiltInFunctionHandle's JSON constructor creator. `#25821 <https://github.com/prestodb/presto/pull/25821>`_
* Add support for configuring http2 server on worker for communication between coordinator and workers. To enable, set the configuration property ``http-server.http2.enabled`` to  ``true``. `#25708 <https://github.com/prestodb/presto/pull/25708>`_
* Add support for cross-cluster query retry. Failed queries can be automatically retried on a backup cluster by providing the retry URL and expiration time as query parameters. `#25625 <https://github.com/prestodb/presto/pull/25625>`_
* Add support for using a Netty client to do HTTP communication between coordinator and worker. To enable, set the configuration property ``reactor.netty-http-client-enabled`` to ``true`` on the coordinator. `#25573 <https://github.com/prestodb/presto/pull/25573>`_
* Add test methods ``assertStartTransaction`` and ``assertEndTransaction`` to better support non-autocommit transaction testing scenarios. `#25053 <https://github.com/prestodb/presto/pull/25053>`_
* Add a database-based session property manager. See :doc:`/admin/session-property-managers`. `#24995 <https://github.com/prestodb/presto/pull/24995>`_
* Add support to use the MariaDB Java client with a MySQL based function server. `#25698 <https://github.com/prestodb/presto/pull/25698>`_
* Add support and plumbing for ``DELETE`` queries to identify modified partitions as outputs in the generated QueryIOMetadata. `#26134 <https://github.com/prestodb/presto/pull/26134>`_
* Add reporting lineage details for columns which are created or inserted to the event listener. `#25913 <https://github.com/prestodb/presto/pull/25913>`_
* Upgrade Presto to require Java 17. The Presto client and Presto-on-Spark remain Java 8-compatible. Presto now requires a Java 17 VM to run both coordinator and workers. `#24866 <https://github.com/prestodb/presto/pull/24866>`_
* Update Provisio packaging to split plugin packaging into ``plugins`` and ``native-plugin`` directory. `#25984 <https://github.com/prestodb/presto/pull/25984>`_
* Update Provisio plugin to package the memory connector plugin under the ``native-plugin`` directory. `#26044 <https://github.com/prestodb/presto/pull/26044>`_
* Update to preserve table name quoting in the output of :doc:`/sql/show-create-view`. `#25528 <https://github.com/prestodb/presto/pull/25528>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix an issue when processing multiple splits for the same plan node from multiple sources. `#26031 <https://github.com/prestodb/presto/pull/26031>`_
* Fix constant folding to handle deeply nested call statements. `#26088 <https://github.com/prestodb/presto/pull/26088>`_
* Fix constant folding in sidecar enabled clusters. `#26125 <https://github.com/prestodb/presto/pull/26125>`_
* Improve native execution of sidecar query analysis by enabling Presto built-in functions. `#25135 <https://github.com/prestodb/presto/pull/25135>`_
* Add the parameterized ``VARCHAR`` type in the list of supported types in NativeTypeManager. `#26003 <https://github.com/prestodb/presto/pull/26003>`_
* Add session property :ref:`presto_cpp/properties-session:\`\`native_index_lookup_join_max_prefetch_batches\`\`` which controls the max number of input batches to prefetch to do index lookup ahead. If it is set to ``0``, then process one input batch at a time. `#25886 <https://github.com/prestodb/presto/pull/25886>`_
* Add session property :ref:`presto_cpp/properties-session:\`\`native_index_lookup_join_split_output\`\``. If set to ``true``, then the index join operator might split output for each input batch based on the output batch size control. Otherwise, it tries to produce a single output for each input batch. `#25886 <https://github.com/prestodb/presto/pull/25886>`_
* Add session property :ref:`presto_cpp/properties-session:\`\`native_unnest_split_output\`\``. If this is set to ``true``, then the unnest operator might split output for each input batch based on the output batch size control. Otherwise, it produces a single output for each input batch. `#25886 <https://github.com/prestodb/presto/pull/25886>`_
* Add session properties :ref:`presto_cpp/properties-session:\`\`native_debug_memory_pool_name_regex\`\`` and :ref:`presto_cpp/properties-session:\`\`native_debug_memory_pool_warn_threshold_bytes\`\`` to help debug memory pool usage patterns. `25750 <https://github.com/prestodb/presto/pull/25750>`_
* Add limited use of the ``CHAR(N)`` type with PrestoC++. When ``CHAR(N)`` is used in a query it is mapped to the Velox ``VARCHAR`` type. As a result ``CHAR(N)`` semantics are not preserved in the exectution engine. `#25843 <https://github.com/prestodb/presto/pull/25843>`_
* Add spatial join support for native execution. `#25823 <https://github.com/prestodb/presto/pull/25823>`_
* Rename ``native_query_trace_node_ids`` to ``native_query_trace_node_id`` to provide a single plan node id for tracing. `#25684 <https://github.com/prestodb/presto/pull/25684>`_
* Update coordinator behavior to validate sidecar function signatures against plugin loaded function signatures at startup. `#25919 <https://github.com/prestodb/presto/pull/25919>`_

Security Changes
________________
* Fix the Content Security Policy (CSP) by adding ``form-action 'self'`` and setting ``img-src 'self'`` in response to `CWE-693 <https://cwe.mitre.org/data/definitions/693.html>`_. `#25910 <https://github.com/prestodb/presto/pull/25910>`_
* Upgrade Netty to version 4.1.126.Final to address `CVE-2025-58056 <https://github.com/advisories/GHSA-fghv-69vj-qj49>`_ and `CVE-2025-58057 <https://github.com/advisories/GHSA-3p8m-j85q-pgmj>`_. `#26006 <https://github.com/prestodb/presto/pull/26006>`_
* Upgrade commons-lang3 to 3.18.0 to address `CVE-2025-48924 <https://github.com/advisories/GHSA-j288-q9x7-2f5v>`_. `#25751 <https://github.com/prestodb/presto/pull/25751>`_
* Upgrade jaxb-runtime to v4.0.5 in response to `CVE-2020-15250 <https://github.com/advisories/GHSA-269g-pwp5-87pp>`_. `#26024 <https://github.com/prestodb/presto/pull/26024>`_
* Upgrade netty dependency to address `CVE-2025-55163 <https://github.com/advisories/GHSA-prj3-ccx8-p6x4>`_. `#25806 <https://github.com/prestodb/presto/pull/25806>`_
* Upgrade reactor-netty-http dependency to address `CVE-2025-22227 <https://github.com/advisories/GHSA-4q2v-9p7v-3v22>`_. `#25739 <https://github.com/prestodb/presto/pull/25739>`_

JDBC Driver Changes
___________________
* Add ``DECIMAL`` type support to query builder. `#25699 <https://github.com/prestodb/presto/pull/25699>`_

Web UI Changes
______________
* Fix the query id tooltip being displayed at an incorrect position. `#25809 <https://github.com/prestodb/presto/pull/25809>`_

Arrow Flight Connector Changes
______________________________
* Add support for `mutual TLS (mTLS) authentication <https://prestodb.io/docs/current/connector/base-arrow-flight.html#mutual-tls-mtls-support>`_. `#25388 <https://github.com/prestodb/presto/pull/25388>`_

BigQuery Connector Changes
__________________________
* Fix query failures on ``SELECT`` operations by aligning BigQuery v1beta1 with protobuf-java 3.25.8, preventing runtime incompatibility with protobuf 4.x. `#25805 <https://github.com/prestodb/presto/pull/25805>`_
* Add support for case-sensitive identifiers in BigQuery. To enable, set the configuration property ``case-sensitive-name-matching=true`` in the catalog file. `#25764 <https://github.com/prestodb/presto/pull/25764>`_

Cassandra Connector Changes
___________________________
* Add support to read ``TUPLE`` type as a Presto ``VARCHAR``. `#25516 <https://github.com/prestodb/presto/pull/25516>`_

ClickHouse Connector Changes
____________________________
* Add support for case-sensitive identifiers in Clickhouse. To enable, set the configuration property ``case-sensitive-name-matching=true`` in the catalog file. `#25863 <https://github.com/prestodb/presto/pull/25863>`_

Delta Lake Connector Changes
____________________________
* Upgrade to Hadoop 3.4.1. `#24799 <https://github.com/prestodb/presto/pull/24799>`_

Hive Connector Changes
______________________
* Fix Hive connector to ignore unsupported table formats when querying ``system.jdbc.columns`` to prevent errors. `#25779 <https://github.com/prestodb/presto/pull/25779>`_
* Add session property ``hive.orc_use_column_names`` to toggle the accessing of columns based on the names recorded in the ORC file rather than their ordinal position in the file. `#25285 <https://github.com/prestodb/presto/pull/25285>`_
* Upgrade to Hadoop 3.4.1. `#24799 <https://github.com/prestodb/presto/pull/24799>`_

Hudi Connector Changes
______________________
* Upgrade to Hadoop 3.4.1. `#24799 <https://github.com/prestodb/presto/pull/24799>`_

Iceberg Connector Changes
_________________________
* Fix null pointer exception (NPE) error in getViews API call when a schema is not provided. `#25695 <https://github.com/prestodb/presto/pull/25695>`_
* Fix implementation of commit to do one operation as opposed to two. `#25615 <https://github.com/prestodb/presto/pull/25615>`_
* Fix Iceberg connector rename column failed if the column is used as source column of non-identity transform. `#25697 <https://github.com/prestodb/presto/pull/25697>`_
* Improve Iceberg's ``apply_changelog`` function by migrating it from the global namespace to the connector-specific namespace. The function is now available as ``iceberg.system.apply_changelog()`` instead of ``apply_changelog()``. `#25871 <https://github.com/prestodb/presto/pull/25871>`_
* Improve the property mechanism to enable a property to accept and process property values of multiple types. `#25862 <https://github.com/prestodb/presto/pull/25862>`_
* Add Iceberg bucket scalar function. `#25951 <https://github.com/prestodb/presto/pull/25951>`_
* Add ``iceberg.engine.hive.lock-enabled`` configuration to disable Hive locks. `#25615 <https://github.com/prestodb/presto/pull/25615>`_
* Add support for specifying multiple transforms when adding a column. `#25862 <https://github.com/prestodb/presto/pull/25862>`_
* Upgrade Iceberg version to 1.8.1. `#25999 <https://github.com/prestodb/presto/pull/25999>`_
* Upgrade Nessie to version 0.95.0. `#25593 <https://github.com/prestodb/presto/pull/25593>`_
* Upgrade to Hadoop 3.4.1. `#24799 <https://github.com/prestodb/presto/pull/24799>`_
* Update to implement ConnectorMetadata::finishDeleteWithOutput(). `#26134 <https://github.com/prestodb/presto/pull/26134>`_

Kudu Connector Changes
______________________
* Update to implement ConnectorMetadata::finishDeleteWithOutput(). `#26134 <https://github.com/prestodb/presto/pull/26134>`_

MongoDB Connector Changes
_________________________
* Add support for case-sensitive identifiers in MongoDB. To enable, set the configuration property ``case-sensitive-name-matching=true`` in the catalog file. `#25853 <https://github.com/prestodb/presto/pull/25853>`_
* Upgrade MongoDB java driver to 3.12.14. `#25436 <https://github.com/prestodb/presto/pull/25436>`_

PostgreSQL Connector Changes
____________________________
* Add support for `GEOMETRY <https://prestodb.io/docs/current/language/types.html#geospatial>`_ type in the PostgreSQL connector. `#25240 <https://github.com/prestodb/presto/pull/25240>`_

Redis Connector Changes
_______________________
* Add changes to enable TLS support. `#25373 <https://github.com/prestodb/presto/pull/25373>`_

SPI Changes
___________
* Add a new ``getSqlInvokedFunctions`` SPI in Presto, which only supports SQL invoked functions. `#25597 <https://github.com/prestodb/presto/pull/25597>`_
* Add a new ``ConnectorMetadata::finishDeleteWithOutput()`` method, returning ``Optional<ConnectorOutputMetadata>``. This allows connectors implementing ``DELETE`` to identify partitions modified in queries, which can be important for tracing lineage. `#26134 <https://github.com/prestodb/presto/pull/26134>`_
* Add AuthenticatorNotApplicableException to prevent irrelevant authenticator errors from being returned to clients. `#25606 <https://github.com/prestodb/presto/pull/25606>`_
* Deprecate the existing ``ConnectorMetadata::finishDelete()`` method. By default, the new ``finishDeleteWithOutput()`` method delegates to the existing ``finishDelete()`` method, and returns ``Optional.empty()``. This allows existing connectors to continue working without changes. `#26134 <https://github.com/prestodb/presto/pull/26134>`_

Documentation Changes
_____________________
* Improve :doc:`/installation/deploy-brew`. `#25924 <https://github.com/prestodb/presto/pull/25924>`_
* Add documentation about the Presto :doc:`/develop/release-process` and :doc:`/admin/version-support`. `#25742 <https://github.com/prestodb/presto/pull/25742>`_



**Credits**
===========

Abhash Jain, Adrian Carpente (Denodo), Amit Dutta, Amritanshu Darbari, Anant Aneja, Andrew Xie, Arjun Gupta, Artem Selishchev, Bryan Cutler, Christian Zentgraf, Dilli-Babu-Godari, Elbin Pallimalil, Facebook Community Bot, Feilong Liu, Gary Helmling, Ge Gao, Hazmi, HeidiHan0000, Jalpreet Singh Nanda (:imjalpreet), James Gill, Jay Narale, Jialiang Tan, Joe Abraham, Joe O'Hallaron, Karthikeyan Natarajan, Ke Wang, Ke Wang, Kevin Tang, Kewen Wang, Krishna Pai, Mahadevuni Naveen Kumar, Maria Basmanova, Mariam Almesfer, Matt Karrmann, Miguel Blanco Godón, Natasha Sehgal, Naveen Nitturu, Nidhin Varghese, Nikhil Collooru, Nishitha-Bhaskaran, PRASHANT GOLASH, Ping Liu, Pradeep Vaka, Pramod Satya, Prashant Sharma, Pratik Joseph Dabre, Raaghav Ravishankar, Rebecca Schlussel, Rebecca Whitworth, Reetika Agrawal, Richard Barnes, Sayari Mukherjee, Sergey Pershin, Shahim Sharafudeen, Shang Ma, Shijin, Shrinidhi Joshi, Steve Burnett, Sumi Mathew, Timothy Meehan, Valery Mironov, Vamsi Karnika, Vivian Hsu, Wei He, Xiaoxuan Meng, Xin Zhang, Yihong Wang, Ying, Zac Blanco, Zac Wen, abhinavmuk04, aditi-pandit, adkharat, aspegren_david, auden-woolfson, beinan, dnskr, ericyuliu, haneel-kumar, j-sund, juwentus1234, lingbin, mehradpk, mohsaka, pratik.pugalia@gmail.com, pratyakshsharma, singcha, unidevel, wangd, yangbin09
