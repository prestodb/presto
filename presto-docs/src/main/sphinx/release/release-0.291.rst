=============
Release 0.291
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix behavior of :func:`width_bucket(x, bins) -> bigint` which previously treated all ``null`` elements in bins as ``0``. The function throws an error if it finds a ``null`` or non-finite element in ``bins``. `#24103 <https://github.com/prestodb/presto/pull/24103>`_
* Fix bug in ``strrpos`` function for multibyte characters. `#24226 <https://github.com/prestodb/presto/pull/24226>`_
* Improve Request Headers in the Authentication Filter Class. `#23380 <https://github.com/prestodb/presto/pull/23380>`_
* Improve coordinator task management performance. `#24369 <https://github.com/prestodb/presto/pull/24369>`_
* Improve efficiency of coordinator when running a large number of tasks. `#24288 <https://github.com/prestodb/presto/pull/24288>`_
* Improve scheduling for CTE materialization: Now, only the stages containing CTE table scans that reference CTE table write stages are blocked till the write is complete, instead of the entire query being blocked as was the case previously. This is controlled by the session property ``enhanced_cte_scheduling_enabled`` (on by default). `#24108 <https://github.com/prestodb/presto/pull/24108>`_
* Improve HTTP responses from async page transport servlet, by sanitizing the URI before use. `#24004 <https://github.com/prestodb/presto/pull/24004>`_
* Add :doc:`/sql/alter-table` SET PROPERTIES statement. `#21495 <https://github.com/prestodb/presto/pull/21495>`_
* Add :func:`google_polyline_decode` function to convert Google polyline to Presto ST_Geometry types. `#23999 <https://github.com/prestodb/presto/pull/23999>`_
* Add :func:`google_polyline_encode` function to convert Presto ST_Geometry to Google polyline types. `#23999 <https://github.com/prestodb/presto/pull/23999>`_
* Add Delete TableWriter TableFinish node to SPI. `#24088 <https://github.com/prestodb/presto/pull/24088>`_
* Add SemiJoin Join TableWriter Delete TableFinish node to connector optimizer. `#24154 <https://github.com/prestodb/presto/pull/24154>`_
* Add ``ClientRequestFilter.java`` interface in Presto-spi. `#23380 <https://github.com/prestodb/presto/pull/23380>`_
* Add ``query-data-cache-enabled-default`` configuration property to align C++ cache behavior with Java. Set it to ``true`` (default) for current C++ behavior or to ``false`` to match Java's cache logic. `#24076 <https://github.com/prestodb/presto/pull/24076>`_
* Add a configuration property ``plan-checker.config-dir`` to set the configuration directory for PlanCheckerProvider configurations. `#23955 <https://github.com/prestodb/presto/pull/23955>`_
* Add a session property ``include_values_node_in_connector_optimizer`` to enable connector optimizer optimize plans with values node.  `#24227 <https://github.com/prestodb/presto/pull/24227>`_
* Add an optional input distribution constraint to DeleteNode. `#24104 <https://github.com/prestodb/presto/pull/24104>`_
* Add ``native_enforce_join_build_input_partition`` session property to not enforce input partition for join build. `#24163 <https://github.com/prestodb/presto/pull/24163>`_
* Add catalog and schema level access checks in :doc:`/sql/use` statement. `#24182 <https://github.com/prestodb/presto/pull/24182>`_
* Add delete node in subfield pruning optimizer. `#24206 <https://github.com/prestodb/presto/pull/24206>`_
* Add single worker execution. To improve latency of tiny queries running on a large cluster, we introduce single worker execution mode: query will only use one node to execute and plan would be optimized accordingly. This feature can be turned on by the configuration property ``single-node-execution-enabled`` or the session property ``single_node_execution_enabled``. `#24172 <https://github.com/prestodb/presto/pull/24172>`_
* Add support for ORC metadata cache invalidation based on file modification time. `#24346 <https://github.com/prestodb/presto/pull/24346>`_
* Add support for ``ALTER VIEW RENAME TO`` operation, including the necessary infrastructure for connector implementations. `#23749 <https://github.com/prestodb/presto/pull/23749>`_
* Add support for ``BigInt`` data type in the SQL Client on Presto UI on supported browsers. See `compatibility <https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/parse#browser_compatibility>`_ for the supported browsers. `#24336 <https://github.com/prestodb/presto/pull/24336>`_
* Add support for the histogram statistic type. `#22365 <https://github.com/prestodb/presto/pull/22365>`_
* Add support for time type partitioning in the ORC file format for Iceberg. `#24091 <https://github.com/prestodb/presto/pull/24091>`_
* Add testing for partitioning using time type in Iceberg. `#24091 <https://github.com/prestodb/presto/pull/24091>`_
* Add catalog and schema level access checks in USE statement. `#23882 <https://github.com/prestodb/presto/pull/23882>`_
* Enable ``scale-writers`` by default. `#24107 <https://github.com/prestodb/presto/pull/24107>`_
* Update usage of MD5 to SHA256. `#23903 <https://github.com/prestodb/presto/pull/23903>`_

Prestissimo (native Execution) Changes
______________________________________
* Improve partitioned remote exchanges for wide data sets (more than 500 columns) to use row wise encoding. `#23929 <https://github.com/prestodb/presto/pull/23929>`_
* Add native plan checker to the native sidecar plugin and native endpoint for Velox plan conversion. `#23596 <https://github.com/prestodb/presto/pull/23596>`_
* Add session properties ``native_spill_prefixsort_enabled``, ``native_prefixsort_normalized_key_max_bytes``, and ``native_prefixsort_min_rows``. `#24043 <https://github.com/prestodb/presto/pull/24043>`_
* Add support for automatic scaling of writer threads for partitioned tables. Can be enabled with the ``native_execution_scale_partitioned_writer_threads_enabled`` session property. Native execution only. `#24155 <https://github.com/prestodb/presto/pull/24155>`_
* Remove the ``experimental.table-writer-merge-operator-enabled`` configuration property and the ``table_writer_merge_operator_enabled`` session property. `#24145 <https://github.com/prestodb/presto/pull/24145>`_
* Remove deprecated  ``native_query_trace_task_reg_exp session`` property from Prestissimo. `#24270 <https://github.com/prestodb/presto/pull/24270>`_
* Remove ``query-data-cache-enabled-default`` configuration property, which is no longer needed as per-split fine-grained cache control has been introduced. `#24372 <https://github.com/prestodb/presto/pull/24372>`_

Security Changes
________________
* Fix security vulnerability in presto-pinot-toolkit and presto-product-tests in response to `CVE-2020-0287 <https://nvd.nist.gov/vuln/detail/CVE-2020-0287>`_. `#24249 <https://github.com/prestodb/presto/pull/24249>`_
* Fix security vulnerability in swagger-ui jar in response to `CVE-2018-25031 <https://nvd.nist.gov/vuln/detail/CVE-2018-25031>`_.  `#24153 <https://github.com/prestodb/presto/pull/24153>`_
* Fix security vulnerability in swagger-ui jar in response to `CVE-2018-25031 <https://nvd.nist.gov/vuln/detail/CVE-2018-25031>`_. `#24199 <https://github.com/prestodb/presto/pull/24199>`_
* Improve pbkdf2 hashing using SHA-256 cipher in response to `CWE-759 <https://cwe.mitre.org/data/definitions/759.htm>`_. `#24132 <https://github.com/prestodb/presto/pull/24132>`_
* Add re2j regex matching in QueryStateInfoResource to protect from ReDoS attacks in response to `CVE-2024-45296 <https://www.cve.org/CVERecord?id=CVE-2024-45296>`_. `#24048 <https://github.com/prestodb/presto/pull/24048>`_
* Add security-related headers to the static resources serving from the Presto UI, including: ``Content-Security-Policy``, ``X-Content-Type-Options``. See reference docs `Content-Security-Policy <https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP>`_ and `X-Content-Type-Options <https://learn.microsoft.com/en-us/previous-versions/windows/internet-explorer/ie-developer/compatibility/gg622941(v=vs.85)>`_. `#24272 <https://github.com/prestodb/presto/pull/24272>`_
* Add support for pluggable Custom Presto Authenticators. `#24111 <https://github.com/prestodb/presto/pull/24111>`_
* Replace `alluxio-shaded-client` with `alluxio-core` libraries in response to `CVE-2023-44981 <https://github.com/advisories/GHSA-7286-pgfv-vxvh>`_. `#24231 <https://github.com/prestodb/presto/pull/24231>`_
* Upgrade avro to version 1.11.4 in response to `CVE-2024-47561 <https://github.com/advisories/GHSA-r7pg-v2c8-mfg3>`_. `#23943 <https://github.com/prestodb/presto/pull/23943>`_
* Upgrade grpc dependencies to version 1.68.0 in response to `CVE-2022-25647 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25647>`_. `#24051 <https://github.com/prestodb/presto/pull/24051>`_
* Upgrade gson from version 2.8.9 to v2.11.0. `#24247 <https://github.com/prestodb/presto/pull/24247>`_
* Upgrade gson to version 2.8.9 in response to `CVE-2022-25647 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25647>`_. `#24051 <https://github.com/prestodb/presto/pull/24051>`_
* Upgrade jetty dependencies to version 9.4.56.v20240826 in response to `CVE-2024-8184 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-8184>`_. `#24184 <https://github.com/prestodb/presto/pull/24184>`_
* Upgrade netty dependencies to version 4.1.115.Final in response to `CVE-2024-47535 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47535>`_. `#24137 <https://github.com/prestodb/presto/pull/24137>`_

BigQuery Connector Changes
__________________________
* Fix for BigQuery ``SELECT``. `#23957 <https://github.com/prestodb/presto/pull/23957>`_

Cassandra Connector Changes
___________________________
* Improve cryptographic protocol in response to `Weak SSL/TLS protocols should not be used <https://sonarqube.ow2.org/coding_rules?open=java%3AS4423&rule_key=java%3AS4423>`_. `#24436 <https://github.com/prestodb/presto/pull/24436>`_

Delta Connector Changes
_______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. `#23401 <https://github.com/prestodb/presto/pull/23401>`_

Hive Connector Changes
______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. `#23401 <https://github.com/prestodb/presto/pull/23401>`_

Hudi Connector Changes
______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. `#23401 <https://github.com/prestodb/presto/pull/23401>`_

Iceberg Connector Changes
_________________________
* Improve redundant getTable calls in Iceberg Connector. `#24376 <https://github.com/prestodb/presto/pull/24376>`_
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. `#23401 <https://github.com/prestodb/presto/pull/23401>`_
* Add configuration property ``iceberg.rest.nested.namespace.enabled`` to support nested namespaces in Iceberg's REST Catalog. Defaults to ``true``. `#24083 <https://github.com/prestodb/presto/pull/24083>`_
* Add support for ``ALTER VIEW RENAME TO``. `#23749 <https://github.com/prestodb/presto/pull/23749>`_
* Add support of ``view`` for Iceberg connector when configured with ``REST`` and ``NESSIE``. `#23793 <https://github.com/prestodb/presto/pull/23793>`_
* Add support of specifying table location on creation for Iceberg connector when configured with ``REST`` and ``NESSIE``. `#24218 <https://github.com/prestodb/presto/pull/24218>`_
* Remove in-memory hive metastore cache in Iceberg connector. `#24326 <https://github.com/prestodb/presto/pull/24326>`_

Memory Connector Changes
________________________
* Add support for ``ALTER VIEW RENAME TO``. `#23749 <https://github.com/prestodb/presto/pull/23749>`_

MongoDB Connector Changes
_________________________
* Add steps to connect to MongoDB cluster with TLS CA File to :doc:`/connector/mongodb`. `#24352 <https://github.com/prestodb/presto/pull/24352>`_

SPI Changes
___________
* Improve ExpressionOptimizer#optimize method with a variable resolver to return ``RowExpression``. `#24287 <https://github.com/prestodb/presto/pull/24287>`_
* Add WindowNode, JoinNode, SemiJoinNode, MergeJoinNode, and SpatialJoinNode to the SPI. `#23976 <https://github.com/prestodb/presto/pull/23976>`_
* Add a partition by attribute to specify the scope of sort node. `#24095 <https://github.com/prestodb/presto/pull/24095>`_
* Remove ConnectorJoinNode from the SPI. JoinNode can now be used instead. `#23976 <https://github.com/prestodb/presto/pull/23976>`_
* Remove experimental getPreferredShuffleLayout methods from the connector metadata in favor of existing `getNewTableLayout`, `getInsertLayout` methods `#24106 <https://github.com/prestodb/presto/pull/24106>`_
* Modify the signature of ``PlanCheckerProviderFactory.create`` to pass in a map of configuration properties and replace ``SimplePlanFragmentSerde`` with a ``PlanCheckerProviderContext``. `#23955 <https://github.com/prestodb/presto/pull/23955>`_

**Credits**
===========

Abe Varghese, Abhisek Saikia, Ajay Kharat, Amit Dutta, Anant Aneja, Andrii Rosa, Ann-Megha-Rajesh1, Arjun Gupta, Bryan Cutler, Christian Zentgraf, Deepa-George, Deepak Majeti, Deepak Mehra, Denodo Research Labs, Dilli-Babu-Godari, Emanuel F., Emily Chan, Facebook Community Bot, Feilong Liu, Ge Gao, Georg Schäfer, Hazmi, Heidi Han, Jacob Khaliqi, Jalpreet Singh Nanda (:imjalpreet), Jalpreet Singh Nanda (:imjalpreet), Jeremy Quirke, Jialiang Tan, Jiaqi Zhang, Joe Abraham, Ke, Kevin Tang, Konjac Huang, Leonid Chistov, Linsong Wang, Luís Fernandes, MariamAlmesfer, Matthew Peveler, Minhan Cao, Natasha Sehgal, Naveen Mahadevuni, Nikhil Collooru, Nishitha-Bhaskaran, NivinCS, Pramod Satya, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Richard Barnes, Serge Druzkin, Sergey Pershin, Shakyan Kushwaha, Shang Ma, Shijin, Steve Burnett, SthuthiGhosh9400, Sumi Mathew, Tim Meehan, Xiao Du, Xiaoxuan Meng, Yihong Wang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, auden-woolfson, dependabot[bot], jaystarshot, pratyakshsharma, unidevel, wangd, wypb, zuyu
