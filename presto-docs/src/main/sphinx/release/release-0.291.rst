=============
Release 0.291
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix behavior of :func:`width_bucket(x, bins) -> bigint` which previously treated all ``null`` elements in bins as ``0``. Now the function will throw an error if it finds a ``null`` or non-finite element in ``bins``..  :pr:`24103`. (`#24103 <https://github.com/prestodb/presto/pull/24103>`_)
* Fix bug in strrpos function for multibyte characters. (`#24226 <https://github.com/prestodb/presto/pull/24226>`_)
* Improve Request Headers in the Authentication Filter Class. :pr:`23380`. (`#23380 <https://github.com/prestodb/presto/pull/23380>`_)
* Improve coordinator task management performance. :pr:`24369`. (`#24369 <https://github.com/prestodb/presto/pull/24369>`_)
* Improve efficiency of coordinator when running a large number of tasks. :pr:`24288`. (`#24288 <https://github.com/prestodb/presto/pull/24288>`_)
* Improve scheduling for CTE materialization: Now, only the stages containing CTE table scans that reference CTE table write stages are blocked till the write is complete, instead of the entire query being blocked as was the case previously. This is controlled by the session property ``enhanced_cte_scheduling_enabled`` (on by default) :pr:`24108`. (`#24108 <https://github.com/prestodb/presto/pull/24108>`_)
* Add :doc:`/sql/alter-table` SET PROPERTIES statement :pr:`21495`. (`#21495 <https://github.com/prestodb/presto/pull/21495>`_)
* Add :func:`google_polyline_decode` function to convert Google polyline to Presto ST_Geometry types. :pr:`23999`. (`#23999 <https://github.com/prestodb/presto/pull/23999>`_)
* Add :func:`google_polyline_encode` function to convert Presto ST_Geometry to Google polyline types. :pr:`23999`. (`#23999 <https://github.com/prestodb/presto/pull/23999>`_)
* Add Delete TableWriter TableFinish node to SPI :pr:`24088`. (`#24088 <https://github.com/prestodb/presto/pull/24088>`_)
* Add SemiJoin Join TableWriter Delete TableFinish node to connector optimizer :pr:`24154`. (`#24154 <https://github.com/prestodb/presto/pull/24154>`_)
* Add ``ClientRequestFilter.java`` interface in Presto-spi. :pr:`23380`. (`#23380 <https://github.com/prestodb/presto/pull/23380>`_)
* Add ``query-data-cache-enabled-default`` configuration property to align C++ cache behavior with Java. Set it to ``true``(default) for current C++ behavior or to ``false`` to match Java's cache logic. :pr:`24076`. (`#24076 <https://github.com/prestodb/presto/pull/24076>`_)
* Add a configuration property ``plan-checker.config-dir`` to set the configuration directory for PlanCheckerProvider configurations. :pr:`23955`. (`#23955 <https://github.com/prestodb/presto/pull/23955>`_)
* Add a session property `include_values_node_in_connector_optimizer` to enable connector optimizer optimize plans with values node  :pr:`12345`. (`#24227 <https://github.com/prestodb/presto/pull/24227>`_)
* Add an optional input distribution constraint to DeleteNode :pr:`24104`. (`#24104 <https://github.com/prestodb/presto/pull/24104>`_)
* Add an session property to not enforce input partition for join build :pr:`24163`. (`#24163 <https://github.com/prestodb/presto/pull/24163>`_)
* Add catalog and schema level access checks in :doc:`/sql/use` statement. :pr:`24182`. (`#24182 <https://github.com/prestodb/presto/pull/24182>`_)
* Add delete node in subfield pruning optimizer :pr:`24206`. (`#24206 <https://github.com/prestodb/presto/pull/24206>`_)
* Add single worker execution. To improve latency of tiny queries running on a large cluster, we introduce single worker execution mode: query will only use one node to execute and plan would be optimized accordingly. This feature can be turned on by config `single-node-execution-enabled` or session property `single_node_execution_enabled`.:pr:`24172`. (`#24172 <https://github.com/prestodb/presto/pull/24172>`_)
* Add support for ORC metadata cache invalidation based on file modification time. :pr:`24346`. (`#24346 <https://github.com/prestodb/presto/pull/24346>`_)
* Add support for ``ALTER VIEW RENAME TO`` operation, including the necessary infrastructure for connector implementations. :pr:`23749`. (`#23749 <https://github.com/prestodb/presto/pull/23749>`_)
* Add support for ``BigInt`` data type in the SQL Client on Presto UI on supported browsers. See `compatibility <https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/parse#browser_compatibility>`_ for the supported browsers. :pr:`24336`. (`#24336 <https://github.com/prestodb/presto/pull/24336>`_)
* Add support for the histogram statistic type. :pr:`22365`. (`#22365 <https://github.com/prestodb/presto/pull/22365>`_)
* Add support for time type partitioning in the ORC file format for Iceberg. :pr:`24091`. (`#24091 <https://github.com/prestodb/presto/pull/24091>`_)
* Add testing for partitioning using time type in Iceberg. :pr:`24091`. (`#24091 <https://github.com/prestodb/presto/pull/24091>`_)
* Added catalog and schema level access checks in USE statement :pr:`23882 `. (`#23882 <https://github.com/prestodb/presto/pull/23882>`_)
* Remove ``query-data-cache-enabled-default`` configuration property, which is no longer needed as per-split fine-grained cache control has been introduced. :pr:`24372 `. (`#24372 <https://github.com/prestodb/presto/pull/24372>`_)
* Remove the ``experimental.table-writer-merge-operator-enabled` config property and the ``table_writer_merge_operator_enabled`` session property :pr:`12345`. (`#24145 <https://github.com/prestodb/presto/pull/24145>`_)
* Deprecated  native_query_trace_task_reg_exp session property from Prestissimo:pr:`24270`. (`#24270 <https://github.com/prestodb/presto/pull/24270>`_)
* Enable ``scale-writers`` by default. :pr:`24107`. (`#24107 <https://github.com/prestodb/presto/pull/24107>`_)
* Support automatic scaling of writer threads for partitioned tables. Can be enabled with the `native_execution_scale_partitioned_writer_threads_enabled` session property. Native execution only.  :pr:`24155`. (`#24155 <https://github.com/prestodb/presto/pull/24155>`_)
* Throw exception on invalid http headers in async page transport servlet. :pr:`24004`. (`#24004 <https://github.com/prestodb/presto/pull/24004>`_)
* Update usage of MD5 to SHA256 :pr:`23903`. (`#23903 <https://github.com/prestodb/presto/pull/23903>`_)
* Upgrade avro to version 1.11.4 in response to `CVE-2024-47561 <https://github.com/advisories/GHSA-r7pg-v2c8-mfg3>`_. :pr:`23868`. (`#23943 <https://github.com/prestodb/presto/pull/23943>`_)
* Upgrade grpc dependencies to version 1.68.0 in response to `CVE-2022-25647 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25647>` :pr:`24051`. (`#24051 <https://github.com/prestodb/presto/pull/24051>`_)
* Upgrade gson from version 2.8.9 to v2.11.0 pr: `24247`. (`#24247 <https://github.com/prestodb/presto/pull/24247>`_)
* Upgrade gson to version 2.8.9 in response to `CVE-2022-25647 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25647>` :pr:`24051`. (`#24051 <https://github.com/prestodb/presto/pull/24051>`_)
* Upgrade jetty dependencies to version 9.4.56.v20240826 in response to `CVE-2024-8184 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-8184>`_. :pr:`24184`. (`#24184 <https://github.com/prestodb/presto/pull/24184>`_)
* Upgrade netty dependencies to version 4.1.115.Final in response to `CVE-2024-47535 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47535>`_. :pr:`24137`. (`#24137 <https://github.com/prestodb/presto/pull/24137>`_)

Security Changes
________________
* Fix `CVE-2015-3192 <https://www.mend.io/vulnerability-database/CVE-2015-3192>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2015-5211 <https://www.mend.io/vulnerability-database/CVE-2015-5211>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2016-1000027 <https://www.mend.io/vulnerability-database/CVE-2016-1000027>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2018-1199 <https://www.mend.io/vulnerability-database/CVE-2018-1199>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2018-1272 <https://www.mend.io/vulnerability-database/CVE-2018-1272>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2020-5421 <https://www.mend.io/vulnerability-database/CVE-2020-5421>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2021-0170 <https://www.mend.io/vulnerability-database/CVE-2021-0170>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2021-22060 <https://www.mend.io/vulnerability-database/CVE-2021-22060>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2021-22096 <https://www.mend.io/vulnerability-database/CVE-2021-22096>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2021-22096 <https://www.mend.io/vulnerability-database/CVE-2021-22096>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2022-22965 <https://www.mend.io/vulnerability-database/CVE-2022-22965>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2022-22970 <https://www.mend.io/vulnerability-database/CVE-2022-22970>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2022-22970 <https://www.mend.io/vulnerability-database/CVE-2022-22970>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2022-27772 <https://www.mend.io/vulnerability-database/CVE-2022-27772>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2023-20883 <https://www.mend.io/vulnerability-database/CVE-2023-20883>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-22243 <https://www.mend.io/vulnerability-database/CVE-2024-22243>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-22259 <https://www.mend.io/vulnerability-database/CVE-2024-22259>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-22262 <https://www.mend.io/vulnerability-database/CVE-2024-22262>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-38809 <https://www.mend.io/vulnerability-database/CVE-2024-38809>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-6763 <https://www.mend.io/vulnerability-database/CVE-2024-6763>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-6763 <https://www.mend.io/vulnerability-database/CVE-2024-6763>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix `CVE-2024-8184 <https://www.mend.io/vulnerability-database/CVE-2024-8184>`_. :pr:`24112`. (`#24112 <https://github.com/prestodb/presto/pull/24112>`_)
* Fix security vulnerability in presto-pinot-toolkit and presto-product-tests in response to `CVE-2020-0287 <https://nvd.nist.gov/vuln/detail/CVE-2020-0287>`_. :pr:`24249`. (`#24249 <https://github.com/prestodb/presto/pull/24249>`_)
* Fix security vulnerability in swagger-ui jar in response to 'CVE-2018-25031 <https://nvd.nist.gov/vuln/detail/CVE-2018-25031>' .  :pr:`24153`. (`#24153 <https://github.com/prestodb/presto/pull/24153>`_)
* Fix security vulnerability in swagger-ui jar in response to 'CVE-2018-25031 <https://nvd.nist.gov/vuln/detail/CVE-2018-25031>' .  :pr:`24199`. (`#24199 <https://github.com/prestodb/presto/pull/24199>`_)
* Improve pbkdf2 hashing using SHA-256 cipher in response to `CWE-759 <https://cwe.mitre.org/data/definitions/759.htm>`_. :pr:`24132`. (`#24132 <https://github.com/prestodb/presto/pull/24132>`_)
* Add re2j regex matching in QueryStateInfoResource to protect from ReDoS attacks in response to `CVE-2024-45296 <https://www.cve.org/CVERecord?id=CVE-2024-45296>`_. :pr:`24048`. (`#24048 <https://github.com/prestodb/presto/pull/24048>`_)
* Add security-related headers to the static resources serving from the Presto UI, including: ``Content-Security-Policy``, ``X-Content-Type-Options``. See reference docs `Content-Security-Policy <https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP>`_ and `X-Content-Type-Options <https://learn.microsoft.com/en-us/previous-versions/windows/internet-explorer/ie-developer/compatibility/gg622941(v=vs.85)>`_. :pr:`24272`. (`#24272 <https://github.com/prestodb/presto/pull/24272>`_)
* Add support for pluggable Custom Presto Authenticators :pr:`#24111`. (`#24111 <https://github.com/prestodb/presto/pull/24111>`_)
* Replace `alluxio-shaded-client` with `alluxio-core` libraries libraries in response to `CVE-2023-44981 <https://github.com/advisories/GHSA-7286-pgfv-vxvh>`_. :pr:`24231`. (`#24231 <https://github.com/prestodb/presto/pull/24231>`_)

Bigquery Connector Changes
__________________________
* Fix for BigQuery ``SELECT``. :pr:`23957`. (`#23957 <https://github.com/prestodb/presto/pull/23957>`_)

Cassandra Connector Changes
___________________________
* Improve cryptographic protocol in response to `java:S4423 <https://sonarqube.ow2.org/coding_rules?open=java%3AS4423&rule_key=java%3AS4423>`_. :pr:`24436`. (`#24436 <https://github.com/prestodb/presto/pull/24436>`_)

Delta Connector Changes
_______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`. (`#23401 <https://github.com/prestodb/presto/pull/23401>`_)

Hive Connector Changes
______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`. (`#23401 <https://github.com/prestodb/presto/pull/23401>`_)

Hudi Connector Changes
______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`. (`#23401 <https://github.com/prestodb/presto/pull/23401>`_)

Iceberg Connector Changes
_________________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`. (`#23401 <https://github.com/prestodb/presto/pull/23401>`_)
* Add configuration property ``iceberg.rest.nested.namespace.enabled`` to support nested namespaces in Iceberg's REST Catalog. Defaults to ``true``. :pr:`24083`. (`#24083 <https://github.com/prestodb/presto/pull/24083>`_)
* Add support for ``ALTER VIEW RENAME TO``. :pr:`23749`. (`#23749 <https://github.com/prestodb/presto/pull/23749>`_)
* Add support of ``view`` for Iceberg connector when configured with ``REST`` and ``NESSIE``. :pr:`23793`. (`#23793 <https://github.com/prestodb/presto/pull/23793>`_)
* Add support of specifying table location on creation for Iceberg connector when configured with ``REST`` and ``NESSIE``. :pr:`24218`. (`#24218 <https://github.com/prestodb/presto/pull/24218>`_)
* Remove in-memory hive metastore cache in Iceberg connector. :pr:`24326`. (`#24326 <https://github.com/prestodb/presto/pull/24326>`_)
* Optimize redundant getTable calls in Iceberg Connector :pr:`24376 `. (`#24376 <https://github.com/prestodb/presto/pull/24376>`_)

Memory Connector Changes
________________________
* Add support for ``ALTER VIEW RENAME TO``. :pr:`23749`. (`#23749 <https://github.com/prestodb/presto/pull/23749>`_)

Mongodb Connector Changes
_________________________
* Add steps to connect to MongoDB cluster with TLS CA File to :doc:`/connector/mongodb`. :pr:`24352`. (`#24352 <https://github.com/prestodb/presto/pull/24352>`_)

SPI Changes
___________
* Improve ExpressionOptimizer#optimize method with a variable resolver to return ``RowExpression``. :pr:`24287`. (`#24287 <https://github.com/prestodb/presto/pull/24287>`_)
* Add WindowNode, JoinNode, SemiJoinNode, MergeJoinNode, and SpatialJoinNode to the SPI. :pr:`23976`. (`#23976 <https://github.com/prestodb/presto/pull/23976>`_)
* Add a partition by attribute to specify the scope of sort node. :pr:`24095`. (`#24095 <https://github.com/prestodb/presto/pull/24095>`_)
* Remove ConnectorJoinNode from the SPI. JoinNode can now be used instead. :pr:`23976`. (`#23976 <https://github.com/prestodb/presto/pull/23976>`_)
* Remove experimental getPreferredShuffleLayout methods from the connector metadata in favor of existing `getNewTableLayout`, `getInsertLayout` methods :pr:`12345`. (`#24106 <https://github.com/prestodb/presto/pull/24106>`_)
* Modify the signature of ``PlanCheckerProviderFactory.create`` to pass in a map of configuration properties and replace ``SimplePlanFragmentSerde`` with a ``PlanCheckerProviderContext``. :pr:`23955`. (`#23955 <https://github.com/prestodb/presto/pull/23955>`_)

Prestissimo (native Execution) Changes
______________________________________
* Improve partitioned remote exchanges for wide data sets (more than 500 columns) to use row wise encoding. :pr:`23929 `. (`#23929 <https://github.com/prestodb/presto/pull/23929>`_)
* Add native plan checker to the native sidecar plugin and native endpoint for Velox plan conversion. :pr:`23596`. (`#23596 <https://github.com/prestodb/presto/pull/23596>`_)
* Add session properties ``native_spill_prefixsort_enabled``, ``native_prefixsort_normalized_key_max_bytes ``, and ``native_prefixsort_min_rows ``. :pr:`24043`. (`#24043 <https://github.com/prestodb/presto/pull/24043>`_)

**Credits**
===========

Abe Varghese, Abhisek Saikia, Ajay Kharat, Amit Dutta, Anant Aneja, Andrii Rosa, Ann-Megha-Rajesh1, Arjun Gupta, Bryan Cutler, Christian Zentgraf, Deepa-George, Deepak Majeti, Deepak Mehra, Denodo Research Labs, Dilli-Babu-Godari, Emanuel F., Emily Chan, Facebook Community Bot, Feilong Liu, Ge Gao, Georg Schäfer, Hazmi, Heidi Han, Jacob Khaliqi, Jalpreet Singh Nanda (:imjalpreet), Jalpreet Singh Nanda (:imjalpreet), Jeremy Quirke, Jialiang Tan, Jiaqi Zhang, Joe Abraham, Ke, Kevin Tang, Konjac Huang, Leonid Chistov, Linsong Wang, Luís Fernandes, MariamAlmesfer, Matthew Peveler, Minhan Cao, Natasha Sehgal, Naveen Mahadevuni, Nikhil Collooru, Nishitha-Bhaskaran, NivinCS, Pramod Satya, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Richard Barnes, Serge Druzkin, Sergey Pershin, Shakyan Kushwaha, Shang Ma, Shijin, Steve Burnett, SthuthiGhosh9400, Sumi Mathew, Tim Meehan, Xiao Du, Xiaoxuan Meng, Yihong Wang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, auden-woolfson, dependabot[bot], jaystarshot, pratyakshsharma, unidevel, wangd, wypb, zuyu
