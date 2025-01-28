=============
Release 0.291
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix behavior of :func:`width_bucket(x, bins) -> bigint` which previously treated all ``null`` elements in bins as ``0``. Now the function will throw an error if it finds a ``null`` or non-finite element in ``bins``..  :pr:`24103`.
* Fix bug in strrpos function for multibyte characters.
* Improve Request Headers in the Authentication Filter Class. :pr:`23380`.
* Improve coordinator task management performance. :pr:`24369`.
* Improve efficiency of coordinator when running a large number of tasks. :pr:`24288`.
* Improve scheduling for CTE materialization: Now, only the stages containing CTE table scans that reference CTE table write stages are blocked till the write is complete, instead of the entire query being blocked as was the case previously. This is controlled by the session property ``enhanced_cte_scheduling_enabled`` (on by default) :pr:`24108`.
* Add :doc:`/sql/alter-table` SET PROPERTIES statement :pr:`21495`.
* Add :func:`google_polyline_decode` function to convert Google polyline to Presto ST_Geometry types. :pr:`23999`.
* Add :func:`google_polyline_encode` function to convert Presto ST_Geometry to Google polyline types. :pr:`23999`.
* Add Delete TableWriter TableFinish node to SPI :pr:`24088`.
* Add SemiJoin Join TableWriter Delete TableFinish node to connector optimizer :pr:`24154`.
* Add ``ClientRequestFilter.java`` interface in Presto-spi. :pr:`23380`.
* Add ``query-data-cache-enabled-default`` configuration property to align C++ cache behavior with Java. Set it to ``true``(default) for current C++ behavior or to ``false`` to match Java's cache logic. :pr:`24076`.
* Add a configuration property ``plan-checker.config-dir`` to set the configuration directory for PlanCheckerProvider configurations. :pr:`23955`.
* Add a session property `include_values_node_in_connector_optimizer` to enable connector optimizer optimize plans with values node  :pr:`12345`.
* Add an optional input distribution constraint to DeleteNode :pr:`24104`.
* Add an session property to not enforce input partition for join build :pr:`24163`.
* Add catalog and schema level access checks in :doc:`/sql/use` statement. :pr:`24182`.
* Add delete node in subfield pruning optimizer :pr:`24206`.
* Add single worker execution. To improve latency of tiny queries running on a large cluster, we introduce single worker execution mode: query will only use one node to execute and plan would be optimized accordingly. This feature can be turned on by config `single-node-execution-enabled` or session property `single_node_execution_enabled`.:pr:`24172`.
* Add support for ORC metadata cache invalidation based on file modification time. :pr:`24346`.
* Add support for ``ALTER VIEW RENAME TO`` operation, including the necessary infrastructure for connector implementations. :pr:`23749`.
* Add support for ``BigInt`` data type in the SQL Client on Presto UI on supported browsers. See `compatibility <https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/parse#browser_compatibility>`_ for the supported browsers. :pr:`24336`.
* Add support for the histogram statistic type. :pr:`22365`.
* Add support for time type partitioning in the ORC file format for Iceberg. :pr:`24091`.
* Add testing for partitioning using time type in Iceberg. :pr:`24091`.
* Added catalog and schema level access checks in USE statement :pr:`23882 `.
* Remove ``query-data-cache-enabled-default`` configuration property, which is no longer needed as per-split fine-grained cache control has been introduced. :pr:`24372 `.
* Remove the ``experimental.table-writer-merge-operator-enabled` config property and the ``table_writer_merge_operator_enabled`` session property :pr:`12345`.
* Deprecated  native_query_trace_task_reg_exp session property from Prestissimo:pr:`24270`.
* Enable ``scale-writers`` by default. :pr:`24107`.
* Support automatic scaling of writer threads for partitioned tables. Can be enabled with the `native_execution_scale_partitioned_writer_threads_enabled` session property. Native execution only.  :pr:`24155`.
* Throw exception on invalid http headers in async page transport servlet. :pr:`24004`.
* Update usage of MD5 to SHA256 :pr:`23903`.
* Upgrade avro to version 1.11.4 in response to `CVE-2024-47561 <https://github.com/advisories/GHSA-r7pg-v2c8-mfg3>`_. :pr:`23868`.
* Upgrade grpc dependencies to version 1.68.0 in response to `CVE-2022-25647 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25647>` :pr:`24051`.
* Upgrade gson from version 2.8.9 to v2.11.0 pr: `24247`.
* Upgrade gson to version 2.8.9 in response to `CVE-2022-25647 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25647>` :pr:`24051`.
* Upgrade jetty dependencies to version 9.4.56.v20240826 in response to `CVE-2024-8184 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-8184>`_. :pr:`24184`.
* Upgrade netty dependencies to version 4.1.115.Final in response to `CVE-2024-47535 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47535>`_. :pr:`24137`.

Security Changes
________________
* Fix `CVE-2015-3192 <https://www.mend.io/vulnerability-database/CVE-2015-3192>`_. :pr:`24112`.
* Fix `CVE-2015-5211 <https://www.mend.io/vulnerability-database/CVE-2015-5211>`_. :pr:`24112`.
* Fix `CVE-2016-1000027 <https://www.mend.io/vulnerability-database/CVE-2016-1000027>`_. :pr:`24112`.
* Fix `CVE-2018-1199 <https://www.mend.io/vulnerability-database/CVE-2018-1199>`_. :pr:`24112`.
* Fix `CVE-2018-1272 <https://www.mend.io/vulnerability-database/CVE-2018-1272>`_. :pr:`24112`.
* Fix `CVE-2020-5421 <https://www.mend.io/vulnerability-database/CVE-2020-5421>`_. :pr:`24112`.
* Fix `CVE-2021-0170 <https://www.mend.io/vulnerability-database/CVE-2021-0170>`_. :pr:`24112`.
* Fix `CVE-2021-22060 <https://www.mend.io/vulnerability-database/CVE-2021-22060>`_. :pr:`24112`.
* Fix `CVE-2021-22096 <https://www.mend.io/vulnerability-database/CVE-2021-22096>`_. :pr:`24112`.
* Fix `CVE-2021-22096 <https://www.mend.io/vulnerability-database/CVE-2021-22096>`_. :pr:`24112`.
* Fix `CVE-2022-22965 <https://www.mend.io/vulnerability-database/CVE-2022-22965>`_. :pr:`24112`.
* Fix `CVE-2022-22970 <https://www.mend.io/vulnerability-database/CVE-2022-22970>`_. :pr:`24112`.
* Fix `CVE-2022-22970 <https://www.mend.io/vulnerability-database/CVE-2022-22970>`_. :pr:`24112`.
* Fix `CVE-2022-27772 <https://www.mend.io/vulnerability-database/CVE-2022-27772>`_. :pr:`24112`.
* Fix `CVE-2023-20883 <https://www.mend.io/vulnerability-database/CVE-2023-20883>`_. :pr:`24112`.
* Fix `CVE-2024-22243 <https://www.mend.io/vulnerability-database/CVE-2024-22243>`_. :pr:`24112`.
* Fix `CVE-2024-22259 <https://www.mend.io/vulnerability-database/CVE-2024-22259>`_. :pr:`24112`.
* Fix `CVE-2024-22262 <https://www.mend.io/vulnerability-database/CVE-2024-22262>`_. :pr:`24112`.
* Fix `CVE-2024-38809 <https://www.mend.io/vulnerability-database/CVE-2024-38809>`_. :pr:`24112`.
* Fix `CVE-2024-6763 <https://www.mend.io/vulnerability-database/CVE-2024-6763>`_. :pr:`24112`.
* Fix `CVE-2024-6763 <https://www.mend.io/vulnerability-database/CVE-2024-6763>`_. :pr:`24112`.
* Fix `CVE-2024-8184 <https://www.mend.io/vulnerability-database/CVE-2024-8184>`_. :pr:`24112`.
* Fix security vulnerability in presto-pinot-toolkit and presto-product-tests in response to `CVE-2020-0287 <https://nvd.nist.gov/vuln/detail/CVE-2020-0287>`_. :pr:`24249`.
* Fix security vulnerability in swagger-ui jar in response to 'CVE-2018-25031 <https://nvd.nist.gov/vuln/detail/CVE-2018-25031>' .  :pr:`24153`.
* Fix security vulnerability in swagger-ui jar in response to 'CVE-2018-25031 <https://nvd.nist.gov/vuln/detail/CVE-2018-25031>' .  :pr:`24199`.
* Improve pbkdf2 hashing using SHA-256 cipher in response to `CWE-759 <https://cwe.mitre.org/data/definitions/759.htm>`_. :pr:`24132`.
* Add re2j regex matching in QueryStateInfoResource to protect from ReDoS attacks in response to `CVE-2024-45296 <https://www.cve.org/CVERecord?id=CVE-2024-45296>`_. :pr:`24048`.
* Add security-related headers to the static resources serving from the Presto UI, including: ``Content-Security-Policy``, ``X-Content-Type-Options``. See reference docs `Content-Security-Policy <https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP>`_ and `X-Content-Type-Options <https://learn.microsoft.com/en-us/previous-versions/windows/internet-explorer/ie-developer/compatibility/gg622941(v=vs.85)>`_. :pr:`24272`.
* Add support for pluggable Custom Presto Authenticators :pr:`#24111`.
* Replace `alluxio-shaded-client` with `alluxio-core` libraries libraries in response to `CVE-2023-44981 <https://github.com/advisories/GHSA-7286-pgfv-vxvh>`_. :pr:`24231`.

Bigquery Connector Changes
__________________________
* Fix for BigQuery ``SELECT``. :pr:`23957`.

Cassandra Connector Changes
___________________________
* Improve cryptographic protocol in response to `java:S4423 <https://sonarqube.ow2.org/coding_rules?open=java%3AS4423&rule_key=java%3AS4423>`_. :pr:`24436`.

Delta Connector Changes
_______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`.

Hive Connector Changes
______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`.

Hudi Connector Changes
______________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`.

Iceberg Connector Changes
_________________________
* Add ``catalog.system.invalidate_metastore_cache`` procedure to invalidate all, or portions of, the metastore cache. :pr:`23401`.
* Add configuration property ``iceberg.rest.nested.namespace.enabled`` to support nested namespaces in Iceberg's REST Catalog. Defaults to ``true``. :pr:`24083`.
* Add support for ``ALTER VIEW RENAME TO``. :pr:`23749`.
* Add support of ``view`` for Iceberg connector when configured with ``REST`` and ``NESSIE``. :pr:`23793`.
* Add support of specifying table location on creation for Iceberg connector when configured with ``REST`` and ``NESSIE``. :pr:`24218`.
* Remove in-memory hive metastore cache in Iceberg connector. :pr:`24326`.
* Optimize redundant getTable calls in Iceberg Connector :pr:`24376 `.

Memory Connector Changes
________________________
* Add support for ``ALTER VIEW RENAME TO``. :pr:`23749`.

Mongodb Connector Changes
_________________________
* Add steps to connect to MongoDB cluster with TLS CA File to :doc:`/connector/mongodb`. :pr:`24352`.

SPI Changes
___________
* Improve ExpressionOptimizer#optimize method with a variable resolver to return ``RowExpression``. :pr:`24287`.
* Add WindowNode, JoinNode, SemiJoinNode, MergeJoinNode, and SpatialJoinNode to the SPI. :pr:`23976`.
* Add a partition by attribute to specify the scope of sort node. :pr:`24095`.
* Remove ConnectorJoinNode from the SPI. JoinNode can now be used instead. :pr:`23976`.
* Remove experimental getPreferredShuffleLayout methods from the connector metadata in favor of existing `getNewTableLayout`, `getInsertLayout` methods :pr:`12345`.
* Modify the signature of ``PlanCheckerProviderFactory.create`` to pass in a map of configuration properties and replace ``SimplePlanFragmentSerde`` with a ``PlanCheckerProviderContext``. :pr:`23955`.

Prestissimo (native Execution) Changes
______________________________________
* Improve partitioned remote exchanges for wide data sets (more than 500 columns) to use row wise encoding. :pr:`23929 `.
* Add native plan checker to the native sidecar plugin and native endpoint for Velox plan conversion. :pr:`23596`.
* Add session properties ``native_spill_prefixsort_enabled``, ``native_prefixsort_normalized_key_max_bytes ``, and ``native_prefixsort_min_rows ``. :pr:`24043`.

**Credits**
===========

Abe Varghese, Abhisek Saikia, Ajay Kharat, Amit Dutta, Anant Aneja, Andrii Rosa, Ann-Megha-Rajesh1, Arjun Gupta, Bryan Cutler, Christian Zentgraf, Deepa-George, Deepak Majeti, Deepak Mehra, Denodo Research Labs, Dilli-Babu-Godari, Emanuel F., Emily Chan, Facebook Community Bot, Feilong Liu, Ge Gao, Georg Schäfer, Hazmi, Heidi Han, Jacob Khaliqi, Jalpreet Singh Nanda (:imjalpreet), Jalpreet Singh Nanda (:imjalpreet), Jeremy Quirke, Jialiang Tan, Jiaqi Zhang, Joe Abraham, Ke, Kevin Tang, Konjac Huang, Leonid Chistov, Linsong Wang, Luís Fernandes, MariamAlmesfer, Matthew Peveler, Minhan Cao, Natasha Sehgal, Naveen Mahadevuni, Nikhil Collooru, Nishitha-Bhaskaran, NivinCS, Pramod Satya, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Richard Barnes, Serge Druzkin, Sergey Pershin, Shakyan Kushwaha, Shang Ma, Shijin, Steve Burnett, SthuthiGhosh9400, Sumi Mathew, Tim Meehan, Xiao Du, Xiaoxuan Meng, Yihong Wang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, auden-woolfson, dependabot[bot], jaystarshot, pratyakshsharma, unidevel, wangd, wypb, zuyu
