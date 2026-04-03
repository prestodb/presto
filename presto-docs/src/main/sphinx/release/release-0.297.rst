=============
Release 0.297
=============

**Breaking Changes**
====================
* Add support for procedure calls in access control. For existing access controls (file-based, ranger, and sql-standard), procedure calls are restricted by default. See :doc:`/connector/hive-security` for details on how to allow them. `#26803 <https://github.com/prestodb/presto/pull/26803>`_
* Remove implicit bundling of SQL invoked function plugins from the default Presto server Provisio build. `#26926 <https://github.com/prestodb/presto/pull/26926>`_

**Highlights**
==============
* Add support for :ref:`predicate stitching <admin/materialized-views:Predicate Stitching (USE_STITCHING Mode)>` in MaterializedViewRewrite. `#26728 <https://github.com/prestodb/presto/pull/26728>`_
* Add single-table multi-statement writes :ref:`transaction <connector/iceberg:Transaction support>` on snapshot isolation level. `#25003 <https://github.com/prestodb/presto/pull/25003>`_
* Add support for ``MERGE`` command in the Iceberg connector. `#25470 <https://github.com/prestodb/presto/pull/25470>`_
* Add support for :doc:`/sql/create-vector-index`, which creates vector search indexes on table columns with configurable index properties and partition filtering using an ``UPDATING FOR`` clause. `#27307 <https://github.com/prestodb/presto/pull/27307>`_
* Add :doc:`/connector/lance` for reading and writing LanceDB datasets. `#27185 <https://github.com/prestodb/presto/pull/27185>`_
* Upgrade Iceberg version to 1.10.0. `#26879 <https://github.com/prestodb/presto/pull/26879>`_

**Details**
===========

General Changes
_______________
* Fix ``DESCRIBE`` and ``SHOW COLUMNS`` queries hanging in ``PLANNING`` state on clusters with single-node execution enabled. `#27456 <https://github.com/prestodb/presto/pull/27456>`_
* Fix ``EXPLAIN TYPE IO`` to support columns with temporal types. `#26942 <https://github.com/prestodb/presto/pull/26942>`_
* Fix materialized view query optimizer by correctly resolving table references to schema-qualified names. `#27059 <https://github.com/prestodb/presto/pull/27059>`_
* Fix Materialized Views with ``DEFINER`` rights to require ``CREATE_VIEW_WITH_SELECT_COLUMNS`` on base tables. `#26902 <https://github.com/prestodb/presto/pull/26902>`_
* Fix a bug where adaptive partial aggregation could incorrectly bypass INTERMEDIATE aggregation steps in the optimizer. `#27290 <https://github.com/prestodb/presto/pull/27290>`_
* Fix a bug where queries could get permanently stuck in resource groups when coordinator task-based throttling (``experimental.max-total-running-task-count-to-not-execute-new-query``) is enabled. `#27146 <https://github.com/prestodb/presto/pull/27146>`_
* Fix infinite loop in ``UnaliasSymbolReferences`` when alias mapping contains a cycle caused by multiple variables mapped to the same constant expression across different ProjectNodes. `#27428 <https://github.com/prestodb/presto/pull/27428>`_
* Fix to modify the Content-Type of endpoint ``/v1/info/metrics`` to ``application/json`` or ``text/plain`` based on the request's ``ACCEPT`` header. `#26639 <https://github.com/prestodb/presto/pull/26639>`_
* Improve ``LIKE '%substring%'`` pattern matching by rewriting to ``STRPOS`` instead of ``CARDINALITY(SPLIT(...))``, improving CPU and memory efficiency. `#27311 <https://github.com/prestodb/presto/pull/27311>`_
* Improve size estimates for constant variables. `#27188 <https://github.com/prestodb/presto/pull/27188>`_
* Add DDL statements for ``CREATE BRANCH``. `#26898 <https://github.com/prestodb/presto/pull/26898>`_
* Add DDL support for dropping a branch from a table. `#23614 <https://github.com/prestodb/presto/pull/23614>`_
* Add DDL support for dropping a tag from a table. `#23614 <https://github.com/prestodb/presto/pull/23614>`_
* Add HTTP support to the resource manager. See :ref:`admin/properties:\`\`resource-manager.http-server-enabled\`\`` and :ref:`admin/properties:\`\`internal-communication.resource-manager-communication-protocol\`\``. `#26635 <https://github.com/prestodb/presto/pull/26635>`_
* Add OpenLineage event listener plugin for emitting query lifecycle events in the OpenLineage format. The plugin supports console and HTTP transports, configurable query type filtering, and column-level lineage tracking. See :doc:`/develop/openlineage-event-listener` for configuration details. `#27249 <https://github.com/prestodb/presto/pull/27249>`_
* Add ``PushdownThroughUnnest`` optimizer rule that pushes projections and filter conjuncts not dependent on unnest output variables below the UnnestNode, gated by the ``pushdown_through_unnest`` session property (default enabled). `#27125 <https://github.com/prestodb/presto/pull/27125>`_
* Add ``USE_STITCHING`` mode for ``materialized_view_stale_read_behavior`` session property to selectively recompute stale data instead of full recomputation. `#26728 <https://github.com/prestodb/presto/pull/26728>`_
* Add ``materialized_view_force_stale`` session property for testing stale read behavior. `#26728 <https://github.com/prestodb/presto/pull/26728>`_
* Add ``materialized_view_staleness_window`` session property to configure acceptable staleness duration. `#26728 <https://github.com/prestodb/presto/pull/26728>`_
* Add a description field in function metadata. `#26843 <https://github.com/prestodb/presto/pull/26843>`_
* Add a new ``http-server.https.keystore.scan-interval-seconds`` configuration flag to scan the keystore file periodically for new certs. `#26739 <https://github.com/prestodb/presto/pull/26739>`_
* Add ability to disable the UI. This can be toggled with the ``webui-enabled`` configuration property. `#26682 <https://github.com/prestodb/presto/pull/26682>`_
* Add an optimizer which can do function rewrite. The name of the expression optimizer is specified with session property ``expression_optimizer_in_row_expression_rewrite`` which defaults to disabled when it's empty. `#26859 <https://github.com/prestodb/presto/pull/26859>`_
* Add comprehensive JMX metrics for metadata operations. `#26875 <https://github.com/prestodb/presto/pull/26875>`_
* Add configurable freshness thresholds for materialized views using :ref:`admin/properties-session:\`\`materialized_view_stale_read_behavior\`\`` session property and :ref:`admin/properties:\`\`materialized-view-stale-read-behavior\`\`` configuration property. `#26764 <https://github.com/prestodb/presto/pull/26764>`_
* Add cost-based selection for materialized view query rewriting, choosing the lowest-cost plan when multiple views are applicable. This can be enabled with the ``materialized_view_query_rewrite_cost_based_selection_enabled`` session property. `#27222 <https://github.com/prestodb/presto/pull/27222>`_
* Add expression simplification rule to flatten nested ``IF`` expressions: ``IF(x, IF(y, v, E), E)`` is rewritten to ``IF(x AND y, v, E)`` when the outer and inner else branches are identical. `#27267 <https://github.com/prestodb/presto/pull/27267>`_
* Add materialized CTE support for single node execution. `#26794 <https://github.com/prestodb/presto/pull/26794>`_
* Add optimizer rule ``SimplifyCoalesceOverJoinKeys`` that simplifies redundant ``COALESCE`` expressions over equi-join key pairs based on join type, enabling bucketed join optimizations for tool-generated queries. Controlled by the ``simplify_coalesce_over_join_keys`` session property (disabled by default). `#27250 <https://github.com/prestodb/presto/pull/27250>`_
* Add optimizer rule to pre-aggregate data before GroupId node in grouping sets queries, reducing row multiplication. Enabled via session property ``pre_aggregate_before_grouping_sets``. `#27290 <https://github.com/prestodb/presto/pull/27290>`_
* Add optimizer rule ``SimplifyAggregationsOverConstant`` that simplifies aggregations over constant inputs (MIN, MAX, ARBITRARY, APPROX_DISTINCT) to constants. Controlled by the ``simplify_aggregations_over_constant`` session property (disabled by default). `#27246 <https://github.com/prestodb/presto/pull/27246>`_
* Add optimizer rule ``PushSemiJoinThroughUnion`` that pushes a ``SemiJoinNode``, along with the underlying ``ProjectNode``, through a ``UnionNode``. Controlled by the ``optimizer.push-semi-join-through-union`` configuration property or the ``push_semi_join_through_union`` session property (disabled by default). `#27176 <https://github.com/prestodb/presto/pull/27176>`_
* Add options to control the number of tasks for remote project node. `#27044 <https://github.com/prestodb/presto/pull/27044>`_
* Add options to force shuffle table scan input if the number of files to be scanned is small. `#26941 <https://github.com/prestodb/presto/pull/26941>`_
* Add options to skip projection pushdown through exchange rule. `#26943 <https://github.com/prestodb/presto/pull/26943>`_
* Add support for ``America/Coyhaique`` timezone (Chile's Aysén Region). `#26981 <https://github.com/prestodb/presto/pull/26981>`_
* Add support for ``CREATE TABLE AS SELECT`` and ``INSERT`` from materialized views. `#27227 <https://github.com/prestodb/presto/pull/27227>`_
* Add support for :doc:`/sql/create-vector-index`, which creates vector search indexes on table columns with configurable index properties and partition filtering using an ``UPDATING FOR`` clause. `#27307 <https://github.com/prestodb/presto/pull/27307>`_
* Add the ``materialized_views`` table to the information schema. `#26688 <https://github.com/prestodb/presto/pull/26688>`_
* Add warning message on ``CREATE TABLE AS SELECT`` with ``IF NOT EXISTS``. `#27083 <https://github.com/prestodb/presto/pull/27083>`_
* Add SQL Support for ``ADD COLUMN DEFAULT``. `#27353 <https://github.com/prestodb/presto/pull/27353>`_
* Add support for ``WHEN MATCHED THEN DELETE`` clause in ``MERGE INTO`` statements, completing the SQL:2011 MERGE specification. `#27409 <https://github.com/prestodb/presto/pull/27409>`_
* Add MV data consistency support for ``CTAS`` and ``INSERT`` statements. `#27302 <https://github.com/prestodb/presto/pull/27302>`_
* Add DDL statements for ``CREATE TAG``. `#27113 <https://github.com/prestodb/presto/pull/27113>`_
* Replace ``experimental.max-total-running-task-count-to-not-execute-new-query`` with :ref:`admin/properties:\`\`max-total-running-task-count-to-not-execute-new-query\`\``. This is backwards compatible. `#27146 <https://github.com/prestodb/presto/pull/27146>`_
* Remove implicit bundling of SQL invoked function plugins from the default Presto server Provisio build. `#26926 <https://github.com/prestodb/presto/pull/26926>`_
* Update Session to serialize and deserialize selectedUser and reasonForSelect to SessionRepresentation, allowing ``INSERT`` and ``DELETE`` query sessions to contain these fields. `#27360 <https://github.com/prestodb/presto/pull/27360>`_
* Update timezone data to 2025b by upgrading to Joda-Time 2.14.0. `#26981 <https://github.com/prestodb/presto/pull/26981>`_

Prestissimo (Native Execution) Changes
______________________________________
* Add Window filter pushdown in native engine for ``rank`` and ``dense_rank`` functions. Use session property ``optimizer.optimize-top-n-rank`` to enable the rewrite. `#24138 <https://github.com/prestodb/presto/pull/24138>`_
* Add TextReader support for tables in ``TEXTFILE`` format. `#25995 <https://github.com/prestodb/presto/pull/25995>`_
* Add ``native_max_target_file_size`` session property to control the maximum target file size for writers. When a file exceeds this size during writing, the writer will close the current file and start writing to a new file. See :ref:`connector/hive:hive session properties`. `#27054 <https://github.com/prestodb/presto/pull/27054>`_
* Add ``native_aggregation_compaction_bytes_threshold`` and ``native_aggregation_compaction_unused_memory_ratio`` session properties to control string compaction during global aggregation. `#26874 <https://github.com/prestodb/presto/pull/26874>`_
* Add ``native_merge_join_output_batch_start_size`` session property to control the initial output batch size for merge joins, with optional dynamic adjustment based on observed row sizes (disabled by default). `#27086 <https://github.com/prestodb/presto/pull/27086>`_
* Add a native expression optimizer for optimizing expressions in the sidecar. `#24602 <https://github.com/prestodb/presto/pull/24602>`_
* Add support for ``NativeFunctionHandle`` parsing. `#26948 <https://github.com/prestodb/presto/pull/26948>`_
* Add worker uptime metric ``presto_cpp.worker_runtime_uptime_secs`` to track worker process runtime. `#26979 <https://github.com/prestodb/presto/pull/26979>`_
* Add http endpoint ``v1/expressions`` in sidecar for expression optimization. See :doc:`/presto_cpp/sidecar`. `#26475 <https://github.com/prestodb/presto/pull/26475>`_
* Add ``ssd-cache-max-entries`` config to limit SSD cache entries and prevent unbounded metadata memory growth (default value: 10M entries, ~500MB metadata). `#26795 <https://github.com/prestodb/presto/pull/26795>`_

Security Changes
________________
* Add a temporary configuration property ``hive.restrict-procedure-call`` for ranger and sql-standard access control. It defaults to ``true``, meaning procedure calls are restricted. To allow procedure calls, set this configuration property to ``false``. `#26803 <https://github.com/prestodb/presto/pull/26803>`_
* Add fine-grained access control for procedure calls in the file-based access control system. `#26803 <https://github.com/prestodb/presto/pull/26803>`_
* Add support for procedure calls in access control. `#26803 <https://github.com/prestodb/presto/pull/26803>`_
* Remove ``http: https:`` in response to `CWE-693 <https://cwe.mitre.org/data/definitions/693.html>`_. :pr:`25910`. `#26790 <https://github.com/prestodb/presto/pull/26790>`_
* Update aircompressor dependency from 0.27 to version 2.0.2 to fix `CVE-2025-67721 <https://www.cve.org/CVERecord?id=CVE-2025-67721>`_. `#27152 <https://github.com/prestodb/presto/pull/27152>`_
* Upgrade Druid to version 35.0.1 to address `CVE-2024-53990 <https://github.com/advisories/GHSA-mfj5-cf8g-g2fv>`_ and `CVE-2025-12183 <https://github.com/advisories/GHSA-vqf4-7m7x-wgfc>`_. `#26820 <https://github.com/prestodb/presto/pull/26820>`_
* Upgrade Netty to version 4.2.12.Final to address `CVE-2026-33871 <https://github.com/advisories/GHSA-w9fj-cfpg-grvv>`_ and `CVE-2025-67735 <https://github.com/advisories/GHSA-84h7-rjj3-6jx4>`_. `#27464 <https://github.com/prestodb/presto/pull/27464>`_
* Upgrade Rhino to version 1.8.1 to address `CVE-2025-66453 <https://github.com/advisories/GHSA-3w8q-xq97-5j7x>`_. `#26820 <https://github.com/prestodb/presto/pull/26820>`_
* Upgrade ``flatted`` from ``3.3.3`` to ``3.4.2`` in response to `GHSA-rf6f-7fwh-wjgh <https://github.com/WebReflection/flatted/security/advisories/GHSA-rf6f-7fwh-wjgh>`_ addressing a HIGH severity prototype pollution vulnerability ( `CWE-1321 <https://cwe.mitre.org/data/definitions/1321.html>`_ ) in the parse() function. This dependency is used by the UI development tooling and does not affect production runtime. `#27402 <https://github.com/prestodb/presto/pull/27402>`_
* Upgrade ``handlebars`` from ``4.7.8`` to ``4.7.9`` in response to multiple security advisories including `GHSA-2w6w-674q-4c4q <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-2w6w-674q-4c4q>`_, `GHSA-3mfm-83xf-c92r <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-3mfm-83xf-c92r>`_, `GHSA-xhpv-hc6g-r9c6 <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-xhpv-hc6g-r9c6>`_, `GHSA-xjpj-3mr7-gcpf <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-xjpj-3mr7-gcpf>`_, `GHSA-9cx6-37pm-9jff <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-9cx6-37pm-9jff>`_, `GHSA-2qvq-rjwj-gvw9 <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-2qvq-rjwj-gvw9>`_, `GHSA-7rx3-28cr-v5wh <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-7rx3-28cr-v5wh>`_, and `GHSA-442j-39wm-28r2 <https://github.com/handlebars-lang/handlebars.js/security/advisories/GHSA-442j-39wm-28r2>`_. This dependency is used by the ``ts-jest`` testing framework and does not affect production runtime. `#27447 <https://github.com/prestodb/presto/pull/27447>`_
* Upgrade ``webpack`` from ``5.97.1`` to ``5.104.1`` to address security vulnerabilities including a user information bypass in HttpUriPlugin and SSRF prevention improvements. This is a development dependency used for building the Presto UI and does not affect production runtime. `#27105 <https://github.com/prestodb/presto/pull/27105>`_
* Upgrade ajv to 8.18.0 in response to `CVE-2025-69873 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2025-69873>`_. `#27154 <https://github.com/prestodb/presto/pull/27154>`_
* Upgrade highlight version to 10.1.2 to address `CVE-2020-26237 <https://github.com/advisories/GHSA-vfrc-7r7c-w9mx>`_. `#26907 <https://github.com/prestodb/presto/pull/26907>`_
* Upgrade lodash from 4.17.21 to 4.17.23 to address `CVE-2025-13465 <https://github.com/advisories/GHSA-xxjr-mmjv-4gpg>`_. `#27009 <https://github.com/prestodb/presto/pull/27009>`_
* Upgrade lodash-es from 4.17.21 to 4.17.23 to address `CVE-2025-13465 <https://github.com/advisories/GHSA-xxjr-mmjv-4gpg>`_. `#27051 <https://github.com/prestodb/presto/pull/27051>`_
* Upgrade lz4-java to version 1.10.2 across connectors to address `CVE-2025-66566 <https://nvd.nist.gov/vuln/detail/CVE-2025-66566>`_ and `CVE-2025-12183 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2025-12183>`_. `#26931 <https://github.com/prestodb/presto/pull/26931>`_
* Upgrade mssql-jdbc to 13.2.1.jre11 in response to `CVE-2025-59250 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2025-59250>`_. `#26674 <https://github.com/prestodb/presto/pull/26674>`_
* Upgrade node-forge from 1.3.1 to 1.4.0 in response to multiple security advisories including `CVE-2026-33891 <https://www.cve.org/CVERecord?id=CVE-2026-33891>`_ (DoS in BigInteger.modInverse), `CVE-2026-33894 <https://www.cve.org/CVERecord?id=CVE-2026-33894>`_ (RSA-PKCS signature forgery), `CVE-2026-33895 <https://www.cve.org/CVERecord?id=CVE-2026-33895>`_ (Ed25519 signature forgery), and `CVE-2026-33896 <https://www.cve.org/CVERecord?id=CVE-2026-33896>`_ (basicConstraints bypass in certificate chain verification). This dependency is used by ``webpack-dev-server`` for development and does not affect production runtime. `#27448 <https://github.com/prestodb/presto/pull/27448>`_
* Upgrade org.apache.logging.log4j:log4j-core from from 2.24.3 to 2.25.3 to address `CVE-2025-68161 <https://nvd.nist.gov/vuln/detail/CVE-2025-68161>`_. `#26885 <https://github.com/prestodb/presto/pull/26885>`_
* Upgrade transitive dependency org.apache.logging.log4j:log4j-core to 2.25.3 to fix `CVE-2025-68161 <https://nvd.nist.gov/vuln/detail/CVE-2025-68161>`_. `#26906 <https://github.com/prestodb/presto/pull/26906>`_
* Upgrade webpack-dev-server from 5.2.0 to 5.2.1 to address security vulnerabilities in cross-origin request handling and WebSocket connections. The update enforces proper ``Access-Control-Allow-Origin`` header validation for cross-origin requests and restricts WebSocket connections from IP addresses in the ``Origin`` header unless explicitly configured via ``allowedHosts``. This dependency is used for local development only and does not affect production runtime. `#26275 <https://github.com/prestodb/presto/pull/26275>`_
* Upgrade zookeeper to version 3.9.5 in response to `CVE-2026-24281 <https://github.com/advisories/GHSA-7xrh-hqfc-g7qr>`_, `CVE-2026-24308 <https://github.com/advisories/GHSA-crhr-qqj8-rpxc>`_. `#27319 <https://github.com/prestodb/presto/pull/27319>`_
* Upgrade Jetty to 12.0.29 in response to `CVE-2025-5115 <https://nvd.nist.gov/vuln/detail/CVE-2025-5115>`_. `#26739 <https://github.com/prestodb/presto/pull/26739>`_

Web UI Changes
______________
* Add support for the ``MERGE`` statement in the Presto SQL Client web app. `#26825 <https://github.com/prestodb/presto/pull/26825>`_

Cassandra Connector Changes
___________________________
* Drop stale tables if table creation process fails. `#27100 <https://github.com/prestodb/presto/pull/27100>`_

Delta Lake Connector Changes
____________________________
* Add support to show the external table location of Delta tables when running the ``SHOW CREATE TABLE`` command. `#26986 <https://github.com/prestodb/presto/pull/26986>`_
* Upgrade AWS Glue Client to AWS SDK v2. `#26670 <https://github.com/prestodb/presto/pull/26670>`_

Druid Connector Changes
_______________________
* Add validation for schema names in Druid connector. `#26723 <https://github.com/prestodb/presto/pull/26723>`_

Hive Connector Changes
______________________
* Add support for custom TEXTFILE SerDe parameters ``textfile_field_delim``, ``textfile_escape_delim``, ``textfile_collection_delim``, and ``textfile_mapkey_delim``. `#27167 <https://github.com/prestodb/presto/pull/27167>`_
* Add support for fine-grained configuration of Hive metastore caches. `#26918 <https://github.com/prestodb/presto/pull/26918>`_
* Add support for ``skip_header_line_count`` and ``skip_footer_line_count``. See :ref:`connector/hive:avro configuration properties`. `#26446 <https://github.com/prestodb/presto/pull/26446>`_
* Upgrade AWS Glue Client to AWS SDK v2. `#26670 <https://github.com/prestodb/presto/pull/26670>`_

Hudi Connector Changes
______________________
* Upgrade AWS Glue Client to AWS SDK v2. `#26670 <https://github.com/prestodb/presto/pull/26670>`_

Iceberg Connector Changes
_________________________
* Improve partition loading for Iceberg tables by making it lazy, preventing unnecessary loading. `#23645 <https://github.com/prestodb/presto/pull/23645>`_
* Add ``INSERT`` operations into Iceberg V3 tables. `#27021 <https://github.com/prestodb/presto/pull/27021>`_
* Add Iceberg metadata table ``$metadata_log_entries``. `#24302 <https://github.com/prestodb/presto/pull/24302>`_
* Add ``CREATE BRANCH`` support for Iceberg. `#26898 <https://github.com/prestodb/presto/pull/26898>`_
* Add ``iceberg.materialized-view-max-changed-partitions`` config property (default: 100) to limit partition tracking for predicate stitching. `#26728 <https://github.com/prestodb/presto/pull/26728>`_
* Add ``stale_read_behavior`` and ``staleness_window`` table properties for materialized views. See :ref:`connector/iceberg:table properties`.   `#26764 <https://github.com/prestodb/presto/pull/26764>`_
* Add reading from Iceberg V3 tables, including partitioned tables. `#27021 <https://github.com/prestodb/presto/pull/27021>`_
* Add :ref:`connector/iceberg:rewrite manifests` procedure for Iceberg. `#26888 <https://github.com/prestodb/presto/pull/26888>`_
* Add single-table multi-statement writes :ref:`transaction <connector/iceberg:Transaction support>` on snapshot isolation level. `#25003 <https://github.com/prestodb/presto/pull/25003>`_
* Add support for ``MERGE`` command in the Iceberg connector. `#25470 <https://github.com/prestodb/presto/pull/25470>`_
* Add support for :ref:`connector/iceberg:materialized views` in Iceberg catalog. `#26958 <https://github.com/prestodb/presto/pull/26958>`_
* Add support for ``SMALLINT`` and ``TINYINT`` columns in presto-iceberg by mapping them to Iceberg ``INTEGER`` type. `#27461 <https://github.com/prestodb/presto/pull/27461>`_
* Add support for configuring access control in Iceberg using the ``iceberg.security`` property in the Iceberg catalog properties file. The supported types are ``allow-all`` and ``file``. See :ref:`connector/iceberg:authorization`. `#26803 <https://github.com/prestodb/presto/pull/26803>`_
* Add support for creating Iceberg tables with format-version = ``3``. `#27021 <https://github.com/prestodb/presto/pull/27021>`_
* Add support for dropping a branch from an Iceberg table. `#23614 <https://github.com/prestodb/presto/pull/23614>`_
* Add support for dropping a tag from an Iceberg table. `#23614 <https://github.com/prestodb/presto/pull/23614>`_
* Add support for fine-grained configuration of Hive metastore caches. `#26918 <https://github.com/prestodb/presto/pull/26918>`_
* Add support for mutating an Iceberg branch. `#27147 <https://github.com/prestodb/presto/pull/27147>`_
* Add support for tracking changed partitions in materialized views to enable predicate stitching optimization. `#26728 <https://github.com/prestodb/presto/pull/26728>`_
* Add support for upgrading existing V2 tables to V3 using the Iceberg API. `#27021 <https://github.com/prestodb/presto/pull/27021>`_
* Add SQL support for ``ADD COLUMN DEFAULT``. `#27353 <https://github.com/prestodb/presto/pull/27353>`_
* Add support for calling distributed procedure in Iceberg connector. `#26374 <https://github.com/prestodb/presto/pull/26374>`_
* Add :ref:`rewrite_data_files <connector/iceberg:Rewrite Data Files>` procedure in Iceberg connector. `#26374 <https://github.com/prestodb/presto/pull/26374>`_
* Add ``CREATE TAG`` support for Iceberg. `#27113 <https://github.com/prestodb/presto/pull/27113>`_
* Upgrade AWS Glue Client to AWS SDK v2. `#26670 <https://github.com/prestodb/presto/pull/26670>`_
* Upgrade Avro version to 1.12.0. `#26879 <https://github.com/prestodb/presto/pull/26879>`_
* Upgrade Iceberg version to 1.10.0. `#26879 <https://github.com/prestodb/presto/pull/26879>`_
* Upgrade Parquet version to 1.16.0. `#26879 <https://github.com/prestodb/presto/pull/26879>`_

Lance Connector Changes
_______________________
* Fix ClassCastException when reading Float16 columns by widening to Float32. `#27324 <https://github.com/prestodb/presto/pull/27324>`_
* Add :doc:`/connector/lance` for reading and writing LanceDB datasets. `#27185 <https://github.com/prestodb/presto/pull/27185>`_

Pinot Connector Changes
_______________________
* Add TLS support for self-signed certificate. `#26151 <https://github.com/prestodb/presto/pull/26151>`_
* Add validation for schema names in Pinot connector. `#26725 <https://github.com/prestodb/presto/pull/26725>`_
* Upgrade Apache Pinot to 1.4.0. `#26684 <https://github.com/prestodb/presto/pull/26684>`_

SPI Changes
___________
* Update SPI method ``Connector.beginTransaction`` in a backward compatible way to support passing the autocommit context into connector transactions. `#25003 <https://github.com/prestodb/presto/pull/25003>`_

Documentation Changes
_____________________
* Improve documentation of plugin loaded functions by grouping them in :ref:`functions/plugin-loaded-functions:array functions`. `#26926 <https://github.com/prestodb/presto/pull/26926>`_
* Add developer documentation for :doc:`/develop/table-functions`. `#27367 <https://github.com/prestodb/presto/pull/27367>`_
* Add documentation for :doc:`/functions/table`. `#27333 <https://github.com/prestodb/presto/pull/27333>`_
* Add documentation for Presto queries to run in Presto C++ to :doc:`/presto_cpp/limitations`. `#27120 <https://github.com/prestodb/presto/pull/27120>`_

**Credits**
===========

Aditi Pandit, Adrian Carpente (Denodo), Ajay Kharat, Alexey Matskoff, Allen Shen, Amit Dutta, Anant Aneja, Andrii Rosa, Apurva Kumar, Artem Selishchev, Auden Woolfson, Beinan, Chandrakant Vankayalapati, Chandrashekhar Kumar Singh, Christian Zentgraf, Deepak Majeti, Deepak Mehra, Denis Krivenko, Dilli-Babu-Godari, Dong Wang, Garima Uttam, Ge Gao, Han Yan, HeidiHan0000, Henry Dikeman, Ishaan Bansal, Ivan Ponomarev, Jalpreet Singh Nanda, Jay Feldblum, Jay Narale, Jiaqi Zhang, Joe Abraham, KNagaVivek, Karthikeyan, Ke, Ke Wang, Kevin Tang, Kiersten Stokes, Kyle Wong, Li, LingBin, Linsong Wang, Lithin Purushothaman, Madhavan, Maria Basmanova, Mariam AlMesfer, Matt Karrmann, Miguel Blanco Godón, Namya Sehgal, Natasha Sehgal, Naveen Mahadevuni, Nikhil Collooru, Nivin C S, PRASHANT GOLASH, Pedro Pedreira, Ping Liu, Prabhu Shankar, Pradeep Vaka, Pramod Satya, Pratik Joseph Dabre, Pratik Pugalia, Pratyaksh Sharma, Reetika Agrawal, Rui Mo, Saurabh Mahawar, Sayari Mukherjee, Sergey Pershin, Shahim Sharafudeen, Shang Ma, Shrinidhi Joshi, Simon Eves, Sreeni Viswanadha, Steve Burnett, Swapnil, Timothy Meehan, Vrindha Ramachandran, Vyacheslav Andreykiv, Wei He, XiaoDu, Xiaoxuan, Xin Zhang, Yihong Wang, Ying, Zac, adheer-araokar, bibith4, dependabot[bot], feilong-liu, iahs, inf, jja725, jkhaliqi, lexprfuncall, maniloya, mohsaka, nishithakbhaskaran, rdtr, shelton408, sumi-mathew, tanjialiang
