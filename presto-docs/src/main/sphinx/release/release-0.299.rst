=============
Release 0.299
=============

**Breaking Changes**
====================

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix BATCH-mode RPC dispatch dropping and under-batching rows by gathering RPC input to a single driver in the local plan. `#28230 <https://github.com/prestodb/presto/pull/28230>`_
* Fix incorrect epoch-second and nanosecond decomposition for pre-1970 (negative) timestamps in TimestampType; getEpochSecond and getNanos now use floor division instead of truncation toward zero. `#27935 <https://github.com/prestodb/presto/pull/27935>`_
* Fix the ``optimize_row_in_predicate`` optimization so it applies to constant-folded ``ROW(...) IN (...)`` predicates, enabling per-column predicate derivation and partition pruning that previously did not occur. `#27942 <https://github.com/prestodb/presto/pull/27942>`_
* Fix timestamp operations to match the SQL specification. The value of a TIMESTAMP type is not affected by the session time zone. `#24571 <https://github.com/prestodb/presto/pull/24571>`_
* Improve outer joins on skewed ``NULL`` join keys by spreading the null keys across partitions in their native type. This is controlled by the existing ``randomize_outer_join_null_key`` session property. `#28153 <https://github.com/prestodb/presto/pull/28153>`_
* Improve the ``optimize_cascading_filters_and_projections`` optimization to avoid duplicating multiply-referenced non-trivial expressions when coalescing cascading projections. `#28216 <https://github.com/prestodb/presto/pull/28216>`_
* Add JOIN support to the materialized view query optimizer. Queries that join a base table covered by a materialized view with another table can now be rewritten to scan the materialized view in place of the base table, subject to safety guards (matching GROUP BY, no aggregates over non-swapped tables, supported join types). `#27733 <https://github.com/prestodb/presto/pull/27733>`_
* Add ``scanRawInputDataSizeInBytes`` to task statistics, reporting the raw input data size read by table scan operators. `#28222 <https://github.com/prestodb/presto/pull/28222>`_
* Add ``scanRawInputDataSize`` to basic query statistics. `#28222 <https://github.com/prestodb/presto/pull/28222>`_
* Add `native_exchange_materialization_enabled` session property (Presto on Spark native codepath only) to control whether Velox native workers use MaterializedOutput/MaterializedExchange operators. When set to `true`, enables materialized exchange; when `false` (default), falls back to PartitionAndSerialize + ShuffleWrite. `#27881 <https://github.com/prestodb/presto/pull/27881>`_
* Add a driver-side metadata sidecar that registers native-only Velox functions into the Java planner at driver bootstrap. `#27698 <https://github.com/prestodb/presto/pull/27698>`_
* Add config properties for legacy ST_Equals behavior. `#27015 <https://github.com/prestodb/presto/pull/27015>`_
* Add configuration property ``server.startup-complete-required-for-active`` to report a node as not ready (``/v1/info`` ``starting`` and ``/v1/info/state``) until server startup has fully completed. Defaults to ``false``. `#28193 <https://github.com/prestodb/presto/pull/28193>`_
* Add createTimestampType(int precision) factory supporting TIMESTAMP precisions p=0–12, with instance interning and semantic helpers toEpochMillis, toEpochMicros, and fromEpochComponents. Part of parameterized TIMESTAMP(p) support (#27934). `#27935 <https://github.com/prestodb/presto/pull/27935>`_
* Add indirect column lineage (``JOIN``, ``WHERE``/``HAVING``, ``GROUP BY``, ``ORDER BY``, window, and conditional expressions from :pr:`27695`) to the OpenLineage event listener's ``columnLineage`` dataset facet, exposing it via ``InputField.transformations``. Direct-lineage emission is unchanged. See :doc:`/develop/openlineage-event-listener`. `#27994 <https://github.com/prestodb/presto/pull/27994>`_
* Add indirect column lineage tracking (JOIN, FILTER, GROUP BY, ORDER BY, CONDITIONAL) to query analysis, building on the direct column lineage added in #25913. Indirect relationships are exposed to event listeners via a new relationship-metadata field on ``OutputColumnMetadata``; existing direct-lineage consumers are unaffected. `#27695 <https://github.com/prestodb/presto/pull/27695>`_
* Add multi-arch (amd64 + arm64) support for presto-native and presto-native-dependency release Docker images, with the arm64 build using a portable -march=armv8-a+crc+crypto baseline for broad ARM CPU compatibility. `#28046 <https://github.com/prestodb/presto/pull/28046>`_
* Add session property ``grouped_execution_when_capable`` (default disabled) that, together with ``grouped_execution``, runs grouped execution for any bucketed grouped-execution-capable fragment even when no downstream operator makes grouping individually beneficial (for example a bucketed scan feeding a shuffle, or a bucketed-to-bucketed table write). This avoids re-partitioning already-bucketed data and bounds per-lifespan memory to a single bucket. :pr:`INSERT_PR_NUMBER`. `#28097 <https://github.com/prestodb/presto/pull/28097>`_
* Add session property ``pull_constant_projection_above_exchange`` (disabled by default) that pulls constant projection assignments above remote exchanges, avoiding serialization and shuffling of constant values across the network. :pr:`27499`. `#27499 <https://github.com/prestodb/presto/pull/27499>`_
* Add session property ``pull_row_local_chain_above_exchange_strategy`` (default ``DISABLED``) that pulls a chain of row-local operators (``UNNEST`` and deterministic projections) above a repartitioning remote exchange so the exchange shuffles the smaller pre-expansion input, reducing network shuffle. :pr:`28079`. `#28079 <https://github.com/prestodb/presto/pull/28079>`_
* Add support for ANSI SQL syntax in trim function. `#28190 <https://github.com/prestodb/presto/pull/28190>`_
* Add support for additional predicates on ``WHEN`` clauses in ``MERGE`` (``WHEN MATCHED AND <condition>``, ``WHEN NOT MATCHED AND <condition>``). `#27855 <https://github.com/prestodb/presto/pull/27855>`_
* Add support for an any-typed variadic tail in JSON-file-based function definitions. `#28207 <https://github.com/prestodb/presto/pull/28207>`_
* Add support for hyphenated struct field names in nested ROW type columns. `#27470 <https://github.com/prestodb/presto/pull/27470>`_
* Add support for publishing release Docker images to GitHub Container Registry (GHCR) via the REGISTRY repository variable. `#28010 <https://github.com/prestodb/presto/pull/28010>`_
* Add the ``optimize_cascading_filters_and_projections`` session property (config property ``optimizer.optimize-cascading-filters-and-projections``, default disabled). When enabled, the optimizer coalesces cascading projections by fully inlining deterministic child expressions and merges adjacent filter/projection nodes, co-locating shared subexpressions within a single operator so the native (Velox) backend's common-subexpression elimination can deduplicate them. `#28016 <https://github.com/prestodb/presto/pull/28016>`_
* Add the ``optimize_join_fan_out`` session property (config property ``optimizer.optimize-join-fan-out``, default disabled). When enabled, the optimizer collapses a fan-out equi-join whose preserved side is an aggregation grouped by a strict superset of the join keys: it packs the non-key columns with ``array_agg(row(...))`` so the join becomes unique on the join key, then re-expands them with a local ``UNNEST`` above the join, reducing the rows shuffled through the distributed join. `#27970 <https://github.com/prestodb/presto/pull/27970>`_
* Add the materialized view query optimizer to the inner query of ``CREATE TABLE AS`` and ``INSERT`` statements, not just bare ``SELECT``. `#27917 <https://github.com/prestodb/presto/pull/27917>`_
* Add validation to reject non-deterministic and session-time functions in ``CREATE MATERIALIZED VIEW`` definitions. `#28220 <https://github.com/prestodb/presto/pull/28220>`_
* Added optimizer rule ``parallelize_chained_aggregation`` (default: false) that inserts a local round-robin exchange to parallelize the outer PARTIAL in chained aggregations. `#27884 <https://github.com/prestodb/presto/pull/27884>`_
* Update ST_Equals function for empty geometries to return true regardless of geometry types. `#27015 <https://github.com/prestodb/presto/pull/27015>`_
* Update default value of `deprecated.legacy-timestamp` to false. `#24571 <https://github.com/prestodb/presto/pull/24571>`_

General Presto-on-Spark Changes
_______________________________
* Update the driver-side metadata sidecar registration of worker functions into the Airlift bootstrap. `#27699 <https://github.com/prestodb/presto/pull/27699>`_

Prestissimo (native Execution) Changes
______________________________________
* Add ``PRESTO_OPTIONAL_FEATURES`` build variable to enable or disable optional features using a comma-separated list (e.g., ``PRESTO_OPTIONAL_FEATURES="s3,hdfs,jwt,no-parquet" make release``). Default-ON features (``parquet``, ``spatial``) can be disabled with the ``no-`` prefix. Invalid feature names cause an immediate build error. `#28108 <https://github.com/prestodb/presto/pull/28108>`_
* Add an Arrow federation connector to run federated queries. `#26404 <https://github.com/prestodb/presto/pull/26404>`_
* Add registration for Presto-specific cuDF functions when cuDF is enabled in Presto native. `#28093 <https://github.com/prestodb/presto/pull/28093>`_
* Add support for setting gflags via ``config.properties`` using the ``gflag.`` prefix. Property names use hyphens in place of underscores (e.g., ``gflag.velox-memory-num-shared-leaf-pools=64``). Command-line flags take precedence over config values. See :doc:`/presto_cpp/properties` for the full list of supported gflag properties. `#28127 <https://github.com/prestodb/presto/pull/28127>`_
* Deprecate individual feature environment variables (e.g., ``PRESTO_ENABLE_S3``) in favor of ``PRESTO_OPTIONAL_FEATURES``. `#28108 <https://github.com/prestodb/presto/pull/28108>`_

Security Changes
________________
* Update Elasticsearch client dependencies to version 9 in response to `CVE-2024-52980 <https://github.com/advisories/GHSA-ghfh-p92w-j4mg>`_. `#25320 <https://github.com/prestodb/presto/pull/25320>`_
* Upgrade Netty to 4.2.15.Final to address `CVE-2026-44250 <https://github.com/advisories/GHSA-3244-j874-rhc2>`_. `#27966 <https://github.com/prestodb/presto/pull/27966>`_
* Upgrade Netty to 4.2.16.Final to address `CVE-2026-44891 <https://github.com/advisories/GHSA-vhch-2wf3-m8rp>`_. `#28169 <https://github.com/prestodb/presto/pull/28169>`_
* Upgrade async-http-client to 3.0.11 in response to `CVE-2026-55688  <https://nvd.nist.gov/vuln/detail/CVE-2026-55688>`_. `#28168 <https://github.com/prestodb/presto/pull/28168>`_
* Upgrade calcite version to 1.42.0 to address `CVE-2026-46718 <https://github.com/advisories/GHSA-c2rv-hwqm-wjpg>`_. `#28039 <https://github.com/prestodb/presto/pull/28039>`_
* Upgrade fast-uri version to 3.1.4 to address `CVE-2026-13676 <https://nvd.nist.gov/vuln/detail/CVE-2026-13676>`_. `#28202 <https://github.com/prestodb/presto/pull/28202>`_
* Upgrade highlight version to 10.4.1 to address `WS-2020-0208 <https://github.com/OSWeekends/miniestaciones/issues/9>`_. `#27956 <https://github.com/prestodb/presto/pull/27956>`_
* Upgrade jersey-bom version  to 2.41  in response to the use of an outdated version. `#25807 <https://github.com/prestodb/presto/pull/25807>`_
* Upgrade lz4-java to 1.11.1 to address CVE-2026-59949<https://github.com/advisories/GHSA-xx22-p4ch-683r>`_. `#28244 <https://github.com/prestodb/presto/pull/28244>`_
* Upgrade minimum Elasticsearch version from 6 to 9 (breaking) in response to `CVE-2024-52980 <https://github.com/advisories/GHSA-ghfh-p92w-j4mg>`_. `#25320 <https://github.com/prestodb/presto/pull/25320>`_
* Upgrade org.apache.logging.log4j to 2.25.5  to address `CVE-2026-49844 <https://github.com/advisories/GHSA-qv9r-c865-cp47>`_. `#28186 <https://github.com/prestodb/presto/pull/28186>`_

Cassandra Connector Changes
___________________________
* Upgrade to Cassandra Java Driver `4.x`. `#27029 <https://github.com/prestodb/presto/pull/27029>`_

Delta Lake Connector Changes
____________________________
* Add support for reading Variant columns as JSON columns in Presto when reading Parquet files. `#27552 <https://github.com/prestodb/presto/pull/27552>`_
* Add support for reading Variant data as JSON. `#27552 <https://github.com/prestodb/presto/pull/27552>`_

Hive Connector Changes
______________________
* Fix Parquet RLE and PLAIN dictionary decoding for decimals backed by byte arrays. `#28086 <https://github.com/prestodb/presto/pull/28086>`_
* Fix partition filter cache metrics association. **BREAKING**: Metric names changed from `partitionnamescache*` to `partitionfiltercache*`. Users monitoring these JMX metrics must update their dashboards, alerts, and scripts to use the new metric names. The old metrics tracked the partition filter cache (filtered partition queries), not the partition names cache, as the name suggested. `#27960 <https://github.com/prestodb/presto/pull/27960>`_
* Add Azure filesystem impl registration for ABFSS and WASB/S schemes. `#28054 <https://github.com/prestodb/presto/pull/28054>`_
* Add comprehensive cache metrics for all metastore caches. `#27960 <https://github.com/prestodb/presto/pull/27960>`_
* Add support for AWS Glue Table and Column Statistics. `#27112 <https://github.com/prestodb/presto/pull/27112>`_
* Add support for reading and writing per-column type attributes in DWRF files. `#27940 <https://github.com/prestodb/presto/pull/27940>`_
* Upgrade to Hive 4.0.1. `#24571 <https://github.com/prestodb/presto/pull/24571>`_

Iceberg Connector Changes
_________________________
* Fix DROP TABLE for Hive-backed Iceberg tables to properly delete all data and metadata files on S3 using Iceberg's CatalogUtil instead of relying on Hive metastore directory deletion. `#27938 <https://github.com/prestodb/presto/pull/27938>`_
* Fix timestamp-to-micros conversion and legacy-timezone adjustment in IcebergPageSink for pre-epoch timestamps, using the corrected TimestampType epoch helpers. `#27935 <https://github.com/prestodb/presto/pull/27935>`_
* Improve Iceberg table statistics computation by using snapshot-level total record counts instead of re-scanning manifests. `#28248 <https://github.com/prestodb/presto/pull/28248>`_
* Add Iceberg version validation to prevent silent data loss. `#27655 <https://github.com/prestodb/presto/pull/27655>`_
* Add configuration property ``iceberg.commit-number-retries`` to specify the default number of commit retries for newly created tables. `#28055 <https://github.com/prestodb/presto/pull/28055>`_
* Add explicit strategy parameter to rewrite_data_files procedure with binpack and sort strategies. Breaking change: Default behavior changed from sort to binpack for faster rewrites. Existing queries that rely on data ordering must add strategy => 'sort' to maintain previous behavior. Queries using sorted_by must now explicitly specify strategy => 'sort'. `#28092 <https://github.com/prestodb/presto/pull/28092>`_
* Add optional ``delete_data_on_drop`` parameter to the ``register_table`` procedure to control whether underlying Iceberg data is deleted when the registered table is dropped. `#27938 <https://github.com/prestodb/presto/pull/27938>`_
* Add proxy support for Iceberg REST catalogs. `#28217 <https://github.com/prestodb/presto/pull/28217>`_
* Add support for Basic Auth against the REST catalog server. `#28103 <https://github.com/prestodb/presto/pull/28103>`_
* Add support for iceberg write-default. `#27912 <https://github.com/prestodb/presto/pull/27912>`_
* Add support to enable TLS for REST catalog communication. `#28103 <https://github.com/prestodb/presto/pull/28103>`_
* Add read support for row lineage columns as per Iceberg V3 spec. `#27743 <https://github.com/prestodb/presto/pull/27743>`_

Lance Connector Changes
_______________________
* Fix catalog-wide table listing returning no tables for multi-level Lance catalogs (``lance.single-level-ns=false``). This affected ``information_schema.tables`` scans. `#28268 <https://github.com/prestodb/presto/pull/28268>`_
* Add `LIMIT` pushdown for Lance table scans to reduce rows read by Lance for simple limit queries. `#28049 <https://github.com/prestodb/presto/pull/28049>`_
* Add pluggable namespace support to the Lance connector using the lance-namespace API, enabling directory, REST, and custom namespace implementations. `#27481 <https://github.com/prestodb/presto/pull/27481>`_
* Add the ``lance.parent`` configuration property to select a parent namespace prefix for namespaces with three or more levels. `#27481 <https://github.com/prestodb/presto/pull/27481>`_
* Replace the ``lance.root-url`` configuration property with ``lance.root``. This is a breaking change: catalogs that set ``lance.root-url`` must be updated to ``lance.root`` before upgrading. `#27481 <https://github.com/prestodb/presto/pull/27481>`_

Oracle Connector Changes
________________________
* Add support for REAL datatype insert. `#28059 <https://github.com/prestodb/presto/pull/28059>`_

SPI Changes
___________
* Add ``OutputColumnMetadata.getColumnLineage()`` returning a unified ``Set<ColumnLineageEntry>`` that covers both DIRECT and INDIRECT lineage from :pr:`27695`, with direct entries carrying ``IDENTITY``, ``TRANSFORMATION``, or ``AGGREGATION`` subtypes derived from the SELECT-list expression. ``getSourceColumns()`` and ``getIndirectSourceColumns()`` are retained as derived views and marked ``@Deprecated``; existing event listeners and JSON consumers are unaffected. `#27995 <https://github.com/prestodb/presto/pull/27995>`_
* Add ``getScanRawInputBytes`` to ``QueryStatistics``. Note: this adds a required constructor argument; plugins that construct ``QueryStatistics`` directly must be updated. `#28222 <https://github.com/prestodb/presto/pull/28222>`_

**Credits**
===========

Aditi Pandit, Ajay Kharat, Allen Shen, Amit Dutta, Andrii Rosa, Apurva Kumar, Auden Woolfson, Ayasaz, Bryan Cutler, Chandrakant Vankayalapati, ChenXing Yang, Christian Zentgraf, Deepak Majeti, Deepak Mehra, Denis Krivenko, Denodo Research Labs, Dilli Babu Godari, Dong Wang, Henry Dikeman, Hongtao Yang, Jack Luo, Jalpreet Singh Nanda, Jianjian Xie, Joe Abraham, Kevin Tang, Madhavan, Maria Basmanova, Matt Gara, Miguel Blanco Godón, Natasha Sehgal, Neerad Somanchi, Nidhin Varghese, Nishitha K Bhaskaran, Nivin C S, Patrick Sullivan, Pramod Satya, Pratik Joseph Dabre, Reetika Agrawal, Sayari Mukherjee, Shahim Sharafudeen, Shakyan Kushwaha, Shreya, Shrinidhi Joshi, Sreeni Viswanadha, Steve Burnett, Timothy Meehan, Tirumala Saiteja Goruganthu, Vyacheslav Andreykiv, Yihong Wang, Ying, Zac, Zac Blanco, bcam-meta, bibith4, deepthibose01, dependabot[bot], feilong-liu, jkhaliqi, mohsaka, sumi-mathew, zhichenxu-meta
