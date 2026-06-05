=============
Release 0.298
=============

**Breaking Changes**
====================
* Fix query statistics so that `planningTime` and `finishingTime` are no longer added to `executionTime`. `executionTime` is now the true execution time — how long it took the query to run the compute. It can be used to measure the efficiency of the workers without added planning time or the time spent on final steps such as partition registration. `#27691 <https://github.com/prestodb/presto/pull/27691>`_
* Remove configuration property ``use-new-nan-definition``. `#27829 <https://github.com/prestodb/presto/pull/27829>`_
* Remove ``warn-on-common-nan-patterns`` server config and ``warn_on_common_nan_patterns`` session property. The NaN definition migration is complete and these warnings are no longer needed. `#27830 <https://github.com/prestodb/presto/pull/27830>`_
* Update the default behavior of ``field_names_in_json_cast_enabled`` from false to true. When ``field_names_in_json_cast_enabled = true``, JSON fields are assigned to ROW fields by matching field names regardless of their order in the JSON object. Queries that rely on JSON field order when casting to ROW may return different results after upgrading. If your workload depends on the previous positional behavior, restore it by setting: ``SET SESSION field_names_in_json_cast_enabled = false;``. `#26833 <https://github.com/prestodb/presto/pull/26833>`_

**Highlights**
==============
* Improve materialized view query rewriting to support ``HAVING`` clauses. `#27677 <https://github.com/prestodb/presto/pull/27677>`_
* Improve coordinator-to-worker communication efficiency with 20-40% smaller payload sizes and 2-3x faster serialization compared to JSON. `#27486 <https://github.com/prestodb/presto/pull/27486>`_
* Improve query planning performance for wide-column projections by adding fast paths that skip unnecessary processing for variable references, constants, and identity assignments across multiple optimizer rules. `#27547 <https://github.com/prestodb/presto/pull/27547>`_
* Add incremental refresh for materialized views in the Iceberg connector, enabling efficient partial refreshes instead of full recomputation. `#26959 <https://github.com/prestodb/presto/pull/26959>`_
* Add support for Azure Blob Storage (``wasb[s]://``) and Azure Data Lake Storage Gen2 (``abfs[s]://``) in the Hive connector, with shared key and OAuth2 authentication. `#25107 <https://github.com/prestodb/presto/pull/25107>`_
* Add ``ALTER MATERIALIZED VIEW <name> SET PROPERTIES (...)`` SQL statement to update materialized view properties after creation. `#27806 <https://github.com/prestodb/presto/pull/27806>`_
* Add TopN late materialization optimization for ``ORDER BY ... LIMIT`` over wide tables with a unique ``$row_id`` column, sorting only sort keys first and fetching full rows via SemiJoin. `#27641 <https://github.com/prestodb/presto/pull/27641>`_
* Add TLS/SSL configuration, i18n character set, and configurable JDBC fetch size support for the Oracle connector. `#27671 <https://github.com/prestodb/presto/pull/27671>`_ `#27670 <https://github.com/prestodb/presto/pull/27670>`_ `#27669 <https://github.com/prestodb/presto/pull/27669>`_
* Add read support for Iceberg V3 row lineage hidden columns `_row_id` and `_last_updated_sequence_number`. `#27240 <https://github.com/prestodb/presto/pull/27240>`_
* Add support for ``min/max/count`` aggregation push down based on file stats. This can be toggled with the ``aggregate_push_down_enabled`` session property or the ``iceberg.aggregate-push-down-enabled`` configuration property. See :ref:`connector/iceberg:session properties` and :ref:`connector/iceberg:configuration properties`. `#27085 <https://github.com/prestodb/presto/pull/27085>`_
* Add view querying capabilities and upgrade to mongodb-driver-sync in the MongoDB connector. `#26995 <https://github.com/prestodb/presto/pull/26995>`_ `#27685 <https://github.com/prestodb/presto/pull/27685>`_
* Add support for reading Delta Lake tables with column mapping enabled. `#27483 <https://github.com/prestodb/presto/pull/27483>`_

**Details**
===========

General Changes
_______________
* Fix query statistics so that `planningTime` and `finishingTime` are no longer added to `executionTime`. `executionTime` is now the true execution time — how long it took the query to run the compute. It can be used to measure the efficiency of the workers without added planning time or the time spent on final steps such as partition registration. `#27691 <https://github.com/prestodb/presto/pull/27691>`_
* Fix RPC options argument parsing to use the last argument instead of hardcoding to use the third argument. `#27700 <https://github.com/prestodb/presto/pull/27700>`_
* Fix `UnsupportedOperationException` when using `remote_function_names_for_fixed_parallelism` with queries containing ``UNION ALL`` below the remote function projection. `#27714 <https://github.com/prestodb/presto/pull/27714>`_
* Fix a bug in the `PushProjectionThroughCrossJoin` optimizer rule where cascading projections above a cross join could cause validation errors by dropping pushed variables from intermediate residual projects. `#27568 <https://github.com/prestodb/presto/pull/27568>`_
* Fix a gap in query commit for ``DELETE`` queries when running on Spark. `#26195 <https://github.com/prestodb/presto/pull/26195>`_
* Fix data correctness bugs in ``MaterializedViewQueryOptimizer`` where queries without ``GROUP BY`` could be incorrectly rewritten to use materialized views with ``GROUP BY``, producing fewer rows than expected. Previously, alias mismatches and scalar expression bypasses allowed invalid rewrites that silently collapsed duplicate rows. `#27778 <https://github.com/prestodb/presto/pull/27778>`_
* Fix materialized view query rewriting for ``CUBE``, ``ROLLUP``, and ``GROUPING SETS`` clauses. Column references inside these grouping elements are now correctly rewritten to materialized view columns. `#27538 <https://github.com/prestodb/presto/pull/27538>`_
* Fix a race condition in `pruneFinishedQueryInfo` causing task memory leak. `#27597 <https://github.com/prestodb/presto/pull/27597>`_
* Fix runtime type mismatch crashes in Velox native execution caused by non-deterministic HashMap iteration order in ``PreAggregateBeforeGroupId``, ``PushPartialAggregationThroughExchange``, and ``MultipleDistinctAggregationToMarkDistinct`` optimizer rules. `#27493 <https://github.com/prestodb/presto/pull/27493>`_
* Fix ``IllegalStateException`` during planning of ``ORDER BY ... LIMIT`` (TopN) queries over tables with a unique column. `#27664 <https://github.com/prestodb/presto/pull/27664>`_
* Fix coordinator memory leak caused by orphaned listener objects accumulating during scheduling cycles in ``HttpRemoteTaskWithEventLoop.whenSplitQueueHasSpace()``. `#27673 <https://github.com/prestodb/presto/pull/27673>`_
* Improve convergence speed of ``GROUP BY + LIMIT`` queries on partitioned tables by excluding partition keys from the `PrefilterForLimitingAggregation` prefilter. `#27678 <https://github.com/prestodb/presto/pull/27678>`_
* Improve `PrefilterForLimitingAggregation` optimizer to use scan limiting instead of timeouts for more predictable performance. The optimization now limits the source scan to ``1000 * LIMIT`` rows before applying ``DISTINCT LIMIT``. `#27819 <https://github.com/prestodb/presto/pull/27819>`_
* Improve plan efficiency for ``UNION ALL`` queries with empty branches (for example, branches pruned by partition or snapshot filtering) by removing those branches from the plan. `#27765
  <https://github.com/prestodb/presto/pull/27765>`_
* Improve efficiency of coordinator-to-worker communication with 20-40% smaller payload sizes and 2-3x faster serialization compared to JSON. `#27486 <https://github.com/prestodb/presto/pull/27486>`_
* Improve ``map_from_entries(ARRAY[ROW(...), ...])`` by rewriting to ``MAP(ARRAY[keys], ARRAY[values])`` at plan time, avoiding intermediate ROW construction. `#27491 <https://github.com/prestodb/presto/pull/27491>`_
* Improve logical planner performance for wide-column queries by indexing `RelationType.resolveFields()` for O(1) field lookup instead of O(N) linear scan. `#27553 <https://github.com/prestodb/presto/pull/27553>`_
* Improve query planning performance for wide-column projections by adding fast paths that skip unnecessary processing for variable references, constants, and identity assignments across multiple optimizer rules. `#27547 <https://github.com/prestodb/presto/pull/27547>`_
* Improve materialized view query rewriting to support ``HAVING`` clauses. `#27677 <https://github.com/prestodb/presto/pull/27677>`_
* Improve disjunction rewrite by adding ``ROW IN`` to disjunction rewrite to fire for all columns, not just partition keys, enabling better predicate pushdown and domain extraction. Gated behind session property ``rewrite_row_constructor_in_to_disjunction``. `#27680 <https://github.com/prestodb/presto/pull/27680>`_
* Add ``ALTER MATERIALIZED VIEW <name> SET PROPERTIES (...)`` SQL statement to update materialized view properties after creation. `#27806 <https://github.com/prestodb/presto/pull/27806>`_
* Add :ref:`admin/properties-session:\`\`push_aggregation_through_disjoint_union\`\`` session property (default off) that pushes a ``GROUP BY`` aggregation completely below ``UNION ALL`` when at least one grouping key has constant values that are pairwise distinct across the union branches, eliminating the final aggregation. `#27764 <https://github.com/prestodb/presto/pull/27764>`_
* Add ``rpc_dispatch_batch_size`` session property to control batch size for RPC dispatch in ``BATCH`` mode. Default: ``128``. A value of ``0`` collects all rows before dispatching. `#27700 <https://github.com/prestodb/presto/pull/27700>`_
* Add ``rpc_streaming_mode`` session property to control RPC function execution mode (``PER_ROW`` or ``BATCH``). Default: ``PER_ROW``. `#27700 <https://github.com/prestodb/presto/pull/27700>`_
* Add :ref:`admin/properties-session:\`\`partition_aware_grouped_execution\`\`` session property to schedule each (bucket, partition) as a separate lifespan in grouped execution, reducing per-lifespan data volumes for bucketed tables. Disabled by default. `#27663 <https://github.com/prestodb/presto/pull/27663>`_
* Add incremental refresh for materialized views. `#26959 <https://github.com/prestodb/presto/pull/26959>`_
* Add session property :ref:`admin/properties-session:\`\`join_prefilter_build_side_with_complex_probe_side\`\`` (default false) to extend join prefilter optimization to support complex probe-side patterns including UNION ALL, cross join, unnest, and aggregation. `#27598 <https://github.com/prestodb/presto/pull/27598>`_
* Add session property :ref:`admin/properties-session:\`\`rewrite_bucketed_semi_join_to_join\`\``  (default disabled) that rewrites bucketed semi-joins into joins to avoid a data shuffle. `#27510 <https://github.com/prestodb/presto/pull/27510>`_
* Add session property ``rewrite_row_constructor_in_to_disjunction`` (default disabled) that rewrites ROW IN ROW predicates into OR of AND equality chains when all ROW fields are partition keys, enabling per-column TupleDomain extraction for partition pruning. `#27500 <https://github.com/prestodb/presto/pull/27500>`_
* Add session property :ref:`admin/properties-session:\`\`always_analyze_create_table_query_enabled\`\`` to enable analyzing inner queries on ``CREATE TABLE AS SELECT IF NOT EXISTS`` statements when the target table already exists. `#27504 <https://github.com/prestodb/presto/pull/27504>`_
* Add support for ``ALTER TABLE ... ALTER COLUMN ... SET DEFAULT`` syntax to update Iceberg column write-default values. `#27810 <https://github.com/prestodb/presto/pull/27810>`_
* Add support for ``GROUP BY`` and ``ORDER BY`` ordinal references in materialized view query rewriting. Previously, queries like ``SELECT a, SUM(b) FROM t GROUP BY 1`` would silently skip materialized view optimization. `#27422 <https://github.com/prestodb/presto/pull/27422>`_
* Add support for scalar functions in materialized view query rewriting. Queries using functions like ``CONCAT``, ``ABS``, ``JSON_EXTRACT``, ``CAST``, ``IF``, ``COALESCE``, and ``CASE`` expressions now correctly rewrite to scan the materialized view. `#27549 <https://github.com/prestodb/presto/pull/27549>`_
* Add :ref:`admin/properties:\`\`cluster-overload.bypass-resource-groups\`\`` configuration property to allow named resource groups to bypass cluster-overload throttling while continuing to honor per-group concurrency, memory, and CPU limits. `#27642 <https://github.com/prestodb/presto/pull/27642>`_
* Add :ref:`admin/properties-session:\`\`optimize_row_in_predicate\`\`` session property (default off) that rewrites multi-column ``ROW IN`` / ``ROW NOT IN`` predicates to expose per-column ``IN`` / ``NOT IN`` predicates, enabling partition pruning and other domain-based optimizations. `#27708 <https://github.com/prestodb/presto/pull/27708>`_
* Add :ref:`admin/properties-session:\`\`push_filter_through_selecting_aggregation\`\`` session property and ``optimizer.push-filter-through-selecting-aggregation`` configuration property (default ``false``) to push HAVING predicates beneath single-value aggregates (MAX/MIN/ARBITRARY) for earlier row reduction. `#27712 <https://github.com/prestodb/presto/pull/27712>`_
* Add TopN late materialization optimization for ``ORDER BY ... LIMIT`` over wide tables with a unique ``$row_id`` column. Sorts only sort keys plus ``$row_id`` first, then fetches full rows via SemiJoin. `#27641 <https://github.com/prestodb/presto/pull/27641>`_
* Add ``split_part_reverse`` as a global Presto SQL function, replacing the Velox C++ UDF with a SQL-invoked scalar function available in all queries. `#27480 <https://github.com/prestodb/presto/pull/27480>`_
* Remove configuration property ``use-new-nan-definition``. `#27829 <https://github.com/prestodb/presto/pull/27829>`_
* Remove ``warn-on-common-nan-patterns`` server config and ``warn_on_common_nan_patterns`` session property. The NaN definition migration is complete and these warnings are no longer needed. `#27830 <https://github.com/prestodb/presto/pull/27830>`_
* Update the default behavior of ``field_names_in_json_cast_enabled`` from false to true. When ``field_names_in_json_cast_enabled = true``, JSON fields are assigned to ROW fields by matching field names regardless of their order in the JSON object. Queries that rely on JSON field order when casting to ROW may return different results after upgrading. If your workload depends on the previous positional behavior, restore it by setting: ``SET SESSION field_names_in_json_cast_enabled = false;``. `#26833 <https://github.com/prestodb/presto/pull/26833>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix MaterializedOutput operator lifecycle bugs: silent data loss on ``noMoreData()`` exceptions, Velox contract violation crashes during OOM teardown, and missing ``MemoryReclaimer`` causing memory arbitration failures. `#27833 <https://github.com/prestodb/presto/pull/27833>`_
* Add support for iceberg V3 initialDefaultValue. `#27767 <https://github.com/prestodb/presto/pull/27767>`_
* Add support for plugin-registered custom types (such as those from the MongoDB and ML plugins) in native clusters. `#27748 <https://github.com/prestodb/presto/pull/27748>`_
* Add ``native_min_shuffle_compression_page_size_bytes`` session property to tune the small-page shuffle-compression skip threshold. `#27683 <https://github.com/prestodb/presto/pull/27683>`_

Security Changes
________________
* Add optional authorizedPrincipal to AuthorizedIdentity to support gateway identity propagation, allowing the session principal to reflect the original client instead of the gateway. `#27639 <https://github.com/prestodb/presto/pull/27639>`_
* Upgrade Netty to 4.2.13.Final in response to `CVE-2026-41417  <https://github.com/advisories/GHSA-fghv-69vj-qj49>`_, `CVE-2026-44248  <https://github.com/advisories/GHSA-jfg9-48mv-9qgx>`_, `CVE-2026-42577  <https://github.com/advisories/GHSA-rwm7-x88c-3g2p>`_, `CVE-2026-42578  <https://github.com/advisories/GHSA-45q3-82m4-75jr>`_, `CVE-2026-42579  <https://github.com/advisories/GHSA-cm33-6792-r9fm>`_, `CVE-2026-42580  <https://github.com/advisories/GHSA-m4cv-j2px-7723>`_, `CVE-2026-42581  <https://github.com/advisories/GHSA-xxqh-mfjm-7mv9>`_, `CVE-2026-42582  <https://github.com/advisories/GHSA-2c5c-chwr-9hqw>`_, `CVE-2026-42583  <https://github.com/advisories/GHSA-mj4r-2hfc-f8p6>`_, `CVE-2026-42584  <https://github.com/advisories/GHSA-57rv-r2g8-2cj3>`_, `CVE-2026-42585  <https://github.com/advisories/GHSA-38f8-5428-x5cv>`_, `CVE-2026-42586  <https://github.com/advisories/GHSA-rgrr-p7gp-5xj7>`_, and `CVE-2026-42587  <https://github.com/advisories/GHSA-f6hv-jmp6-3vwv>`_. `#27769 <https://github.com/prestodb/presto/pull/27769>`_
* Upgrade async-http-client to version 3.0.9 to address `CVE-2026-40490 <https://github.com/advisories/GHSA-cmxv-58fp-fm3g>`_. `#27613 <https://github.com/prestodb/presto/pull/27613>`_
* Upgrade google-oauth-client version to 1.34.1 to address `CVE-2020-7692 <https://github.com/advisories/GHSA-f263-c949-w85g>`_ and `CVE-2021-22573 <https://github.com/advisories/GHSA-hw42-3568-wj87>`_. `#25424 <https://github.com/prestodb/presto/pull/25424>`_
* Upgrade http-proxy-middleware from 2.0.7 to 2.0.9  in /presto-ui/src to resolve `CVE-2025-32996 <https://nvd.nist.gov/vuln/detail/CVE-2025-32996>`_. `#27715 <https://github.com/prestodb/presto/pull/27715>`_
* Upgrade jackson dependency from 2.15.4 to version 2.18.6 to address `GHSA-72hv-8253-57qq <https://github.com/advisories/GHSA-72hv-8253-57qq>`_. `#27293 <https://github.com/prestodb/presto/pull/27293>`_
* Upgrade jetty dependency from 0.27 to version 2.0.2 to address `CVE-2025-11143 <https://github.com/advisories/GHSA-wjpw-4j6x-6rwh>`_ and `CVE-2026-1605 <https://github.com/advisories/GHSA-xxh7-fcf3-rj7f>`_. `#27294 <https://github.com/prestodb/presto/pull/27294>`_
* Upgrade libthrift 0.23.0 in response to `CVE-2026-41604 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2026-41604>`_. `#27777 <https://github.com/prestodb/presto/pull/27777>`_
* Upgrade lodash from 4.17.23 to 4.18.1 to address multiple security vulnerabilities: - `CVE-2026-4800 <https://nvd.nist.gov/vuln/detail/CVE-2026-4800>`_. This dependency is used for local development only and does not affect production runtime. `#27497 <https://github.com/prestodb/presto/pull/27497>`_
* Upgrade lodash-es from 4.17.23 to 4.18.1 to address `CVE-2026-4800 <https://nvd.nist.gov/vuln/detail/CVE-2026-4800>`_. This dependency is used for local development only and does not affect production runtime. `#27496 <https://github.com/prestodb/presto/pull/27496>`_
* Upgrade org.apache.kafka:kafka-clients from 3.9.1 to 3.9.2 in order to address `CVE-2026-35554 <https://github.com/advisories/GHSA-5qcv-4rpc-jp93>`_. `#27574 <https://github.com/prestodb/presto/pull/27574>`_
* Upgrade org.apache.logging.log4j:log4j-core from 2.25.3 to 2.25.4 in order to address `CVE-2026-34480 <https://nvd.nist.gov/vuln/detail/CVE-2026-34480>`_. `#27583 <https://github.com/prestodb/presto/pull/27583>`_
* Upgrade org.bouncycastle:bcprov-jdk18on from 1.81 to 1.84 to resolve `CVE-2026-0636 <https://nvd.nist.gov/vuln/detail/CVE-2026-0636>`_. `#27606 <https://github.com/prestodb/presto/pull/27606>`_
* Upgrade org.postgresql:postgresql from 42.7.9 to 42.7.11 to resolve `CVE-2026-42198 <https://nvd.nist.gov/vuln/detail/CVE-2026-42198>`_. `#27722 <https://github.com/prestodb/presto/pull/27722>`_
* Upgrade parquet-jackson to 1.17.1 in response to `GHSA-72hv-8253-57qq <https://github.com/advisories/GHSA-72hv-8253-57qq>`_. `#27803 <https://github.com/prestodb/presto/pull/27803>`_
* Upgrade redshift-jdbc42 to 2.2.7 in response to `CVE-2026-8178 <https://github.com/advisories/GHSA-wmmv-vvg5-993q>`_. `#27828 <https://github.com/prestodb/presto/pull/27828>`_
* Upgrade opentelemetry-api to 1.62.0 in response to `CVE-2026-45292 <https://github.com/advisories/GHSA-rcgg-9c38-7xpx>`_. `#27865 <https://github.com/prestodb/presto/pull/27865>`_

JDBC Driver Changes
___________________
* Add connection validation feature to enhance connection reliability. This can be enabled with the :ref:`admin/properties-session:\`\`validateConnection\`\`` session property to execute a validation query immediately after establishing the connection. `#27002 <https://github.com/prestodb/presto/pull/27002>`_
* Add support for `execute` procedure in JDBC connectors. `#27282 <https://github.com/prestodb/presto/pull/27282>`_

Delta Lake Connector Changes
____________________________
* Fix a bug that made the metastore inconsistent if a Delta Lake table was created  to an inaccessible location. `#27129 <https://github.com/prestodb/presto/pull/27129>`_
* Add support for reading Delta Lake tables with column mapping enabled. `#27483 <https://github.com/prestodb/presto/pull/27483>`_

Hive Connector Changes
______________________
* Fix race where concurrent ``REFRESH MATERIALIZED VIEW`` on the same Hive-backed Iceberg materialized view could lose a watermark update. `#27835 <https://github.com/prestodb/presto/pull/27835>`_
* Fix integer overflow when converting exclusive bounds to inclusive bounds in ``BigintRange``, ``HugeintRange``, and ``TimestampRange`` filters in the Hive connector. `#27600 <https://github.com/prestodb/presto/pull/27600>`_
* Add support for partition-aware grouped execution in the Hive connector, creating per-(bucket, partition) split queues and compound partition handles. `#27663 <https://github.com/prestodb/presto/pull/27663>`_
* Add support for Azure Blob Storage (``wasb[s]://``) and Azure Data Lake Storage Gen2 (``abfs[s]://``) in the Hive connector, with shared key and OAuth2 authentication. `#25107 <https://github.com/prestodb/presto/pull/25107>`_

Iceberg Connector Changes
_________________________
* Fix access control for materialized view storage tables when ``legacy_materialized_views=false``: storage-table access control is bypassed during MV expansion, while direct queries by name still go through access control. `#27728 <https://github.com/prestodb/presto/pull/27728>`_
* Fix failure during ``INSERT`` into Iceberg tables partitioned by day() when using timestamp with time zone columns. `#27645 <https://github.com/prestodb/presto/pull/27645>`_
* Improve updating of ``stale_read_behavior``, ``staleness_window``, and ``refresh_type`` on existing materialized views with ``ALTER MATERIALIZED VIEW ... SET PROPERTIES`` (requires ``legacy_materialized_views=false``). `#27806 <https://github.com/prestodb/presto/pull/27806>`_
* Add ``iceberg.materialized-view-default-max-snapshots-per-refresh`` configuration property and matching session property to set the default bound. See :ref:`connector/iceberg:catalog configuration`. `#27774 <https://github.com/prestodb/presto/pull/27774>`_
* Add ``iceberg.materialized-view-default-storage-schema`` configuration property to route storage tables into a single schema. Defaults to the materialized view's own schema; per-MV ``storage_schema`` overrides. See :ref:`connector/iceberg:catalog configuration`. `#27728 <https://github.com/prestodb/presto/pull/27728>`_
* Add ``max_snapshots_per_refresh`` materialized view property to bound how far each base table advances per ``REFRESH MATERIALIZED VIEW``. Defaults to ``0`` (unbounded). Requires Iceberg V3 row lineage; V2 tables fall back to unbounded refresh. See :ref:`connector/iceberg:materialized view properties`. `#27774 <https://github.com/prestodb/presto/pull/27774>`_
* Add ``materialized_view_stitching_strategy`` and ``materialized_view_incremental_refresh_strategy`` session properties (values: `ALWAYS`, `NEVER`, `AUTOMATIC`; default: `ALWAYS`). Under `AUTOMATIC`, the optimizer selects between the rewrite and the full alternative based on cost; when stats are unavailable it falls back to row-count comparison. See :ref:`connector/iceberg:session properties`. `#27820 <https://github.com/prestodb/presto/pull/27820>`_
* Add read support for Iceberg V3 column initial-default values. `#27659 <https://github.com/prestodb/presto/pull/27659>`_
* Add incremental refresh for materialized views in the Iceberg connector, enabling efficient partial refreshes instead of full recomputation. `#26959 <https://github.com/prestodb/presto/pull/26959>`_
* Add min/max statistics for ``VARCHAR`` / ``CHAR`` columns in Iceberg tables. `#27357 <https://github.com/prestodb/presto/pull/27357>`_
* Add metastore cache invalidation procedure for Iceberg connector. `#27200 <https://github.com/prestodb/presto/pull/27200>`_
* Add predicate push down on ``_last_updated_sequence_number`` for file-level pruning. `#27766 <https://github.com/prestodb/presto/pull/27766>`_
* Add read support for Iceberg V3 row lineage hidden columns `_row_id` and `_last_updated_sequence_number`. `#27240 <https://github.com/prestodb/presto/pull/27240>`_
* Add support for ``min/max/count`` aggregation push down based on file stats. This can be toggled with the ``aggregate_push_down_enabled`` session property or the ``iceberg.aggregate-push-down-enabled`` configuration property. See :ref:`connector/iceberg:session properties` and :ref:`connector/iceberg:configuration properties`. `#27085 <https://github.com/prestodb/presto/pull/27085>`_
* Add support for updating column write-default values using ``ALTER TABLE ... SET DEFAULT`` (requires Iceberg format version 3+). `#27810 <https://github.com/prestodb/presto/pull/27810>`_
* Add support for ``ALTER COLUMN SET DATA TYPE`` DDL statements in the Iceberg connector. `#25418 <https://github.com/prestodb/presto/pull/25418>`_
* Add warning when predicate stitching or incremental refresh falls back to full recompute. `#27816 <https://github.com/prestodb/presto/pull/27816>`_
* Update write-default operations to preserve existing initial-default values as metadata-only changes. `#27810 <https://github.com/prestodb/presto/pull/27810>`_

Lance Connector Changes
_______________________
* Add SQL filter pushdown to reduce data read from disk for selective queries. Supports equality, comparisons, IN lists, IS NULL, and range predicates on Boolean, Integer, Bigint, Real, Double, Varchar, Date, and Timestamp types. See :ref:`connector/lance:predicate pushdown`. `#27430 <https://github.com/prestodb/presto/pull/27430>`_
* Add configurable index and metadata cache sizes via lance.index-cache-size and lance.metadata-cache-size. `#27325 <https://github.com/prestodb/presto/pull/27325>`_
* Add version-aware dataset caching with snapshot isolation for consistent query reads. `#27325 <https://github.com/prestodb/presto/pull/27325>`_

MongoDB Connector Changes
_________________________
* Add view querying capabilities in the Mongo connector. `#26995 <https://github.com/prestodb/presto/pull/26995>`_
* Upgrade mongo-java-driver to mongodb-driver-sync. `#27685 <https://github.com/prestodb/presto/pull/27685>`_


Oracle Connector Changes
________________________
* Add TLS/SSL configuration support for the Oracle connector with ``oracle.tls.enabled``, ``oracle.tls.truststore-path``, and ``oracle.tls.truststore-password`` properties. `#27671 <https://github.com/prestodb/presto/pull/27671>`_
* Add Oracle i18n character set support. `#27670 <https://github.com/prestodb/presto/pull/27670>`_
* Add ``jdbc-fetch-size`` configuration property to control the number of rows fetched per database round-trip for the Oracle connector. `#27669 <https://github.com/prestodb/presto/pull/27669>`_

Prometheus Connector Changes
____________________________
* Add mixed case-sensitive identifier support for Prometheus connector. `#26260 <https://github.com/prestodb/presto/pull/26260>`_

Singlestore Connector Changes
_____________________________
* Fix ``TINYINT`` type mapping to preserve ``TINYINT`` semantics instead of incorrectly mapping to ``BOOLEAN`` after a JDBC driver upgrade. `#27790 <https://github.com/prestodb/presto/pull/27790>`_
* Fix varchar type mapping for ``TEXT`` types to use byte-based thresholds matching the JDBC driver's ``COLUMN_SIZE`` reporting. `#27790 <https://github.com/prestodb/presto/pull/27790>`_

Verifier Changes
________________
* Add ``query-rewriter-factory`` configuration property to allow extending the verifier ``QueryRewriter`` with custom implementations. `#27703 <https://github.com/prestodb/presto/pull/27703>`_

**Credits**
===========

Aditi Pandit, Allen Shen, Amit Dutta, Apurva Kumar, Arjun Gupta, Asish Kumar, Auden Woolfson, Ben Hu, Bryan Cutler, Chandrakant Vankayalapati, Christian Zentgraf, Daniel Bauer, Deepak Majeti, Deepak Mehra, Dilli Babu Godari, Dong Wang, Gary Helmling, Glerin Pinhero, Han Yan, Henry Dikeman, Jalpreet Singh Nanda, Jamille Shao-Ni, Jianjian Xie, Joe Abraham, Ke Wang, Kevin Tang, Konjac Huang, Li, Maria Basmanova, Miguel Blanco Godón, Nandakumar Balagopal, Natasha Sehgal, Naveen Mahadevuni, Nivin C S, Pramod Satya, Prashant Sharma, Pratik Joseph Dabre, Pratyaksh Sharma, Rebecca Schlussel, Reetika Agrawal, Rui Mo, Saurabh Mahawar, Sayari Mukherjee, Sergey Pershin, Shahim Sharafudeen, Shakyan Kushwaha, Shrinidhi Joshi, Sreeni Viswanadha, Steve Burnett, Swapnil, Timothy Meehan, Tirumala Saiteja Goruganthu, XiaoDu, Xiaoxuan, Yabin Ma, Yihong Wang, Zac, Zac Blanco, abhinavmuk04, bibith4, dependabot[bot], feilong-liu, jkhaliqi, join-theory-de, mohsaka, nishithakbhaskaran, peterenescu, shelton408, sumi-mathew, vhsu14, zhichenxu-meta
