=============
Release 0.287
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix a bug in CTE reference node creation, where different CTEs may be incorrectly considered as the same CTE. :pr:`22515`
* Fix an issue with heuristic CTE materialization strategy where incorrect CTEs were materialized. :pr:`22433`
* Fix plan canonicalization by canonicalizing plan node ids. :pr:`22033`
* Fix bug with spilling in TopNRowNumber. :pr:`22281`
* Fix problem when writing large varchar values to throw a user error when it exceeds internal limits. :pr:`22063`
* Fix queries that filter with ``LIKE '%...%'`` over char columns. :pr:`22076`
* Fix the regr_count, regr_avgx, regr_avgy, regr_syy, regr_sxx, and regr_sxy functions result to be null when the input data is null, not 0. :pr:`22112`
* Fix precision loss when timestamp yielded from ``from_unixtime(double)`` function. :pr:`21899`
* Fix CAST(str as INTEGER), CAST(str as BIGINT), CAST(str as SMALLINT), CAST(str as TINYINT) to allow leading and trailing spaces in the string. :pr:`22284`
* Improve latency of materialized CTEs by scheduling multiple dependent subgraphs independently. :pr:`22205`
* Improve propagation of logical properties by enabling it by default. :pr:`22266`
* Improve accuracy and performance of HyperLogLog functions. :pr:`21943`
* Improve repeat function to create RunLengthEncodedBlock to improve performance. :pr:`21984`
* Add Heuristic CTE Materialization strategy which auto materialized expensive CTEs. This is configurable by setting ``cte_materialization_strategy`` to ``HEURISTIC`` or ``HEURISTIC_COMPLEX_QUERIES_ONLY``. (default ``NONE``). :pr:`21720`
* Add a session property ``track_history_based_plan_statistics_from_complete_stages_in_failed_query`` to enable tracking hbo statistics from complete stages in failed queries. :pr:`20947`
* Add session property ``history_optimization_plan_canonicalize_strategy`` to specify the plan canonicalization strategies to use for HBO. :pr:`21832`
* Add worker type and query ID information in HBO stats. :pr:`22234`
* Add log of stats equivalent plan and canonicalized plan for HBO. This feature is controlled by session property ``log_query_plans_used_in_history_based_optimizer``. :pr:`22306`
* Add limit to the amount of data written during CTE Materialization. This is configurable by the session property ``query_max_written_intermediate_bytes`` (default is 2TB). :pr:`22017`
* Add a new plan canonicalization strategy ``ignore_scan_constants`` which canonicalizes predicates for both partitioned and non-partitioned columns in scan node. :pr:`21832`
* Add an optimizer rule to get rid of map cast in map access functions when possible. :pr:`22059`
* Add histogram column statistic to Presto for the optimizer. Connectors can now implement support for them. :pr:`21236`
* Add Quick stats, a mechanism to build stats from metadata for tables and partitions that are missing stats. :pr:`21436`
* Add DDL support for Table constraints (primary key and unique constraints). :pr:`20384`
* Add optimization for query plans which contain RowNumber and TopNRowNumber nodes with empty input. :pr:`21914`
* Add support for Apache DataSketches KLL sketch with the ``sketch_kll`` and related family of functions. :pr:`21568`
* Add support for ``map_key_exists`` builtin SQL UDF. :pr:`21966`
* Add configuration property ``legacy_json_cast`` whose default value is ``true``. See `Legacy Compatible Properties <../admin/properties.html#legacy-compatible-properties>`_. :pr:`21869`
* Add support for tracking of the input data size when there is a fragment result cache hit. This can be enabled by setting the configuration property ``fragment-result-cache.input-data-stats-enabled=true``. :pr:`22145`
* Add JSON as a supported output format in the Presto CLI. :pr:`22181`
* Add documentation for supported data `Type mapping <../connector/iceberg.html#type-mapping>`_  in the Iceberg connector. :pr:`22093`
* Add usage documentation for :doc:`/clients/presto-cli`. :pr:`22265`
* Add usage documentation for :doc:`/clients/presto-console`. :pr:`22349`
* Improve ``map_normalize`` builtin SQL UDF to avoid repeated reduce computation. :pr:`22211`
* Remove ``native_execution_enabled``, ``native_execution_executable_path`` and ``native_execution_program_arguments`` session properties. Corresponding configuration properties are still available. :pr:`22183`
* Remove the configuration property ``use-legacy-scheduler`` and the corresponding session property ``use_legacy_scheduler``.   The property previously defaulted to true, and the new scheduler, which was intended to replace it eventually, was never productionized and is no longer needed. The configuration property ``max-stage-retries`` and the session property ``max_stage_retries`` have also been removed. :pr:`21952`
* Upgrade Alluxio to 312. :pr:`22452`

Security Changes
________________
* Remove logback 1.2.3. :pr:`21819`
* Add session property ``default-view-security-mode`` to choose the default security mode for view creation. :pr:`21956`

Verifier Changes
________________
* Add support for extended bucket verification of INSERT and CTAS queries. This can be enabled by the configuration property ``extended-verification`` to verify each bucket's data checksum if the inserted table is bucketed. :pr:`22001`
* Add support for extended partition verification of INSERT and CTAS queries. This can be enabled by the configuration property ``extended-verification`` to verify each partition's data checksum if the inserted table is partitioned. :pr:`21983`

SPI Changes
___________
* Add replaceColumn method to com.facebook.common.Page. :pr:`22493`
* Remove SPI method ConnectorMetadata.getTableLayouts() as deprecated. Add ConnectorMetadata.getTableLayoutForConstraint() as replacement. :pr:`21933`
* Move `SortNode` to SPI module to be utilized in connector. :pr:`22497`

Hive Connector Changes
______________________
* Fix a potential wrong results bug when footer stats are marked unreliable and partial aggregation pushdown is enabled.  Such queries will now fail with an error. :pr:`22011`
* Improve the ``hive.orc.use-column-names`` configuration setting to no longer fail on reading ORC files without column names, but fall back to using Hive's schema. This change improves compatibility with legacy ORC files. :pr:`21391`
* Add session property ``hive.dynamic_split_sizes_enabled`` to use dynamic split sizes based on data selected by query.  :pr:`22051`
* Add support for Filelist caching for symlink tables.  :pr:`19145`
* Add $row_id as a new hidden column. :pr:`22008`
* Add system procedure ``system.invalidate_directory_list_cache()`` to invalidate directory list cache in Hive Catalog. :pr:`19821`

Iceberg Connector Changes
_________________________
* Upgrade Iceberg from 1.4.3 to 1.5.0. :pr:`21961`
* Fix identity and truncate transforms on DecimalType columns. :pr:`21958`
* Fix the bug that ``CAST`` from non-legacy timestamp to date rounding to future when the timestamp is prior than `1970-01-01 00:00:00.000`. :pr:`21959`
* Add support to set ``commit.retry.num-retries`` table property with table creation to make the number of attempts to make in case of concurrent upserts configurable. :pr:`21250`
* Add year/month/day/hour transforms both on legacy and non-legacy TimestampType column. :pr:`21959`
* Fix error encountered when attempting to execute an ``INSERT INTO`` statement where column names contain white spaces. :pr:`21827`
* Add support for row-level deletes on Iceberg V2 tables. The delete mode can be changed from ``merge-on-read`` to ``copy-on-write`` by setting table property ``delete_mode``. :pr:`21571`
* Add support for Iceberg V1 tables in Prestissimo. :pr:`21584`
* Add support to read Iceberg V2 tables with Position Deletes in Prestissimo. :pr:`21980`
* Add support for Iceberg concurrent insertions. :pr:`21250`

MySQL Connector Changes
_______________________
* Add support for timestamp column type. :pr:`21937`

**Credits**
===========

8dukongjian, Ajay George, Amit Dutta, Anant Aneja, Andrii Rosa, Athmaja N, Avinash Jain, Bikramjeet Vig, Christian Zentgraf, Deepa George, Deepak Majeti, Eduard Tudenhoefner, Elliotte Rusty Harold, Emanuel F, Fazal Majid, Jalpreet Singh Nanda (:imjalpreet), Jialiang Tan, Jimmy Lu, Jonathan Hehir, Karteekmurthys, Ke, Kevin Wilfong, Konjac Huang, Lyublena Antova, Masha Basmanova, Mohan Dhar, Nikhil Collooru, Pranjal Shankhdhar, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Sanika Babtiwale, Sergey Pershin, Sergii Druzkin, Sreeni Viswanadha, Steve Burnett, Sudheesh, Swapnil Tailor, Tai Le Manh, Timothy Meehan, Todd Gao, Vivek, Will, Yihong Wang, Ying, Zac Blanco, Zac Wen, Zhenxiao Luo, aditi-pandit, dnskr, feilong-liu, hainenber, ico01, jaystarshot, kedia,Akanksha, kiersten-stokes, polaris6, pratyakshsharma, s-akhtar-baig, sabbasani, wangd, wypb, xiaodou, xiaoxmeng
