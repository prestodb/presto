=============
Release 0.285
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix High Memory Task killer to kill highest memory consuming query.
* Fix logging of optimizer triggering, eliminating false positives where optimizer is logged as triggered but actually not. The PlanOptimizer now return both result plan and whether the plan changed or not.
* Fix the simplify plan with empty input rule to also work for union node with empty input but more than one non-empty input.
* Fix typo in high memory task killer strategy config `experiemental.task.high-memory-task-killer-strategy` to `experimental.task.high-memory-task-killer-stragtegy`.
* Improve history optimizer latency for queries which timeout during query registration in history optimizer.
* Improve latency of history based optimizer.
* Add High Memory Task Killer which troggers if worker is running out of memory and garbage collection not able to reclaim enough memory. The feature is enabled by `experimental.task.high-memory-task-killer-enabled`.  There are two strategies for the task killer which can be set via `experiemental.task.high-memory-task-killer-strategy`. (default: `FREE_MEMORY_ON_FULL_GC`, `FREE_MEMORY_ON_FREQUENT_FULL_GC`).
* Add a session property `history_input_table_statistics_matching_threshold` to set the threshold for input statistics match in HBO.
* Add an optimization, which removes the redundant cast to varchar expression in join condition. The optimizer is controlled by session property `remove_redundant_cast_to_varchar_in_join` and enabled by default.
* Add config to enable memory based split processing slow down. This can be enabled via `task.memory-based-slowdown-threshold`.
* Add optimization for scaled writers with HBO, it can improve latency for query with scaled writer enabled. It's controlled by session property `enable_hbo_for_scaled_writer` and default to false.
* Added links for the REST API resource pages to the Presto documentation.
* Added type widening support to the parquet column readers (int, bigint, and float) to be able to perform type conversions between inmediate numerical types.
* Change PushPartialAggregationThroughExchange optimization to use partial aggregation statistics when available, and when use_partial_aggregation_history=true.
* Enable optimization of values node followed by false filter to empty values node.
* Enhance join-reordering to work with non-simple equi-join predicates.
* Extends HBO to track statistics of execution partial aggregation nodes.
* Free memory on frequent full garbage collection strategy triggers task killer if worker is having frequent garbage collection and enough memory is not reclaimed during the garbage collections. Frequent garbage collection is identified via `experimental.task.high-memory-task-killer-frequent-full-gc-duration-threshold` and reclaim memory threshold is configured via `experimental.task.high-memory-task-killer-reclaim-memory-threshold`.
* Free memory on full garbage collection strategy triggers task killer if worker is running low memory and full GC is not able to reclaim enough memory. Low memory threhold is set by  `experimental.task.high-memory-task-killer-heap-memory-threshold`  and Full GC reclaim memory threhold is set by `experimental.task.high-memory-task-killer-reclaim-memory-threshold`.
* Iceberg connector now supports ANALYZE when configured with a Hive catalog and the table is unpartitioned.
* Introduces a new flag use_partial_aggregation_history, which controls whether or not partial aggregation histories are used in deciding whether to split aggregates into partial and final.
* MinbyN/maxbyN's third argument (N) is now checked to be unique.
* Print information about cost-based optimizers and the source of stats they use (CBO/HBO) in explain plans when session property verbose_optimizer_info_enabled=true.
* The Iceberg connector now supports `ALTER TABLE <table> ADD COLUMN <column> [WITH (partitioning = '<transform_func>')]` statements to indicate partition transform when add column to table.
* The Iceberg connector now supports `DELETE FROM <table> [where <filter>]` statements to delete one or more entire partitions.
* Track null probe keys in join and use it to enable outer join null salt in history based optimization.
* Update Babel and related npm packages to solve critical vulnerability: https://github.com/advisories/GHSA-67hx-6x53-jw92 The Babel packages are used to generate the JavaScript files for Presto UI. The update prevents the build servers or developers' machines from running arbitrary code while building the JavaScript files.
* Update Joda-Time to 2.12.5 to use 2023c tzdata. Note: a corresponding update to the Java runtime should also be made to ensure consistent timezone data. For example, Oracle JDK 8u381, tzdata-java-2023c rpm for OpenJDK, or use Timezone Updater Tool to apply 2023c tzdata to existing JVM.
* Updated docs accordingly (hive connector - schema evolution).
* When partial aggregation statistics are used in PushPartialAggregationThroughExchange, the optimization also applies to multi-key aggregations (as opposed to single-key only when CBO is used).

Hive Changes
____________
* Changes in ParquetPageSourceFactory so that the type-check does not fail with the new widenings supported.
* Support for Parquet writer versions V1 and V2 is now added for Hive and Iceberg catalogs. It can be toggled using session property `parquet_writer_version` and config property `hive.parquet.writer.version`. Valid values for these properties are PARQUET_1_0 and PARQUET_2_0. Default is PARQUET_2_0. E.g., set session parquet_writer_version=PARQUET_1_0 / hive.parquet.writer.version=PARQUET_1_0.

Iceberg Changes
_______________
* Fix iceberg table creation using glue metastore.
* SHOW STATS command will support timestamp column based on user session's time zone.
* Support view creation via Iceberg connector.
* Timestamp with time zone data type is not allowed in create table and alter table statements.
* Update Iceberg version from 1.3.1 to 1.4.1.

Pinot Changes
_____________
* Fix pinot single quote literal push down issue.

Prestissimo (native Execution) Changes
______________________________________
* Add support for internal authentication using JWT. It can be configured using configs "internal-communication.jwt.enabled=[true/false]", "internal-communication.shared-secret=<shared-secret-value>" and "internal-communication.jwt.expiration-seconds=<value in seconds>".

**Credits**
===========

Ajay George, Ajay Gupte, Amit Dutta, Anant Aneja, Andrii Rosa, Arjun Gupta, Avinash Jain, Beinan, Bikramjeet Vig, Chandrashekhar Kumar Singh, Christian Zentgraf, Chunxu Tang, Deepak Majeti, Eduard Tudenhoefner, James Xu, Jialiang Tan, JiamingMai, Jimmy Lu, Jonathan Hehir, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Lyublena Antova, Mahadevuni Naveen Kumar, Masha Basmanova, Michael Shang, Miguel Blanco Godón, Nikhil Collooru, Pedro Pedreira, Pranjal Shankhdhar, Pratyush Verma, Ruslan Mardugalliamov, Sergey Pershin, Sergii Druzkin, Shrinidhi Joshi, Sotirios Delimanolis, Sreeni Viswanadha, Steve Burnett, Sudheesh, Swapnil Tailor, Tim Meehan, Xiang Fu, Yihong Wang, Zac Blanco, aditi-pandit, feilong-liu, kedia,Akanksha, kiersten-stokes, mmorgan98, pratyakshsharma, wangd, wypb, xiaoxmeng, yingsu00
