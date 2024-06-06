=============
Release 0.286
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix a bug for ``min_by`` and ``max_by`` for window functions, where results are incorrect when the function specifies number of elements to keep and the window does not have “unbounded following” in the frame. :pr:`21793`
* Fix a potential bug in ``EXCEPT`` and ``INTERSECT`` queries by not pruning unreferenced output for intersect and except nodes in the PruneUnreferencedOutputs rule. :pr:`21343`
* Fix a bug in PullUpExpressionInLambdaRules optimizer where expressions being extracted were of JoniRegexType or LikePatternType. :pr:`21407`
* Extend PullUpExpressionInLambdaRulesFix optimizers to now extract independent expressions from within conditional expressions. :pr:`21344`
* Fix an issue with the serialization of the retriable property in QueryError. :pr:`19741`
* Fix the compilation error of merge function for KHyperLogLog state and add a config property ``limit-khyperloglog-agg-group-number-enabled`` to control whether to limit the number of groups for aggregation functions using KHyperLogLog state. (enabled by default) :pr:`21824`
* Fix a bug in CTE materialization which was causing incorrect plan generation in some cases. :pr:`21580`
* Fix :doc:`/plugin/redis-hbo-provider` documentation to include the correct configuration properties and added documentation for coordinator HBO configurations. :pr:`21477`
* Fix writtenIntermediateBytes metric by ensuring its correct population through temporary table writers. :pr:`21626`
* Add support for adaptive partial aggregation which disables partial aggregation in cases where it could be inefficient. This feature is configurable by the session property ``adaptive_partial_aggregation`` (disabled by default) :pr:`20979`
* Improve predicate pushdown through Joins. :pr:`21353`
* Improve the readability of query plans by formatting numbers with commas for easier interpretation. :pr:`21486`
* Add additional linear regression functions like REGR_AVGX, REGR_AVGY, REGR_COUNT, REGR_R2, REGR_SXX, REGR_SXY, and REGR_SYY. :pr:`21630`
* Add UPDATE sql support in Presto. :pr:`21435`
* Add a session property ``skip_hash_generation_for_join_with_table_scan_input`` to skip hash precomputation for join when the input is table scan, and the hash is on a single big int and is not reused later. The property defaults to not enabled. :pr:`20948`
* Add a config property ``khyperloglog-agg-group-limit`` to limit the maximum number of groups that ``khyperloglog_agg`` function can have. The query will fail when the limit is exceeded. (The default is 0 which means no limit). :pr:`21510`
* Add a feature config property ``limit-khyperloglog-agg-group-number-enabled`` to control whether to limit the number of groups for aggregation functions using KHyperLogLog state. :pr:`21824`
* Add session property ``rewrite_expression_with_constant_expression`` which defaults to enabled. This optimizes queries which have an equivalence check filter or constant assignments. :pr:`19836`
* Add session property ``rewrite_left_join_array_contains_to_equi_join`` that transforms left joins with an ARRAY CONTAINS condition in the join criteria into an equi join. :pr:`21420`
* Add an option in the Presto client to disable redirects that fixes advisory `GHSA-xm7x-f3w2-4hjm <https://github.com/prestodb/presto/security/advisories/GHSA-xm7x-f3w2-4hjm>`_. :pr:`21024`
* Improve prestodb/presto docker image by including ``config.properties`` and ``jvm.config`` files. :pr:`21384`
* Upgrade ``hadoop-apache2`` to ``2.7.4-12``.  This fixes errors like ``library not found: /nativelib/Linux-aarch64/libhadoop.so`` when running presto on ARM64.  :pr:`21483`
* Add validation in Presto client to ensure that the host and port of the next URI do not change during query execution in Presto, enhancing security by preventing redirection to untrusted sources. :pr:`21101`
* Add :doc:`Ecosystem </ecosystem/list>` documentation. :pr:`21698`
* Add ``cte_hash_partition_count`` session property to specify the number of buckets or writers to be used when using CTE Materialization. :pr:`21625`
* Add :doc:`/installation/deploy-helm` to Installation documentation. :pr:`21812`
* Remove redundant sort columns from query plans if a unique constraint can be identified for a prefix of the ordering list. :pr:`21371`
* Add changelog table ``$changelog`` that allows users  to track when records were added or deleted in snapshots. :pr:`20937`
* Add `reservoir_sample <../functions/aggregate.html#reservoir_sample>`_ aggregation function which is useful for generating fixed-size samples. :pr:`21296`
* Remove ``exchange.async-page-transport-enabled`` configuration property as deprecated. :pr:`21772`
* Pass extra credentials such as CAT tokens for definer mode in views. :pr:`21685`
* Add function ``map_top_n_keys_by_value`` which returns top ``n`` keys of a map by value. :pr:`21259`
* Add support for materialization of Common Table Expressions (CTEs) in queries. The underlying connectors must support creating temporary tables, a functionality presently exclusive to the Hive connector. :pr:`20887`

SPI Changes
___________
* Add support for connectors to return joins in ``ConnectorPlanOptimizer.optimize``. :pr:`21605`

Hive Connector Changes
______________________
* Fix parquet dereference pushdown which was not working unless the ``parquet_use_column_names`` session property was set. :pr:`21647`
* Fix CTE materialization for unsupported Hive bucket types. :pr:`21549`
* Remove hive config ``hive.s3.use-instance-credentials`` as deprecated. :pr:`21648`

Hudi Connector Changes
______________________
* Upgrade Hudi version to 0.14.0. :pr:`21012`

Iceberg Connector Changes
_________________________
* Upgrade Apache Iceberg to 1.4.3.  :pr:`21714`
* Add Iceberg Filter Pushdown Optimizer Rule for execution with Velox. :pr:`20501`
* Add ``iceberg.pushdown-filter-enabled`` config property to Iceberg Connector. This config property controls the behaviour of Filter Pushdown in the Iceberg connector. :pr:`20501`
* Add `register <../connector/iceberg.html#register-table>`_  and `unregister <../connector/iceberg.html#unregister-table>`_ procedures for Iceberg tables. :pr:`21335`
* Add session property ``iceberg.delete_as_join_rewrite_enabled`` (enabled by default) to apply equality deletes as a join. :pr:`21605`
* Add support for querying ``"$data_sequence_number"`` which returns the Iceberg data sequence number of the file containing the row. :pr:`21605`
* Add support for querying ``"$path"`` which returns the file path containing the row. :pr:`21605`
* Add support for reading v2 row level deletes in Iceberg connector. :pr:`21189`
* Add support for Day, Month, and Year transform function with partition column for date type in Presto Iceberg connector. :pr:`21303`
* Add support for Day, Month, and Year transform function with partition column for timestamp type in Presto Iceberg connector. :pr:`21303`
* Add support for Day, Month, Year, and Hour partition column transform functions when altering a table to add partition columns in Presto Iceberg connector. :pr:`21575`
* Optimize Table Metadata calls for Iceberg tables. :pr:`21629`
* Add support for `time travel <../connector/iceberg.html#time-travel-using-version-system-version-and-timestamp-system-time>`_, enabling the retrieval of historical data with the `AS OF` syntax. :pr:`21425`
* Add support for `time travel <../connector/iceberg.html#time-travel-using-version-system-version-and-timestamp-system-time>`_ ``TIMESTAMP (SYSTEM_TIME)`` syntax includes timestamp-with-time-zone data type. It will return data based on snapshot with matching timestamp or before. :pr:`21425`
* Add support for `time travel <../connector/iceberg.html#time-travel-using-version-system-version-and-timestamp-system-time>`_ ``VERSION (SYSTEM_VERSION)`` syntax includes snapshot id using bigint data type. :pr:`21425`
* Add manifest file caching support for Iceberg native catalogs. :pr:`21399`
* Fix Iceberg memory leak with ``DeleteFile``.  :pr:`21612`

**Credits**
===========

8dukongjian, AbhijitKulkarni1, Aditi Pandit, Ajay George, Ajay Gupte, Amit Dutta, Anant Aneja, Andrii Rosa, Anil Gupta Somisetty, Antoine Pultier, Arjun Gupta, Avinash Jain, Beinan, Bikramjeet Vig, Changli Liu, Christian Zentgraf, Chunxu Tang, Deepak Majeti, Diana Meehan, Facebook Community Bot, Ge Gao, Jalpreet Singh Nanda (:imjalpreet), Jason Fine, Jialiang Tan, Jimmy Lu, Jonathan Hehir, Junhao Liu, Ke, Kevin Wilfong, Krishna Pai, Linsong Wang, Luis Paolini, Lyublena Antova, Mahadevuni Naveen Kumar, Masha Basmanova, Matthew Peveler, Michael Shang, Nikhil Collooru, Nilay Pochhi, Patrick Stuedi, Paul Meng, Pedro Pedreira, Pramod, Pranjal Shankhdhar, Pratik Joseph Dabre, Reetika Agrawal, Richard Barnes, Rohit Jain, Sagar Sumit, Sergey Pershin, Sergii Druzkin, Shrinidhi Joshi, Steve Burnett, Sudheesh, Tai Le, Tim Meehan, TommyLemon, Vigneshwar Selvaraj, VishnuSanal, Vivek, Yihong Wang, Ying, Zac, Zac Blanco, Zhenxiao Luo, abhiseksaikia, feilong-liu, hainenber, jaystarshot, karteekmurthys, kedia,Akanksha, kiersten-stokes, mohsaka, pratyakshsharma, prithvip, renurajagop, rui-mo, shenhong, wangd, wypb, xiaoxmeng, xumingming
