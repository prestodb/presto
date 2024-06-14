=============
Release 0.284
=============

**Details**
===========

General Changes
_______________
* Fix correctness bug in the optimizer when ``randomize_outer_join_null_key`` is enabled.
* Fix bug in array ``CONTAINS`` expression rewrite rule.
* Fix bugs parsing lambda expressions.  The bugs may cause compile exception with message "Can not compile special form: WHEN" and lead to runtime exception of array out of bound access.
* Fix :func:`!min` and :func:`!max` to verify second argument is unique.
* Fix access control checks for ``IS NULL`` and ``IS NOT NULL``.
* Improve cache efficiency of history-based optimizer.
* Improve cost-based optimizer to work with complex equi-join predicates.
* Improve performance of outer join when nulls are present. It can be enabled by setting ``randomize_outer_join_null_key_strategy`` to be ``cost_based``. The trigger condition can be set by session properties ``randomize_outer_join_null_key_null_count_threshold`` and ``randomize_outer_join_null_key_null_ratio_threshold``.
* Add field names when casting row to JSON.
* Add :func:`!array_top_n` to return an array of top ``N`` elements of a given array.
* Add :func:`!bitwise_xor_agg` function.
* Add :func:`!NOISY_COUNT_GAUSSIAN`.
* Add :func:`!NOISY_SUM_GAUSSIAN`.
* Add :func:`!NOISY_AVG_GAUSSIAN`.
* Add session property ``restrict_history_based_optimization_to_complex_query``. When set to ``TRUE``, only queries with join or aggregation will try to use HBO. The default value is ``TRUE``.
* Add session property ``pull_expression_from_lambda_enabled`` to optimize lambda functions which have expressions not referring to arguments of the lambda function.  The default state is enabled using the value ``TRUE``.
* Add ``rewrite_constant_array_contains_to_in_expression`` session property to improve the performance of ``CONTAINS`` expressions.  The default state is enabled using the value ``TRUE``.
* Add function :func:`!trail`.
* Add session property ``optimizers_to_enable_verbose_runtime_stats`` to enable runtime tracking for a set of optimizers.
* Add support for ``EXPLAIN (TYPE VALIDATE)`` of ``EXPLAIN`` queries. Previously such queries would fail with an error.
* Improve performance of cluster statistics reporting in the Presto coordinator by adding the configuration property ``cluster-stats-cache-expiration-duration``. The property is ``0`` (disabled) by default.
* Add support for serializing nested data structures via server-side configuration properties using the configuration property ``nested-data-serialization-enabled``. The property is enabled by default with the value ``TRUE``.
* Add a Redis-based historical statistics provider.  For more information, see :doc:`Redis HBO Provider </plugin/redis-hbo-provider>`.
* Add principal name support in the resource group selection criteria.
* Add ``noisy_count_if_gaussian(condition, noiseScale[, randomSeed])`` aggregation which calculates the number of ``TRUE`` input values, and then adds random Gaussian noise with 0 mean and standard deviation of ``noise_scale`` to the true count. Optional ``randomSeed`` is used to get a fixed value of noise, often for reproducibility purposes. If ``randomSeed`` is omitted, ``SecureRandom`` is used. If ``randomSeed`` is provided, ``Random`` is used.

Hive Connector Changes
______________________
* Fix directory listing over directories with content-type ``application/octet-stream`` (:issue:`20310`).
* Add DWRF filetype to min, max filtering for special column types such as tinyint, varbinary and timestamp.

Iceberg Connector Changes
_________________________
* Add Iceberg table location property in ``SHOW CREATE TABLE``.
* Add validation for copy-on-write mode for Iceberg tables. This can be disabled with the ``merge_on_read_enabled`` session property or the ``iceberg.enable-merge-on-read-mode`` configuration property.
* Add support for ``TRUNCATE TABLE <table>``.
* Remove support for specifying ``NOT NULL`` constraint when adding a new column in ``ALTER TABLE`` statement.
* Upgrade Iceberg from 1.3.0 to 1.3.1.

PostgreSQL Connector Changes
____________________________
* Upgrade JDBC driver version to 42.6.0.

Presto Verifier Changes
_______________________
* Add support for Presto Verifier to run in query-bank mode and save query results as a snapshot. (See :doc:`Presto Verifier </admin/verifier>`.)

**Credits**
===========

8dukongjian, Ajay George, Ajay Gupte, Alex Perez, Amit Dutta, Amit Patil, Anant Aneja, Ann Rose Benny, Arjun Gupta, Arun D Panicker, Ashwin Krishna Kumar, Avinash Jain, Beinan, Chengcheng Jin, Christian Zentgraf, Chunxu Tang, Darren Fu, Deepak Majeti, Dongsheng Wang, Eduard Tudenhoefner, Efrat Levitan, Facebook Community Bot, Gary Ho, Ge Gao, Haritha K, Ivan Millan, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Xu, Jialiang Tan, JiamingMai, Jimmy Lu, Jobbine, Jon Janzen, Karteekmurthys, Ke, Kevin Wilfong, Kien Nguyen, Krishna-Prasad-P-V, Lyublena Antova, Mahadevuni Naveen Kumar, Masha Basmanova, Melissa Guo, Michael Shang, Nikhil Collooru, Patrick Stuedi, Pedro Pedreira, Pramod, Pranjal Shankhdhar, Pratik Joseph Dabre, Pratyush Verma, Rebecca Schlussel, Reetika Agrawal, Rohan Pednekar, Rohit Jain, Sagar Sumit, SeanIFitch, Sergey Pershin, Sergii Druzkin, Setyven Lnu, Shrinidhi Joshi, Shubham Chaurasia, Sreeni Viswanadha, Steve Burnett, Sudheesh, Swapnil Tailor, Timothy Meehan, Vivek, Yihong Wang, Ying, Zac, Zac Blanco, abhiseksaikia, adamzwakk, aditi-pandit, dnskr, feilong-liu, gopukrishnasIBM, guhanjie, jaystarshot, kedia,Akanksha, kiersten-stokes, lingbin, mayunlei, oyeliseiev, pratyakshsharma, prithvip, rrando901, s-akhtar-baig, v-jizhang, wangd, xiaoxmeng, xpengahana
