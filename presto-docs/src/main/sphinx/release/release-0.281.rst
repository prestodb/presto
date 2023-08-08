=============
Release 0.281
=============

**Details**
===========

General Changes
_______________

* Fix memory accounting for hash join.
* Improve outer join where the join key is from the output of another outer join which can have many null values. The optimization is controlled by session property ``randomize_outer_join_null_key_strategy`` and the default value is ``DISABLED``
* Add timeouts for long running optimization rules.
* Improve performance for queries with multiple similar aggregations, with and without filters. The optimization is controlled by session parameter merge_aggs_with_and_without_filter and by default it is set to false.* Add an optimization rule to merge aggregations when there are multiple duplicate aggregations in the aggregation node. The optimization is controlled by session property ``merge_duplicate_aggregations`` and default value is ``true``.
* Add :func:`f_cdf()` and :func:`inverse_f_cdf()` functions.
* Add :func:`gamma_cdf()`` and :func:`inverse_gamma_cdf()` functions.
* Add groups support to :doc:`/functions/window`.
* Add optimization for joins when build side is empty at runtime. The optimization is controlled by session parameter ``optimize_join_probe_for_empty_build_runtime`` and by default it is set to ``false``.
* Add system config to enable access log on presto-on-spark native. This can be enabled with the system config ``http-server.enable-access-log`` and default value is ``true``.
* Remove session property ``deprecated.legacy-date-timestamp-to-varchar-coercion``.
* Remove access control checks after semantic analysis. Queries with both semantic and permission issues that previously failed with permission errors may now fail with semantic errors.
* Reduce memory usage in parquet reader.

Resource Groups Changes
_______________________
* Add ability to express concurrency levels as a function of number of available workers. ``workersPerQueryLimit``, which specifies the minimum number of workers that have to be available for each query. This is optional and defaults to 0. See :doc:`/admin/resource-groups`.

SPI Changes
___________
* Add various methods to create variables in the ``VariableAllocator``. ``VariableAllocator`` is also changed from interface to a class.
* Add ``MetadataResolver`` in the ``AnalyzerContext``, which is passed in the pluggable analyzer interface.


Hive Changes
____________
* Fix a bug where the filter on a ``CHAR(n)`` column is not evaluated correctly for ORC/DWRF files when filter pushdown is enabled.
* Fix issue while accessing HUDI Merge-on-Read Realtime tables (:issue:`18911`).
* Enable Hive splits for uncompressed inputs in S3 Select connector by leveraging the scan range feature of the service.

Apache Hudi Changes
____________
* Add the asynchronous split generation in Hudi connector to speed up the query execution and reduce overall query finishing time. ``hudi.max-outstanding-splits`` session property controls the maximum outstanding splits in a batch enqueued for processing.  ``hudi.split-generator-parallelism`` session property controls the number of threads to generate splits from partitions.
* Upgrade Apache Hudi version to 0.12.1.


Apache Iceberg Changes
_______________
* Upgrade Apache Iceberg version from 1.1.0 to 1.2.0.

JDBC Changes
____________
* Add ConnectorSession parameter to  all methods of ``JdbcClient`` interface. That makes it possible to pass specific options to JDBC driver implementation via session parameters.

**Credits**
===========

Ajay George, Ali Parsaei, Amit Dutta, Anant Aneja, Ankur Pathela, Arun Thirupathi, Chandrashekhar Kumar Singh, Chunxu Tang, Deepak Majeti, Eduard Tudenhoefner, Ge Gao, Ivan Sadikov, Jacob Wujciak-Jens, Jalpreet Singh Nanda (:imjalpreet), James Petty, Jaromir Vanek, Jialiang Tan, Jimmy Lu, Ke, Krishna Pai, Krishna Pai, Linsong Wang, Lyublena Antova, Masha Basmanova, Miaojiang (MJ) Deng, Michael Shang, Paul Meng, Pedro Pedreira, Pranjal Shankhdhar, Pratyaksh Sharma, Pratyush Verma, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Ruslan Mardugalliamov, Sagar Sumit, Sergey Pershin, Sergii Druzkin, Shrinidhi Joshi, Sreeni Viswanadha, Tal Galili, Timothy Meehan, Vivek, Zhenxiao Luo, dnnanuti, feilong-liu, guhanjie, meng duan, patzar, rohanpednekar, vinoth chandar, xiaoxmeng, yingsu00
 
