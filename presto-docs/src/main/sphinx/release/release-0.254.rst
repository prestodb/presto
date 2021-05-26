=============
Release 0.254
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix a bug where queries that have both remote functions and a local function with only constant arguments could throw an IndexOutOfBoundException during planning. The bug was introduced in release 0.253 by :pr:`16039`.
* Add documentation for Glue Catalog support in Hive. :doc:`/connector/hive`.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add iceberg connector.
* Add support to fragment result caching for unnest.
* Remove spilling strategy ``PER_QUERY_MEMORY_LIMIT`` and instead add configuration property ``experimental.query-limit-spill-enabled`` and session property ``query_limit_spill_enabled``.  When this property is set to ``true``, and the spill strategy is not ``PER_TASK_MEMORY_THRESHOLD``, then we will spill whenever a query uses more than the per-node total memory limit in combined revocable and non-revocable memory, in addition to whenever the memory pool exceeds the spill threshold.  This fixes an issue where using the ``PER_QUERY_MEMORY_LIMIT`` spilling strategy could prevent the oom killer from running when the memory pool was full.  The issue is still present for the ``PER_TASK_MEMORY_THRESHOLD`` spilling strategy.
* Remove spilling strategy ``PER_QUERY_MEMORY_LIMIT`` and instead add configuration property ``experimental.query-limit-spill-enabled`` and session property ``query_limit_spill_enabled``.  When this property is set to ``true``, and the spill strategy is not ``PER_TASK_MEMORY_THRESHOLD``, then we will spill whenever a query uses more than the per-node total memory limit in combined revocable and non-revocable memory, in addition to whenever the memory pool exceeds the spill threshold.  This fixes an issue where using the ``PER_QUERY_MEMORY_LIMIT`` spilling strategy could prevent the oom killer from running when the memory pool was full.  The issue is still present for the ``PER_TASK_MEMORY_THRESHOLD`` spilling strategy.
* Remove spilling strategy ``PER_QUERY_MEMORY_LIMIT`` and instead add configuration property ``experimental.query-limit-spill-enabled`` and session property ``query_limit_spill_enabled``.  When this property is set to ``true``, and the spill strategy is not ``PER_TASK_MEMORY_THRESHOLD``, then we will spill whenever a query uses more than the per-node total memory limit in combined revocable and non-revocable memory, in addition to whenever the memory pool exceeds the spill threshold.  This fixes an issue where using the ``PER_QUERY_MEMORY_LIMIT`` spilling strategy could prevent the oom killer from running when the memory pool was full.  The issue is still present for the ``PER_TASK_MEMORY_THRESHOLD`` spilling strategy.
* Remove spilling strategy ``PER_QUERY_MEMORY_LIMIT`` and instead add configuration property ``experimental.query-limit-spill-enabled`` and session property ``query_limit_spill_enabled``.  When this property is set to ``true``, and the spill strategy is not ``PER_TASK_MEMORY_THRESHOLD``, then we will spill whenever a query uses more than the per-node total memory limit in combined revocable and non-revocable memory, in addition to whenever the memory pool exceeds the spill threshold.  This fixes an issue where using the ``PER_QUERY_MEMORY_LIMIT`` spilling strategy could prevent the oom killer from running when the memory pool was full.  The issue is still present for the ``PER_TASK_MEMORY_THRESHOLD`` spilling strategy.
* Do not allocate resources within test constructor for a cleaner code.
* Memory tracking in TableFinishOperator can be enabled by setting the `table-finish-operator-memory-tracking-enabled` configuration property to `true`.

Hive Connector Changes
______________________
* Add max error retry config option to glue client. Defaults to 10. :doc:`/connector/hive`.
* Add support for Glue endpoint URL :doc:`/connector/hive`.
* Added intelligent tiering storage class. The S3 storage class to use when writing the data. STANDARD and INTELLIGENT_TIERING storage classes are supported. Default storage class is STANDARD :doc:`/connector/hive`.

Hive Changes
____________
* Add support for MaxResults on Glue Hive Metastore.
* Add support for bucket sort order in Glue when creating or updating a table or partition.
* Add support for partition cache validation. This can be enabled by setting `hive.partition-cache-validation-percentage` configuration parameter.
* Add support for partition schema evolution for parquet.
* Add support for partition schema evolution for parquet.
* Allow accessing tables in Glue metastore that do not have a table type.

Presto On Spark Changes
_______________________
* Reduce commit memory footprint on the Driver.
* Reduce commit memory footprint on the Driver.
* Reduce commit memory footprint on the Driver.

**Contributors**
================

Abhisek Gautam Saikia, Akhil Umesh Mehendale, Andrii Rosa, Arjun Gupta, Beinan, Bhavani Hari, Chunxu Tang, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Sun, Ke Wang, Maria Basmanova, Mayank Garg, Nikhil Collooru, Rebecca Schlussel, Rohit Jain, Rongrong Zhong, Sergey Pershin, Sergii Druzkin, Shixuan Fan, Tal Galili, Tim Meehan, Vic Zhang, Zhenxiao Luo, guhanjie, linjunhua, v-jizhang
