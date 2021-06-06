=============
Release 0.254
=============

.. warning::

  There is a backward compatibility issue in the DWRF writer that might cause other engines to be unable to read files written by this release.

**Details**
===========

General Changes
_______________
* Fix a bug where queries that have both remote functions and a local function with only constant arguments could throw an ``IndexOutOfBoundException`` during planning. The bug was introduced by :pr:`16039`.
* Fix a CPU regression for queries using :func:`element_at` for ``MAP``. Introduced by :pr:`16027`.
* Add fragment result caching support for ``UNNEST`` queries.
* Add :func:`poisson_cdf` and :func:`inverse_poisson_cdf` functions.
* Add memory tracking in ``TableFinishOperator`` which can be enabled by setting the ``table-finish-operator-memory-tracking-enabled`` configuration property to ``true``. Enabling this property can help with investigating GC issues on the coordinator by allowing us to debug whether stats collection uses a lot of memory.
* Remove spilling strategy ``PER_QUERY_MEMORY_LIMIT`` and add configuration property ``experimental.query-limit-spill-enabled`` and session property ``query_limit_spill_enabled``. When ``query_limit_spill_enabled`` is set to ``true`` and the spill strategy is not ``PER_TASK_MEMORY_THRESHOLD``, then we will spill whenever a query uses more than the per-node total memory limit in combined revocable and non-revocable memory.

Hive Changes
____________
* Fix a bug where the files would not be sorted when inserting into bucketed sorted tables with Glue.
* Add support for validating the values returned from the partition cache with the actual value from Metastore. This can be enabled by setting the configuration property ``hive.partition-cache-validation-percentage``.
* Add support for allowing to match columns between table and partition schemas by names when the configuration property ``hive.parquet.use-column-names`` or the hive catalog session property ``parquet_use_column_names`` is set to ``true``. By default they are mapped by index.
* Add support for configuring the Glue endpoint URL. :doc:`/connector/hive`.
* Add support for accessing tables in Glue metastore that do not have a table type.
* Add support for the S3 Intelligent-Tiering storage class writing data. This can be enabled by setting the configuration property ``hive.s3.storage-class`` to ``INTELLIGENT_TIERING``.
* Add configuration property ``hive.metastore.glue.max-error-retries`` for the maximum number of retries for glue client connections. The default value is 10.  :doc:`/connector/hive`.

Presto On Spark Changes
_______________________
* Optimize Driver commit memory footprint.
* Add session property ``spark_memory_revoking_threshold`` and configuration property ``spark.memory-revoking-threshold``. Spilling is triggered when total memory is beyond this threshold.

SPI Changes
_______________________
* Add support for custom query prerequisites to be checked and satisfied through ``QueryPrerequisites`` interface. See :pr:`16073`.

**Contributors**
================

Abhisek Gautam Saikia, Akhil Umesh Mehendale, Andrii Rosa, Arjun Gupta, Beinan, Bhavani Hari, Chunxu Tang, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Sun, Ke Wang, Maria Basmanova, Mayank Garg, Nikhil Collooru, Rebecca Schlussel, Rohit Jain, Rongrong Zhong, Sergey Pershin, Sergii Druzkin, Shixuan Fan, Tal Galili, Tim Meehan, Vic Zhang, Zhenxiao Luo, guhanjie, linjunhua, v-jizhang
