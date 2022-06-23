=============
Release 0.260
=============

.. warning::
   There is a concurrency issue in the metric tracking framework in this release which could cause query failures when Alluxio caching is enabled.

.. warning::
   ``optimizer.aggregation-if-to-filter-rewrite-enabled`` is enabled by default in this release which could cause query failures when the ``IF`` branch returns exceptions for rows not meeting the ``IF`` condition, e.g, ``SUM(IF(CARDINALITY(array) > 0, array[1]))``.

**Details**
===========

General Changes
_______________
* Fix a bug in SQL functions that would cause compiler error when session property ``inline_sql_function`` is set to ``false`` and SQL function references input in lambda expression.
* Fix a bug in fragment result cache that caused query failures.
* Add :ref:`uuid_type` type to represent UUIDs.
* Add configuration property ``experimental.aggregation-spill-enabled`` and session property ``aggregation_spill_enabled`` to control aggregate spills explicitly.
* Add configuration property ``experimental.order-by-spill-enabled`` and session property ``order_by_spill_enabled`` to control order by spills explicitly.
* Add configuration property ``experimental.window-spill-enabled`` and session property ``window_spill_enabled`` to control window spills explicitly.
* Add configuration property ``fragment-result-cache.max-single-pages-size`` to control the max fragement cache file size.
* Add session property ``query_max_revocable_memory_per_node`` to override existing configuration property ``experimental.max-revocable-memory-per-node``.
* Add configuration property ``optimizer.aggregation-if-to-filter-rewrite-enabled`` and session property ``aggregation_if_to_filter_rewrite_enabled`` to toggle an optimizer rule which improves the performance of ``IF`` expressions inside aggregation functions.
* Enable join spilling by default when spill is enabled.  This can be disabled by setting the configuration property ``experimental.join-spill-enabled`` or session property ``join_spill_enabled`` to ``false``.

Hive Connector Changes
______________________
* Add support to create files for empty buckets when writing data.
  This can be configured by ``hive.create-empty-bucket-files`` configuration property or the ``create_empty_bucket_files`` session property.

Iceberg Connector Changes
_________________________
* Upgrade Iceberg version to 0.11.1.

Prometheus Connector Changes
____________________________
* Added Prometheus Connector. See :doc:`/connector/prometheus`

**Credits**
===========

Andrii Rosa, Ariel Weisberg, Arjun Gupta, Arunachalam Thirupathi, Basar Hamdi Onat, Beinan Wang, Bin Fan, Bin Fan, George Wang, Jack Ye, James Sun, Julian Zhuoran Zhao, Maria Basmanova, Mayank Garg, Rebecca Schlussel, Rongrong Zhong, Shixuan Fan, Timothy Meehan, Zac Wen, Zhan Yuan, Zhenxiao Luo, gxin@fb.com, linzebing, shenh062326, superqtqt, v-jizhang, vaishnavibatni
