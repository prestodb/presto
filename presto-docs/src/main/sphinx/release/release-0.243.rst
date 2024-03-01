=============
Release 0.243
=============

.. warning::
    There is a bug in LambdaDefinitionExpression canonicalization introduced since 0.238. For more details, go to :issue:`15424`.

.. warning::
    There is a bug that results in columns in the ``USING`` clause of a query to not be checked properly for column-level access control (:pr:`15333`).

.. warning::
    There is a bug that causes a failure in reading ORC files having MAP columns with MAP_FLAT encoding where all the entries in the column are empty maps (:pr:`15468`).

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

**Highlights**
==============
* Add :func:`approx_most_frequent` aggregation function.
* Add support to :func:`truncate` function for specifying the number of digits to the right of the decimal point in the truncated result.
* Add support for ignoring access checks on columns that are referenced in the query, but are not required to compute the query results. This can be enabled with the ``check_access_control_on_utilized_columns_only`` session property.
* Add support in verifier to verify ``CREATE VIEW`` and ``CREATE TABLE`` queries.

**Details**
===========

General Changes
_______________
* Fix query failure when using ``EXPLAIN`` with a ``USE`` statement.
* Add :func:`approx_most_frequent` aggregation function.
* Add support to :func:`truncate` function for specifying the number of digits to the right of the decimal point in the truncated result.
* Remove configuration property ``optimizer.optimize-full-outer-join-with-coalesce`` and the corresponding session property ``optimize_full_outer_join_with_coalesce``. The feature will always be enabled.
* Remove deprecated configuration properties ``grouped-execution-for-aggregation-enabled`` and ``grouped-execution-for-join-enabled`` and their corresponding session properties ``grouped_execution_for_aggregation`` and ``grouped_execution_for_join``.  These have been replaced by a single configuration property ``grouped-execution-enabled`` and session property ``grouped_execution``.
* Remove deprecated configuration property ``dynamic-schedule-for-grouped-execution`` and session property ``dynamic_schedule_for_grouped_execution``.

Security Changes
________________
* Add support for ignoring access checks on columns that are referenced in the query, but are not required to compute the query results. This can be enabled with the ``check_access_control_on_utilized_columns_only`` session property.

Geospatial Changes
__________________
* Add support to allow geometries outside of the spatial partitioning to match in a partitioned spatial join.

Hive Changes
____________
* Improve metadata query optimizer.
* Add support for Parquet file metadata caching.

Pinot Changes
_____________
* Add support for pushing down aggregation function ``approx_distinct(x, e)``.
* Add support for pushing down array functions ``array_sum``, ``array_min``, ``array_max``, ``array_average`` and ``contains``.

Verifier Changes
________________
* Add support for removing explicitly set broadcast memory limits.
* Add support to verify ``CREATE VIEW`` and ``CREATE TABLE`` queries.

**Contributors**
================

Ajay George, Andrii Rosa, Ariel Weisberg, Ashish Tadose, Bin Fan, Daniel Ohayon, George Wang, James A. Gill, James Sun, Ke, Leiqing Cai, Maria Basmanova, Mayank Garg, Nikhil Collooru, Rebecca Schlussel, Rongrong Zhong, Shixuan Fan, Timothy Meehan, Vivek, Wenlei Xie, Xiang Fu, Ying Su, Zhenxiao Luo, Zhenyuan Zhao, ankit0811, leonpanokarren, prithvip
