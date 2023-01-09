=============
Release 0.272
=============

**Details**
===========

General Changes
_______________
* Fix parameter ordering for prepared statement queries using a WITH clause.
* Improve performance by enabling :doc:`/optimizer/cost-based-optimizations` by default.
* Add ability to stream data for partial aggregation instead of building hash tables.
    This improves the performance of aggregation when the data is already ordered by the group-by keys.
    Streaming aggregation can be enabled with the ``streaming_for_partial_aggregation_enabled`` session property or the ``streaming-for-partial-aggregation-enabled`` configuration property.
* Add an adaptive stage scheduling policy that switches to phased execution mode once a query's stage count exceeds a configurable upper bound.
    This can be enabled by setting the session property ``execution_policy`` to ``phased`` and the stage count limit can be configured by the session property ``max_stage_count_for_eager_scheduling``.
* Add :func:`secure_random()` function to return a cryptographically secure random number.

Hive Changes
____________
* Fix integer overflow exception in Parquet writer when writing files larger than ~2 GB.
* Add ability to do streaming aggregation for Hive table scans to improve query performance with aggregation when group-by keys are the same as order-by keys.
    Cases where group-by keys are a subset of order-by keys can't enable streaming aggregation for now.
    This can be enabled with the ``streaming_aggregation_enabled`` session property or the ``hive.streaming-aggregation-enabled`` configuration property.
* Add ability to disable splitting file in Hive connector.
    This can be disabled with the ``file_splittable`` session property or the ``hive.file-splittable`` configuration property.
* Add support for using Parquet page-level statistics to skip pages.
    This feature can be enabled by setting the ``hive.parquet-column-index-filter-enabled`` configuration property.
* Add support for metadata-based listing and bootstrap for Hudi tables.

JDBC Changes
____________
* Add a new parameter ``timeZoneID`` which will set the time zone used for the timestamp columns. (See :doc:`/installation/jdbc` :issue:`16680`).

MongoDB Connector Changes
_________________________
* Fix the spelling of the write concern option ``JOURNAL_SAFE`` for the property ``mongodb.write-concern``.

Iceberg Changes
_______________
* Add support for concurrent insertion from the same Presto cluster or multiple Presto clusters which share the same Metastore.

Pinot Changes
_____________
* Add support for querying Pinot ``JSON`` type.

Lark Sheets Connector Changes
_____________________
* Add Lark Sheets connector.

**Credits**
===========

Alan Xu, Amit Dutta, Ariel Weisberg, Arjun Gupta, Arunachalam Thirupathi, Beinan Wang, Chen, Chunxu Tang, Darren Fu, David N Perkins, George Wang, Guy Moore, Harsha Rastogi, James Petty, James Sun, James Sun, Josh Soref, Ke Wang, Maksim Dmitriyevich Podkorytov, Masha Basmanova, Mayank Garg, Neerad Somanchi, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Rongrong Zhong, Ruslan Mardugalliamov, Sagar Sumit, Sergey Smirnov, Sergii Druzkin, Swapnil Tailor, Tim Meehan, Todd Gao, Varun Gajjala, Xiang Fu, Xinli shang, Ying, Zhenxiao Luo, ahouzheng, guojianhua, mengdilin, panyliu, v-jizhang, zhangyanbing
