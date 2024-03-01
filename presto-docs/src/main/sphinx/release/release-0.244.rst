=============
Release 0.244
=============

.. warning::
    There is a bug that causes a failure in reading ORC files having MAP columns with MAP_FLAT encoding where all the entries in the column are empty maps (:pr:`15468`).

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

**Highlights**
==============
* Improve performance of cross joins.
* Add support for spilling joins when using materialized exchanges.


**Details**
===========

General Changes
_______________
* Fix an issue where queries could encounter an int overflow error during spilling.
* Fix :func:`hamming_distance` to handle code point zero correctly.
* Fix caching and case-sensitivity bugs with type signatures for enum types.
* Fix a bug where queries could produce wrong results when they have multiple lambda expressions that only differ in complex type constants (:issue:15424).
* Improve performance of cross joins.
* Add ``peak_node_total_memory`` to statistics to allow monitoring the peak sum of memory used across tasks at any node by a query.
* Add support for spilling joins when using materialized exchanges.
* Add support for thrift transport for TaskStatus. This can be enabled using config property ``experimental.internal-communication.thrift-transport-enabled``.

Druid Changes
_____________
* Fix druid reading issue caused by using the ARRAY_LINES result format.

Hive Changes
____________
* Fix a bug with reading encrypted DWRF tables where queries could fail with a ``NullPointerException``.
* Add support for AWS SDK client request metrics and Glue API metrics to ``GlueHiveMetastore``.

Pinot Changes
_____________
* Add support for querying Pinot server with streaming API (with Pinot version >= 0.6.0). The API can be enabled by setting the configuration property ``pinot.use-streaming-for-segment-queries=true``.


Verifier Changes
________________
* Fix an issue where queries are always directed to the first cluster when multiple control or test clusters are specified.
* Improve handling of memory limits by using the verifier cluster's memory limits instead of the limits defined while running the query.
* Add control and test session properties to Verifier output.

**Contributors**
================

Adli Mousa, Ajay George, Andrii Rosa, Ariel Weisberg, Bhavani Hari, Bin Fan, Daniel Ohayon, Guy Moore, James Petty, James Sun, Ke Wang, Leiqing Cai, Masha Basmanova, Mohamed Shafee, Nikhil Collooru, Rebecca Schlussel, Rongrong Zhong, Saksham Sachdev, Sergii Druzkin, Shixuan Fan, Stone Shi, Vic Zhang, Wenlei Xie, Xiang Fu, Ying Su, Zhenxiao Luo, prithvip
