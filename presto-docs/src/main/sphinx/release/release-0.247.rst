=============
Release 0.247
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix a bug for reporting output data sizes for optimized repartitioning.
* Fix accounting for revocable memory that could cause some queries not to spill when they should.
* Fixed a race condition in enum key lookup which caused queries using the `ENUM_KEY` UDF to crash occasionally with an internal error #15607.
* Add function ``array_intersect`` that takes an array of array as input.
* Add support for temporary (session-scoped) functions.
* Adding retry behavior for verifier requests in case of throttling error.
* Add support for specifying session properties via regex matching on client info using :doc:`/admin/session-property-managers`
* Add maven plugin for drift to generate the Thrift spec for a list of classes, current class list is TaskStatus
* Change theme of the Presto docs site (https://prestodb.io/docs/), adding a table of contents, page navigation, Sphinx search, ability to copy code to clipboard, and a new color scheme.

SPI Changes
___________
* Change `ConnectorTableLayoutHandle#getIdentifier` to accept a new field `Optional<ConnectorSplit> split`.

Hive Changes
____________
* Fixed a bug that query would fail with HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT when scanning large bucketed table using grouped execution.
* Support partition stats based optimization, including partition pruning and column domain stripping for fragment result caching.

JDBC Changes
____________
* Add the interface `QueryInterceptor` to allow for custom logic to be executed before or after query execution. (:pr:`15565`).

**Contributors**
================

Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Ashish Tadose, Bhavani Hari, Cem Cayiroglu, Daniel Ohayon, James Petty, James Sun, John Roll, Leiqing Cai, Marilyn Beck, Masha Basmanova, Mayank Garg, Rebecca Schlussel, Rob Peterson, Rongrong Zhong, Sean Wang, Shixuan Fan, Swapnil Tailor, Timothy Meehan, Wenlei Xie, Ying, Zhenxiao Luo, prithvip, tanjialiang
