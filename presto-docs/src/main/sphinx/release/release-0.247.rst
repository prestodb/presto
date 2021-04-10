=============
Release 0.247
=============

.. warning::
    There is a bug causing ``SORT`` or``LIMIT`` to be incorrectly eliminated when using ``GROUPING SETS (())`, ``CUBE`` or ``ROLLUP``,
    first introduced in 0.246 by :pr:`14915`

**Highlights**
==============
* Add the interface ``QueryInterceptor`` to allow for custom logic to be executed before or after query execution. (:pr:`15565`).
* Add support for temporary (session-scoped) functions.

**Details**
===========

General Changes
_______________
* Fix a bug for reporting output data sizes for optimized repartitioning.
* Fix accounting for revocable memory that could cause some queries not to spill when they should.
* Fix a race condition in enum key lookup which caused queries using ``enum_key`` to crash occasionally with an internal error. (:pr:`15607`).
* Add an implementation of :func:`array_intersect` that takes an array of arrays as input.
* Add support for temporary (session-scoped) functions.
* Add support for specifying session properties via regex matching on client info using :doc:`/admin/session-property-managers`.

SPI Changes
___________
* Add a new field ``Optional<ConnectorSplit> split`` to ``ConnectorTableLayoutHandle#getIdentifier``.

Hive Changes
____________
* Fix a bug that could cause queries to fail with ``HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT`` error when scanning large bucketed tables using grouped execution.
* Add support for partition stats based optimization, including partition pruning and column domain stripping for fragment result caching.

JDBC Changes
____________
* Add the interface ``QueryInterceptor`` to allow for custom logic to be executed before or after query execution. (:pr:`15565`).

Verifier changes
________________
* Add retries for verifier queries that fail due to HTTP Error 429 (Too many requests).

**Contributors**
================

Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Ashish Tadose, Bhavani Hari, Cem Cayiroglu, Daniel Ohayon, James Petty, James Sun, John Roll, Leiqing Cai, Marilyn Beck, Masha Basmanova, Mayank Garg, Rebecca Schlussel, Rob Peterson, Rongrong Zhong, Sean Wang, Shixuan Fan, Swapnil Tailor, Timothy Meehan, Wenlei Xie, Ying, Zhenxiao Luo, prithvip, tanjialiang
