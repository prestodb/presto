=============
Release 0.251
=============

**Highlights**
==============
* Introduced BigQuery Connector.

**Details**
===========

General Changes
_______________
* Improve ORC writer performance for string columns.
* Add support for improving cache affinity hit rate by preventing repartitioning when nodes go down for a limited time. This can be enabled by setting the ``internal-communication.memoize-dead-nodes-enabled`` configuration property to ``true``.
* Add support to enforce ``NOT NULL`` constraints by checking values for ``NOT NULL`` columns.


JDBC Connector Changes
______________________
* Add support to accept username and password.

BigQuery Connector Changes
___________________________
* Introduced BigQuery Connector. See :doc:`/connector/bigquery` to get started.

Hive Changes
____________
* Add support for cache quota when using Alluxio cache.
* Add configuration property ``cache.alluxio.eviction-policy`` to set eviction policy for Alluxio caching.

Verifier Changes
________________
* Increase verifier retry and determinism analysis limits.

**Contributors**
================

Ajay George, Andrii Rosa, Arjun Gupta, Arunachalam Thirupathi, Bhavani Hari, Bin Fan, Chi Tsai, George Wang, James Petty, James Sun, Jinlin Zhang, Nikhil Collooru, Rebecca Schlussel, Shixuan Fan, Stephen Dimmick, Timothy Meehan, Venki Korukanti, Vic Zhang, Yang Yang, Zhenxiao Luo
