=============
Release 0.248
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Improve output buffer memory tracking to reduce lock contention.
* Add support for overriding session properties using session property managers :doc:`/admin/session-property-managers`. Setting 'overrideSessionProperties` to true will cause the property to be overridden and remain overridden even if subsequent rules match the property but don't have `overrideSessionProperties` set.
* Add support to drop multiple UDFs at the same time.
* Added a REST endpoint to get TaskInfo without directly going to worker endpoint.
* :func `map_union_sum`.
* Configurable ZSTD compression Level.
* In StatementAnalyzer.java, checked if the JOIN expression needed a warning and created one if necessary.
* In TestAnalyzer, added JUnit tests that check for warning when OR is directly in predicate and no warning when OR not present.
* New configuration parameter `internal-communication.https.trust-store-password` to set the Java Truststore password used for https in internal communications between nodes.

Verifier Changes
________________
* Add output table names to Presto Verifier's outputs.

**Contributors**
================

Ajay George, Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Bin Fan, Chi Tsai, David Stryker, James Petty, James Sun, Ke Wang, Leiqing Cai, Luca, Lung-Yen Chen, Nikhil Collooru, Rebecca Schlussel, Rongrong Zhong, Shixuan Fan, Sreeni Viswanadha, Stephen Dimmick, Tim Meehan, Venki Korukanti, Vic Zhang, Wenlei Xie, Yang Yang
