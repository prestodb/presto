=============
Release 0.257
=============

**Details**
===========

General Changes
_______________
* Fix queries failing with ``EXCEEDED_LOCAL_MEMORY_LIMIT`` error due to incorrect memory tracking while reading spilled data.
* Fix deadlock for queries with ``JOIN`` and ``LIMIT`` with spilling enabled.
* Fix query failures due to expressions with multiple ``is null`` checks on the same variable.
* Improve memory usage of queries spilling in the join operator.
* Improve expressions printed in query plans to be closer to valid SQL.
* Add support to find the n-th instance in :func:`array_position`.
* Add support for correlated subqueries with complex expressions in the correlation.
* Add support for the ``OFFSET`` clause in SQL query expressions. This feature can be enabled by setting the session property ``offset_clause_enabled`` or configuration property ``offset-clause-enabled`` to ``true``.

Hive Changes
____________
* Fix a bug in reading Avro format table with schema located in a Kerberos enabled HDFS compliant filesystem.
* Fix dynamic pruning for null keys in hive partition.

**Contributors**
================

Abhisek Gautam Saikia, Andrii Rosa, Arjun Gupta, Basar Hamdi Onat, Chen, George Wang, Grace Xin, Huameng Jiang, James Petty, James Sun, Masha Basmanova, Mayank Garg, Rebecca Schlussel, Rohit Jain, Roman Zeyde, Rongrong Zhong, Saumitra Shahapure, Seba Villalobos, Sergey Pershin, Sergii Druzkin, Shixuan Fan, Swapnil Tailor, Timothy Meehan, Venki Korukanti, Vic Zhang, Xiang Fu, Zhenxiao Luo, beinan, prithvip, tanjialiang
