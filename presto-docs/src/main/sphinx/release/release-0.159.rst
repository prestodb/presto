=============
Release 0.159
=============

General Changes
---------------

* Improve predicate performance for ``JOIN`` queries.

Hive Changes
------------

* Optimize filtering of partition names to reduce object creation.
* Add limit on the number of partitions that can potentially be read per table scan.
  This limit is configured using ``hive.max-partitions-per-scan`` and defaults to 100,000.
