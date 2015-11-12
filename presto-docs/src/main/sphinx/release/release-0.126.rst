=============
Release 0.126
=============

General Changes
---------------

* Improve handling of physical properties which can increase performance for
  queries involving window functions.
* Fix reset of session properties in CLI when running :doc:`/sql/use`.
* Fix query planning failure that occurs in some cases with the projection
  push down optimizer.
* Add ability to control whether index join lookups and caching are shared
  within a task. This allows us to optimize for index cache hits or for more
  CPU parallelism. This option is toggled by the ``task.share-index-loading``
  config property or the ``task_share_index_loading`` session property.

Hive Changes
------------

* Fix reading structural types containing nulls in Parquet.
* Fix writing DATE type when timezone offset is negative. Previous versions
  would write the wrong date (off by one day).
