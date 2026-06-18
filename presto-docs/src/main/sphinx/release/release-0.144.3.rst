===============
Release 0.144.3
===============

General Changes
---------------

* Fix bugs in planner where coercions were not taken into account when computing
  types.
* Fix compiler failure when `TRY` is a sub-expression.
* Fix compiler failure when `TRY` is called on a constant or an input reference.
* Fix race condition that can cause queries that process data from non-columnar data
  sources to fail.

Hive Changes
------------

* Fix reading symlinks when the target is in a different HDFS instance.
