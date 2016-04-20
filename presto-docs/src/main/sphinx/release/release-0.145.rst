=============
Release 0.145
=============

General Changes
---------------

* Fix potential memory leak in coordinator query history.
* Fix bugs in planner where coercions were not taken into account when computing
  types.
* Fix compiler failure when `TRY` is a sub-expression.
* Fix compiler failure when `TRY` is called on a constant or an input reference.
* Add support for the ``integer`` type to the Presto engine and the Hive,
  Raptor, Redis, Kafka, Cassandra and example-http connectors.
* Add type inference for literals with integral types (``bigint`` and
  ``integer``).
* Add ``driver.max-page-partitioning-buffer-size`` config to control buffer size
  used to repartition pages for exchanges.

CLI Changes
-----------

* Improve performance of output in batch mode.
* Fix hex rendering in batch mode.
* Abort running queries when CLI is terminated.

Hive Changes
------------

* Fix bug when grouping on a bucketed column which causes incorrect results.
* Add retries to the loadRoles and loadTablePriveleges metastore calls.
