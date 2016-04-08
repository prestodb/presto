=============
Release 0.145
=============

General Changes
---------------

* Add support for the ``integer`` type to the Presto engine and the Hive,
  Raptor, Redis, Kafka, Cassandra and example-http connectors.

CLI Changes
-----------

* Improve performance of output in batch mode.
* Fix hex rendering in batch mode.
* Abort running queries when CLI is terminated.

Hive Changes
------------

* Fix bug when grouping on a bucketed column which causes incorrect results.
