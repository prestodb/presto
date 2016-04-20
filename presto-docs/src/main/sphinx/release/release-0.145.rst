=============
Release 0.145
=============

General Changes
---------------

* Fix potential memory leak in coordinator query history.
* Add support for the ``integer`` type to the Presto engine and the Hive,
  Raptor, Redis, Kafka, Cassandra and example-http connectors.
* Add ``driver.max-page-partitioning-buffer-size`` config to control buffer size
  used to repartition pages for exchanges.

Verifier Changes
----------------

* Change verifier to only run read-only queries by default. This behavior can be
  changed with the ``control.query-types`` and ``test.query-types`` config flags.

CLI Changes
-----------

* Improve performance of output in batch mode.
* Fix hex rendering in batch mode.
* Abort running queries when CLI is terminated.

Hive Changes
------------

* Fix bug when grouping on a bucketed column which causes incorrect results.
