=============
Release 0.145
=============

General Changes
---------------

* Fix potential memory leak in coordinator query history.
* Fix column resolution issue when qualified name refers to a view.
* Fail arithmetic operations on overflow.
* Fix bugs in planner where coercions were not taken into account when computing
  types.
* Fix compiler failure when `TRY` is a sub-expression.
* Fix compiler failure when `TRY` is called on a constant or an input reference.
* Add support for the ``integer`` type to the Presto engine and the Hive,
  Raptor, Redis, Kafka, Cassandra and example-http connectors.
* Add initial support for the ``decimal`` data type.
* Add ``driver.max-page-partitioning-buffer-size`` config to control buffer size
  used to repartition pages for exchanges.
* Improve performance for distributed JOIN and GROUP BY queries with billions
  of groups.
* Improve reliability in highly congested networks by adjusting the default
  connection idle timeouts.

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
* Add ``max_split_size`` and ``max_initial_split_size`` session properties to control
  the size of generated splits.
* Add retries to the metastore security calls.
