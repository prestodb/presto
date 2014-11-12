============
Release 0.83
============

Raptor Changes
--------------
* Raptor now enables specifying the backup storage location. This feature is highly experimental.
* Fix the handling of shards not assigned to any node.

General Changes
---------------

* Fix resource leak in query queues.
* Fix NPE when writing null ``ARRAY/MAP`` to Hive.
* Fix :func:`json_array_get` to handle nested structures.
* Fix ``UNNEST`` on null collections.
* Fix a regression where queries that fail during parsing or analysis do not expire.
* Make ``JSON`` type comparable.
* Added an optimization for hash aggregations. This optimization is turned off by default.
  To turn it on, add ``optimizer.optimize-hash-generation=true`` to the coordinator config properties.
