============
Release 0.83
============

* Fix resource leak in query queues
* Fix NPE when writing null ``ARRAY/MAP`` to Hive
* Fix :func:`json_array_get` to handle nested structures
* Fix ``UNNEST`` on null collections
* Fix a regression where queries that fail during parsing or analysis do not expire
* Make ``JSON`` type comparable
