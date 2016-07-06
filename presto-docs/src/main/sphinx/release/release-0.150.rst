=============
Release 0.150
=============

General Changes
---------------

* Fix web UI bug that caused rendering to fail when a stage has no tasks.
* Fix failure due to ambiguity when calling :func:`round` on ``tinyint`` arguments.
* Fix race in exchange HTTP endpoint, which could cause queries to fail randomly.
* Add support for parsing timestamps with nanosecond precision in :func:`date_parse`.
* Add CPU quotas to resource groups.

Hive Changes
------------

* Add support for writing to bucketed tables.
* Add execution optimizations for bucketed tables.
