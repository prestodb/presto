=============
Release 0.150
=============

.. warning::

    The Hive bucketing changes are broken in this release. You should
    disable them by adding ``hive.bucket-execution=false`` and
    ``hive.bucket-writing=false`` to your Hive catalog properties.

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
