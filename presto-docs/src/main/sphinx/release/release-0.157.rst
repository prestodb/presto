=============
Release 0.157
=============

General Changes
---------------

* Fix regression that could cause queries containing scalar subqueries to fail
  during planning.
* Reduce CPU usage of coordinator in large, heavily loaded clusters.
* Add support for ``DESCRIBE OUTPUT``.
* Add :func:`bitwise_and_agg` and :func:`bitwise_or_agg` aggregation functions.
* Add JMX stats for the scheduler.
* Add ``query.min-schedule-split-batch-size`` config flag to set the minimum number of
  splits to consider for scheduling per batch.
* Remove support for scheduling multiple tasks in the same stage on a single worker.
* Rename ``node-scheduler.max-pending-splits-per-node-per-stage`` to
  ``node-scheduler.max-pending-splits-per-task``. The old name may still be used, but is
  deprecated and will be removed in a future version.

Hive Changes
------------

* Fail attempts to create tables that are bucketed on non-existent columns.
* Improve error message when trying to query tables that are bucketed on non-existent columns.
* Add support for processing partitions whose schema does not match the table schema.
* Add support for creating external Hive tables using the ``external_location`` table property.
