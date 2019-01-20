=============
Release 0.208
=============

.. warning::

    This release has the potential for data loss in the Hive connector
    when writing bucketed sorted tables.

General Changes
---------------

* Fix an issue with memory accounting that would lead to garbage collection pauses
  and out of memory exceptions.
* Fix an issue that produces incorrect results when ``push_aggregation_through_join``
  is enabled (:issue:`x10724`).
* Preserve field names when unnesting columns of type ``ROW``.
* Make the cluster out of memory killer more resilient to memory accounting leaks.
  Previously, memory accounting leaks on the workers could effectively disable
  the out of memory killer.
* Improve planning time for queries over tables with high column count.
* Add a limit on the number of stages in a query.  The default is ``100`` and can
  be changed with the ``query.max-stage-count`` configuration property and the
  ``query_max_stage_count`` session property.
* Add :func:`spooky_hash_v2_32` and :func:`spooky_hash_v2_64` functions.
* Add a cluster memory leak detector that logs queries that have possibly accounted for
  memory usage incorrectly on workers. This is a tool to for debugging internal errors.
* Add support for correlated subqueries requiring coercions.
* Add experimental support for running on Linux ppc64le.

CLI Changes
-----------

* Fix creation of the history file when it does not exist.
* Add ``PRESTO_HISTORY_FILE`` environment variable to override location of history file.

Hive Connector Changes
----------------------

* Remove size limit for writing bucketed sorted tables.
* Support writer scaling for Parquet.
* Improve stripe size estimation for the optimized ORC writer. This reduces the
  number of cases where tiny ORC stripes will be written.
* Provide the actual size of CHAR, VARCHAR, and VARBINARY columns to the cost based optimizer.
* Collect column level statistics when writing tables. This is disabled by default,
  and can be enabled by setting the ``hive.collect-column-statistics-on-write`` property.

Thrift Connector Changes
------------------------

* Include error message from remote server in query failure message.
