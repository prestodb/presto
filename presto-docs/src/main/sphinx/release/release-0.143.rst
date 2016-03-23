=============
Release 0.143
=============

General Changes
---------------

* Fix case-sensitivity issue when de-referencing row fields.
* Add config option ``query.max-cpu-time`` to limit CPU time used by a query.
* Add loading indicator and error message to query detail page in UI.
* Add query teardown to query timeline visualizer.
* Add string padding functions :func:`lpad` and :func:`rpad`.
* Add :func:`width_bucket` function.
* Add :func:`truncate` function.
* Improve query startup time in large clusters.
* Improve error messages for ``CAST`` and :func:`slice`.

Hive Changes
------------

* Fix native memory leak when reading or writing gzip compressed data.
* Fix data corruption in :doc:`/sql/create-table-as` when
  ``hive.respect-table-format`` config is set to false and user-specified
  storage format does not match default.
