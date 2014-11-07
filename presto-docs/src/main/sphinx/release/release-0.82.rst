============
Release 0.82
============

* Presto now supports the :ref:`row_type` type, and all Hive structs are converted to ROWs,
  instead of JSON encoded VARCHARs.
* Add :func:`current_timezone` function.
* Improve planning performance for queries with thousands of columns.

