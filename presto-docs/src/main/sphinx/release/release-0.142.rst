=============
Release 0.142
=============

General Changes
---------------

* Fix planning bug for ``JOIN`` criteria that optimizes to a ``FALSE`` expression.
* Add support for :ref:`complex grouping operations<complex_grouping_operations>`
  - ``CUBE``, ``ROLLUP`` and ``GROUPING SETS``.
* Improve performance of :func:`json_extract`.

Hive Changes
------------

* Change ORC input format to report actual bytes read as opposed to estimated bytes.
* Fix cache invalidation when renaming tables.
* Fix Parquet reader to handle uppercase column names.
* Add :doc:`hive.compression-codec </connector/hive>` config option to control
  compression used when writing. The default is now ``GZIP`` for all formats.
