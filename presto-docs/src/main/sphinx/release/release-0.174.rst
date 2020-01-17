=============
Release 0.174
=============

General Changes
---------------

* Fix correctness issue for correlated subqueries containing a ``LIMIT`` clause.
* Fix query failure when :func:`reduce` function is used with lambda expressions
  containing :func:`array_sort`, :func:`shuffle`, :func:`reverse`, :func:`array_intersect`,
  :func:`arrays_overlap`, :func:`concat` (for arrays) or :func:`map_concat`.
* Fix a bug that causes underestimation of the amount of memory used by :func:`max_by`,
  :func:`min_by`, :func:`max`, :func:`min`, and :func:`arbitrary` aggregations over
  varchar/varbinary columns.
* Fix a memory leak in the coordinator that causes long-running queries in highly loaded
  clusters to consume unnecessary memory.
* Improve performance of aggregate window functions.
* Improve parallelism of queries involving ``GROUPING SETS``, ``CUBE`` or ``ROLLUP``.
* Improve parallelism of ``UNION`` queries.
* Filter and projection operations are now always processed columnar if possible, and Presto
  will automatically take advantage of dictionary encodings where effective.
  The ``processing_optimization`` session property and ``optimizer.processing-optimization``
  configuration option have been removed.
* Add support for escaped unicode sequences in string literals.
* Add :doc:`/sql/show-grants` and ``information_schema.table_privileges`` table.

Hive Changes
------------

* Change default value of ``hive.metastore-cache-ttl`` and ``hive.metastore-refresh-interval`` to 0
  to disable cross-transaction metadata caching.

Web UI changes
--------------

* Fix ES6 compatibility issue with older browsers.
* Display buffered bytes for every stage in the live plan UI.

SPI changes
-----------

* Add support for retrieving table grants.
* Rename SPI access control check from ``checkCanShowTables`` to ``checkCanShowTablesMetadata``,
  which is used for both :doc:`/sql/show-tables` and :doc:`/sql/show-grants`.
