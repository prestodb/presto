=============
Release 0.172
=============

General Changes
---------------

* Fix correctness issue in ``ORDER BY`` queries due to improper implicit coercions.
* Fix planning failure when ``GROUP BY`` queries contain lambda expressions.
* Fix planning failure when left side of ``IN`` expression contains subqueries.
* Fix incorrect permissions check for ``SHOW TABLES``.
* Fix planning failure when ``JOIN`` clause contains lambda expressions that reference columns or variables from the enclosing scope.
* Reduce memory usage of :func:`map_agg` and :func:`map_union`.
* Reduce memory usage of ``GROUP BY`` queries.
