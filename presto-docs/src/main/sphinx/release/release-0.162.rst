=============
Release 0.162
=============

.. warning::

    The :func:`xxhash64` function introduced in this release will return a
    varbinary instead of a bigint in the next release.

General Changes
---------------

* Fix correctness issue when the type of the value in the ``IN`` predicate does
  not match the type of the elements in the subquery.
* Fix correctness issue when the value on the left-hand side of an ``IN``
  expression or a quantified comparison is ``NULL``.
* Fix correctness issue when the subquery of a quantified comparison produces no rows.
* Fix correctness issue due to improper inlining of TRY arguments.
* Fix correctness issue when the right side of a JOIN produces a very large number of rows.
* Fix correctness issue for expressions with multiple nested ``AND`` and ``OR`` conditions.
* Improve performance of window functions with similar ``PARTITION BY`` clauses.
* Improve performance of certain multi-way JOINs by automatically choosing the
  best evaluation order. This feature is turned off by default and can be enabled
  via the ``reorder-joins`` config option or ``reorder_joins`` session property.
* Add :func:`xxhash64` and :func:`to_big_endian_64` functions.
* Add aggregated operator statistics to final query statistics.
* Allow specifying column comments for :doc:`/sql/create-table`.

Hive Changes
------------

* Fix performance regression when querying Hive tables with large numbers of partitions.

SPI Changes
-----------

* Connectors can now return optional output metadata for write operations.
* Add ability for event listeners to get connector-specific output metadata.
* Add client-supplied payload field ``X-Presto-Client-Info`` to ``EventListener``.
