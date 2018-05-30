=============
Release 0.175
=============

General Changes
---------------

* Fix *"position is not valid"* query execution failures.
* Fix memory accounting bug that can potentially cause ``OutOfMemoryError``.
* Fix regression that could cause certain queries involving ``UNION`` and
  ``GROUP BY`` or ``JOIN`` to fail during planning.
* Fix planning failure for ``GROUP BY`` queries containing correlated
  subqueries in the ``SELECT`` clause.
* Fix execution failure for certain ``DELETE`` queries.
* Reduce occurrences of *"Method code too large"* errors.
* Reduce memory utilization for certain queries involving ``ORDER BY``.
* Improve performance of map subscript from O(n) to O(1) when the map is
  produced by an eligible operation, including the map constructor and
  Hive readers (except ORC and optimized Parquet). More read and write
  operations will take advantage of this in future releases.
* Add ``enable_intermediate_aggregations`` session property to enable the
  use of intermediate aggregations within un-grouped aggregations.
* Add support for ``INTERVAL`` data type to :func:`avg` and :func:`sum` aggregation functions.
* Add support for ``INT`` as an alias for the ``INTEGER`` data type.
* Add resource group information to query events.

Hive Changes
------------

* Make table creation metastore operations idempotent, which allows
  recovery when retrying timeouts or other errors.

MongoDB Changes
---------------

* Rename ``mongodb.connection-per-host`` config option to ``mongodb.connections-per-host``.
