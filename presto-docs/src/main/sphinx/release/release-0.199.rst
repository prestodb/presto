=============
Release 0.199
=============

General Changes
---------------

* Allow users to create views for their own use when they do not have permission
  to grant others access to the underlying tables or views. To enable this,
  creation permission is now only checked at query time, not at creation time,
  and the query time check is skipped if the user is the owner of the view.
* Add support for spatial left join.
* Add :func:`hmac_md5`, :func:`hmac_sha1`, :func:`hmac_sha256`, and :func:`hmac_sha512` functions.
* Add :func:`array_sort` function that takes a lambda as a comparator.
* Add :func:`line_locate_point` geospatial function.
* Add support for ``ORDER BY`` clause in aggregations for queries that use grouping sets.
* Add support for yielding when unspilling an aggregation.
* Expand grouped execution support to ``GROUP BY`` and ``UNION ALL``, making it possible
  to execute aggregations with less peak memory usage.
* Change the signature of ``round(x, d)`` and ``truncate(x, d)`` functions so that
  ``d`` is of type ``INTEGER``. Previously, ``d`` could be of type ``BIGINT``.
  This behavior can be restored with the ``deprecated.legacy-round-n-bigint`` config option
  or the ``legacy_round_n_bigint`` session property.
* Accessing anonymous row fields via ``.field0``, ``.field1``, etc., is no longer allowed.
  This behavior can be restored with the ``deprecated.legacy-row-field-ordinal-access``
  config option or the ``legacy_row_field_ordinal_access`` session property.
* Optimize the :func:`ST_Intersection` function for rectangles aligned with coordinate axes
  (e.g., polygons produced by the :func:`ST_Envelope` and :func:`bing_tile_polygon` functions).
* Finish joins early when possible if one side has no rows. This happens for
  either side of an inner join, for the left side of a left join, and for the
  right side of a right join.
* Improve predicate evaluation performance during predicate pushdown in planning.
* Improve the performance of queries that use ``LIKE`` predicates on the columns of ``information_schema`` tables.
* Improve the performance of map-to-map cast.
* Improve the performance of :func:`ST_Touches`, :func:`ST_Within`, :func:`ST_Overlaps`, :func:`ST_Disjoint`,
  and :func:`ST_Crosses` functions.
* Improve the serialization performance of geometry values.
* Improve the performance of functions that return maps.
* Improve the performance of joins and aggregations that include map columns.

Server RPM Changes
------------------

* Add support for installing on machines with OpenJDK.

Security Changes
----------------

* Add support for authentication with JWT access token.

JDBC Driver Changes
-------------------

* Make driver compatible with Java 9+. It previously failed with ``IncompatibleClassChangeError``.

Hive Changes
------------

* Fix ORC writer failure when writing ``NULL`` values into columns of type ``ROW``, ``MAP``,  or ``ARRAY``.
* Fix ORC writers incorrectly writing non-null values as ``NULL`` for all types.
* Support reading Hive partitions that have a different bucket count than the table,
  as long as the ratio is a power of two (``1:2^n`` or ``2^n:1``).
* Add support for the ``skip.header.line.count`` table property.
* Prevent reading from tables with the ``skip.footer.line.count`` table property.
* Partitioned tables now have a hidden system table that contains the partition values.
  A table named ``example`` will have a partitions table named ``example$partitions``.
  This provides the same functionality and data as ``SHOW PARTITIONS``.
* Partition name listings, both via the ``$partitions`` table and using
  ``SHOW PARTITIONS``, are no longer subject to the limit defined by the
  ``hive.max-partitions-per-scan`` config option.
* Allow marking partitions as offline via the ``presto_offline`` partition property.

Thrift Connector Changes
------------------------

* Most of the config property names are different due to replacing the
  underlying Thrift client implementation. Please see :doc:`/connector/thrift`
  for details on the new properties.

SPI Changes
-----------

* Allow connectors to provide system tables dynamically.
* Add ``resourceGroupId`` and ``queryType`` fields to ``SessionConfigurationContext``.
* Simplify the constructor of ``RowBlock``.
* ``Block.writePositionTo()`` now closes the current entry.
* Replace the ``writeObject()`` method in ``BlockBuilder`` with ``appendStructure()``.
