=============
Release 0.206
=============

General Changes
---------------

* Fix execution failure for certain queries containing a join followed by an aggregation
  when ``dictionary_aggregation`` is enabled.
* Fix planning failure when a query contains a ``GROUP BY``, but the cardinality of the
  grouping columns is one. For example: ``SELECT c1, sum(c2) FROM t WHERE c1 = 'foo' GROUP BY c1``
* Fix high memory pressure on the coordinator during the execution of queries
  using bucketed execution.
* Add :func:`ST_Union`, :func:`ST_Geometries`, :func:`ST_PointN`, :func:`ST_InteriorRings`,
  and :func:`ST_InteriorRingN` geospatial functions.
* Add :func:`split_to_multimap` function.
* Expand the :func:`approx_distinct` function to support the following types:
  ``INTEGER``, ``SMALLINT``, ``TINYINT``, ``DECIMAL``, ``REAL``, ``DATE``,
  ``TIMESTAMP``, ``TIMESTAMP WITH TIME ZONE``, ``TIME``, ``TIME WITH TIME ZONE``, ``IPADDRESS``.
* Add a resource group ID column to the ``system.runtime.queries`` table.
* Add support for executing ``ORDER BY`` without ``LIMIT`` in a distributed manner.
  This can be disabled with the ``distributed-sort`` configuration property
  or the ``distributed_sort`` session property.
* Add implicit coercion from ``VARCHAR(n)`` to ``CHAR(n)``, and remove implicit coercion the other way around.
  As a result, comparing a ``CHAR`` with a ``VARCHAR`` will now follow
  trailing space insensitive ``CHAR`` comparison semantics.
* Improve query cost estimation by only including non-null rows when computing average row size.
* Improve query cost estimation to better account for overhead when estimating data size.
* Add new semantics that conform to the SQL standard for temporal types.
  It affects the ``TIMESTAMP`` (aka ``TIMESTAMP WITHOUT TIME ZONE``) type,
  ``TIME`` (aka ``TIME WITHOUT TIME ZONE``) type, and ``TIME WITH TIME ZONE`` type.
  The legacy behavior remains default.
  At this time, it is not recommended to enable the new semantics.
  For any connector that supports temporal types, code changes are required before the connector
  can work correctly with the new semantics. No connectors have been updated yet.
  In addition, the new semantics are not yet stable as more breaking changes are planned,
  particularly around the ``TIME WITH TIME ZONE`` type.

JDBC Driver Changes
-------------------

* Add ``applicationNamePrefix`` parameter, which is combined with
  the ``ApplicationName`` property to construct the client source name.

Hive Connector Changes
----------------------

* Reduce ORC reader memory usage by reducing unnecessarily large internal buffers.
* Support reading from tables with ``skip.footer.line.count`` and ``skip.header.line.count``
  when using HDFS authentication with Kerberos.
* Add support for case-insensitive column lookup for Parquet readers.
