=============
Release 0.204
=============

General Changes
---------------

* Use distributed join if one side is naturally partitioned on join keys.
* Improve performance of correlated subqueries when filters from outer query
  can be propagated to the subquery.
* Improve performance for correlated subqueries that contain inequalities.
* Add support for all geometry types in :func:`ST_Area`.
* Add :func:`ST_EnvelopeAsPts` function.
* Add :func:`to_big_endian_32` and :func:`from_big_endian_32` functions.
* Add cast between ``VARBINARY`` type and ``IPADDRESS`` type.
* Make :func:`lpad` and :func:`rpad` functions support ``VARBINARY`` in addition to ``VARCHAR``.
* Allow using arrays of mismatched lengths with :func:`zip_with`.
  The missing positions are filled with ``NULL``.
* Track execution statistics of ``AddExchanges`` and ``PredicatePushdown`` optimizer rules.

Event Listener Changes
----------------------

* Add resource estimates to query events.

Web UI Changes
--------------

* Fix kill query button.
* Display resource estimates in Web UI query details page.

Resource Group Changes
----------------------

* Fix unnecessary queuing in deployments where no resource group configuration was specified.

Hive Connector Changes
----------------------

* Fix over-estimation of memory usage for scan operators when reading ORC files.
* Fix memory accounting for sort buffer used for writing sorted bucketed tables.
* Disallow creating tables with unsupported partition types.
* Support overwriting partitions for insert queries. This behavior is controlled
  by session property ``insert_existing_partitions_behavior``.
* Prevent the optimized ORC writer from writing excessively large stripes for
  highly compressed, dictionary encoded columns.
* Enable optimized Parquet reader and predicate pushdown by default.

Cassandra Connector Changes
---------------------------

* Add support for reading from materialized views.
* Optimize partition list retrieval for Cassandra 2.2+.
