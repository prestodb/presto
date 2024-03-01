=============
Release 0.191
=============

General Changes
---------------

* Fix regression that could cause high CPU usage for join queries when dictionary
  processing for joins is enabled.
* Fix :func:`bit_count` for bits between 33 and 63.
* The ``query.low-memory-killer.enabled`` config property has been replaced
  with ``query.low-memory-killer.policy``. Use ``total-reservation`` to continue
  using the previous policy of killing the largest query. There is also a new
  policy, ``total-reservation-on-blocked-nodes``, which kills the query that
  is using the most memory on nodes that are out of memory (blocked).
* Add support for grouped join execution. When both sides of a join have the
  same table partitioning and the partitioning is addressable, partial data
  can be loaded into memory at a time, making it possible to execute the join
  with less peak memory usage. The colocated join feature must be enabled with
  the ``colocated-joins-enabled`` config property or the ``colocated_join``
  session property, and the ``concurrent_lifespans_per_task`` session property
  must be specified.
* Allow connectors to report the amount of physical written data.
* Add ability to dynamically scale out the number of writer tasks rather
  than allocating a fixed number of tasks. Additional tasks are added when the
  the average amount of physical data per writer is above a minimum threshold.
  Writer scaling can be enabled with the ``scale-writers`` config property or
  the ``scale_writers`` session property. The minimum size can be set with the
  ``writer-min-size`` config property or the ``writer_min_size`` session property.
  The tradeoff for writer scaling is that write queries can take longer to run
  due to the decreased writer parallelism while the writer count ramps up.

Resource Groups Changes
-----------------------

*  Add query type to the exact match source selector in the DB resource group selectors.

CLI Changes
-----------

* Improve display of values of the Geometry type.

Hive Changes
------------

* Add support for grouped join execution for Hive tables when both
  sides of a join have the same bucketing property.
* Report physical written data for the legacy RCFile writer, optimized RCFile
  writer, and optimized ORC writer. These writers thus support writer scaling,
  which can both reduce the number of written files and create larger files.
  This is especially important for tables that have many small partitions, as
  small files can take a disproportionately longer time to read.

Thrift Connector Changes
------------------------

* Add page size distribution metrics.

MySQL, PostgreSQL, Redshift, and SQL Server Changes
---------------------------------------------------

* Fix querying ``information_schema.columns`` if there are tables with
  no columns or no supported columns.
