=============
Release 0.153
=============

General Changes
---------------

* Fix incorrect results for grouping sets when ``task.concurrency`` is greater than one.
* Fix silent numeric overflow when casting ``INTEGER`` to large ``DECIMAL``
  types.
* Fix issue where GROUP BY () would produce no results if the input
  had no rows.
* Fix null handling in :func:`array_distinct` where applied to ``array(bigint)`` type.
* Fix potential thread deadlock in coordinator.
* Fix rare correctness issue with an aggregation on a single threaded `RIGHT JOIN` when
  `task.concurrencty` is `1`.
* Fix query failure when casting a map with null elements in values.
* Fix failure when view column names contain upper-case letters.
* Fix potential performance regression due to skew issue when
  grouping or joining on columns of the following types: ``TINYINT``,
  ``SMALLINT``, ``INTEGER``, ``BIGINT``, ``REAL``, ``DOUBLE``,
  ``COLOR``, ``DATE``, ``INTERVAL``, ``TIME``, ``TIMESTAMP``.
* Fix potential memory leak for delete queries.
* Make resource group configuration more flexible. See "SPI Changes" below, and the
  :doc:`resource groups documentation </admin/resource-groups>`.
* Improve performance of :ref:`array_type` when underlying data is dictionary encoded.
* Improve performance of ORC reader when decoding dictionary encoded :ref:`map_type`.
* Allow running Presto with early-access Java versions.
* Allow subqueries in non-equality outer join criteria.
* Add initial support for correlated subqueries.
* Add support for prepared statements.
* Add :func:`type_of` for discovering expression types.
* Add ``DOUBLE PRECISION`` as an alias to ``DOUBLE`` type.
* Add decimal support to :func:`avg`, :func:`ceil`, :func:`floor`, :func:`round`,
  :func:`truncate`, :func:`abs`, :func:`mod`, :func:`sign`.
* Add :func:`shuffle` function.
* Add :func:`empty_approx_set` function.
* Compile `OUTER JOIN` with filer function.
* Require task concurrency and task writer count to be a power of two.
* Apply nulls-last order to :func:`array_sort`.

Hive Changes
------------

* Fix permissions for new tables in Hive SQL standard permissions implementation.
* Allow certain combinations of queries to be executed in a transaction-ish manner,
  for example dropping a partition and then recreating it. Atomicity is not guaranteed
  because of Hive limitation.
* Fail queries that attempt to rename partition columns.
* Add support for ORC bloom filter in predicate push down.  This is can be enabled with
  the `hive.orc.bloom-filters.enabled` configuration parameter or the `orc_bloom_filters_enabled`
  session property.
* Add new optimized RCFile reader.  This can be enabled `hive.rcfile-optimized-reader.enabled`
  configuration property or the `rcfile_optimized_reader_enabled` session property.
* Add support for `REAL`, which is a 32bit floating point number, in Hive.
* Record Presto query ID in Hive metastore for Hive writes.
* Support per-transaction cache for Hive metastore.

JMX Changes
-----------

* Make name configuration for history tables case-insensitive.

JDBC Changes
------------

* Optimize fetching column names when describing a single table.
* Add support for ``REAL`` data type.
* Add support for `REAL`, which is a 32bit floating point number, in JDBC driver.

PostgreSQL Changes
------------------

* Add support for querying materialized views.

SPI Changes
-----------

* Add support for pluggable resource group management. A ``Plugin`` can now
  provide management factories via ``getResourceGroupConfigurationManagerFactories()``
  and the factory can be enabled via the ``etc/resource-groups.properties``
  configuration file by setting the ``resource-groups.configuration-manager``
  property. See the ``presto-resource-group-managers`` plugin for an example.
