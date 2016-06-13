=============
Release 0.147
=============

General Changes
---------------

* Fix race condition that can cause queries that process data from non-columnar
  data sources to fail.
* Fix incorrect formatting of dates and timestamps before year 1680.
* Fix handling of syntax errors when parsing ``EXTRACT``.
* Fix potential scheduling deadlock for connectors that expose node-partitioned data.
* Fix performance regression that increased planning time.
* Fix incorrect results for grouping sets for some queries with filters.
* Add :doc:`/sql/show-create-view` and :doc:`/sql/show-create-table`.
* Add support for column aliases in ``WITH`` clause.
* Support ``LIKE`` clause for :doc:`/sql/show-catalogs` and :doc:`/sql/show-schemas`.
* Add support for ``INTERSECT``.
* Add support for casting row types.
* Add :func:`sequence` function.
* Add :func:`sign` function.
* Add :func:`flatten` function.
* Add experimental implementation of :doc:`resource groups </admin/resource-groups>`.
* Add :doc:`/connector/localfile`.
* Remove experimental intermediate aggregation optimizer. The ``optimizer.use-intermediate-aggregations``
  config option and ``task_intermediate_aggregation`` session property are no longer supported.
* Add support for colocated joins for connectors that expose node-partitioned data.
* Improve the performance of :func:`array_intersect`.
* Generalize the intra-node parallel execution system to work with all query stages.
  The ``task.concurrency`` configuration property replaces the old ``task.join-concurrency``
  and ``task.default-concurrency`` options. Similarly, the ``task_concurrency`` session
  property replaces the ``task_join_concurrency``, ``task_hash_build_concurrency``, and
  ``task_aggregation_concurrency`` properties.

Hive Changes
------------

* Fix reading symlinks when the target is in a different HDFS instance.
* Fix ``NoClassDefFoundError`` for ``SubnetUtils`` in HDFS client.
* Fix error when reading from Hive tables with inconsistent bucketing metadata.
* Correctly report read bytes when reading Parquet data.
* Include path in unrecoverable S3 exception messages.
* When replacing an existing Presto view, update the view data
  in the Hive metastore rather than dropping and recreating it.
* Rename table property ``clustered_by`` to ``bucketed_by``.
* Add support for ``varchar(n)``.

Kafka Changes
-------------

* Fix ``error code 6`` when reading data from Kafka.
* Add support for ``varchar(n)``.

Redis Changes
-------------

* Add support for ``varchar(n)``.

MySQL and PostgreSQL Changes
----------------------------

* Cleanup temporary data when a ``CREATE TABLE AS`` fails.
* Add support for ``varchar(n)``.
