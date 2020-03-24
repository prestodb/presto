=============
Release 0.233
=============

.. warning::

   There is a bug in this release that will cause queries with the predicate ``IS NULL`` on
   bucketed columns to produce incorrect results.

General Changes
_______________
* Fix an optimizer failure introduced in ``0.229``, where a ``LIKE`` pattern is deduced into a constant, e.g., ``col LIKE 'a' and col = 'b'``.
* Fix correctness issue in queries with joins over ``UNNEST``. (:issue:`14257`).
* Fix ``ArbitraryOutputBuffer`` to avoid skewing output data distribution. (:pr:`14083`).
* Fix an issue where :func:`classification_fall_out` cannot be found.
* Add support for async page transport with non-blocking IO. This can be enabled by the ``exchange.async-page-transport-enabled`` configuration property.
* Add support to handle http request timeouts using multiple thread pools. This can be controlled by the ``task.http-timeout-concurrency`` configuration property.
* Add support for soft affinity scheduling, which makes the best effort to fetch the same piece of data from the same worker,
  while allowing fallback to random workers if the preferred workers are too busy to handle additional splits. (See :doc:`/develop/connectors`).
* Add hash functions ``fnv1_32``, ``fnv1_64``, ``fnv1a_32``, and ``fnv1a_64``.
* Add IP address functions :func:`ip_subnet_min`, :func:`ip_subnet_max`, :func:`ip_subnet_range`, and :func:`is_subnet_of`.
* Improve performance of ``StreamingAggregationOperator``.

Hive Changes
____________
* Fix an issue where Presto fail to start when configuration property  ``hive.s3-file-system-type`` is set to ``HADOOP_DEFAULT``.
* Add directory listing cache for Hive Connector. This can be enabled by setting configuration property ``hive.file-status-cache-tables``.
* Add support for storing column names and types for views in the Hive metastore. Views in the Hive connector can now only use types supported by Hive.
* Add configuration property ``hive.insert-overwrite-immutable-partitions-enabled`` to allow admin to set insert overwrite
  as the default insertion behavior for Hive connector.
* Add configuration property ``hive.node-selection-strategy`` to choose ``NodeSelectionStrategy``. When ``SOFT_AFFINITY`` is selected,
  scheduler will make the best effort to request the same worker to fetch the same file.
* Remove configuration property ``hive.force-local-scheduling``. The same functionality can be achieved by setting
  ``hive.node-selection-strategy`` to ``HARD_AFFINITY``.

Verifier Changes
________________
* Fix an issue where invalid checksum queries can be generated for certain queries containing columns of ``RowType``.
* Fix an issue where checksum query would fail for queries containing map columns whose key or value types are arrays or rows.
* Fix incorrect decision for determinism analysis of queries with top-level ``LIMIT`` clause. (:pr:`14176`).
* Add checks for keys, values, and cardinality sum when validating a map column.
* Add support to disable individual failure resolvers (:pr:`14148`).
* Add support to auto-resolve control checksum query failures with ``COMPILER_ERROR``, instead of skipping the verification.
* Add support for specifying non-deterministic catalogs by the ``determinism.non-determinism-catalogs`` configuration property.
  Queries explicitly referencing tables from those catalogs are treated as non-deterministic.
* Improve query performance during determinism analysis of queries with top-level ``LIMIT`` clause.
* Improve correctness check for floating point columns whose mean values of either the control query or the test query is closed to 0.

Druid Changes
_____________
* Add Druid Connector.

Geospatial Changes
__________________
* Improve :func:`ST_Points` to add support for major well-known spatial objects.
  :func:`ST_Points` now supports ``POINT``, ``LINESTRING``, ``POLYGON``, ``MULTIPOINT``, ``MULTILINESTRING``, ``MULTIPOLYGON`` and ``GEOMETRYCOLLECTION``.
* Improve :func:`ST_IsValid` and :func:`ST_IsSimple` to adhere to the ISO/OGC standards more closely.
  The two functions used to return the same result but may now be different. Users should check both functions to be sure their geometries are well-behaved.
  :func:`geometry_invalid_reason` will return different but semantically similar strings.
* Improve performance of :func:`ST_Intersection` by simply returning the geometry if it has an enclosing envelope.
  This can reduce CPU cost by up to ``10^5x`` for complex polygons.

SPI Changes
___________
* Add parameter ``NodeSelectionStrategy nodeSelectionStrategy`` in method ``ConnectorBucketNodeMap#createBucketNodeMap`` to indicate
  which affinity strategy to use when creating a bucket node map.
* Add parameter ``List<Node> sortedNodes`` in method ``ConnectorNodePartitioningProvider#getBucketNodeMap`` to provide
  a sorted list of nodes from which a connector can choose to perform affinity scheduling.
* Add enum ``NodeSelectionStrategy``. ``NO_PREFERENCE`` indicates data is remotely accessible from workers,
  ``HARD_AFFINITY`` to indicate data and workers are collocated, and ``SOFT_AFFINITY`` to indicate data is remotely accessible
  but scheduler will make the best effort to fetch the same piece of data from the same worker.
* Replace ``ConnectorSplit#isRemoteAccessible`` with ``getNodeSelectionStrategy``.
* Replace ``ConnectorSplit#getAddresses`` with ``getPreferredNodes``, to provide hints to the scheduler where to schedule splits.
* Replace the ``SchemaTableName`` parameter in ``ConnectorMetadata#createView`` with a ``ConnectorTableMetadata``.
* Move ``JsonType`` to SPI.
