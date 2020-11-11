=============
Release 0.241
=============

.. warning::
    There is a bug in LambdaDefinitionExpression canonicalization introduced since 0.238. For more details, go to :issue:`15424`.

General Changes
_______________
* Fix incorrect results from function :func:`classification_precision` introduced in release 0.239.
* Improve performance for queries with broadcast or collocated joins by adding dynamic filtering and bucket pruning support. This can be enabled with the ``experimental.enable-dynamic-filtering`` configuration property and ``enable_dynamic_filtering`` system session property. For more information please refer to (:pr:`15077`).
* Add new warning message for ``UNION`` queries without ``ALL`` or ``DISTINCT`` keywords.
* Downgrade the ZSTD JNI compressor version to resolve the frequent excessive GC events introduced in version 0.238.

Security Changes
________________
* Implement REST endpoint authorization in Presto. See :doc:`/security/authorization`.

JDBC Changes
____________
* Add support for ``ResultSet.getStatement``.
* Add support for Oracle JDBC connections.

Hive Changes
____________
* Fix several memory accounting bugs in ``OrcRecordReader`` and ``StreamReader``.
* Add procedure ``system.sync_partition_metadata()`` to synchronize the partitions in the metastore with the partitions that are physically in the file system.
* Add support for direct recursive file listings in ``PrestoS3FileSystem``.
* Add support for non-Hive types to Hive views. This support had been removed in 0.233. If a view uses an unsupported type for any columns, only a single dummy column will be saved in the metastore.
* Add support for pushing dereferences into Parquet table scan, so that only the required nested column is read when other projected nested columns are in the same base column. This can be enabled with the ``hive.enable-parquet-dereference-pushdown`` Hive configuration property and ``parquet_batch_reader_verification_enabled`` Hive session property.

Druid Changes
_____________
* Add support for data ingestion. Three modes are supported: ``INSERT INTO SELECT``, ``CREATE TABLE AS``, and from local or HDFS folders.

Oracle Changes
______________
* Add Oracle connector.

Verifier Changes
________________
* Fix an issue during determinism analysis that queries with ``LIMIT`` clause are not identified as non-deterministic when a rerun of the control query fails.
* Add support to allow multiple control and test clusters.

SPI Changes
___________
* Add ``StageStatistics`` and ``OperatorStatistics`` to ``QueryCompletedEvent``, and remove stage and operator statistics from ``QueryStatistics``.

Geospatial Changes
__________________
* Fix a bug when two envelopes intersect at a point for :func:`ST_Intersection`.
* Add :func:`geometry_nearest_points` to find nearest points of a pair of geometries.
