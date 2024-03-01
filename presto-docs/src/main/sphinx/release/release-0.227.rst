=============
Release 0.227
=============

General Changes
_______________
* Fix a bug where index joins would fail with the error ``driver should never block``.
  Queries will now fail if the index is unable to load within the time specified by
  the ``index-loader-timeout`` configuration property and the ``index_loader_timeout``
  session property.
* Fix a bug that could cause ``NullPointerException`` in spatial join with geometry
  collections on the build side.
* Improve performance for queries with ``FULL OUTER JOIN`` where join keys have the
  ``COALESCE`` function applied.
* Improve performance for ``UNNEST`` queries.
* Improve performance of repartitioning data between stages.  The optimization can be
  enabled by the ``optimized_repartitioning`` session property or the
  ``experimental.optimized-repartitioning`` configuration property.
* Add spatial join (broadcast and partitioned) support for :func:`ST_Equals`,
  :func:`ST_Overlaps`, :func:`ST_Crosses`, and :func:`ST_Touches`.
* Add ``task_partitioned_writer_count`` session property to allow setting the number
  of concurrent writers for partitioned (bucketed) writes.
* Add ``IPPREFIX`` type and :func:`ip_prefix` function.
* Add :func:`differential_entropy` functions to compute differential entropy.
* Remove syntax support for ``SET PATH`` and ``CURRENT_PATH``. The path information was
  never used by Presto.

Hive Changes
____________
* Fix a bug that might lead to corruption when writing sorted table in the recoverable
  grouped execution mode.
* Fix ORC stripe skipping when using bloom filter.
* Improve the CPU load on coordinator by reducing the cost of serializing ``HiveSplit``.
* Improve GC pressure from Parquet reader by constraining the maximum column read size.
  This can be configured by the ``parquet_max_read_block_size`` session property or the
  ``hive.parquet.max-read-block-size`` configuration property.
* Add support for sub-field pruning when reading Parquet files, so that only necessary
  sub-fields are extracted from struct columns.
* Add configuration property ``hive.s3-file-system-type=HADOOP_DEFAULT`` to allow
  users to switch different Hadoop file system implementations for ``s3://`` addresses.
  The corresponding Hadoop File system implementation should be specified in ``core-site.xml``

Raptor Changes
______________
* Fix memory leak in file descriptor during shard compaction. The regression was introduced in 0.219.

Verifier Changes
________________
* Add support for auto-resolving query failures with ``HIVE_TOO_MANY_OPEN_PARTITIONS`` error.
* Add support to perform additional determinism analysis for queries with ``LIMIT`` clause.
* Add detailed determinism analysis result to ``VerifierOutputEvent``.

SPI Changes
________________
* Move ``AggregationNode`` to SPI. Connectors can now push down aggregation to table scan.
* Move ``ProjectNode`` to SPI. Connectors can now push down projection to table scan.
* Rename ``Block#getObject`` to ``Block#getBlock`` and remove unnecessary ``clazz`` parameter.