=============
Release 0.226
=============

General Changes
_______________
* Fix ST_Buffer for tiny geometries (:issue:`13194`)
* Fix ST_Centroid for tiny geometries (:issue:`10629`)
* Fix failure when reading unpartitioned table from some connectors with session property ``grouped_execution_for_eligible_table_scans`` turned on.
* Improve memory tracking for geospatial query indexing and prevent worker node from crashing due to out of memory.
* Improve memory usage of spatial joins.
* Add peak running task count to statistics field of QueryCompletedEvent.

Hive Changes
____________
* Fix computation for number of buckets accessed for ``max_buckets_for_grouped_execution``.
* Fix a bug where the bucket column was not available if ``max_buckets_for_grouped_execution`` was exceeded.
* Fix high CPU usage when writing ORC files with too many row groups.
* Improve parallelism of writes to hive bucketed tables by respecting the ``task.writer-count`` configuration property.
* Add debug mode that allows reading from offline table and partition. This is controlled by session property ``offline_data_debug_mode_enabled``.
