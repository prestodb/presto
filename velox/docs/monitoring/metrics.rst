===============
Runtime Metrics
===============

Runtime metrics are used to collect the metrics of important velox runtime events
for monitoring purpose. The collected metrics can provide insights into the
continuous availability and performance analysis of a Velox runtime system. For
instance, the collected data can help automatically generate alerts at an
outage. Velox provides a framework to collect the metrics which consists of
three steps:

**Define**: define the name and type for the metric through DEFINE_METRIC and
DEFINE_HISTOGRAM_METRIC macros. DEFINE_HISTOGRAM_METRIC is used for histogram
metric type and DEFINE_METRIC is used for the other types (see metric type
definition below). BaseStatsReporter provides methods for metric definition.
Register metrics during startup using registerVeloxMetrics() API.

**Record**: record the metric data point using RECORD_METRIC_VALUE and
RECORD_HISTOGRAM_METRIC_VALUE macros when the corresponding event happens.
BaseStatsReporter provides methods for metric recording.

**Export**: aggregates the collected data points based on the defined metrics,
and periodically exports to the backend monitoring service, such as ODS used by
Meta, Apache projects `OpenCensus <https://opencensus.io/>`_  and `Prometheus <https://prometheus.io/>`_ provided by OSS. A derived
implementation of BaseStatsReporter is required to integrate with a specific
monitoring service. The metric aggregation granularity and export interval are
also configured based on the actual used monitoring service.

Velox supports five metric types:

**Count**: tracks the count of events, such as the number of query failures.

**Sum**: tracks the sum of event data point values, such as sum of query scan
read bytes.

**Avg**: tracks the average of event data point values, such as average of query
execution time.

**Rate**: tracks the sum of event data point values per second, such as the
number of shuffle requests per second.

**Histogram**: tracks the distribution of event data point values, such as query
execution time distribution. The histogram metric divides the entire data range
into a series of adjacent equal-sized intervals or buckets, and then count how
many data values fall into each bucket. DEFINE_HISTOGRAM_METRIC specifies the data
range by min/max values, and the number of buckets. Any collected data value
less than min is counted in min bucket, and any one larger than max is counted
in max bucket. It also allows to specify the value percentiles to report for
monitoring. This allows BaseStatsReporter and the backend monitoring service to
optimize the aggregated data storage.

Task Execution
--------------
.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - driver_yield_count
     - Count
     - The number of times that a driver has yielded from the thread when it
       hits the per-driver cpu time slice limit if enforced.
   * - driver_queue_time_ms
     - Histogram
     - The distribution of driver queue latency in range of [0, 10s] with
       20 buckets. It is configured to report the latency at P50, P90, P99,
       and P100 percentiles.
   * - driver_exec_time_ms
     - Histogram
     - The distribution of driver execution time in range of [0, 30s] with
       30 buckets. It is configured to report the latency at P50, P90, P99,
       and P100 percentiles.

Memory Management
-----------------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - cache_shrink_count
     - Count
     - The number of times that in-memory data cache has been shrunk under
       memory pressure.
   * - cache_shrink_ms
     - Histogram
     - The distribution of cache shrink latency in range of [0, 100s] with 10
       buckets. It is configured to report the latency at P50, P90, P99, and
       P100 percentiles.
   * - memory_reclaim_count
     - Count
     - The count of operator memory reclaims.
   * - memory_reclaim_exec_ms
     - Histogram
     - The distribution of memory reclaim execution time in range of [0, 600s]
       with 20 buckets. It is configured to report latency at P50, P90, P99, and
       P100 percentiles.
   * - memory_reclaim_bytes
     - Histogram
     - The distribution of reclaimed bytes in range of [0, 4GB] with 64 buckets
       and reports P50, P90, P99, and P100.
   * - task_memory_reclaim_count
     - Count
     - The count of task memory reclaims.
   * - task_memory_reclaim_wait_ms
     - Histogram
     - The distribution of task memory reclaim wait time in range of [0, 60s]
       with 60 buckets. It is configured to report latency at P50, P90, P99,
       and P100 percentiles.
   * - task_memory_reclaim_exec_ms
     - Histogram
     - The distribution of task memory execution time in range of [0, 240s]
       with 60 buckets. It is configured to report latency at P50, P90, P99,
       and P100 percentiles.
   * - task_memory_reclaim_wait_timeout_count
     - Count
     - The number of times that the task memory reclaim wait timeouts.
   * - memory_non_reclaimable_count
     - Count
     - The number of times that the memory reclaim fails because the operator is executing a
       non-reclaimable section where it is expected to have reserved enough memory to execute
       without asking for more. Therefore, it is an indicator that the memory reservation
       is not sufficient. It excludes counting instances where the operator is in a
       non-reclaimable state due to currently being on-thread and running or being already
       cancelled.
   * - arbitrator_requests_count
     - Count
     - The number of times a memory arbitration request was initiated by a
       memory pool attempting to grow its capacity.
   * - arbitrator_local_arbitration_count
     - Count
     - The number of arbitration that reclaims the used memory from the query which initiates
       the memory arbitration request itself. It ensures the memory arbitration request won't
       exceed its per-query memory capacity limit.
   * - arbitrator_global_arbitration_count
     - Count
     - The number of arbitration which ensures the total allocated query capacity won't exceed
       the arbitrator capacity limit. It may or may not reclaim memory from the query which
       initiate the memory arbitration request. This indicates the velox runtime doesn't have
       enough memory to run all the queries at their peak memory usage. We have to trigger
       spilling to let them run through completion.
   * - arbitrator_slow_global_arbitration_count
     - Count
     - The number of global arbitration that reclaims used memory by slow disk spilling.
   * - arbitrator_aborted_count
     - Count
     - The number of times a query level memory pool is aborted as a result of
       a memory arbitration process. The memory pool aborted will eventually
       result in a cancelling the original query.
   * - arbitrator_failures_count
     - Count
     - The number of times a memory arbitration request failed. This may occur
       either because the requester was terminated during the processing of
       its request, the arbitration request would surpass the maximum allowed
       capacity for the requester, or the arbitration process couldn't release
       the requested amount of memory.
   * - arbitrator_wait_time_ms
     - Histogram
     - The distribution of the amount of time an arbitration request stays in
       arbitration queues and waits the arbitration r/w locks in range of [0, 600s]
       with 20 buckets. It is configured to report the latency at P50, P90, P99,
       and P100 percentiles.
   * - arbitrator_arbitration_time_ms
     - Histogram
     - The distribution of the amount of time it take to complete a single
       arbitration request stays queued in range of [0, 600s] with 20
       buckets. It is configured to report the latency at P50, P90, P99,
       and P100 percentiles.
   * - arbitrator_free_capacity_bytes
     - Average
     - The average of total free memory capacity which is managed by the
       memory arbitrator.
   * - arbitrator_free_reserved_capacity_bytes
     - Average
     - The average of free memory capacity reserved to ensure each query has
       the minimal required capacity to run.
   * - memory_pool_initial_capacity_bytes
     - Histogram
     - The distribution of a root memory pool's initial capacity in range of [0 256MB]
       with 32 buckets. It is configured to report the capacity at P50, P90, P99,
       and P100 percentiles.
   * - memory_pool_capacity_growth_count
     - Histogram
     - The distribution of a root memory pool cappacity growth attemps through
       memory arbitration in range of [0, 256] with 32 buckets. It is configured
       to report the count at P50, P90, P99, and P100 percentiles.
   * - memory_pool_usage_leak_bytes
     - Sum
     - The leaf memory pool usage leak in bytes.
   * - memory_pool_reservation_leak_bytes
     - Sum
     - The leaf memory pool reservation leak in bytes.
   * - memory_pool_capacity_leak_bytes
     - Sum
     - The root memory pool reservation leak in bytes.
   * - memory_allocator_double_free_count
     - Count
     - Tracks the count of double frees in memory allocator, indicating the
       possibility of buffer ownership issues when a buffer is freed more
       than once.
   * - memory_allocator_mapped_bytes
     - Avg
     - Number of bytes currently mapped in MemoryAllocator. These bytes represent
       the bytes that are either currently being allocated or were in the past
       allocated, not yet been returned back to the operating system, in the
       form of 'Allocation' or 'ContiguousAllocation'.
   * - memory_allocator_alloc_bytes
     - Avg
     - Number of bytes currently allocated (used) from MemoryAllocator in the form
       of 'Allocation' or 'ContiguousAllocation'.
   * - mmap_allocator_external_mapped_bytes
     - Avg
     - Number of bytes currently mapped in MmapAllocator, in the form of
       'ContiguousAllocation'.
       NOTE: This applies only to MmapAllocator
   * - mmap_allocator_delegated_alloc_bytes
     - Avg
     - Number of bytes currently allocated from MmapAllocator directly from raw
       allocateBytes() interface, and internally allocated by malloc. Only small
       chunks of memory are delegated to malloc
       NOTE: This applies only to MmapAllocator

Cache
--------------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - cache_max_age_secs
     - Avg
     - Max possible age of AsyncDataCache and SsdCache entries since the raw file
       was opened to load the cache.
   * - memory_cache_num_entries
     - Avg
     - Total number of cache entries.
   * - memory_cache_num_empty_entries
     - Avg
     - Total number of cache entries that do not cache anything.
   * - memory_cache_num_shared_entries
     - Avg
     - Total number of cache entries that are pinned for shared access.
   * - memory_cache_num_exclusive_entries
     - Avg
     - Total number of cache entries that are pinned for exclusive access.
   * - memory_cache_num_prefetched_entries
     - Avg
     - Total number of cache entries that are being or have been prefetched but
       have not been hit.
   * - memory_cache_total_tiny_bytes
     - Avg
     - Total number of bytes of the cached data that is much smaller than kTinyDataSize.
   * - memory_cache_total_large_bytes
     - Avg
     - Total number of bytes of the cached data excluding 'memory_cache_total_tiny_bytes'
   * - memory_cache_total_tiny_padding_bytes
     - Avg
     - Total unused capacity bytes in 'memory_cache_total_tiny_bytes'.
   * - memory_cache_total_large_padding_bytes
     - Avg
     - Total unused capacity bytes in 'memory_cache_total_large_bytes'.
   * - memory_cache_total_prefetched_bytes
     - Avg
     - Total bytes of cache entries in prefetch state.
   * - memory_cache_sum_evict_score
     - Sum
     - Sum of scores of evicted entries. This serves to infer an average lifetime
       for entries in cache.
   * - memory_cache_num_hits
     - Sum
     - Number of hits (saved IO) since last counter retrieval. The first hit to a
       prefetched entry does not count.
   * - memory_cache_hit_bytes
     - Sum
     - Amount of hit bytes (saved IO) since last counter retrieval. The first hit
       to a prefetched entry does not count.
   * - memory_cache_num_new
     - Sum
     - Number of new entries created since last counter retrieval.
   * - memory_cache_num_evicts
     - Sum
     - Number of times a valid entry was removed in order to make space, since
       last counter retrieval.
   * - memory_cache_num_savable_evicts
     - Sum
     - Number of times a valid entry was removed in order to make space but has not
       been saved to SSD yet, since last counter retrieval.
   * - memory_cache_num_evict_checks
     - Sum
     - Number of entries considered for evicting, since last counter retrieval.
   * - memory_cache_num_wait_exclusive
     - Sum
     - Number of times a user waited for an entry to transit from exclusive to
       shared mode, since last counter retrieval.
   * - memory_cache_num_alloc_clocks
     - Sum
     - Clocks spent in allocating or freeing memory for backing cache entries,
       since last counter retrieval
   * - memory_cache_num_aged_out_entries
     - Sum
     - Number of AsyncDataCache entries that are aged out and evicted.
       given configured TTL.
   * - memory_cache_num_stale_entries
     - Count
     - Number of AsyncDataCache entries that are stale because of cache request
       size mismatch.
   * - ssd_cache_cached_regions
     - Avg
     - Number of regions currently cached by SSD.
   * - ssd_cache_cached_entries
     - Avg
     - Number of entries currently cached by SSD.
   * - ssd_cache_cached_bytes
     - Avg
     - Total bytes currently cached by SSD.
   * - ssd_cache_read_entries
     - Sum
     - Total number of entries read from SSD.
   * - ssd_cache_read_bytes
     - Sum
     - Total number of bytes read from SSD.
   * - ssd_cache_written_entries
     - Sum
     - Total number of entries written to SSD.
   * - ssd_cache_written_bytes
     - Sum
     - Total number of bytes written to SSD.
   * - ssd_cache_aged_out_entries
     - Sum
     - Total number of SsdCache entries that are aged out and evicted given
       configured TTL.
   * - ssd_cache_aged_out_regions
     - Sum
     - Total number of SsdCache regions that are aged out and evicted given
       configured TTL.
   * - ssd_cache_open_ssd_errors
     - Sum
     - Total number of SSD file open errors.
   * - ssd_cache_open_checkpoint_errors
     - Sum
     - Total number of SSD checkpoint file open errors.
   * - ssd_cache_open_log_errors
     - Sum
     - Total number of SSD evict log file open errors.
   * - ssd_cache_delete_checkpoint_errors
     - Sum
     - Total number of errors while deleting SSD checkpoint files.
   * - ssd_cache_read_without_checksum
     - Sum
     - Total number of SSD cache reads without checksum verification
       due to SSD cache request size mismatch
   * - ssd_cache_grow_file_errors
     - Sum
     - Total number of errors while growing SSD cache files.
   * - ssd_cache_write_ssd_errors
     - Sum
     - Total number of error while writing to SSD cache files.
   * - ssd_cache_write_ssd_dropped
     - Sum
     - Total number of writes dropped due to no cache space.
   * - ssd_cache_write_checkpoint_errors
     - Sum
     - Total number of errors while writing SSD checkpoint file.
   * - ssd_cache_read_corruptions
     - Sum
     - Total number of corrupted SSD data read detected by checksum.
   * - ssd_cache_read_ssd_errors
     - Sum
     - Total number of errors while reading from SSD cache files.
   * - ssd_cache_read_checkpoint_errors
     - Sum
     - Total number of errors while reading from SSD checkpoint files.
   * - ssd_cache_checkpoints_read
     - Sum
     - Total number of checkpoints read.
   * - ssd_cache_checkpoints_written
     - Sum
     - Total number of checkpoints written.
   * - ssd_cache_regions_evicted
     - Sum
     - Total number of cache regions evicted.
   * - ssd_cache_recovered_entries
     - Sum
     - Total number of cache entries recovered from checkpoint.

Storage
-------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - storage_throttled_duration_ms
     - Histogram
     - The time distribution of storage IO throttled duration in range of [0, 30s]
       with 30 buckets. It is configured to report the capacity at P50, P90, P99,
       and P100 percentiles.
   * - storage_local_throttled_count
     - Count
     - The number of times that storage IOs get throttled in a storage directory.
   * - storage_global_throttled_count
     - Count
     - The number of times that storage IOs get throttled in a storage cluster.

Spilling
--------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - spill_max_level_exceeded_count
     - Count
     - The number of times that a spill-able operator hits the max spill level
       limit.
   * - spill_input_bytes
     - Sum
     - The number of bytes in memory to spill.
   * - spill_bytes
     - Sum
     - The number of bytes spilled to disk which can be the number of compressed
       bytes if compression is enabled.
   * - spill_rows_count
     - Count
     - The number of spilled rows.
   * - spill_files_count
     - Count
     - The number of spilled files.
   * - spill_fill_time_ms
     - Histogram
     - The distribution of the amount of time spent on filling rows for spilling
       in range of [0, 600s] with 20 buckets. It is configured to report the
       latency at P50, P90, P99, and P100 percentiles.
   * - spill_sort_time_ms
     - Histogram
     - The distribution of the amount of time spent on sorting rows for spilling
       in range of [0, 600s] with 20 buckets. It is configured to report the
       latency at P50, P90, P99, and P100 percentiles.
   * - spill_serialization_time_ms
     - Histogram
     - The distribution of the amount of time spent on serializing rows for
       spilling in range of [0, 600s] with 20 buckets. It is configured to report
       the latency at P50, P90, P99, and P100 percentiles.
   * - spill_disk_writes_count
     - Count
     - The number of disk writes to spill rows.
   * - spill_flush_time_ms
     - Histogram
     - The distribution of the amount of time spent on copy out serialized
       rows for disk write in range of [0, 600s] with 20 buckets. It is configured
       to report the latency at P50, P90, P99, and P100 percentiles. Note:  If
       compression is enabled, this includes the compression time.
   * - spill_write_time_ms
     - Histogram
     - The distribution of the amount of time spent on writing spilled rows to
       disk in range of [0, 600s] with 20 buckets. It is configured to report the
       latency at P50, P90, P99, and P100 percentiles.
   * - file_writer_early_flushed_raw_bytes
     - Sum
     - Number of bytes pre-maturely flushed from file writers because of memory reclaiming.
   * - spill_memory_bytes
     - Avg
     - The current spilling memory usage in bytes.
   * - spill_peak_memory_bytes
     - Avg
     - The peak spilling memory usage in bytes.

Exchange
--------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - exchange_data_time_ms
     - Histogram
     - The distribution of data exchange latency in range of [0, 50s] with 50
       buckets. It is configured to report latency at P50, P90, P99, and P100
       percentiles.
   * - exchange_data_bytes
     - Sum
     - The exchange data size in bytes.
   * - exchange_data_size
     - Histogram
     - The distribution of exchange data size in range of [0, 128MB] with 128
       buckets. It is configured to report the capacity at P50, P90, P99, and P100
       percentiles.
   * - exchange_data_count
     - Count
     - The number of data exchange requests.
   * - exchange_data_size_time_ms
     - Histogram
     - The distribution of data exchange size latency in range of [0, 5s] with 50
       buckets. It is configured to report latency at P50, P90, P99, and P100
       percentiles.
   * - exchange_data_size_count
     - Count
     - The number of data size exchange requests.

Hive Connector
--------------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - hive_file_handle_generate_latency_ms
     - Histogram
     - The distribution of hive file open latency in range of [0, 100s] with 10
       buckets. It is configured to report latency at P50, P90, P99, and P100
       percentiles.
