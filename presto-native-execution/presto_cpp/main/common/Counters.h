/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <folly/Range.h>

// Here we have all the counters presto cpp worker would export.
namespace facebook::presto {

// Sets up all the counters in the presto cpp, but specifying their types.
// See velox/common/base/StatsReporter.h for the interface.
void registerPrestoMetrics();

constexpr folly::StringPiece kCounterDriverCPUExecutorQueueSize{
    "presto_cpp.driver_cpu_executor_queue_size"};
constexpr folly::StringPiece kCounterDriverCPUExecutorLatencyMs{
    "presto_cpp.driver_cpu_executor_latency_ms"};

constexpr folly::StringPiece kCounterHTTPExecutorLatencyMs{
    "presto_cpp.http_executor_latency_ms"};
constexpr folly::StringPiece kCounterNumHTTPRequest{
    "presto_cpp.num_http_request"};
constexpr folly::StringPiece kCounterNumHTTPRequestError{
    "presto_cpp.num_http_request_error"};
constexpr folly::StringPiece kCounterHTTPRequestLatencyMs{
    "presto_cpp.http_request_latency_ms"};

/// Number of http client onBody calls in PrestoExchangeSource.
constexpr folly::StringPiece kCounterHttpClientPrestoExchangeNumOnBody{
    "presto_cpp.http.client.presto_exchange_source.num_on_body"};
/// Received IOBuf chain byte size in http client onBody call from
/// PrestoExchangeSource.
constexpr folly::StringPiece kCounterHttpClientPrestoExchangeOnBodyBytes{
    "presto_cpp.http.client.presto_exchange_source.on_body_bytes"};

/// SerializedPage size in bytes from PrestoExchangeSource.
constexpr folly::StringPiece kCounterPrestoExchangeSerializedPageSize{
    "presto_cpp.presto_exchange_source.serialized_page_size"};

constexpr folly::StringPiece kCounterNumQueryContexts{
    "presto_cpp.num_query_contexts"};

constexpr folly::StringPiece kCounterNumTasks{"presto_cpp.num_tasks"};
constexpr folly::StringPiece kCounterNumTasksRunning{
    "presto_cpp.num_tasks_running"};
constexpr folly::StringPiece kCounterNumTasksFinished{
    "presto_cpp.num_tasks_finished"};
constexpr folly::StringPiece kCounterNumTasksCancelled{
    "presto_cpp.num_tasks_cancelled"};
constexpr folly::StringPiece kCounterNumTasksAborted{
    "presto_cpp.num_tasks_aborted"};
constexpr folly::StringPiece kCounterNumTasksFailed{
    "presto_cpp.num_tasks_failed"};
constexpr folly::StringPiece kCounterNumZombieVeloxTasks{
    "presto_cpp.num_zombie_velox_tasks"};
constexpr folly::StringPiece kCounterNumZombiePrestoTasks{
    "presto_cpp.num_zombie_presto_tasks"};
constexpr folly::StringPiece kCounterNumTasksWithStuckOperator{
    "presto_cpp.num_tasks_with_stuck_operator"};
constexpr folly::StringPiece kCounterNumRunningDrivers{
    "presto_cpp.num_running_drivers"};
constexpr folly::StringPiece kCounterNumBlockedDrivers{
    "presto_cpp.num_blocked_drivers"};

/// Number of total OutputBuffer managed by all
/// OutputBufferManager
constexpr folly::StringPiece kCounterTotalPartitionedOutputBuffer{
    "presto_cpp.num_partitioned_output_buffer"};
/// Latency in millisecond of the get data call of a
/// OutputBufferManager.
constexpr folly::StringPiece kCounterPartitionedOutputBufferGetDataLatencyMs{
    "presto_cpp.partitioned_output_buffer_get_data_latency_ms"};

/// ================== OS Counters =================

/// User CPU time of the presto_server process in microsecond since the process
/// start.
constexpr folly::StringPiece kCounterOsUserCpuTimeMicros{
    "presto_cpp.os_user_cpu_time_micros"};
/// System CPU time of the presto_server process in microsecond since the
/// process start.
constexpr folly::StringPiece kCounterOsSystemCpuTimeMicros{
    "presto_cpp.os_system_cpu_time_micros"};
/// Total number of soft page faults of the presto_server process in microsecond
/// since the process start.
constexpr folly::StringPiece kCounterOsNumSoftPageFaults{
    "presto_cpp.os_num_soft_page_faults"};
/// Total number of hard page faults of the presto_server process in microsecond
/// since the process start.
constexpr folly::StringPiece kCounterOsNumHardPageFaults{
    "presto_cpp.os_num_hard_page_faults"};
/// Total number of voluntary context switches in the presto_server process.
constexpr folly::StringPiece kCounterOsNumVoluntaryContextSwitches{
    "presto_cpp.os_num_voluntary_context_switches"};
/// Total number of involuntary context switches in the presto_server process.
constexpr folly::StringPiece kCounterOsNumForcedContextSwitches{
    "presto_cpp.os_num_forced_context_switches"};

/// ================== Memory Counters =================

/// Number of bytes currently mapped in MemoryAllocator. These bytes represent
/// the bytes that are either currently being allocated or were in the past
/// allocated, not yet been returned back to the operating system, in the
/// form of 'Allocation' or 'ContiguousAllocation'.
constexpr folly::StringPiece kCounterMappedMemoryBytes{
    "presto_cpp.mapped_memory_bytes"};
/// Number of bytes currently allocated (used) from MemoryAllocator in the form
/// of 'Allocation' or 'ContiguousAllocation'.
constexpr folly::StringPiece kCounterAllocatedMemoryBytes{
    "presto_cpp.allocated_memory_bytes"};
/// Number of bytes currently mapped in MmapAllocator, in the form of
/// 'ContiguousAllocation'.
///
/// NOTE: This applies only to MmapAllocator
constexpr folly::StringPiece kCounterMmapExternalMappedBytes{
    "presto_cpp.mmap_external_mapped_bytes"};
/// Number of bytes currently allocated from MmapAllocator directly from raw
/// allocateBytes() interface, and internally allocated by malloc. Only small
/// chunks of memory are delegated to malloc
///
/// NOTE: This applies only to MmapAllocator
constexpr folly::StringPiece kCounterMmapRawAllocBytesSmall{
    "presto_cpp.mmap_raw_alloc_bytes_small"};

/// Peak number of bytes queued in PrestoExchangeSource waiting for consume.
constexpr folly::StringPiece kCounterExchangeSourcePeakQueuedBytes{
    "presto_cpp.exchange_source_peak_queued_bytes"};

/// ================== Memory Arbitrator Counters =================

/// The number of arbitration requests.
constexpr folly::StringPiece kCounterArbitratorNumRequests{
    "presto_cpp.arbitrator_num_requests"};
/// The number of aborted arbitration requests.
constexpr folly::StringPiece kCounterArbitratorNumAborted{
    "presto_cpp.arbitrator_num_aborted"};
/// The number of arbitration request failures.
constexpr folly::StringPiece kCounterArbitratorNumFailures{
    "presto_cpp.arbitrator_num_failures"};
/// The sum of all the arbitration request queue times in microseconds.
constexpr folly::StringPiece kCounterArbitratorQueueTimeUs{
    "presto_cpp.arbitrator_queue_time_us"};
/// The sum of all the arbitration run times in microseconds.
constexpr folly::StringPiece kCounterArbitratorArbitrationTimeUs{
    "presto_cpp.arbitrator_arbitration_time_us"};
/// The amount of memory bytes freed by reducing the memory pool's capacity
/// without actually freeing memory.
constexpr folly::StringPiece kCounterArbitratorNumShrunkBytes{
    "presto_cpp.arbitrator_num_shrunk_bytes"};
/// The amount of memory bytes freed by memory reclamation.
constexpr folly::StringPiece kCounterArbitratorNumReclaimedBytes{
    "presto_cpp.arbitrator_num_reclaimed_bytes"};
/// The free memory capacity in bytes.
constexpr folly::StringPiece kCounterArbitratorFreeCapacityBytes{
    "presto_cpp.arbitrator_free_capacity_bytes"};
/// The number of non-reclaimable operator reclaim attempts.
constexpr folly::StringPiece kCounterArbitratorNonReclaimableAttempts{
    "presto_cpp.arbitrator_non_reclaimable_attempts"};

/// ================== Disk Spilling Counters =================

/// The number of times that spilling runs on a velox operator.
constexpr folly::StringPiece kCounterSpillRuns{"presto_cpp.spill_run_count"};
/// The number of spilled files.
constexpr folly::StringPiece kCounterSpilledFiles{
    "presto_cpp.spilled_file_count"};
/// The number of spilled rows.
constexpr folly::StringPiece kCounterSpilledRows{
    "presto_cpp.spilled_row_count"};
/// The number of bytes spilled to disks.
///
/// NOTE: if compression is enabled, this counts the compressed bytes.
constexpr folly::StringPiece kCounterSpilledBytes{"presto_cpp.spilled_bytes"};
/// The time spent on filling rows for spilling.
constexpr folly::StringPiece kCounterSpillFillTimeUs{
    "presto_cpp.spill_fill_time_us"};
/// The time spent on sorting rows for spilling.
constexpr folly::StringPiece kCounterSpillSortTimeUs{
    "presto_cpp.spill_sort_time_us"};
/// The time spent on serializing rows for spilling.
constexpr folly::StringPiece kCounterSpillSerializationTimeUs{
    "presto_cpp.spill_serialization_time_us"};
/// The number of disk writes to spill rows.
constexpr folly::StringPiece kCounterSpillDiskWrites{
    "presto_cpp.spill_disk_write_count"};
/// The time spent on copy out serialized rows for disk write. If compression
/// is enabled, this includes the compression time.
constexpr folly::StringPiece kCounterSpillFlushTimeUs{
    "presto_cpp.spill_flush_time_us"};
/// The time spent on writing spilled rows to disk.
constexpr folly::StringPiece kCounterSpillWriteTimeUs{
    "presto_cpp.spill_write_time_us"};
/// The number of times that a spillable operator exceeds the max spill level
/// limit that can't spill.
constexpr folly::StringPiece kCounterSpillMaxLevelExceeded{
    "presto_cpp.spill_exceeded_max_level_count"};
/// The current spilling memory usage in bytes.
constexpr folly::StringPiece kCounterSpillMemoryBytes{
    "presto_cpp.spill_memory_bytes"};
/// The peak spilling memory usage in bytes.
constexpr folly::StringPiece kCounterSpillPeakMemoryBytes{
    "presto_cpp.spill_peak_memory_bytes"};

/// ================== Cache Counters ==================

/// Total number of cache entries.
constexpr folly::StringPiece kCounterMemoryCacheNumEntries{
    "presto_cpp.memory_cache_num_entries"};
/// Total number of cache entries that do not cache anything.
constexpr folly::StringPiece kCounterMemoryCacheNumEmptyEntries{
    "presto_cpp.memory_cache_num_empty_entries"};
/// Total number of cache entries that are pinned for shared access.
constexpr folly::StringPiece kCounterMemoryCacheNumSharedEntries{
    "presto_cpp.memory_cache_num_shared_entries"};
/// Total number of cache entries that are pinned for exclusive access.
constexpr folly::StringPiece kCounterMemoryCacheNumExclusiveEntries{
    "presto_cpp.memory_cache_num_exclusive_entries"};
/// Total number of cache entries that are being or have been prefetched but
/// have not been hit.
constexpr folly::StringPiece kCounterMemoryCacheNumPrefetchedEntries{
    "presto_cpp.memory_cache_num_prefetched_entries"};
/// Total number of bytes of the cached data that is much smaller than a
/// 'MappedMemory' page (AsyncDataCacheEntry::kTinyDataSize).
constexpr folly::StringPiece kCounterMemoryCacheTotalTinyBytes{
    "presto_cpp.memory_cache_total_tiny_bytes"};
/// Total number of bytes of the cached data excluding
/// 'kCounterMemoryCacheTotalTinyBytes'.
constexpr folly::StringPiece kCounterMemoryCacheTotalLargeBytes{
    "presto_cpp.memory_cache_total_large_bytes"};
/// Total unused capacity bytes in 'kCounterMemoryCacheTotalTinyBytes'.
constexpr folly::StringPiece kCounterMemoryCacheTotalTinyPaddingBytes{
    "presto_cpp.memory_cache_total_tiny_padding_bytes"};
/// Total unused capacity bytes in 'kCounterMemoryCacheTotalLargeBytes'.
constexpr folly::StringPiece kCounterMemoryCacheTotalLargePaddingBytes{
    "presto_cpp.memory_cache_total_large_padding_bytes"};
/// Total bytes of cache entries in prefetch state.
constexpr folly::StringPiece kCounterMemoryCacheTotalPrefetchBytes{
    "presto_cpp.memory_cache_total_prefetched_bytes"};
/// Sum of scores of evicted entries. This serves to infer an average lifetime
/// for entries in cache.
constexpr folly::StringPiece kCounterMemoryCacheSumEvictScore{
    "presto_cpp.memory_cache_sum_evict_score"};
/// Cumulated number of hits (saved IO). The first hit to a prefetched entry
/// does not count.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeHit{
    "presto_cpp.memory_cache_num_cumulative_hit"};
/// Number of hits (saved IO) since last counter retrieval. The first hit to a
/// prefetched entry does not count.
constexpr folly::StringPiece kCounterMemoryCacheNumHit{
    "presto_cpp.memory_cache_num_hit"};
/// Cumulated amount of hit bytes (saved IO). The first hit to a prefetched
/// entry does not count.
constexpr folly::StringPiece kCounterMemoryCacheCumulativeHitBytes{
    "presto_cpp.memory_cache_cumulative_hit_bytes"};
/// Amount of hit bytes (saved IO) since last counter retrieval. The first hit
/// to a prefetched entry does not count.
constexpr folly::StringPiece kCounterMemoryCacheHitBytes{
    "presto_cpp.memory_cache_hit_bytes"};
/// Cumulated number of new entries created.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeNew{
    "presto_cpp.memory_cache_num_cumulative_new"};
/// Number of new entries created since last counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumNew{
    "presto_cpp.memory_cache_num_new"};
/// Cumulated number of times a valid entry was removed in order to make space.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeEvict{
    "presto_cpp.memory_cache_num_cumulative_evict"};
/// Number of times a valid entry was removed in order to make space, since last
/// counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumEvict{
    "presto_cpp.memory_cache_num_evict"};
/// Cumulated number of entries considered for evicting.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeEvictChecks{
    "presto_cpp.memory_cache_num_cumulative_evict_checks"};
/// Number of entries considered for evicting, since last counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumEvictChecks{
    "presto_cpp.memory_cache_num_evict_checks"};
/// Cumulated number of times a user waited for an entry to transit from
/// exclusive to shared mode.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeWaitExclusive{
    "presto_cpp.memory_cache_num_cumulative_wait_exclusive"};
/// Number of times a user waited for an entry to transit from exclusive to
/// shared mode, since last counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumWaitExclusive{
    "presto_cpp.memory_cache_num_wait_exclusive"};
/// Cumulative clocks spent in allocating or freeing memory for backing cache
/// entries.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeAllocClocks{
    "presto_cpp.memory_cache_num_cumulative_alloc_clocks"};
/// Clocks spent in allocating or freeing memory for backing cache entries,
/// since last counter retrieval
constexpr folly::StringPiece kCounterMemoryCacheNumAllocClocks{
    "presto_cpp.memory_cache_num_alloc_clocks"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeReadEntries{
    "presto_cpp.ssd_cache_cumulative_read_entries"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeReadBytes{
    "presto_cpp.ssd_cache_cumulative_read_bytes"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeWrittenEntries{
    "presto_cpp.ssd_cache_cumulative_written_entries"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeWrittenBytes{
    "presto_cpp.ssd_cache_cumulative_written_bytes"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeCachedEntries{
    "presto_cpp.ssd_cache_cumulative_cached_entries"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeCachedBytes{
    "presto_cpp.ssd_cache_cumulative_cached_bytes"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeOpenSsdErrors{
    "presto_cpp.ssd_cache_cumulative_open_ssd_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeOpenCheckpointErrors{
    "presto_cpp.ssd_cache_cumulative_open_checkpoint_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeOpenLogErrors{
    "presto_cpp.ssd_cache_cumulative_open_log_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeDeleteCheckpointErrors{
    "presto_cpp.ssd_cache_cumulative_delete_checkpoint_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeGrowFileErrors{
    "presto_cpp.ssd_cache_cumulative_grow_file_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeWriteSsdErrors{
    "presto_cpp.ssd_cache_cumulative_write_ssd_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeWriteCheckpointErrors{
    "presto_cpp.ssd_cache_cumulative_write_checkpoint_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeReadSsdErrors{
    "presto_cpp.ssd_cache_cumulative_read_ssd_errors"};
constexpr folly::StringPiece kCounterSsdCacheCumulativeReadCheckpointErrors{
    "presto_cpp.ssd_cache_cumulative_read_checkpoint_errors"};

/// ================== HiveConnector Counters ==================
/// Format template strings use 'constexpr std::string_view' to be 'fmt::format'
/// compatible.
constexpr std::string_view kCounterHiveFileHandleCacheNumElementsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_elements"};
constexpr std::string_view kCounterHiveFileHandleCachePinnedSizeFormat{
    "presto_cpp.{}.hive_file_handle_cache_pinned_size"};
constexpr std::string_view kCounterHiveFileHandleCacheCurSizeFormat{
    "presto_cpp.{}.hive_file_handle_cache_cur_size"};
constexpr std::string_view kCounterHiveFileHandleCacheNumAccumulativeHitsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_accumulative_hits"};
constexpr std::string_view
    kCounterHiveFileHandleCacheNumAccumulativeLookupsFormat{
        "presto_cpp.{}.hive_file_handle_cache_num_accumulative_lookups"};
constexpr std::string_view kCounterHiveFileHandleCacheNumHitsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_hits"};
constexpr std::string_view kCounterHiveFileHandleCacheNumLookupsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_lookups"};
} // namespace facebook::presto
