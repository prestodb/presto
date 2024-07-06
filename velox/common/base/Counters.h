/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

namespace facebook::velox {

/// Velox metrics Registration.
void registerVeloxMetrics();

constexpr folly::StringPiece kMetricHiveFileHandleGenerateLatencyMs{
    "velox.hive_file_handle_generate_latency_ms"};

constexpr folly::StringPiece kMetricCacheShrinkCount{
    "velox.cache_shrink_count"};

constexpr folly::StringPiece kMetricCacheShrinkTimeMs{"velox.cache_shrink_ms"};

constexpr folly::StringPiece kMetricMaxSpillLevelExceededCount{
    "velox.spill_max_level_exceeded_count"};

constexpr folly::StringPiece kMetricMemoryReclaimExecTimeMs{
    "velox.memory_reclaim_exec_ms"};

constexpr folly::StringPiece kMetricMemoryReclaimedBytes{
    "velox.memory_reclaim_bytes"};

constexpr folly::StringPiece kMetricMemoryReclaimCount{
    "velox.memory_reclaim_count"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimCount{
    "velox.task_memory_reclaim_count"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimWaitTimeMs{
    "velox.task_memory_reclaim_wait_ms"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimExecTimeMs{
    "velox.task_memory_reclaim_exec_ms"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimWaitTimeoutCount{
    "velox.task_memory_reclaim_wait_timeout_count"};

constexpr folly::StringPiece kMetricMemoryNonReclaimableCount{
    "velox.memory_non_reclaimable_count"};

constexpr folly::StringPiece kMetricMemoryPoolInitialCapacityBytes{
    "velox.memory_pool_initial_capacity_bytes"};

constexpr folly::StringPiece kMetricMemoryPoolCapacityGrowCount{
    "velox.memory_pool_capacity_growth_count"};

constexpr folly::StringPiece kMetricMemoryPoolUsageLeakBytes{
    "velox.memory_pool_usage_leak_bytes"};

constexpr folly::StringPiece kMetricMemoryPoolReservationLeakBytes{
    "velox.memory_pool_reservation_leak_bytes"};

constexpr folly::StringPiece kMetricMemoryAllocatorDoubleFreeCount{
    "velox.memory_allocator_double_free_count"};

constexpr folly::StringPiece kMetricArbitratorLocalArbitrationCount{
    "velox.arbitrator_local_arbitration_count"};

constexpr folly::StringPiece kMetricArbitratorGlobalArbitrationCount{
    "velox.arbitrator_global_arbitration_count"};

constexpr folly::StringPiece kMetricArbitratorSlowGlobalArbitrationCount{
    "velox.arbitrator_slow_global_arbitration_count"};

constexpr folly::StringPiece kMetricArbitratorAbortedCount{
    "velox.arbitrator_aborted_count"};

constexpr folly::StringPiece kMetricArbitratorFailuresCount{
    "velox.arbitrator_failures_count"};

constexpr folly::StringPiece kMetricArbitratorArbitrationTimeMs{
    "velox.arbitrator_arbitration_time_ms"};

constexpr folly::StringPiece kMetricArbitratorWaitTimeMs{
    "velox.arbitrator_wait_time_ms"};

constexpr folly::StringPiece kMetricArbitratorFreeCapacityBytes{
    "velox.arbitrator_free_capacity_bytes"};

constexpr folly::StringPiece kMetricArbitratorFreeReservedCapacityBytes{
    "velox.arbitrator_free_reserved_capacity_bytes"};

constexpr folly::StringPiece kMetricDriverYieldCount{
    "velox.driver_yield_count"};

constexpr folly::StringPiece kMetricDriverQueueTimeMs{
    "velox.driver_queue_time_ms"};

constexpr folly::StringPiece kMetricDriverExecTimeMs{
    "velox.driver_exec_time_ms"};

constexpr folly::StringPiece kMetricSpilledInputBytes{
    "velox.spill_input_bytes"};

constexpr folly::StringPiece kMetricSpilledBytes{"velox.spill_bytes"};

constexpr folly::StringPiece kMetricSpilledRowsCount{"velox.spill_rows_count"};

constexpr folly::StringPiece kMetricSpilledFilesCount{
    "velox.spill_files_count"};

constexpr folly::StringPiece kMetricSpillFillTimeMs{"velox.spill_fill_time_ms"};

constexpr folly::StringPiece kMetricSpillSortTimeMs{"velox.spill_sort_time_ms"};

constexpr folly::StringPiece kMetricSpillSerializationTimeMs{
    "velox.spill_serialization_time_ms"};

constexpr folly::StringPiece kMetricSpillWritesCount{
    "velox.spill_writes_count"};

constexpr folly::StringPiece kMetricSpillFlushTimeMs{
    "velox.spill_flush_time_ms"};

constexpr folly::StringPiece kMetricSpillWriteTimeMs{
    "velox.spill_write_time_ms"};

constexpr folly::StringPiece kMetricSpillMemoryBytes{
    "velox.spill_memory_bytes"};

constexpr folly::StringPiece kMetricSpillPeakMemoryBytes{
    "velox.spill_peak_memory_bytes"};

constexpr folly::StringPiece kMetricFileWriterEarlyFlushedRawBytes{
    "velox.file_writer_early_flushed_raw_bytes"};

constexpr folly::StringPiece kMetricArbitratorRequestsCount{
    "velox.arbitrator_requests_count"};

constexpr folly::StringPiece kMetricMappedMemoryBytes{
    "velox.memory_allocator_mapped_bytes"};

constexpr folly::StringPiece kMetricAllocatedMemoryBytes{
    "velox.memory_allocator_alloc_bytes"};

constexpr folly::StringPiece kMetricMmapExternalMappedBytes{
    "velox.mmap_allocator_external_mapped_bytes"};

constexpr folly::StringPiece kMetricMmapDelegatedAllocBytes{
    "velox.mmap_allocator_delegated_alloc_bytes"};

constexpr folly::StringPiece kMetricCacheMaxAgeSecs{"velox.cache_max_age_secs"};

constexpr folly::StringPiece kMetricMemoryCacheNumEntries{
    "velox.memory_cache_num_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumEmptyEntries{
    "velox.memory_cache_num_empty_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumSharedEntries{
    "velox.memory_cache_num_shared_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumExclusiveEntries{
    "velox.memory_cache_num_exclusive_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumPrefetchedEntries{
    "velox.memory_cache_num_prefetched_entries"};

constexpr folly::StringPiece kMetricMemoryCacheTotalTinyBytes{
    "velox.memory_cache_total_tiny_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalLargeBytes{
    "velox.memory_cache_total_large_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalTinyPaddingBytes{
    "velox.memory_cache_total_tiny_padding_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalLargePaddingBytes{
    "velox.memory_cache_total_large_padding_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalPrefetchBytes{
    "velox.memory_cache_total_prefetched_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheSumEvictScore{
    "velox.memory_cache_sum_evict_score"};

constexpr folly::StringPiece kMetricMemoryCacheNumHits{
    "velox.memory_cache_num_hits"};

constexpr folly::StringPiece kMetricMemoryCacheHitBytes{
    "velox.memory_cache_hit_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheNumNew{
    "velox.memory_cache_num_new"};

constexpr folly::StringPiece kMetricMemoryCacheNumEvicts{
    "velox.memory_cache_num_evicts"};

constexpr folly::StringPiece kMetricMemoryCacheNumSavableEvicts{
    "velox.memory_cache_num_savable_evicts"};

constexpr folly::StringPiece kMetricMemoryCacheNumEvictChecks{
    "velox.memory_cache_num_evict_checks"};

constexpr folly::StringPiece kMetricMemoryCacheNumWaitExclusive{
    "velox.memory_cache_num_wait_exclusive"};

constexpr folly::StringPiece kMetricMemoryCacheNumAllocClocks{
    "velox.memory_cache_num_alloc_clocks"};

constexpr folly::StringPiece kMetricMemoryCacheNumAgedOutEntries{
    "velox.memory_cache_num_aged_out_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumStaleEntries{
    "velox.memory_cache_num_stale_entries"};

constexpr folly::StringPiece kMetricSsdCacheCachedRegions{
    "velox.ssd_cache_cached_regions"};

constexpr folly::StringPiece kMetricSsdCacheCachedEntries{
    "velox.ssd_cache_cached_entries"};

constexpr folly::StringPiece kMetricSsdCacheCachedBytes{
    "velox.ssd_cache_cached_bytes"};

constexpr folly::StringPiece kMetricSsdCacheReadEntries{
    "velox.ssd_cache_read_entries"};

constexpr folly::StringPiece kMetricSsdCacheReadBytes{
    "velox.ssd_cache_read_bytes"};

constexpr folly::StringPiece kMetricSsdCacheWrittenEntries{
    "velox.ssd_cache_written_entries"};

constexpr folly::StringPiece kMetricSsdCacheWrittenBytes{
    "velox.ssd_cache_written_bytes"};

constexpr folly::StringPiece kMetricSsdCacheAgedOutEntries{
    "velox.ssd_cache_aged_out_entries"};

constexpr folly::StringPiece kMetricSsdCacheAgedOutRegions{
    "velox.ssd_cache_aged_out_regions"};

constexpr folly::StringPiece kMetricSsdCacheOpenSsdErrors{
    "velox.ssd_cache_open_ssd_errors"};

constexpr folly::StringPiece kMetricSsdCacheOpenCheckpointErrors{
    "velox.ssd_cache_open_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheOpenLogErrors{
    "velox.ssd_cache_open_log_errors"};

constexpr folly::StringPiece kMetricSsdCacheDeleteCheckpointErrors{
    "velox.ssd_cache_delete_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheGrowFileErrors{
    "velox.ssd_cache_grow_file_errors"};

constexpr folly::StringPiece kMetricSsdCacheWriteSsdErrors{
    "velox.ssd_cache_write_ssd_errors"};

constexpr folly::StringPiece kMetricSsdCacheWriteSsdDropped{
    "velox.ssd_cache_write_ssd_dropped"};

constexpr folly::StringPiece kMetricSsdCacheWriteCheckpointErrors{
    "velox.ssd_cache_write_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheReadCorruptions{
    "velox.ssd_cache_read_corruptions"};

constexpr folly::StringPiece kMetricSsdCacheReadSsdErrors{
    "velox.ssd_cache_read_ssd_errors"};

constexpr folly::StringPiece kMetricSsdCacheReadCheckpointErrors{
    "velox.ssd_cache_read_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheReadWithoutChecksum{
    "velox.ssd_cache_read_without_checksum"};

constexpr folly::StringPiece kMetricSsdCacheCheckpointsRead{
    "velox.ssd_cache_checkpoints_read"};

constexpr folly::StringPiece kMetricSsdCacheCheckpointsWritten{
    "velox.ssd_cache_checkpoints_written"};

constexpr folly::StringPiece kMetricSsdCacheRegionsEvicted{
    "velox.ssd_cache_regions_evicted"};

constexpr folly::StringPiece kMetricSsdCacheRecoveredEntries{
    "velox.ssd_cache_recovered_entries"};

constexpr folly::StringPiece kMetricExchangeDataTimeMs{
    "velox.exchange_data_time_ms"};

constexpr folly::StringPiece kMetricExchangeDataBytes{
    "velox.exchange_data_bytes"};

constexpr folly::StringPiece kMetricExchangeDataSize{
    "velox.exchange_data_size"};

constexpr folly::StringPiece kMetricExchangeDataCount{
    "velox.exchange_data_count"};

constexpr folly::StringPiece kMetricExchangeDataSizeTimeMs{
    "velox.exchange_data_size_time_ms"};

constexpr folly::StringPiece kMetricExchangeDataSizeCount{
    "velox.exchange_data_size_count"};

constexpr folly::StringPiece kMetricStorageThrottledDurationMs{
    "velox.storage_throttled_duration_ms"};

constexpr folly::StringPiece kMetricStorageLocalThrottled{
    "velox.storage_local_throttled_count"};

constexpr folly::StringPiece kMetricStorageGlobalThrottled{
    "velox.storage_global_throttled_count"};
} // namespace facebook::velox
