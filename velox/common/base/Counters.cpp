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

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::velox {

void registerVeloxMetrics() {
  /// ================== Task Execution Counters =================
  // The number of driver yield count when exceeds the per-driver cpu time slice
  // limit if enforced.
  DEFINE_METRIC(kMetricDriverYieldCount, facebook::velox::StatType::COUNT);

  // Tracks driver queue latency in range of [0, 10s] with 20 buckets and
  // reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricDriverQueueTimeMs, 500, 0, 10'000, 50, 90, 99, 100);

  // Tracks driver execution latency in range of [0, 30s] with 30 buckets and
  // reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricDriverExecTimeMs, 1'000, 0, 30'000, 50, 90, 99, 100);

  /// ================== Cache Counters =================

  // Tracks hive handle generation latency in range of [0, 100s] and reports
  // P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricHiveFileHandleGenerateLatencyMs,
      10'000,
      0,
      100'000,
      50,
      90,
      99,
      100);

  DEFINE_METRIC(kMetricCacheShrinkCount, facebook::velox::StatType::COUNT);

  // Tracks cache shrink latency in range of [0, 100s] with 10 buckets and
  // reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricCacheShrinkTimeMs, 10'000, 0, 100'000, 50, 90, 99, 100);

  /// ================== Memory Allocator Counters =================

  // Number of bytes currently mapped in MemoryAllocator. These bytes represent
  // the bytes that are either currently being allocated or were in the past
  // allocated, not yet been returned back to the operating system, in the
  // form of 'Allocation' or 'ContiguousAllocation'.
  DEFINE_METRIC(kMetricMappedMemoryBytes, facebook::velox::StatType::AVG);

  // Number of bytes currently allocated (used) from MemoryAllocator in the form
  // of 'Allocation' or 'ContiguousAllocation'.
  DEFINE_METRIC(kMetricAllocatedMemoryBytes, facebook::velox::StatType::AVG);

  // Number of bytes currently mapped in MmapAllocator, in the form of
  // 'ContiguousAllocation'.
  //
  // NOTE: This applies only to MmapAllocator
  DEFINE_METRIC(kMetricMmapExternalMappedBytes, facebook::velox::StatType::AVG);

  // Number of bytes currently allocated from MmapAllocator directly from raw
  // allocateBytes() interface, and internally allocated by malloc. Only small
  // chunks of memory are delegated to malloc.
  //
  // NOTE: This applies only to MmapAllocator
  DEFINE_METRIC(kMetricMmapDelegatedAllocBytes, facebook::velox::StatType::AVG);

  /// ================== AsyncDataCache Counters =================

  // Max possible age of AsyncDataCache and SsdCache entries since the raw file
  // was opened to load the cache.
  DEFINE_METRIC(kMetricCacheMaxAgeSecs, facebook::velox::StatType::AVG);

  // Total number of cache entries.
  DEFINE_METRIC(kMetricMemoryCacheNumEntries, facebook::velox::StatType::AVG);

  // Total number of cache entries that do not cache anything.
  DEFINE_METRIC(
      kMetricMemoryCacheNumEmptyEntries, facebook::velox::StatType::AVG);

  // Total number of cache entries that are pinned for shared access.
  DEFINE_METRIC(
      kMetricMemoryCacheNumSharedEntries, facebook::velox::StatType::AVG);

  // Total number of cache entries that are pinned for exclusive access.
  DEFINE_METRIC(
      kMetricMemoryCacheNumExclusiveEntries, facebook::velox::StatType::AVG);

  // Total number of cache entries that are being or have been prefetched but
  // have not been hit.
  DEFINE_METRIC(
      kMetricMemoryCacheNumPrefetchedEntries, facebook::velox::StatType::AVG);

  // Total number of bytes of the cached data that is much smaller than
  // kTinyDataSize.
  DEFINE_METRIC(
      kMetricMemoryCacheTotalTinyBytes, facebook::velox::StatType::AVG);

  // Total number of bytes of the cached data excluding
  // 'kMetricMemoryCacheTotalTinyBytes'.
  DEFINE_METRIC(
      kMetricMemoryCacheTotalLargeBytes, facebook::velox::StatType::AVG);

  // Total unused capacity bytes in 'kMetricMemoryCacheTotalTinyBytes'.
  DEFINE_METRIC(
      kMetricMemoryCacheTotalTinyPaddingBytes, facebook::velox::StatType::AVG);

  // Total unused capacity bytes in 'kMetricMemoryCacheTotalLargeBytes'.
  DEFINE_METRIC(
      kMetricMemoryCacheTotalLargePaddingBytes, facebook::velox::StatType::AVG);

  // Total bytes of cache entries in prefetch state.
  DEFINE_METRIC(
      kMetricMemoryCacheTotalPrefetchBytes, facebook::velox::StatType::AVG);

  // Sum of scores of evicted entries. This serves to infer an average lifetime
  // for entries in cache.
  DEFINE_METRIC(
      kMetricMemoryCacheSumEvictScore, facebook::velox::StatType::SUM);

  // Number of hits (saved IO) since last counter retrieval. The first hit to a
  // prefetched entry does not count.
  DEFINE_METRIC(kMetricMemoryCacheNumHits, facebook::velox::StatType::SUM);

  // Amount of hit bytes (saved IO) since last counter retrieval. The first hit
  // to a prefetched entry does not count.
  DEFINE_METRIC(kMetricMemoryCacheHitBytes, facebook::velox::StatType::SUM);

  // Number of new entries created since last counter retrieval.
  DEFINE_METRIC(kMetricMemoryCacheNumNew, facebook::velox::StatType::SUM);

  // Number of times a valid entry was removed in order to make space, since
  // last counter retrieval.
  DEFINE_METRIC(kMetricMemoryCacheNumEvicts, facebook::velox::StatType::SUM);

  // Number of entries considered for evicting, since last counter retrieval.
  DEFINE_METRIC(
      kMetricMemoryCacheNumEvictChecks, facebook::velox::StatType::SUM);

  // Number of times a user waited for an entry to transit from exclusive to
  // shared mode, since last counter retrieval.
  DEFINE_METRIC(
      kMetricMemoryCacheNumWaitExclusive, facebook::velox::StatType::SUM);

  // Clocks spent in allocating or freeing memory for backing cache entries,
  // since last counter retrieval
  DEFINE_METRIC(
      kMetricMemoryCacheNumAllocClocks, facebook::velox::StatType::SUM);

  // Number of AsyncDataCache entries that are aged out and evicted
  // given configured TTL.
  DEFINE_METRIC(
      kMetricMemoryCacheNumAgedOutEntries, facebook::velox::StatType::SUM);

  /// ================== SsdCache Counters ==================

  // Number of regions currently cached by SSD.
  DEFINE_METRIC(kMetricSsdCacheCachedRegions, facebook::velox::StatType::AVG);

  // Number of entries currently cached by SSD.
  DEFINE_METRIC(kMetricSsdCacheCachedEntries, facebook::velox::StatType::AVG);

  // Total bytes currently cached by SSD.
  DEFINE_METRIC(kMetricSsdCacheCachedBytes, facebook::velox::StatType::AVG);

  // Total number of entries read from SSD.
  DEFINE_METRIC(kMetricSsdCacheReadEntries, facebook::velox::StatType::SUM);

  // Total number of bytes read from SSD.
  DEFINE_METRIC(kMetricSsdCacheReadBytes, facebook::velox::StatType::SUM);

  // Total number of entries written to SSD.
  DEFINE_METRIC(kMetricSsdCacheWrittenEntries, facebook::velox::StatType::SUM);

  // Total number of bytes written to SSD.
  DEFINE_METRIC(kMetricSsdCacheWrittenBytes, facebook::velox::StatType::SUM);

  // Total number of SsdCache entries that are aged out and evicted given
  // configured TTL.
  DEFINE_METRIC(kMetricSsdCacheAgedOutEntries, facebook::velox::StatType::SUM);

  // Total number of SsdCache regions that are aged out and evicted given
  // configured TTL.
  DEFINE_METRIC(kMetricSsdCacheAgedOutRegions, facebook::velox::StatType::SUM);

  // Total number of SSD file open errors.
  DEFINE_METRIC(kMetricSsdCacheOpenSsdErrors, facebook::velox::StatType::SUM);

  // Total number of SSD checkpoint file open errors.
  DEFINE_METRIC(
      kMetricSsdCacheOpenCheckpointErrors, facebook::velox::StatType::SUM);

  // Total number of SSD evict log file open errors.
  DEFINE_METRIC(kMetricSsdCacheOpenLogErrors, facebook::velox::StatType::SUM);

  // Total number of errors while deleting SSD checkpoint files.
  DEFINE_METRIC(
      kMetricSsdCacheDeleteCheckpointErrors, facebook::velox::StatType::SUM);

  // Total number of errors while growing SSD cache files.
  DEFINE_METRIC(kMetricSsdCacheGrowFileErrors, facebook::velox::StatType::SUM);

  // Total number of error while writing to SSD cache files.
  DEFINE_METRIC(kMetricSsdCacheWriteSsdErrors, facebook::velox::StatType::SUM);

  // Total number of errors while writing SSD checkpoint file.
  DEFINE_METRIC(
      kMetricSsdCacheWriteCheckpointErrors, facebook::velox::StatType::SUM);

  // Total number of writes dropped due to no cache space.
  DEFINE_METRIC(kMetricSsdCacheWriteSsdDropped, facebook::velox::StatType::SUM);

  // Total number of errors while reading from SSD cache files.
  DEFINE_METRIC(kMetricSsdCacheReadSsdErrors, facebook::velox::StatType::SUM);

  // Total number of corrupted SSD data read detected by checksum.
  DEFINE_METRIC(kMetricSsdCacheReadCorruptions, facebook::velox::StatType::SUM);

  // Total number of errors while reading from SSD checkpoint files.
  DEFINE_METRIC(
      kMetricSsdCacheReadCheckpointErrors, facebook::velox::StatType::SUM);

  // Total number of checkpoints read.
  DEFINE_METRIC(kMetricSsdCacheCheckpointsRead, facebook::velox::StatType::SUM);

  // Total number of checkpoints written.
  DEFINE_METRIC(
      kMetricSsdCacheCheckpointsWritten, facebook::velox::StatType::SUM);

  // Total number of cache regions evicted.
  DEFINE_METRIC(kMetricSsdCacheRegionsEvicted, facebook::velox::StatType::SUM);

  /// ================== Memory Arbitration Counters =================

  // The number of arbitration requests.
  DEFINE_METRIC(
      kMetricArbitratorRequestsCount, facebook::velox::StatType::COUNT);

  // The number of times a query level memory pool is aborted as a result of a
  // memory arbitration process. The memory pool aborted will eventually result
  // in a cancelling of the original query.
  DEFINE_METRIC(
      kMetricArbitratorAbortedCount, facebook::velox::StatType::COUNT);

  // The number of times a memory arbitration request failed. This may occur
  // either because the requester was terminated during the processing of its
  // request, the arbitration request would surpass the maximum allowed capacity
  // for the requester, or the arbitration process couldn't release the
  // requested amount of memory.
  DEFINE_METRIC(
      kMetricArbitratorFailuresCount, facebook::velox::StatType::COUNT);

  // Tracks the memory reclaim count on an operator.
  DEFINE_METRIC(kMetricMemoryReclaimCount, facebook::velox::StatType::COUNT);

  // Tracks op memory reclaim exec time in range of [0, 600s] with 20 buckets
  // and reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimExecTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // Tracks op memory reclaim bytes distribution in range of [0, 4GB] with 64
  // buckets and reports P50, P90, P99, and P100
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimedBytes,
      67'108'864,
      0,
      4'294'967'296,
      50,
      90,
      99,
      100);

  // Tracks the memory reclaim count on an operator.
  DEFINE_METRIC(
      kMetricTaskMemoryReclaimCount, facebook::velox::StatType::COUNT);

  // Tracks memory reclaim task wait time in range of [0, 60s] with 60 buckets
  // and reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricTaskMemoryReclaimWaitTimeMs, 1'000, 0, 60'000, 50, 90, 99, 100);

  // Tracks memory reclaim task wait time in range of [0, 240s] with 60 buckets
  // and reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricTaskMemoryReclaimExecTimeMs, 4'000, 0, 240'000, 50, 90, 99, 100);

  // Tracks the number of times that the task memory reclaim wait timeouts.
  DEFINE_METRIC(
      kMetricTaskMemoryReclaimWaitTimeoutCount,
      facebook::velox::StatType::COUNT);

  // The number of times that the memory reclaim fails because the operator is
  // executing a non-reclaimable section where it is expected to have reserved
  // enough memory to execute without asking for more. Therefore, it is an
  // indicator that the memory reservation is not sufficient. It excludes
  // counting instances where the operator is in a non-reclaimable state due to
  // currently being on-thread and running or being already cancelled.
  DEFINE_METRIC(
      kMetricMemoryNonReclaimableCount, facebook::velox::StatType::COUNT);

  // The number of arbitration that reclaims the used memory from the query
  // which initiates the memory arbitration request itself. It ensures the
  // memory arbitration request won't exceed its per-query memory capacity
  // limit.
  DEFINE_METRIC(
      kMetricArbitratorLocalArbitrationCount, facebook::velox::StatType::COUNT);

  // The number of arbitration which ensures the total allocated query capacity
  // won't exceed the arbitrator capacity limit. It may or may not reclaim
  // memory from the query which initiate the memory arbitration request. This
  // indicates the velox runtime doesn't have enough memory to run all the
  // queries at their peak memory usage. We have to trigger spilling to let them
  // run through completion.
  DEFINE_METRIC(
      kMetricArbitratorGlobalArbitrationCount,
      facebook::velox::StatType::COUNT);

  // The number of global arbitration that reclaims used memory by slow disk
  // spilling.
  DEFINE_METRIC(
      kMetricArbitratorSlowGlobalArbitrationCount,
      facebook::velox::StatType::COUNT);

  // The distribution of the amount of time an arbitration operation stays in
  // arbitration queues and waits the arbitration r/w locks in range of [0,
  // 600s] with 20 buckets. It is configured to report the latency at P50, P90,
  // P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricArbitratorWaitTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time it takes to complete a single
  // arbitration request stays queued in range of [0, 600s] with 20
  // buckets. It is configured to report the latency at P50, P90, P99,
  // and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricArbitratorArbitrationTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // Tracks the average of free memory capacity managed by the arbitrator in
  // bytes.
  DEFINE_METRIC(
      kMetricArbitratorFreeCapacityBytes, facebook::velox::StatType::AVG);

  DEFINE_METRIC(
      kMetricArbitratorFreeReservedCapacityBytes,
      facebook::velox::StatType::AVG);

  // Tracks the leaf memory pool usage leak in bytes.
  DEFINE_METRIC(
      kMetricMemoryPoolUsageLeakBytes, facebook::velox::StatType::SUM);

  // Tracks the leaf memory pool reservation leak in bytes.
  DEFINE_METRIC(
      kMetricMemoryPoolReservationLeakBytes, facebook::velox::StatType::SUM);

  // The distribution of a root memory pool's initial capacity in range of [0,
  // 256MB] with 32 buckets. It is configured to report the capacity at P50,
  // P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryPoolInitialCapacityBytes,
      8L << 20,
      0,
      256L << 20,
      50,
      90,
      99,
      100);

  // The distribution of a root memory pool cappacity growth attempts through
  // memory arbitration in range of [0, 256] with 32 buckets. It is configured
  // to report the count at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryPoolCapacityGrowCount, 8, 0, 256, 50, 90, 99, 100);

  // Tracks the count of double frees in memory allocator, indicating the
  // possibility of buffer ownership issues when a buffer is freed more than
  // once.
  DEFINE_METRIC(
      kMetricMemoryAllocatorDoubleFreeCount, facebook::velox::StatType::COUNT);

  /// ================== Spill related Counters =================

  // The number of bytes in memory to spill.
  DEFINE_METRIC(kMetricSpilledInputBytes, facebook::velox::StatType::SUM);

  // The number of bytes spilled to disk which can be number of compressed
  // bytes if compression is enabled.
  DEFINE_METRIC(kMetricSpilledBytes, facebook::velox::StatType::SUM);

  // The number of spilled rows.
  DEFINE_METRIC(kMetricSpilledRowsCount, facebook::velox::StatType::COUNT);

  // The number of spilled files.
  DEFINE_METRIC(kMetricSpilledFilesCount, facebook::velox::StatType::COUNT);

  // The distribution of the amount of time spent on filling rows for spilling.
  // in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillFillTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time spent on sorting rows for spilling
  // in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillSortTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time spent on serializing rows for
  // spilling in range of [0, 600s] with 20 buckets. It is configured to report
  // the latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillSerializationTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The number of spill writes to storage, which is the number of write calls
  // to velox filesystem.
  DEFINE_METRIC(kMetricSpillWritesCount, facebook::velox::StatType::COUNT);

  // The distribution of the amount of time spent on copy out serialized
  // rows for disk write in range of [0, 600s] with 20 buckets. It is configured
  // to report the latency at P50, P90, P99, and P100 percentiles. Note:  If
  // compression is enabled, this includes the compression time.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillFlushTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time spent on writing spilled rows to
  // disk in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillWriteTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // Tracks the number of times that we hit the max spill level limit.
  DEFINE_METRIC(
      kMetricMaxSpillLevelExceededCount, facebook::velox::StatType::COUNT);

  // Tracks the total number of bytes in file writers that's pre-maturely
  // flushed due to memory reclaiming.
  DEFINE_METRIC(
      kMetricFileWriterEarlyFlushedRawBytes, facebook::velox::StatType::SUM);

  // The current spilling memory usage in bytes.
  DEFINE_METRIC(kMetricSpillMemoryBytes, facebook::velox::StatType::AVG);

  // The peak spilling memory usage in bytes.
  DEFINE_METRIC(kMetricSpillPeakMemoryBytes, facebook::velox::StatType::AVG);
}
} // namespace facebook::velox
