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

#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto {

void registerPrestoCppCounters() {
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterDriverCPUExecutorQueueSize, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterDriverCPUExecutorLatencyMs, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterHTTPExecutorLatencyMs, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumHTTPRequest, facebook::velox::StatType::COUNT);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumHTTPRequestError, facebook::velox::StatType::COUNT);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterHTTPRequestLatencyMs, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterHttpClientPrestoExchangeNumOnBody,
      facebook::velox::StatType::COUNT);
  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE(
      kCounterHttpClientPrestoExchangeOnBodyBytes,
      1000,
      0,
      1000000,
      50,
      90,
      95,
      99,
      100);
  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE(
      kCounterPrestoExchangeSerializedPageSize,
      10000,
      0,
      10000000,
      50,
      90,
      95,
      99,
      100);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumQueryContexts, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(kCounterNumTasks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksRunning, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksFinished, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksCancelled, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksAborted, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksFailed, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumZombieVeloxTasks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumZombiePrestoTasks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumRunningDrivers, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumBlockedDrivers, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMappedMemoryBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterAllocatedMemoryBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMmapRawAllocBytesSmall, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMmapExternalMappedBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterTotalPartitionedOutputBuffer, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterPartitionedOutputBufferGetDataLatencyMs,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterOsUserCpuTimeMicros, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterOsSystemCpuTimeMicros, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterOsNumSoftPageFaults, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterOsNumHardPageFaults, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterOsNumVoluntaryContextSwitches, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterOsNumForcedContextSwitches, facebook::velox::StatType::AVG);
  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE(
      kCounterExchangeSourcePeakQueuedBytes,
      1l * 1024 * 1024 * 1024,
      0,
      62l * 1024 * 1024 * 1024, // max bucket value: 62GB
      100);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumEmptyEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumSharedEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumExclusiveEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumPrefetchedEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheTotalTinyBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheTotalLargeBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheTotalTinyPaddingBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheTotalLargePaddingBytes,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheTotalPrefetchBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheTotalTinyPaddingBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheSumEvictScore, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumCumulativeHit, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumHit, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheCumulativeHitBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheHitBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumCumulativeNew, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumNew, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumCumulativeEvict, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumEvict, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumCumulativeEvictChecks,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumEvictChecks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumCumulativeWaitExclusive,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumWaitExclusive, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumCumulativeAllocClocks,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMemoryCacheNumAllocClocks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeReadEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeReadBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeWrittenEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeWrittenBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeCachedEntries, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeCachedBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeOpenSsdErrors, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeOpenCheckpointErrors,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeOpenLogErrors, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeDeleteCheckpointErrors,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeGrowFileErrors, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeWriteSsdErrors, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeWriteCheckpointErrors,
      facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeReadSsdErrors, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSsdCacheCumulativeReadCheckpointErrors,
      facebook::velox::StatType::AVG);
  // Disk spilling stats.
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillRuns, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpilledFiles, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpilledRows, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpilledBytes, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillFillTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillSortTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillSerializationTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillDiskWrites, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillFlushTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillWriteTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterSpillMemoryBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE(
      kCounterSpillPeakMemoryBytes,
      1l * 512 * 1024 * 1024,
      0,
      20l * 1024 * 1024 * 1024, // max bucket value: 20GB
      100);
  // Memory arbitrator stats.
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorNumRequests, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorNumAborted, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorNumFailures, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorQueueTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorArbitrationTimeUs, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorNumShrunkBytes, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorNumReclaimedBytes, facebook::velox::StatType::SUM);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterArbitratorFreeCapacityBytes, facebook::velox::StatType::AVG);

  // NOTE: Metrics type exporting for file handle cache counters are in
  // PeriodicTaskManager because they have dynamic names. The following counters
  // have their type exported there:
  // [
  //  kCounterHiveFileHandleCacheNumElementsFormat,
  //  kCounterHiveFileHandleCachePinnedSizeFormat,
  //  kCounterHiveFileHandleCacheCurSizeFormat,
  //  kCounterHiveFileHandleCacheNumAccumulativeHitsFormat,
  //  kCounterHiveFileHandleCacheNumAccumulativeLookupsFormat
  // ]
}

} // namespace facebook::presto
