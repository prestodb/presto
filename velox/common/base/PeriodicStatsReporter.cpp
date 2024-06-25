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

#include "velox/common/base/PeriodicStatsReporter.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/CacheTTLController.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"

namespace facebook::velox {

namespace {
#define REPORT_IF_NOT_ZERO(name, counter)   \
  if ((counter) != 0) {                     \
    RECORD_METRIC_VALUE((name), (counter)); \
  }

std::mutex& instanceMutex() {
  static std::mutex instanceMu;
  return instanceMu;
}

// Global instance. Must be called while holding a lock over instanceMutex().
std::unique_ptr<PeriodicStatsReporter>& instance() {
  static std::unique_ptr<PeriodicStatsReporter> reporter;
  return reporter;
}
} // namespace

void startPeriodicStatsReporter(const PeriodicStatsReporter::Options& options) {
  std::lock_guard<std::mutex> l(instanceMutex());
  auto& instanceRef = instance();
  VELOX_CHECK_NULL(
      instanceRef, "The periodic stats reporter has already started.");
  instanceRef = std::make_unique<PeriodicStatsReporter>(options);
  instanceRef->start();
}

void stopPeriodicStatsReporter() {
  std::lock_guard<std::mutex> l(instanceMutex());
  auto& instanceRef = instance();
  VELOX_CHECK_NOT_NULL(instanceRef, "No periodic stats reporter to stop.");
  instanceRef->stop();
  instanceRef.reset();
}

PeriodicStatsReporter::PeriodicStatsReporter(const Options& options)
    : allocator_(options.allocator),
      cache_(options.cache),
      arbitrator_(options.arbitrator),
      spillMemoryPool_(options.spillMemoryPool),
      options_(options) {}

void PeriodicStatsReporter::start() {
  LOG(INFO) << "Starting PeriodicStatsReporter with options "
            << options_.toString();
  addTask(
      "report_allocator_stats",
      [this]() { reportAllocatorStats(); },
      options_.allocatorStatsIntervalMs);
  addTask(
      "report_cache_stats",
      [this]() { reportCacheStats(); },
      options_.cacheStatsIntervalMs);
  addTask(
      "report_arbitrator_stats",
      [this]() { reportArbitratorStats(); },
      options_.arbitratorStatsIntervalMs);
  addTask(
      "report_spill_stats",
      [this]() { reportSpillStats(); },
      options_.spillStatsIntervalMs);
}

void PeriodicStatsReporter::stop() {
  LOG(INFO) << "Stopping PeriodicStatsReporter";
  scheduler_.stop();
}

void PeriodicStatsReporter::reportArbitratorStats() {
  if (arbitrator_ == nullptr) {
    return;
  }

  const auto stats = arbitrator_->stats();
  RECORD_METRIC_VALUE(
      kMetricArbitratorFreeCapacityBytes,
      stats.freeCapacityBytes + stats.freeReservedCapacityBytes);
  RECORD_METRIC_VALUE(
      kMetricArbitratorFreeReservedCapacityBytes,
      stats.freeReservedCapacityBytes);
}

void PeriodicStatsReporter::reportAllocatorStats() {
  if (allocator_ == nullptr) {
    return;
  }
  RECORD_METRIC_VALUE(
      kMetricMappedMemoryBytes,
      (velox::memory::AllocationTraits::pageBytes(allocator_->numMapped())));
  RECORD_METRIC_VALUE(
      kMetricAllocatedMemoryBytes,
      (velox::memory::AllocationTraits::pageBytes(allocator_->numAllocated())));
  // TODO(jtan6): Remove condition after T150019700 is done
  if (auto* mmapAllocator =
          dynamic_cast<const velox::memory::MmapAllocator*>(allocator_)) {
    RECORD_METRIC_VALUE(
        kMetricMmapDelegatedAllocBytes, (mmapAllocator->numMallocBytes()));
    RECORD_METRIC_VALUE(
        kMetricMmapExternalMappedBytes,
        velox::memory::AllocationTraits::pageBytes(
            (mmapAllocator->numExternalMapped())));
  }
  // TODO(xiaoxmeng): add memory allocation size stats.
}

void PeriodicStatsReporter::reportCacheStats() {
  if (cache_ == nullptr) {
    return;
  }
  const auto cacheStats = cache_->refreshStats();

  // Memory cache snapshot stats.
  RECORD_METRIC_VALUE(kMetricMemoryCacheNumEntries, cacheStats.numEntries);
  RECORD_METRIC_VALUE(
      kMetricMemoryCacheNumEmptyEntries, cacheStats.numEmptyEntries);
  RECORD_METRIC_VALUE(kMetricMemoryCacheNumSharedEntries, cacheStats.numShared);
  RECORD_METRIC_VALUE(
      kMetricMemoryCacheNumExclusiveEntries, cacheStats.numExclusive);
  RECORD_METRIC_VALUE(
      kMetricMemoryCacheNumPrefetchedEntries, cacheStats.numPrefetch);
  RECORD_METRIC_VALUE(kMetricMemoryCacheTotalTinyBytes, cacheStats.tinySize);
  RECORD_METRIC_VALUE(kMetricMemoryCacheTotalLargeBytes, cacheStats.largeSize);
  RECORD_METRIC_VALUE(
      kMetricMemoryCacheTotalTinyPaddingBytes, cacheStats.tinyPadding);
  RECORD_METRIC_VALUE(
      kMetricMemoryCacheTotalLargePaddingBytes, cacheStats.largePadding);
  RECORD_METRIC_VALUE(
      kMetricMemoryCacheTotalPrefetchBytes, cacheStats.prefetchBytes);

  // Memory cache cumulative stats.
  const auto deltaCacheStats = cacheStats - lastCacheStats_;

  REPORT_IF_NOT_ZERO(kMetricMemoryCacheNumHits, deltaCacheStats.numHit);
  REPORT_IF_NOT_ZERO(kMetricMemoryCacheHitBytes, deltaCacheStats.hitBytes);
  REPORT_IF_NOT_ZERO(kMetricMemoryCacheNumNew, deltaCacheStats.numNew);
  REPORT_IF_NOT_ZERO(kMetricMemoryCacheNumEvicts, deltaCacheStats.numEvict);
  REPORT_IF_NOT_ZERO(
      kMetricMemoryCacheNumEvictChecks, deltaCacheStats.numEvictChecks);
  REPORT_IF_NOT_ZERO(
      kMetricMemoryCacheNumWaitExclusive, deltaCacheStats.numWaitExclusive);
  REPORT_IF_NOT_ZERO(
      kMetricMemoryCacheNumAllocClocks, deltaCacheStats.allocClocks);
  REPORT_IF_NOT_ZERO(
      kMetricMemoryCacheNumAgedOutEntries, deltaCacheStats.numAgedOut);
  REPORT_IF_NOT_ZERO(
      kMetricMemoryCacheSumEvictScore, deltaCacheStats.sumEvictScore);

  // SSD cache snapshot stats.
  if (cacheStats.ssdStats != nullptr) {
    RECORD_METRIC_VALUE(
        kMetricSsdCacheCachedEntries, cacheStats.ssdStats->entriesCached);
    RECORD_METRIC_VALUE(
        kMetricSsdCacheCachedRegions, cacheStats.ssdStats->regionsCached);
    RECORD_METRIC_VALUE(
        kMetricSsdCacheCachedBytes, cacheStats.ssdStats->bytesCached);
  }

  // SSD cache cumulative stats.
  if (deltaCacheStats.ssdStats != nullptr) {
    const auto deltaSsdStats = *deltaCacheStats.ssdStats;
    REPORT_IF_NOT_ZERO(kMetricSsdCacheReadEntries, deltaSsdStats.entriesRead)
    REPORT_IF_NOT_ZERO(kMetricSsdCacheReadBytes, deltaSsdStats.bytesRead);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheWrittenEntries, deltaSsdStats.entriesWritten);
    REPORT_IF_NOT_ZERO(kMetricSsdCacheWrittenBytes, deltaSsdStats.bytesWritten);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheOpenSsdErrors, deltaSsdStats.openFileErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheOpenCheckpointErrors,
        deltaSsdStats.openCheckpointErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheOpenLogErrors, deltaSsdStats.openLogErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheDeleteCheckpointErrors,
        deltaSsdStats.deleteCheckpointErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheGrowFileErrors, deltaSsdStats.growFileErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheWriteSsdErrors, deltaSsdStats.writeSsdErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheWriteSsdDropped, deltaSsdStats.writeSsdDropped);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheWriteCheckpointErrors,
        deltaSsdStats.writeCheckpointErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheReadSsdErrors, deltaSsdStats.readSsdErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheReadCorruptions, deltaSsdStats.readSsdCorruptions);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheReadCheckpointErrors,
        deltaSsdStats.readCheckpointErrors);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheCheckpointsRead, deltaSsdStats.checkpointsRead);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheCheckpointsWritten, deltaSsdStats.checkpointsWritten);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheRegionsEvicted, deltaSsdStats.regionsEvicted);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheAgedOutEntries, deltaSsdStats.entriesAgedOut)
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheAgedOutRegions, deltaSsdStats.regionsAgedOut);
    REPORT_IF_NOT_ZERO(
        kMetricSsdCacheReadWithoutChecksum,
        deltaSsdStats.readWithoutChecksumChecks);
  }

  // TTL controler snapshot stats.
  if (auto* cacheTTLController =
          velox::cache::CacheTTLController::getInstance()) {
    RECORD_METRIC_VALUE(
        kMetricCacheMaxAgeSecs,
        cacheTTLController->getCacheAgeStats().maxAgeSecs);
  }

  lastCacheStats_ = cacheStats;
}

void PeriodicStatsReporter::reportSpillStats() {
  if (spillMemoryPool_ == nullptr) {
    return;
  }
  const auto spillMemoryStats = spillMemoryPool_->stats();
  LOG(INFO) << "Spill memory usage: current["
            << velox::succinctBytes(spillMemoryStats.usedBytes) << "] peak["
            << velox::succinctBytes(spillMemoryStats.peakBytes) << "]";
  RECORD_METRIC_VALUE(kMetricSpillMemoryBytes, spillMemoryStats.usedBytes);
  RECORD_METRIC_VALUE(kMetricSpillPeakMemoryBytes, spillMemoryStats.peakBytes);
}

} // namespace facebook::velox
