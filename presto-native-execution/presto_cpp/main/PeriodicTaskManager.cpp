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

#include "presto_cpp/main/PeriodicTaskManager.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/stop_watch.h>
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/http/filters/HttpEndpointLatencyFilter.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdFile.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/Driver.h"

#include <sys/resource.h>

namespace {
#define REPORT_IF_NOT_ZERO(name, counter)   \
  if ((counter) != 0) {                     \
    RECORD_METRIC_VALUE((name), (counter)); \
  }
} // namespace

namespace facebook::presto {

// Every two seconds we export server counters.
static constexpr size_t kTaskPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every two seconds we export memory counters.
static constexpr size_t kMemoryPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every two seconds we export exchange source counters.
static constexpr size_t kExchangeSourcePeriodGlobalCounters{
    2'000'000}; // 2 seconds.
// Every 1 minute we clean old tasks.
static constexpr size_t kTaskPeriodCleanOldTasks{60'000'000}; // 60 seconds.
// Every 1 minute we export cache counters.
static constexpr size_t kCachePeriodGlobalCounters{60'000'000}; // 60 seconds.
// Every 1 minute we export connector counters.
static constexpr size_t kConnectorPeriodGlobalCounters{
    60'000'000}; // 60 seconds.
static constexpr size_t kOsPeriodGlobalCounters{2'000'000}; // 2 seconds
static constexpr size_t kSpillStatsUpdateIntervalUs{60'000'000}; // 60 seconds
static constexpr size_t kArbitratorStatsUpdateIntervalUs{
    60'000'000}; // 60 seconds
// Every 1 minute we print endpoint latency counters.
static constexpr size_t kHttpEndpointLatencyPeriodGlobalCounters{
    60'000'000}; // 60 seconds.

PeriodicTaskManager::PeriodicTaskManager(
    folly::CPUThreadPoolExecutor* const driverCPUExecutor,
    folly::IOThreadPoolExecutor* const httpExecutor,
    TaskManager* const taskManager,
    const velox::memory::MemoryAllocator* const memoryAllocator,
    const velox::cache::AsyncDataCache* const asyncDataCache,
    const std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::Connector>>& connectors)
    : driverCPUExecutor_(driverCPUExecutor),
      httpExecutor_(httpExecutor),
      taskManager_(taskManager),
      memoryAllocator_(memoryAllocator),
      asyncDataCache_(asyncDataCache),
      arbitrator_(velox::memory::defaultMemoryManager().arbitrator()),
      connectors_(connectors) {}

void PeriodicTaskManager::start() {
  // If executors are null, don't bother starting this task.
  if ((driverCPUExecutor_ != nullptr) || (httpExecutor_ != nullptr)) {
    addExecutorStatsTask();
  }

  VELOX_CHECK_NOT_NULL(taskManager_);
  addTaskStatsTask();

  if (SystemConfig::instance()->enableOldTaskCleanUp()) {
    addOldTaskCleanupTask();
  }

  if (memoryAllocator_ != nullptr) {
    addMemoryAllocatorStatsTask();
  }

  addPrestoExchangeSourceMemoryStatsTask();

  if (asyncDataCache_ != nullptr) {
    addCacheStatsUpdateTask();
  }

  addConnectorStatsTask();

  addOperatingSystemStatsUpdateTask();

  addSpillStatsUpdateTask();

  if (SystemConfig::instance()->enableHttpEndpointLatencyFilter()) {
    addHttpEndpointLatencyStatsTask();
  }

  VELOX_CHECK_NOT_NULL(arbitrator_);
  if (arbitrator_->kind() != "NOOP") {
    addArbitratorStatsTask();
  }

  // This should be the last call in this method.
  scheduler_.start();
}

void PeriodicTaskManager::stop() {
  scheduler_.cancelAllFunctionsAndWait();
  scheduler_.shutdown();
}

void PeriodicTaskManager::updateExecutorStats() {
  if (driverCPUExecutor_ != nullptr) {
    // Report the current queue size of the thread pool.
    RECORD_METRIC_VALUE(
        kCounterDriverCPUExecutorQueueSize,
        driverCPUExecutor_->getTaskQueueSize());

    // Report driver execution latency.
    folly::stop_watch<std::chrono::milliseconds> timer;
    driverCPUExecutor_->add([timer = timer]() {
      RECORD_METRIC_VALUE(
          kCounterDriverCPUExecutorLatencyMs, timer.elapsed().count());
    });
  }

  if (httpExecutor_ != nullptr) {
    // Report the latency between scheduling the task and its execution.
    folly::stop_watch<std::chrono::milliseconds> timer;
    httpExecutor_->add([timer = timer]() {
      RECORD_METRIC_VALUE(
          kCounterHTTPExecutorLatencyMs, timer.elapsed().count());
    });
  }
}

void PeriodicTaskManager::addExecutorStatsTask() {
  addTask(
      [this]() { updateExecutorStats(); },
      kTaskPeriodGlobalCounters,
      "executor_counters");
}

void PeriodicTaskManager::updateTaskStats() {
  // Report the number of tasks and drivers in the system.
  size_t numTasks{0};
  auto taskNumbers = taskManager_->getTaskNumbers(numTasks);
  RECORD_METRIC_VALUE(kCounterNumTasks, taskManager_->getNumTasks());
  RECORD_METRIC_VALUE(
      kCounterNumTasksRunning, taskNumbers[velox::exec::TaskState::kRunning]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksFinished, taskNumbers[velox::exec::TaskState::kFinished]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksCancelled,
      taskNumbers[velox::exec::TaskState::kCanceled]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksAborted, taskNumbers[velox::exec::TaskState::kAborted]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksFailed, taskNumbers[velox::exec::TaskState::kFailed]);

  auto driverCountStats = taskManager_->getDriverCountStats();
  RECORD_METRIC_VALUE(
      kCounterNumRunningDrivers, driverCountStats.numRunningDrivers);
  RECORD_METRIC_VALUE(
      kCounterNumBlockedDrivers, driverCountStats.numBlockedDrivers);
  RECORD_METRIC_VALUE(
      kCounterTotalPartitionedOutputBuffer,
      velox::exec::OutputBufferManager::getInstance().lock()->numBuffers());
}

void PeriodicTaskManager::addTaskStatsTask() {
  addTask(
      [this]() { updateTaskStats(); },
      kTaskPeriodGlobalCounters,
      "task_counters");
}

void PeriodicTaskManager::cleanupOldTask() {
  // Report the number of tasks and drivers in the system.
  if (taskManager_ != nullptr) {
    taskManager_->cleanOldTasks();
  }
}

void PeriodicTaskManager::addOldTaskCleanupTask() {
  addTask(
      [this]() { cleanupOldTask(); },
      kTaskPeriodCleanOldTasks,
      "clean_old_tasks");
}

void PeriodicTaskManager::updateMemoryAllocatorStats() {
  RECORD_METRIC_VALUE(
      kCounterMappedMemoryBytes,
      (velox::memory::AllocationTraits::pageBytes(
          memoryAllocator_->numMapped())));
  RECORD_METRIC_VALUE(
      kCounterAllocatedMemoryBytes,
      (velox::memory::AllocationTraits::pageBytes(
          memoryAllocator_->numAllocated())));
  // TODO(jtan6): Remove condition after T150019700 is done
  if (auto* mmapAllocator =
          dynamic_cast<const velox::memory::MmapAllocator*>(memoryAllocator_)) {
    RECORD_METRIC_VALUE(
        kCounterMmapRawAllocBytesSmall, (mmapAllocator->numMallocBytes()));
    RECORD_METRIC_VALUE(
        kCounterMmapExternalMappedBytes,
        velox::memory::AllocationTraits::pageBytes(
            (mmapAllocator->numExternalMapped())));
  }
  // TODO(xiaoxmeng): add memory allocation size stats.
}

void PeriodicTaskManager::addMemoryAllocatorStatsTask() {
  addTask(
      [this]() { updateMemoryAllocatorStats(); },
      kMemoryPeriodGlobalCounters,
      "mmap_memory_counters");
}

void PeriodicTaskManager::updatePrestoExchangeSourceMemoryStats() {
  int64_t currQueuedMemoryBytes{0};
  int64_t peakQueuedMemoryBytes{0};
  PrestoExchangeSource::getMemoryUsage(
      currQueuedMemoryBytes, peakQueuedMemoryBytes);
  PrestoExchangeSource::resetPeakMemoryUsage();
  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterExchangeSourcePeakQueuedBytes, peakQueuedMemoryBytes);
}

void PeriodicTaskManager::addPrestoExchangeSourceMemoryStatsTask() {
  addTask(
      [this]() { updatePrestoExchangeSourceMemoryStats(); },
      kExchangeSourcePeriodGlobalCounters,
      "exchange_source_counters");
}

void PeriodicTaskManager::updateCacheStats() {
  const auto memoryCacheStats = asyncDataCache_->refreshStats();

  // Snapshots.
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumEntries, memoryCacheStats.numEntries);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumEmptyEntries, memoryCacheStats.numEmptyEntries);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumSharedEntries, memoryCacheStats.numShared);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumExclusiveEntries, memoryCacheStats.numExclusive);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumPrefetchedEntries, memoryCacheStats.numPrefetch);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheTotalTinyBytes, memoryCacheStats.tinySize);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheTotalLargeBytes, memoryCacheStats.largeSize);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheTotalTinyPaddingBytes, memoryCacheStats.tinyPadding);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheTotalLargePaddingBytes, memoryCacheStats.largePadding);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheTotalPrefetchBytes, memoryCacheStats.prefetchBytes);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheSumEvictScore, memoryCacheStats.sumEvictScore);

  // Interval cumulatives.
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumHit,
      memoryCacheStats.numHit - lastMemoryCacheHits_);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheHitBytes,
      memoryCacheStats.hitBytes - lastMemoryCacheHitsBytes_);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumNew,
      memoryCacheStats.numNew - lastMemoryCacheInserts_);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumEvict,
      memoryCacheStats.numEvict - lastMemoryCacheEvictions_);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumEvictChecks,
      memoryCacheStats.numEvictChecks - lastMemoryCacheEvictionChecks_);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumWaitExclusive,
      memoryCacheStats.numWaitExclusive - lastMemoryCacheStalls_);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumAllocClocks,
      memoryCacheStats.allocClocks - lastMemoryCacheAllocClocks_);

  lastMemoryCacheHits_ = memoryCacheStats.numHit;
  lastMemoryCacheHitsBytes_ = memoryCacheStats.hitBytes;
  lastMemoryCacheInserts_ = memoryCacheStats.numNew;
  lastMemoryCacheEvictions_ = memoryCacheStats.numEvict;
  lastMemoryCacheEvictionChecks_ = memoryCacheStats.numEvictChecks;
  lastMemoryCacheStalls_ = memoryCacheStats.numWaitExclusive;
  lastMemoryCacheAllocClocks_ = memoryCacheStats.allocClocks;

  // All time cumulatives.
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumCumulativeHit, memoryCacheStats.numHit);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheCumulativeHitBytes, memoryCacheStats.hitBytes);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumCumulativeNew, memoryCacheStats.numNew);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumCumulativeEvict, memoryCacheStats.numEvict);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumCumulativeEvictChecks,
      memoryCacheStats.numEvictChecks);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumCumulativeWaitExclusive,
      memoryCacheStats.numWaitExclusive);
  RECORD_METRIC_VALUE(
      kCounterMemoryCacheNumCumulativeAllocClocks,
      memoryCacheStats.allocClocks);
  if (memoryCacheStats.ssdStats != nullptr) {
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeReadEntries,
        memoryCacheStats.ssdStats->entriesRead)
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeReadBytes,
        memoryCacheStats.ssdStats->bytesRead);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeWrittenEntries,
        memoryCacheStats.ssdStats->entriesWritten);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeWrittenBytes,
        memoryCacheStats.ssdStats->bytesWritten);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeCachedEntries,
        memoryCacheStats.ssdStats->entriesCached);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeCachedBytes,
        memoryCacheStats.ssdStats->bytesCached);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeOpenSsdErrors,
        memoryCacheStats.ssdStats->openFileErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeOpenCheckpointErrors,
        memoryCacheStats.ssdStats->openCheckpointErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeOpenLogErrors,
        memoryCacheStats.ssdStats->openLogErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeDeleteCheckpointErrors,
        memoryCacheStats.ssdStats->deleteCheckpointErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeGrowFileErrors,
        memoryCacheStats.ssdStats->growFileErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeWriteSsdErrors,
        memoryCacheStats.ssdStats->writeSsdErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeWriteCheckpointErrors,
        memoryCacheStats.ssdStats->writeCheckpointErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeReadSsdErrors,
        memoryCacheStats.ssdStats->readSsdErrors);
    RECORD_METRIC_VALUE(
        kCounterSsdCacheCumulativeReadCheckpointErrors,
        memoryCacheStats.ssdStats->readCheckpointErrors);
  }
  LOG(INFO) << "Cache stats:\n" << memoryCacheStats.toString();
}

void PeriodicTaskManager::addCacheStatsUpdateTask() {
  addTask(
      [this]() { updateCacheStats(); },
      kCachePeriodGlobalCounters,
      "cache_counters");
}

void PeriodicTaskManager::addConnectorStatsTask() {
  for (const auto& itr : connectors_) {
    static std::unordered_map<std::string, int64_t> oldValues;
    // Export HiveConnector stats
    if (auto hiveConnector =
            std::dynamic_pointer_cast<velox::connector::hive::HiveConnector>(
                itr.second)) {
      auto connectorId = hiveConnector->connectorId();
      const auto kNumElementsMetricName = fmt::format(
          kCounterHiveFileHandleCacheNumElementsFormat, connectorId);
      const auto kPinnedSizeMetricName =
          fmt::format(kCounterHiveFileHandleCachePinnedSizeFormat, connectorId);
      const auto kCurSizeMetricName =
          fmt::format(kCounterHiveFileHandleCacheCurSizeFormat, connectorId);
      const auto kNumAccumulativeHitsMetricName = fmt::format(
          kCounterHiveFileHandleCacheNumAccumulativeHitsFormat, connectorId);
      const auto kNumAccumulativeLookupsMetricName = fmt::format(
          kCounterHiveFileHandleCacheNumAccumulativeLookupsFormat, connectorId);

      const auto kNumHitsMetricName =
          fmt::format(kCounterHiveFileHandleCacheNumHitsFormat, connectorId);
      oldValues[kNumHitsMetricName] = 0;
      const auto kNumLookupsMetricName =
          fmt::format(kCounterHiveFileHandleCacheNumLookupsFormat, connectorId);
      oldValues[kNumLookupsMetricName] = 0;

      // Exporting metrics types here since the metrics key is dynamic
      DEFINE_METRIC(kNumElementsMetricName, facebook::velox::StatType::AVG);
      DEFINE_METRIC(kPinnedSizeMetricName, facebook::velox::StatType::AVG);
      DEFINE_METRIC(kCurSizeMetricName, facebook::velox::StatType::AVG);
      DEFINE_METRIC(
          kNumAccumulativeHitsMetricName, facebook::velox::StatType::AVG);
      DEFINE_METRIC(
          kNumAccumulativeLookupsMetricName, facebook::velox::StatType::AVG);
      DEFINE_METRIC(kNumHitsMetricName, facebook::velox::StatType::AVG);
      DEFINE_METRIC(kNumLookupsMetricName, facebook::velox::StatType::AVG);

      addTask(
          [hiveConnector,
           connectorId,
           kNumElementsMetricName,
           kPinnedSizeMetricName,
           kCurSizeMetricName,
           kNumAccumulativeHitsMetricName,
           kNumAccumulativeLookupsMetricName,
           kNumHitsMetricName,
           kNumLookupsMetricName]() {
            auto fileHandleCacheStats = hiveConnector->fileHandleCacheStats();
            RECORD_METRIC_VALUE(
                kNumElementsMetricName, fileHandleCacheStats.numElements);
            RECORD_METRIC_VALUE(
                kPinnedSizeMetricName, fileHandleCacheStats.pinnedSize);
            RECORD_METRIC_VALUE(
                kCurSizeMetricName, fileHandleCacheStats.curSize);
            RECORD_METRIC_VALUE(
                kNumAccumulativeHitsMetricName, fileHandleCacheStats.numHits);
            RECORD_METRIC_VALUE(
                kNumAccumulativeLookupsMetricName,
                fileHandleCacheStats.numLookups);
            RECORD_METRIC_VALUE(
                kNumHitsMetricName,
                fileHandleCacheStats.numHits - oldValues[kNumHitsMetricName]);
            oldValues[kNumHitsMetricName] = fileHandleCacheStats.numHits;
            RECORD_METRIC_VALUE(
                kNumLookupsMetricName,
                fileHandleCacheStats.numLookups -
                    oldValues[kNumLookupsMetricName]);
            oldValues[kNumLookupsMetricName] = fileHandleCacheStats.numLookups;
          },
          kConnectorPeriodGlobalCounters,
          fmt::format("{}.hive_connector_counters", connectorId));
    }
  }
}

void PeriodicTaskManager::updateOperatingSystemStats() {
  struct rusage usage {};
  memset(&usage, 0, sizeof(usage));
  getrusage(RUSAGE_SELF, &usage);

  const int64_t userCpuTimeUs{
      (int64_t)usage.ru_utime.tv_sec * 1'000'000 +
      (int64_t)usage.ru_utime.tv_usec};
  RECORD_METRIC_VALUE(
      kCounterOsUserCpuTimeMicros, userCpuTimeUs - lastUserCpuTimeUs_);
  lastUserCpuTimeUs_ = userCpuTimeUs;

  const int64_t systemCpuTimeUs{
      (int64_t)usage.ru_stime.tv_sec * 1'000'000 +
      (int64_t)usage.ru_stime.tv_usec};
  RECORD_METRIC_VALUE(
      kCounterOsSystemCpuTimeMicros, systemCpuTimeUs - lastSystemCpuTimeUs_);
  lastSystemCpuTimeUs_ = systemCpuTimeUs;

  const int64_t softPageFaults{usage.ru_minflt};
  RECORD_METRIC_VALUE(
      kCounterOsNumSoftPageFaults, softPageFaults - lastSoftPageFaults_);
  lastSoftPageFaults_ = softPageFaults;

  const int64_t hardPageFaults{usage.ru_majflt};
  RECORD_METRIC_VALUE(
      kCounterOsNumHardPageFaults, hardPageFaults - lastHardPageFaults_);
  lastHardPageFaults_ = hardPageFaults;

  const int64_t voluntaryContextSwitches{usage.ru_nvcsw};
  RECORD_METRIC_VALUE(
      kCounterOsNumVoluntaryContextSwitches,
      voluntaryContextSwitches - lastVoluntaryContextSwitches_);
  lastVoluntaryContextSwitches_ = voluntaryContextSwitches;

  const int64_t forcedContextSwitches{usage.ru_nivcsw};
  RECORD_METRIC_VALUE(
      kCounterOsNumForcedContextSwitches,
      forcedContextSwitches - lastForcedContextSwitches_);
  lastForcedContextSwitches_ = forcedContextSwitches;
}

void PeriodicTaskManager::addOperatingSystemStatsUpdateTask() {
  addTask(
      [this]() { updateOperatingSystemStats(); },
      kOsPeriodGlobalCounters,
      "os_counters");
}

void PeriodicTaskManager::addArbitratorStatsTask() {
  addTask(
      [this]() { updateArbitratorStatsTask(); },
      kArbitratorStatsUpdateIntervalUs,
      "arbitrator_stats");
}

void PeriodicTaskManager::updateArbitratorStatsTask() {
  const auto updatedArbitratorStats = arbitrator_->stats();
  VELOX_CHECK_GE(updatedArbitratorStats, lastArbitratorStats_);
  const auto deltaArbitratorStats =
      updatedArbitratorStats - lastArbitratorStats_;
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorNumRequests, deltaArbitratorStats.numRequests);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorNumAborted, deltaArbitratorStats.numAborted);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorNumFailures, deltaArbitratorStats.numFailures);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorQueueTimeUs, deltaArbitratorStats.queueTimeUs);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorArbitrationTimeUs,
      deltaArbitratorStats.arbitrationTimeUs);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorNumShrunkBytes, deltaArbitratorStats.numShrunkBytes);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorNumReclaimedBytes,
      deltaArbitratorStats.numReclaimedBytes);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorFreeCapacityBytes,
      deltaArbitratorStats.freeCapacityBytes);
  REPORT_IF_NOT_ZERO(
      kCounterArbitratorNonReclaimableAttempts,
      deltaArbitratorStats.numNonReclaimableAttempts);

  if (!deltaArbitratorStats.empty()) {
    LOG(INFO) << "Updated memory arbitrator stats: "
              << updatedArbitratorStats.toString();
    LOG(INFO) << "Memory arbitrator stats change: "
              << deltaArbitratorStats.toString();
  }
  lastArbitratorStats_ = updatedArbitratorStats;
}

void PeriodicTaskManager::addSpillStatsUpdateTask() {
  addTask(
      [this]() { updateSpillStatsTask(); },
      kSpillStatsUpdateIntervalUs,
      "spill_stats");
}

void PeriodicTaskManager::updateSpillStatsTask() {
  const auto updatedSpillStats = velox::common::globalSpillStats();
  VELOX_CHECK_GE(updatedSpillStats, lastSpillStats_);
  const auto deltaSpillStats = updatedSpillStats - lastSpillStats_;
  REPORT_IF_NOT_ZERO(kCounterSpillRuns, deltaSpillStats.spillRuns);
  REPORT_IF_NOT_ZERO(kCounterSpilledFiles, deltaSpillStats.spilledFiles);
  REPORT_IF_NOT_ZERO(kCounterSpilledRows, deltaSpillStats.spilledRows);
  REPORT_IF_NOT_ZERO(kCounterSpilledBytes, deltaSpillStats.spilledBytes);
  REPORT_IF_NOT_ZERO(kCounterSpillFillTimeUs, deltaSpillStats.spillFillTimeUs);
  REPORT_IF_NOT_ZERO(kCounterSpillSortTimeUs, deltaSpillStats.spillSortTimeUs);
  REPORT_IF_NOT_ZERO(
      kCounterSpillSerializationTimeUs,
      deltaSpillStats.spillSerializationTimeUs);
  REPORT_IF_NOT_ZERO(kCounterSpillDiskWrites, deltaSpillStats.spillDiskWrites);
  REPORT_IF_NOT_ZERO(
      kCounterSpillFlushTimeUs, deltaSpillStats.spillFlushTimeUs);
  REPORT_IF_NOT_ZERO(
      kCounterSpillWriteTimeUs, deltaSpillStats.spillWriteTimeUs);
  REPORT_IF_NOT_ZERO(
      kCounterSpillMaxLevelExceeded,
      deltaSpillStats.spillMaxLevelExceededCount);

  if (!deltaSpillStats.empty()) {
    LOG(INFO) << "Updated spill stats: " << updatedSpillStats.toString();
    LOG(INFO) << "Spill stats change:" << deltaSpillStats.toString();
  }

  const auto spillMemoryStats = velox::memory::spillMemoryPool()->stats();
  LOG(INFO) << "Spill memory usage: current["
            << velox::succinctBytes(spillMemoryStats.currentBytes) << "] peak["
            << velox::succinctBytes(spillMemoryStats.peakBytes) << "]";
  RECORD_METRIC_VALUE(kCounterSpillMemoryBytes, spillMemoryStats.currentBytes);
  RECORD_METRIC_VALUE(kCounterSpillPeakMemoryBytes, spillMemoryStats.peakBytes);

  lastSpillStats_ = updatedSpillStats;
}

void PeriodicTaskManager::printHttpEndpointLatencyStats() {
  const auto latencyMetrics =
      http::filters::HttpEndpointLatencyFilter::retrieveLatencies();
  std::ostringstream oss;
  oss << "Http endpoint latency \n[\n";
  for (const auto& metrics : latencyMetrics) {
    oss << metrics.toString() << ",\n";
  }
  oss << "]";
  LOG(INFO) << oss.str();
}

void PeriodicTaskManager::addHttpEndpointLatencyStatsTask() {
  addTask(
      [this]() { printHttpEndpointLatencyStats(); },
      kHttpEndpointLatencyPeriodGlobalCounters,
      "http_endpoint_counters");
}
} // namespace facebook::presto
