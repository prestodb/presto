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
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/Driver.h"

#include <sys/resource.h>

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
static constexpr size_t kOsPeriodGlobalCounters{2'000'000}; // 2 seconds

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
      connectors_(connectors) {}

void PeriodicTaskManager::start() {
  // If executors are null, don't bother starting this task.
  if ((driverCPUExecutor_ != nullptr) || (httpExecutor_ != nullptr)) {
    addExecutorStatsTask();
  }
  if (taskManager_ != nullptr) {
    addTaskStatsTask();
    addTaskCleanupTask();
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
    REPORT_ADD_STAT_VALUE(
        kCounterDriverCPUExecutorQueueSize,
        driverCPUExecutor_->getTaskQueueSize());

    // Report driver execution latency.
    folly::stop_watch<std::chrono::milliseconds> timer;
    driverCPUExecutor_->add([timer = timer]() {
      REPORT_ADD_STAT_VALUE(
          kCounterDriverCPUExecutorLatencyMs, timer.elapsed().count());
    });
  }

  if (httpExecutor_ != nullptr) {
    // Report the latency between scheduling the task and its execution.
    folly::stop_watch<std::chrono::milliseconds> timer;
    httpExecutor_->add([timer = timer]() {
      REPORT_ADD_STAT_VALUE(
          kCounterHTTPExecutorLatencyMs, timer.elapsed().count());
    });
  }
}

void PeriodicTaskManager::addExecutorStatsTask() {
  scheduler_.addFunction(
      [this]() { updateExecutorStats(); },
      std::chrono::microseconds{kTaskPeriodGlobalCounters},
      "executor_counters");
}

void PeriodicTaskManager::updateTaskStats() {
  // Report the number of tasks and drivers in the system.
  size_t numTasks{0};
  auto taskNumbers = taskManager_->getTaskNumbers(numTasks);
  REPORT_ADD_STAT_VALUE(kCounterNumTasks, taskManager_->getNumTasks());
  REPORT_ADD_STAT_VALUE(
      kCounterNumTasksRunning, taskNumbers[velox::exec::TaskState::kRunning]);
  REPORT_ADD_STAT_VALUE(
      kCounterNumTasksFinished, taskNumbers[velox::exec::TaskState::kFinished]);
  REPORT_ADD_STAT_VALUE(
      kCounterNumTasksCancelled,
      taskNumbers[velox::exec::TaskState::kCanceled]);
  REPORT_ADD_STAT_VALUE(
      kCounterNumTasksAborted, taskNumbers[velox::exec::TaskState::kAborted]);
  REPORT_ADD_STAT_VALUE(
      kCounterNumTasksFailed, taskNumbers[velox::exec::TaskState::kFailed]);

  auto driverCountStats = taskManager_->getDriverCountStats();
  REPORT_ADD_STAT_VALUE(
      kCounterNumRunningDrivers, driverCountStats.numRunningDrivers);
  REPORT_ADD_STAT_VALUE(
      kCounterNumBlockedDrivers, driverCountStats.numBlockedDrivers);
  REPORT_ADD_STAT_VALUE(
      kCounterTotalPartitionedOutputBuffer,
      velox::exec::PartitionedOutputBufferManager::getInstance()
          .lock()
          ->numBuffers());
}

void PeriodicTaskManager::addTaskStatsTask() {
  scheduler_.addFunction(
      [this]() { updateTaskStats(); },
      std::chrono::microseconds{kTaskPeriodGlobalCounters},
      "task_counters");
}

void PeriodicTaskManager::updateTaskCleanUp() {
  // Report the number of tasks and drivers in the system.
  if (taskManager_ != nullptr) {
    taskManager_->cleanOldTasks();
  }
}
void PeriodicTaskManager::addTaskCleanupTask() {
  scheduler_.addFunction(
      [this]() { updateTaskCleanUp(); },
      std::chrono::microseconds{kTaskPeriodCleanOldTasks},
      "clean_old_tasks");
}

void PeriodicTaskManager::updateMemoryAllocatorStats() {
  REPORT_ADD_STAT_VALUE(
      kCounterMappedMemoryBytes, (memoryAllocator_->numMapped() * 4096l));
  REPORT_ADD_STAT_VALUE(
      kCounterAllocatedMemoryBytes, (memoryAllocator_->numAllocated() * 4096l));
  // TODO(jtan6): Remove condition after T150019700 is done
  if (auto* mmapAllocator =
          dynamic_cast<const velox::memory::MmapAllocator*>(memoryAllocator_)) {
    REPORT_ADD_STAT_VALUE(
        kCounterMappedMemoryRawAllocBytesSmall,
        (mmapAllocator->numMallocBytes()))
  }
  // TODO(xiaoxmeng): add memory allocation size stats.
}

void PeriodicTaskManager::addMemoryAllocatorStatsTask() {
  scheduler_.addFunction(
      [this]() { updateMemoryAllocatorStats(); },
      std::chrono::microseconds{kMemoryPeriodGlobalCounters},
      "mmap_memory_counters");
}

void PeriodicTaskManager::updatePrestoExchangeSourceMemoryStats() {
  int64_t currQueuedMemoryBytes{0};
  int64_t peakQueuedMemoryBytes{0};
  PrestoExchangeSource::getMemoryUsage(
      currQueuedMemoryBytes, peakQueuedMemoryBytes);
  REPORT_ADD_STAT_VALUE(
      kCounterExchangeSourceQueuedBytes, currQueuedMemoryBytes);
  REPORT_ADD_STAT_VALUE(
      kCounterExchangeSourcePeakQueuedBytes, peakQueuedMemoryBytes);
}

void PeriodicTaskManager::addPrestoExchangeSourceMemoryStatsTask() {
  scheduler_.addFunction(
      [this]() { updatePrestoExchangeSourceMemoryStats(); },
      std::chrono::microseconds{kExchangeSourcePeriodGlobalCounters},
      "exchange_source_counters");
}

void PeriodicTaskManager::updateCacheStats() {
  const auto memoryCacheStats = asyncDataCache_->refreshStats();

  // Snapshots.
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumEntries, memoryCacheStats.numEntries);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumEmptyEntries, memoryCacheStats.numEmptyEntries);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumSharedEntries, memoryCacheStats.numShared);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumExclusiveEntries, memoryCacheStats.numExclusive);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumPrefetchedEntries, memoryCacheStats.numPrefetch);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheTotalTinyBytes, memoryCacheStats.tinySize);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheTotalLargeBytes, memoryCacheStats.largeSize);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheTotalTinyPaddingBytes, memoryCacheStats.tinyPadding);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheTotalLargePaddingBytes, memoryCacheStats.largePadding);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheTotalPrefetchBytes, memoryCacheStats.prefetchBytes);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheSumEvictScore, memoryCacheStats.sumEvictScore);

  // Interval cumulatives.
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumHit,
      memoryCacheStats.numHit - lastMemoryCacheHits_);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumNew,
      memoryCacheStats.numNew - lastMemoryCacheInserts_);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumEvict,
      memoryCacheStats.numEvict - lastMemoryCacheEvictions_);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumEvictChecks,
      memoryCacheStats.numEvictChecks - lastMemoryCacheEvictionChecks_);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumWaitExclusive,
      memoryCacheStats.numWaitExclusive - lastMemoryCacheStalls_);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumAllocClocks,
      memoryCacheStats.allocClocks - lastMemoryCacheAllocClocks_);

  lastMemoryCacheHits_ = memoryCacheStats.numHit;
  lastMemoryCacheInserts_ = memoryCacheStats.numNew;
  lastMemoryCacheEvictions_ = memoryCacheStats.numEvict;
  lastMemoryCacheEvictionChecks_ = memoryCacheStats.numEvictChecks;
  lastMemoryCacheStalls_ = memoryCacheStats.numWaitExclusive;
  lastMemoryCacheAllocClocks_ = memoryCacheStats.allocClocks;

  // All time cumulatives.
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumCumulativeHit, memoryCacheStats.numHit);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumCumulativeNew, memoryCacheStats.numNew);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumCumulativeEvict, memoryCacheStats.numEvict);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumCumulativeEvictChecks,
      memoryCacheStats.numEvictChecks);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumCumulativeWaitExclusive,
      memoryCacheStats.numWaitExclusive);
  REPORT_ADD_STAT_VALUE(
      kCounterMemoryCacheNumCumulativeAllocClocks,
      memoryCacheStats.allocClocks);
  if (memoryCacheStats.ssdStats != nullptr) {
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeReadEntries,
        memoryCacheStats.ssdStats->entriesRead)
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeReadBytes,
        memoryCacheStats.ssdStats->bytesRead);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeWrittenEntries,
        memoryCacheStats.ssdStats->entriesWritten);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeWrittenBytes,
        memoryCacheStats.ssdStats->bytesWritten);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeCachedEntries,
        memoryCacheStats.ssdStats->entriesCached);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeCachedBytes,
        memoryCacheStats.ssdStats->bytesCached);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeOpenSsdErrors,
        memoryCacheStats.ssdStats->openFileErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeOpenCheckpointErrors,
        memoryCacheStats.ssdStats->openCheckpointErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeOpenLogErrors,
        memoryCacheStats.ssdStats->openLogErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeDeleteCheckpointErrors,
        memoryCacheStats.ssdStats->deleteCheckpointErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeGrowFileErrors,
        memoryCacheStats.ssdStats->growFileErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeWriteSsdErrors,
        memoryCacheStats.ssdStats->writeSsdErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeWriteCheckpointErrors,
        memoryCacheStats.ssdStats->writeCheckpointErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeReadSsdErrors,
        memoryCacheStats.ssdStats->readSsdErrors);
    REPORT_ADD_STAT_VALUE(
        kCounterSsdCacheCumulativeReadCheckpointErrors,
        memoryCacheStats.ssdStats->readCheckpointErrors);
  }
}

void PeriodicTaskManager::addCacheStatsUpdateTask() {
  scheduler_.addFunction(
      [this]() { updateCacheStats(); },
      std::chrono::microseconds{kCachePeriodGlobalCounters},
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
      REPORT_ADD_STAT_EXPORT_TYPE(
          kNumElementsMetricName, facebook::velox::StatType::AVG);
      REPORT_ADD_STAT_EXPORT_TYPE(
          kPinnedSizeMetricName, facebook::velox::StatType::AVG);
      REPORT_ADD_STAT_EXPORT_TYPE(
          kCurSizeMetricName, facebook::velox::StatType::AVG);
      REPORT_ADD_STAT_EXPORT_TYPE(
          kNumAccumulativeHitsMetricName, facebook::velox::StatType::AVG);
      REPORT_ADD_STAT_EXPORT_TYPE(
          kNumAccumulativeLookupsMetricName, facebook::velox::StatType::AVG);
      REPORT_ADD_STAT_EXPORT_TYPE(
          kNumHitsMetricName, facebook::velox::StatType::AVG);
      REPORT_ADD_STAT_EXPORT_TYPE(
          kNumLookupsMetricName, facebook::velox::StatType::AVG);

      scheduler_.addFunction(
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
            REPORT_ADD_STAT_VALUE(
                kNumElementsMetricName, fileHandleCacheStats.numElements);
            REPORT_ADD_STAT_VALUE(
                kPinnedSizeMetricName, fileHandleCacheStats.pinnedSize);
            REPORT_ADD_STAT_VALUE(
                kCurSizeMetricName, fileHandleCacheStats.curSize);
            REPORT_ADD_STAT_VALUE(
                kNumAccumulativeHitsMetricName, fileHandleCacheStats.numHits);
            REPORT_ADD_STAT_VALUE(
                kNumAccumulativeLookupsMetricName,
                fileHandleCacheStats.numLookups);
            REPORT_ADD_STAT_VALUE(
                kNumHitsMetricName,
                fileHandleCacheStats.numHits - oldValues[kNumHitsMetricName]);
            oldValues[kNumHitsMetricName] = fileHandleCacheStats.numHits;
            REPORT_ADD_STAT_VALUE(
                kNumLookupsMetricName,
                fileHandleCacheStats.numLookups -
                    oldValues[kNumLookupsMetricName]);
            oldValues[kNumLookupsMetricName] = fileHandleCacheStats.numLookups;
          },
          std::chrono::microseconds{kCachePeriodGlobalCounters},
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
  REPORT_ADD_STAT_VALUE(
      kCounterOsUserCpuTimeMicros, userCpuTimeUs - lastUserCpuTimeUs_);
  lastUserCpuTimeUs_ = userCpuTimeUs;

  const int64_t systemCpuTimeUs{
      (int64_t)usage.ru_stime.tv_sec * 1'000'000 +
      (int64_t)usage.ru_stime.tv_usec};
  REPORT_ADD_STAT_VALUE(
      kCounterOsSystemCpuTimeMicros, systemCpuTimeUs - lastSystemCpuTimeUs_);
  lastSystemCpuTimeUs_ = systemCpuTimeUs;

  const int64_t softPageFaults{usage.ru_minflt};
  REPORT_ADD_STAT_VALUE(
      kCounterOsNumSoftPageFaults, softPageFaults - lastSoftPageFaults_);
  lastSoftPageFaults_ = softPageFaults;

  const int64_t hardPageFaults{usage.ru_majflt};
  REPORT_ADD_STAT_VALUE(
      kCounterOsNumHardPageFaults, hardPageFaults - lastHardPageFaults_);
  lastHardPageFaults_ = hardPageFaults;

  const int64_t voluntaryContextSwitches{usage.ru_nvcsw};
  REPORT_ADD_STAT_VALUE(
      kCounterOsNumVoluntaryContextSwitches,
      voluntaryContextSwitches - lastVoluntaryContextSwitches_);
  lastVoluntaryContextSwitches_ = voluntaryContextSwitches;

  const int64_t forcedContextSwitches{usage.ru_nivcsw};
  REPORT_ADD_STAT_VALUE(
      kCounterOsNumForcedContextSwitches,
      forcedContextSwitches - lastForcedContextSwitches_);
  lastForcedContextSwitches_ = forcedContextSwitches;
}

void PeriodicTaskManager::addOperatingSystemStatsUpdateTask() {
  scheduler_.addFunction(
      [this]() { updateOperatingSystemStats(); },
      std::chrono::microseconds{kOsPeriodGlobalCounters},
      "os_counters");
}
} // namespace facebook::presto
