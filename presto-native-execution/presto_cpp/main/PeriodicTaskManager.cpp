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
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/exec/Driver.h"

#include <sys/resource.h>

namespace facebook::presto {

// Every two seconds we export server counters.
static constexpr size_t kTaskPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every two seconds we export memory counters.
static constexpr size_t kMemoryPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every 1 minute we clean old tasks.
static constexpr size_t kTaskPeriodCleanOldTasks{60'000'000}; // 60 seconds.
// Every 1 minute we export cache counters.
static constexpr size_t kCachePeriodGlobalCounters{60'000'000}; // 60 seconds.
static constexpr size_t kOsPeriodGlobalCounters{2'000'000}; // 2 seconds

PeriodicTaskManager::PeriodicTaskManager(
    folly::CPUThreadPoolExecutor* driverCPUExecutor,
    folly::IOThreadPoolExecutor* httpExecutor,
    TaskManager* taskManager)
    : driverCPUExecutor_(driverCPUExecutor),
      httpExecutor_(httpExecutor),
      taskManager_(taskManager),
      memoryManager_(velox::memory::MemoryManager::getInstance()) {}

void PeriodicTaskManager::start() {
  // Add new functions here.

  // If executors are null, don't bother starting this task.
  if (driverCPUExecutor_ or httpExecutor_) {
    scheduler_.addFunction(
        [driverCPUExecutor = driverCPUExecutor_,
         httpExecutor = httpExecutor_]() {
          if (driverCPUExecutor) {
            // Report the current queue size of the thread pool.
            REPORT_ADD_STAT_VALUE(
                kCounterDriverCPUExecutorQueueSize,
                driverCPUExecutor->getTaskQueueSize());

            // Report the latency between scheduling the task and its execution.
            folly::stop_watch<std::chrono::milliseconds> timer;
            driverCPUExecutor->add([timer = timer]() {
              REPORT_ADD_STAT_VALUE(
                  kCounterDriverCPUExecutorLatencyMs, timer.elapsed().count());
            });
          }

          if (httpExecutor) {
            // Report the latency between scheduling the task and its execution.
            folly::stop_watch<std::chrono::milliseconds> timer;
            httpExecutor->add([timer = timer]() {
              REPORT_ADD_STAT_VALUE(
                  kCounterHTTPExecutorLatencyMs, timer.elapsed().count());
            });
          }
        },
        std::chrono::microseconds{kTaskPeriodGlobalCounters},
        "executor_counters");
  }

  if (taskManager_) {
    scheduler_.addFunction(
        [taskManager = taskManager_]() {
          // Report the number of tasks and drivers in the system.
          size_t numTasks{0};
          auto taskNumbers = taskManager->getTaskNumbers(numTasks);
          REPORT_ADD_STAT_VALUE(kCounterNumTasks, taskManager->getNumTasks());
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksRunning,
              taskNumbers[velox::exec::TaskState::kRunning]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksFinished,
              taskNumbers[velox::exec::TaskState::kFinished]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksCancelled,
              taskNumbers[velox::exec::TaskState::kCanceled]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksAborted,
              taskNumbers[velox::exec::TaskState::kAborted]);
          REPORT_ADD_STAT_VALUE(
              kCounterNumTasksFailed,
              taskNumbers[velox::exec::TaskState::kFailed]);
          auto driverCountStats = taskManager->getDriverCountStats();

          REPORT_ADD_STAT_VALUE(
              kCounterNumRunningDrivers, driverCountStats.numRunningDrivers);
          REPORT_ADD_STAT_VALUE(
              kCounterNumBlockedDrivers, driverCountStats.numBlockedDrivers);

          REPORT_ADD_STAT_VALUE(
              kCounterTotalPartitionedOutputBuffer,
              velox::exec::PartitionedOutputBufferManager::getInstance()
                  .lock()
                  ->numBuffers());
        },
        std::chrono::microseconds{kTaskPeriodGlobalCounters},
        "task_counters");

    scheduler_.addFunction(
        [taskManager = taskManager_]() {
          // Report the number of tasks and drivers in the system.
          taskManager->cleanOldTasks();
        },
        std::chrono::microseconds{kTaskPeriodCleanOldTasks},
        "clean_old_tasks");
  }

  if (auto* allocator = velox::memory::MemoryAllocator::getInstance()) {
    scheduler_.addFunction(
        [allocator]() {
          REPORT_ADD_STAT_VALUE(
              kCounterMappedMemoryBytes, (allocator->numMapped() * 4096l));
          REPORT_ADD_STAT_VALUE(
              kCounterAllocatedMemoryBytes,
              (allocator->numAllocated() * 4096l));
          // TODO(xiaoxmeng): add memory allocation size stats.
        },
        std::chrono::microseconds{kMemoryPeriodGlobalCounters},
        "mmap_memory_counters");
  }

  if (auto* asyncDataCache = dynamic_cast<velox::cache::AsyncDataCache*>(
          velox::memory::MemoryAllocator::getInstance())) {
    scheduler_.addFunction(
        [asyncDataCache]() {
          const auto memoryCacheStats = asyncDataCache->refreshStats();

          // Snapshots.
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumEntries, memoryCacheStats.numEntries);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumEmptyEntries,
              memoryCacheStats.numEmptyEntries);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumSharedEntries, memoryCacheStats.numShared);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumExclusiveEntries,
              memoryCacheStats.numExclusive);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumPrefetchedEntries,
              memoryCacheStats.numPrefetch);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheTotalTinyBytes, memoryCacheStats.tinySize);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheTotalLargeBytes, memoryCacheStats.largeSize);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheTotalTinyPaddingBytes,
              memoryCacheStats.tinyPadding);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheTotalLargePaddingBytes,
              memoryCacheStats.largePadding);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheTotalPrefetchBytes,
              memoryCacheStats.prefetchBytes);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheSumEvictScore, memoryCacheStats.sumEvictScore);

          // Interval cumulatives.
          static int64_t memoryNumHitOld{0};
          static int64_t memoryNumNewOld{0};
          static int64_t memoryNumEvictOld{0};
          static int64_t memoryNumEvictChecksOld{0};
          static int64_t memoryNumWaitExclusiveOld{0};
          static int64_t memoryNumAllocClocksOld{0};
          static int64_t ssdReadEntriesOld{0};
          static int64_t ssdReadBytesOld{0};
          static int64_t ssdWrittenBytesOld{0};
          static int64_t ssdWrittenEntriesOld{0};
          static int64_t ssdCachedBytesOld{0};
          static int64_t ssdCachedEntriesOld{0};
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumHit,
              memoryCacheStats.numHit - memoryNumHitOld);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumNew,
              memoryCacheStats.numNew - memoryNumNewOld);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumEvict,
              memoryCacheStats.numEvict - memoryNumEvictOld);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumEvictChecks,
              memoryCacheStats.numEvictChecks - memoryNumEvictChecksOld);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumWaitExclusive,
              memoryCacheStats.numWaitExclusive - memoryNumWaitExclusiveOld);
          REPORT_ADD_STAT_VALUE(
              kCounterMemoryCacheNumAllocClocks,
              memoryCacheStats.allocClocks - memoryNumAllocClocksOld);
          if (memoryCacheStats.ssdStats) {
            REPORT_ADD_STAT_VALUE(
                kCounterSsdCacheReadEntries,
                memoryCacheStats.ssdStats->entriesRead - ssdReadEntriesOld);
            REPORT_ADD_STAT_VALUE(
                kCounterSsdCacheReadBytes,
                memoryCacheStats.ssdStats->bytesRead - ssdReadBytesOld);
            REPORT_ADD_STAT_VALUE(
                kCounterSsdCacheWrittenEntries,
                memoryCacheStats.ssdStats->entriesWritten -
                    ssdWrittenEntriesOld);
            REPORT_ADD_STAT_VALUE(
                kCounterSsdCacheWrittenBytes,
                memoryCacheStats.ssdStats->bytesWritten - ssdWrittenBytesOld);
            REPORT_ADD_STAT_VALUE(
                kCounterSsdCacheCachedEntries,
                memoryCacheStats.ssdStats->entriesCached - ssdCachedEntriesOld);
            REPORT_ADD_STAT_VALUE(
                kCounterSsdCacheCachedBytes,
                memoryCacheStats.ssdStats->bytesCached - ssdCachedBytesOld);
          }

          memoryNumHitOld = memoryCacheStats.numHit;
          memoryNumNewOld = memoryCacheStats.numNew;
          memoryNumEvictOld = memoryCacheStats.numEvict;
          memoryNumEvictChecksOld = memoryCacheStats.numEvictChecks;
          memoryNumWaitExclusiveOld = memoryCacheStats.numWaitExclusive;
          memoryNumAllocClocksOld = memoryCacheStats.allocClocks;
          if (memoryCacheStats.ssdStats) {
            ssdReadEntriesOld = memoryCacheStats.ssdStats->entriesRead;
            ssdReadBytesOld = memoryCacheStats.ssdStats->bytesRead;
            ssdWrittenEntriesOld = memoryCacheStats.ssdStats->entriesWritten;
            ssdWrittenBytesOld = memoryCacheStats.ssdStats->bytesWritten;
            ssdCachedEntriesOld = memoryCacheStats.ssdStats->entriesCached;
            ssdCachedBytesOld = memoryCacheStats.ssdStats->bytesCached;
          }
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
          if (memoryCacheStats.ssdStats) {
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
          }
        },
        std::chrono::microseconds{kCachePeriodGlobalCounters},
        "cache_counters");
  }

  scheduler_.addFunction(
      []() {
        struct rusage usage;
        memset(&usage, 0, sizeof(usage));
        getrusage(RUSAGE_SELF, &usage);

        REPORT_ADD_STAT_VALUE(
            kCounterCumulativeUserCpuTimeMicros,
            (int64_t)usage.ru_utime.tv_sec * 1'000'000 +
                (int64_t)usage.ru_utime.tv_usec);
        REPORT_ADD_STAT_VALUE(
            kCounterCumulativeSystemCpuTimeMicros,
            (int64_t)usage.ru_stime.tv_sec * 1'000'000 +
                (int64_t)usage.ru_stime.tv_usec);
        REPORT_ADD_STAT_VALUE(
            kCounterNumCumulativeSoftPageFaults, usage.ru_minflt)
        REPORT_ADD_STAT_VALUE(
            kCounterNumCumulativeHardPageFaults, usage.ru_majflt)
      },
      std::chrono::microseconds{kOsPeriodGlobalCounters},
      "os_counters");

  // This should be the last call in this method.
  scheduler_.start();
}

void PeriodicTaskManager::stop() {
  scheduler_.cancelAllFunctionsAndWait();
  scheduler_.shutdown();
}

} // namespace facebook::presto
