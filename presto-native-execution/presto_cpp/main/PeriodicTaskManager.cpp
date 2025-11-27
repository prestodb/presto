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
#include "presto_cpp/main/PrestoServer.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/http/filters/HttpEndpointLatencyFilter.h"
#include "velox/common/base/PeriodicStatsReporter.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/CacheTTLController.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/Driver.h"

#include <sys/resource.h>

namespace {
#define REPORT_IF_NOT_ZERO(name, counter)     \
  do {                                        \
    if ((counter) != 0) {                     \
      RECORD_METRIC_VALUE((name), (counter)); \
    }                                         \
  } while (0)
} // namespace

namespace facebook::presto {

namespace {
folly::StringPiece getCounterForBlockingReason(
    velox::exec::BlockingReason reason) {
  switch (reason) {
    case velox::exec::BlockingReason::kWaitForConsumer:
      return kCounterNumBlockedWaitForConsumerDrivers;
    case velox::exec::BlockingReason::kWaitForSplit:
      return kCounterNumBlockedWaitForSplitDrivers;
    case velox::exec::BlockingReason::kWaitForProducer:
      return kCounterNumBlockedWaitForProducerDrivers;
    case velox::exec::BlockingReason::kWaitForJoinBuild:
      return kCounterNumBlockedWaitForJoinBuildDrivers;
    case velox::exec::BlockingReason::kWaitForJoinProbe:
      return kCounterNumBlockedWaitForJoinProbeDrivers;
    case velox::exec::BlockingReason::kWaitForMergeJoinRightSide:
      return kCounterNumBlockedWaitForMergeJoinRightSideDrivers;
    case velox::exec::BlockingReason::kWaitForMemory:
      return kCounterNumBlockedWaitForMemoryDrivers;
    case velox::exec::BlockingReason::kWaitForConnector:
      return kCounterNumBlockedWaitForConnectorDrivers;
    case velox::exec::BlockingReason::kYield:
      return kCounterNumBlockedYieldDrivers;
    case velox::exec::BlockingReason::kNotBlocked:
      [[fallthrough]];
    default:
      return {};
  }
}

class ThreadPoolExecutorStatsReporter {
 public:
  ThreadPoolExecutorStatsReporter(
      folly::ThreadPoolExecutor* executor,
      const std::string& poolName,
      uint32_t estimatedMaxNumTasks)
      : executor_(executor),
        numThreadsMetricName_(
            fmt::format(kCounterThreadPoolNumThreadsFormat, poolName)),
        numActiveThreadsMetricName_(
            fmt::format(kCounterThreadPoolNumActiveThreadsFormat, poolName)),
        numPendingTasksMetricName_(
            fmt::format(kCounterThreadPoolNumPendingTasksFormat, poolName)),
        numTotalTasksMetricName_(
            fmt::format(kCounterThreadPoolNumTotalTasksFormat, poolName)),
        maxIdleTimeNsMetricName_(
            fmt::format(kCounterThreadPoolMaxIdleTimeNsFormat, poolName)) {
    VELOX_CHECK_NOT_NULL(executor_);
    const auto numThreads = executor_->numThreads();
    const auto numHistogramBuckets = 100;
    DEFINE_METRIC(numThreadsMetricName_, facebook::velox::StatType::AVG);
    DEFINE_HISTOGRAM_METRIC(
        numActiveThreadsMetricName_, 1, 0, numThreads, 50, 90, 100);
    DEFINE_HISTOGRAM_METRIC(
        numPendingTasksMetricName_,
        estimatedMaxNumTasks / numHistogramBuckets,
        0,
        estimatedMaxNumTasks,
        50,
        90,
        100);
    DEFINE_HISTOGRAM_METRIC(
        numTotalTasksMetricName_,
        estimatedMaxNumTasks / numHistogramBuckets,
        0,
        estimatedMaxNumTasks,
        50,
        90,
        100);
    DEFINE_HISTOGRAM_METRIC(
        maxIdleTimeNsMetricName_,
        10'000'000'000 /* 10s */,
        0,
        300'000'000'000 /* 300s */,
        50,
        90,
        100);
  }

  void report() const {
    const auto poolStats = executor_->getPoolStats();
    RECORD_METRIC_VALUE(numThreadsMetricName_, poolStats.threadCount);
    RECORD_HISTOGRAM_METRIC_VALUE(
        numActiveThreadsMetricName_, poolStats.activeThreadCount);
    RECORD_HISTOGRAM_METRIC_VALUE(
        numPendingTasksMetricName_, poolStats.pendingTaskCount);
    RECORD_HISTOGRAM_METRIC_VALUE(
        numTotalTasksMetricName_, poolStats.totalTaskCount);
    RECORD_HISTOGRAM_METRIC_VALUE(
        maxIdleTimeNsMetricName_, poolStats.maxIdleTime.count());
  }

 private:
  folly::ThreadPoolExecutor* const executor_{nullptr};

  const std::string numThreadsMetricName_;
  const std::string numActiveThreadsMetricName_;
  const std::string numPendingTasksMetricName_;
  const std::string numTotalTasksMetricName_;
  const std::string maxIdleTimeNsMetricName_;
};

class HiveConnectorStatsReporter {
 public:
  explicit HiveConnectorStatsReporter(
      std::shared_ptr<velox::connector::hive::HiveConnector> connector)
      : connector_(std::move(connector)),
        numElementsMetricName_(
            fmt::format(
                kCounterHiveFileHandleCacheNumElementsFormat,
                connector_->connectorId())),
        pinnedSizeMetricName_(
            fmt::format(
                kCounterHiveFileHandleCachePinnedSizeFormat,
                connector_->connectorId())),
        curSizeMetricName_(
            fmt::format(
                kCounterHiveFileHandleCacheCurSizeFormat,
                connector_->connectorId())),
        numAccumulativeHitsMetricName_(
            fmt::format(
                kCounterHiveFileHandleCacheNumAccumulativeHitsFormat,
                connector_->connectorId())),
        numAccumulativeLookupsMetricName_(
            fmt::format(
                kCounterHiveFileHandleCacheNumAccumulativeLookupsFormat,
                connector_->connectorId())),
        numHitsMetricName_(
            fmt::format(
                kCounterHiveFileHandleCacheNumHitsFormat,
                connector_->connectorId())),
        numLookupsMetricName_(
            fmt::format(
                kCounterHiveFileHandleCacheNumLookupsFormat,
                connector_->connectorId())) {
    DEFINE_METRIC(numElementsMetricName_, velox::StatType::AVG);
    DEFINE_METRIC(pinnedSizeMetricName_, velox::StatType::AVG);
    DEFINE_METRIC(curSizeMetricName_, velox::StatType::AVG);
    DEFINE_METRIC(numAccumulativeHitsMetricName_, velox::StatType::AVG);
    DEFINE_METRIC(numAccumulativeLookupsMetricName_, velox::StatType::AVG);
    DEFINE_METRIC(numHitsMetricName_, velox::StatType::AVG);
    DEFINE_METRIC(numLookupsMetricName_, velox::StatType::AVG);
  }

  void report() {
    auto stats = connector_->fileHandleCacheStats();
    RECORD_METRIC_VALUE(numElementsMetricName_, stats.numElements);
    RECORD_METRIC_VALUE(pinnedSizeMetricName_, stats.pinnedSize);
    RECORD_METRIC_VALUE(curSizeMetricName_, stats.curSize);
    RECORD_METRIC_VALUE(numAccumulativeHitsMetricName_, stats.numHits);
    RECORD_METRIC_VALUE(numAccumulativeLookupsMetricName_, stats.numLookups);
    RECORD_METRIC_VALUE(numHitsMetricName_, stats.numHits - lastNumHits_);
    lastNumHits_ = stats.numHits;
    RECORD_METRIC_VALUE(
        numLookupsMetricName_, stats.numLookups - lastNumLookups_);
    lastNumLookups_ = stats.numLookups;
  }

 private:
  const std::shared_ptr<velox::connector::hive::HiveConnector> connector_;
  const std::string numElementsMetricName_;
  const std::string pinnedSizeMetricName_;
  const std::string curSizeMetricName_;
  const std::string numAccumulativeHitsMetricName_;
  const std::string numAccumulativeLookupsMetricName_;
  const std::string numHitsMetricName_;
  const std::string numLookupsMetricName_;
  size_t lastNumHits_{0};
  size_t lastNumLookups_{0};
};

} // namespace

// Every two seconds we export server counters.
static constexpr size_t kTaskPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every two seconds we export exchange source counters.
static constexpr size_t kExchangeSourcePeriodGlobalCounters{
    2'000'000}; // 2 seconds.
// Every 1 minute we clean old tasks.
static constexpr size_t kTaskPeriodCleanOldTasks{60'000'000}; // 60 seconds.
// Every 1 minute we export connector counters.
static constexpr size_t kConnectorPeriodGlobalCounters{
    60'000'000}; // 60 seconds.
static constexpr size_t kOsPeriodGlobalCounters{2'000'000}; // 2 seconds
static constexpr size_t kHttpServerPeriodGlobalCounters{
    60'000'000}; // 60 seconds.
static constexpr size_t kHttpClientPeriodGlobalCounters{
    60'000'000}; // 60 seconds.

PeriodicTaskManager::PeriodicTaskManager(
    folly::CPUThreadPoolExecutor* driverCPUExecutor,
    folly::CPUThreadPoolExecutor* spillerExecutor,
    folly::IOThreadPoolExecutor* httpSrvIoExecutor,
    folly::CPUThreadPoolExecutor* httpSrvCpuExecutor,
    folly::IOThreadPoolExecutor* exchangeHttpIoExecutor,
    folly::CPUThreadPoolExecutor* exchangeHttpCpuExecutor,
    TaskManager* taskManager,
    const velox::memory::MemoryAllocator* memoryAllocator,
    const velox::cache::AsyncDataCache* asyncDataCache,
    const std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::Connector>>& connectors,
    PrestoServer* server)
    : driverCPUExecutor_(driverCPUExecutor),
      spillerExecutor_(spillerExecutor),
      httpSrvIoExecutor_(httpSrvIoExecutor),
      httpSrvCpuExecutor_(httpSrvCpuExecutor),
      exchangeHttpIoExecutor_(exchangeHttpIoExecutor),
      exchangeHttpCpuExecutor_(exchangeHttpCpuExecutor),
      taskManager_(taskManager),
      memoryAllocator_(memoryAllocator),
      asyncDataCache_(asyncDataCache),
      arbitrator_(velox::memory::memoryManager()->arbitrator()),
      connectors_(connectors),
      server_(server) {}

void PeriodicTaskManager::start() {
  VELOX_CHECK_NOT_NULL(arbitrator_);
  velox::PeriodicStatsReporter::Options opts;
  opts.arbitrator = arbitrator_->kind() == "NOOP" ? nullptr : arbitrator_;
  opts.allocator = memoryAllocator_;
  opts.cache = asyncDataCache_;
  opts.spillMemoryPool = velox::memory::spillMemoryPool();
  velox::startPeriodicStatsReporter(opts);

  // If executors are null, don't bother starting this task.
  if ((driverCPUExecutor_ != nullptr) || (httpSrvIoExecutor_ != nullptr)) {
    addExecutorStatsTask();
  }

  VELOX_CHECK_NOT_NULL(taskManager_);
  addTaskStatsTask();

  if (SystemConfig::instance()->enableOldTaskCleanUp()) {
    addOldTaskCleanupTask();
  }

  addPrestoExchangeSourceMemoryStatsTask();

  addConnectorStatsTask();

  addOperatingSystemStatsUpdateTask();

  if (SystemConfig::instance()->enableHttpEndpointLatencyFilter()) {
    addHttpServerStatsTask();
  }

  addHttpClientStatsTask();

  if (server_ && server_->hasCoordinatorDiscoverer()) {
    numDriverThreads_ = server_->numDriverThreads();
    addWatchdogTask();
  }

  oneTimeRunner_.start();
}

void PeriodicTaskManager::stop() {
  velox::stopPeriodicStatsReporter();
  oneTimeRunner_.cancelAllFunctionsAndWait();
  oneTimeRunner_.shutdown();
  repeatedRunner_.stop();
}

void PeriodicTaskManager::addExecutorStatsTask() {
  std::vector<ThreadPoolExecutorStatsReporter> reporters;
  auto addExecutorFunc = [&](folly::ThreadPoolExecutor* executor,
                             const std::string& executorName,
                             uint32_t estimatedMaxNumTasks) {
    if (executor != nullptr) {
      reporters.push_back(ThreadPoolExecutorStatsReporter(
          executor, executorName, estimatedMaxNumTasks));
    }
  };
  addExecutorFunc(driverCPUExecutor_, "driver_cpu_executor", 5'000);
  addExecutorFunc(spillerExecutor_, "spiller_executor", 5'000);
  addExecutorFunc(httpSrvIoExecutor_, "http_srv_io_executor", 50'000);
  addExecutorFunc(httpSrvCpuExecutor_, "http_srv_cpu_executor", 50'000);
  addExecutorFunc(exchangeHttpIoExecutor_, "exchange_http_io_executor", 50'000);
  addExecutorFunc(
      exchangeHttpCpuExecutor_, "exchange_http_cpu_executor", 50'000);
  addTask(
      [this, reporters = std::move(reporters)]() {
        for (auto& reporter : reporters) {
          reporter.report();
        }
      },
      kTaskPeriodGlobalCounters,
      "executor_counters");
}

void PeriodicTaskManager::updateTaskStats() {
  // Report the number of tasks and drivers in the system.
  size_t numTasks{0};
  auto taskNumbers = taskManager_->getTaskNumbers(numTasks);
  RECORD_METRIC_VALUE(kCounterNumTasks, taskManager_->getNumTasks());
  RECORD_METRIC_VALUE(
      kCounterNumTasksBytesProcessed, taskManager_->getBytesProcessed());
  RECORD_METRIC_VALUE(
      kCounterNumTasksRunning,
      taskNumbers[static_cast<int>(PrestoTaskState::kRunning)]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksFinished,
      taskNumbers[static_cast<int>(PrestoTaskState::kFinished)]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksCancelled,
      taskNumbers[static_cast<int>(PrestoTaskState::kCanceled)]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksAborted,
      taskNumbers[static_cast<int>(PrestoTaskState::kAborted)]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksFailed,
      taskNumbers[static_cast<int>(PrestoTaskState::kFailed)]);
  RECORD_METRIC_VALUE(
      kCounterNumTasksPlanned,
      taskNumbers[static_cast<int>(PrestoTaskState::kPlanned)]);
  RECORD_METRIC_VALUE(kCounterNumTasksQueued, taskManager_->numQueuedTasks());

  const auto driverCounts = taskManager_->getDriverCounts();
  RECORD_METRIC_VALUE(kCounterNumQueuedDrivers, driverCounts.numQueuedDrivers);
  RECORD_METRIC_VALUE(
      kCounterNumOnThreadDrivers, driverCounts.numOnThreadDrivers);
  RECORD_METRIC_VALUE(
      kCounterNumSuspendedDrivers, driverCounts.numSuspendedDrivers);
  for (const auto& it : driverCounts.numBlockedDrivers) {
    const auto counterName = getCounterForBlockingReason(it.first);
    if (counterName.data() != nullptr) {
      RECORD_METRIC_VALUE(counterName, it.second);
    }
  }
  RECORD_METRIC_VALUE(
      kCounterTotalPartitionedOutputBuffer,
      velox::exec::OutputBufferManager::getInstanceRef()->numBuffers());
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

void PeriodicTaskManager::addConnectorStatsTask() {
  std::vector<HiveConnectorStatsReporter> reporters;
  for (const auto& itr : connectors_) {
    if (auto hiveConnector =
            std::dynamic_pointer_cast<velox::connector::hive::HiveConnector>(
                itr.second)) {
      reporters.emplace_back(std::move(hiveConnector));
    }
  }
  addTask(
      [reporters = std::move(reporters)]() mutable {
        for (auto& reporter : reporters) {
          reporter.report();
        }
      },
      kConnectorPeriodGlobalCounters,
      "ConnectorStats");
}

void PeriodicTaskManager::updateOperatingSystemStats() {
  struct rusage usage{};
  memset(&usage, 0, sizeof(usage));
  getrusage(RUSAGE_SELF, &usage);

  const int64_t userCpuTimeUs{
      static_cast<int64_t>(usage.ru_utime.tv_sec) * 1'000'000 +
      static_cast<int64_t>(usage.ru_utime.tv_usec)};
  RECORD_METRIC_VALUE(kCounterOsUserCpuTimeMicros, userCpuTimeUs);

  const int64_t systemCpuTimeUs{
      static_cast<int64_t>(usage.ru_stime.tv_sec) * 1'000'000 +
      static_cast<int64_t>(usage.ru_stime.tv_usec)};
  RECORD_METRIC_VALUE(kCounterOsSystemCpuTimeMicros, systemCpuTimeUs);

  const int64_t softPageFaults{usage.ru_minflt};
  RECORD_METRIC_VALUE(kCounterOsNumSoftPageFaults, softPageFaults);

  const int64_t hardPageFaults{usage.ru_majflt};
  RECORD_METRIC_VALUE(kCounterOsNumHardPageFaults, hardPageFaults);

  const int64_t voluntaryContextSwitches{usage.ru_nvcsw};
  RECORD_METRIC_VALUE(
      kCounterOsNumVoluntaryContextSwitches, voluntaryContextSwitches);

  const int64_t forcedContextSwitches{usage.ru_nivcsw};
  RECORD_METRIC_VALUE(
      kCounterOsNumForcedContextSwitches, forcedContextSwitches);
}

void PeriodicTaskManager::addOperatingSystemStatsUpdateTask() {
  addTask(
      [this]() { updateOperatingSystemStats(); },
      kOsPeriodGlobalCounters,
      "os_counters");
}

void PeriodicTaskManager::printHttpServerStats() {
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

void PeriodicTaskManager::addHttpServerStatsTask() {
  addTask(
      [this]() { printHttpServerStats(); },
      kHttpServerPeriodGlobalCounters,
      "http_server_stats");
}

void PeriodicTaskManager::updateHttpClientStats() {
  const auto numConnectionsCreated = http::HttpClient::numConnectionsCreated();
  RECORD_METRIC_VALUE(
      kCounterHttpClientNumConnectionsCreated,
      numConnectionsCreated - lastHttpClientNumConnectionsCreated_);
  lastHttpClientNumConnectionsCreated_ = numConnectionsCreated;
}

void PeriodicTaskManager::addHttpClientStatsTask() {
  addTask(
      [this] { updateHttpClientStats(); },
      kHttpClientPeriodGlobalCounters,
      "http_client_stats");
}

void PeriodicTaskManager::addWatchdogTask() {
  addTask(
      [this] {
        std::vector<std::string> deadlockTasks;
        std::vector<velox::exec::Task::OpCallInfo> stuckOpCalls;
        if (!taskManager_->getStuckOpCalls(deadlockTasks, stuckOpCalls)) {
          LOG(ERROR)
              << "Cannot take lock on task manager, likely starving or deadlocked";
          RECORD_METRIC_VALUE(kCounterNumTaskManagerLockTimeOut, 1);
          detachWorker("starving or deadlocked task manager");
          return;
        }
        RECORD_METRIC_VALUE(kCounterNumTaskManagerLockTimeOut, 0);
        for (const auto& taskId : deadlockTasks) {
          LOG(ERROR) << "Starving or deadlocked task: " << taskId;
        }
        RECORD_METRIC_VALUE(kCounterNumTasksDeadlock, deadlockTasks.size());
        for (const auto& call : stuckOpCalls) {
          LOG(ERROR) << "Stuck operator: tid=" << call.tid
                     << " taskId=" << call.taskId << " opCall=" << call.opCall
                     << " duration= " << velox::succinctMillis(call.durationMs);
        }
        RECORD_METRIC_VALUE(kCounterNumStuckDrivers, stuckOpCalls.size());

        const char* detachReason = nullptr;

        // Detach worker from the cluster if more than a certain number of
        // driver threads are blocked by stuck operators (one unique operator
        // can only get stuck on one unique thread).
        const auto numStuckOperatorsToDetachWorker = std::min(
            SystemConfig::instance()->driverNumStuckOperatorsToDetachWorker(),
            numDriverThreads_);
        if (stuckOpCalls.size() >= numStuckOperatorsToDetachWorker) {
          detachReason = "detected stuck operators";
        } else if (!deadlockTasks.empty()) {
          detachReason = "starving or deadlocked task";
        }

        // Detach worker from the cluster if it has been overloaded for too
        // long.
        const auto now = velox::getCurrentTimeSec();
        const auto lastNotOverloadedTime =
            taskManager_->lastNotOverloadedTimeInSecs();
        const auto overloadedDurationSec =
            taskManager_->isServerOverloaded() && (now > lastNotOverloadedTime)
            ? now - lastNotOverloadedTime
            : 0UL;
        RECORD_METRIC_VALUE(
            kCounterOverloadedDurationSec, overloadedDurationSec);
        if (detachReason == nullptr) {
          const uint64_t secondsThreshold =
              SystemConfig::instance()->workerOverloadedSecondsToDetachWorker();
          if (secondsThreshold > 0 &&
              overloadedDurationSec > secondsThreshold) {
            detachReason = "worker has been overloaded for too long";
          }
        }

        if (detachReason != nullptr) {
          detachWorker(detachReason);
        } else {
          maybeAttachWorker();
        }
      },
      60'000'000, // 60 seconds
      "Watchdog");
}

void PeriodicTaskManager::detachWorker(const char* reason) {
  LOG(WARNING) << "TraceContext::status:\n"
               << velox::process::TraceContext::statusLine();
  if (server_ && server_->nodeState() == NodeState::kActive) {
    LOG(WARNING) << "Will detach worker due to " << reason;
    server_->detachWorker();
  }
}

void PeriodicTaskManager::maybeAttachWorker() {
  if (server_ && !server_->isShuttingDown() &&
      server_->nodeState() == NodeState::kShuttingDown) {
    LOG(WARNING) << "Will attach worker due to the absence of stuck operators";
    server_->maybeAttachWorker();
  }
}

} // namespace facebook::presto
