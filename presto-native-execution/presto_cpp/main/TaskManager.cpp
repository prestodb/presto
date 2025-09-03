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

#include "presto_cpp/main/TaskManager.h"

#include <utility>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <folly/container/F14Set.h>
#include <velox/core/PlanNode.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/types/PrestoToVeloxSplit.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"

using namespace facebook::velox;

using facebook::presto::protocol::TaskId;
using facebook::presto::protocol::TaskInfo;

namespace facebook::presto {

// Unlimited concurrent lifespans is translated to this limit.
constexpr uint32_t kMaxConcurrentLifespans{16};

namespace {

// We request cancellation for tasks which haven't been accessed by coordinator
// for a considerable time.
void cancelAbandonedTasksInternal(const TaskMap& taskMap, int32_t abandonedMs) {
  for (const auto& [id, prestoTask] : taskMap) {
    if (prestoTask->task != nullptr) {
      if (prestoTask->task->isRunning()) {
        if (prestoTask->timeSinceLastCoordinatorHeartbeatMs() >= abandonedMs) {
          LOG(INFO) << "Cancelling abandoned task '" << id << "'.";
          prestoTask->task->requestCancel();
        }
      }
    }
  }
}

// If spilling is enabled and the given Task can spill, then this helper
// generates the spilling directory path for the Task, and sets the path to it
// in the Task.
static void maybeSetupTaskSpillDirectory(
    const core::PlanFragment& planFragment,
    exec::Task& execTask,
    const std::string& baseSpillDirectory) {
  if (baseSpillDirectory.empty() ||
      !planFragment.canSpill(execTask.queryCtx()->queryConfig())) {
    return;
  }

  const auto includeNodeInSpillPath =
      SystemConfig::instance()->includeNodeInSpillPath();
  auto nodeConfig = NodeConfig::instance();
  const auto [taskSpillDirPath, dateSpillDirPath] =
      TaskManager::buildTaskSpillDirectoryPath(
          baseSpillDirectory,
          nodeConfig->nodeInternalAddress(),
          nodeConfig->nodeId(),
          execTask.queryCtx()->queryId(),
          execTask.taskId(),
          includeNodeInSpillPath);
  execTask.setSpillDirectory(taskSpillDirPath, /*alreadyCreated=*/false);

  execTask.setCreateSpillDirectoryCb(
      [spillDir = taskSpillDirPath, dateStrDir = dateSpillDirPath]() {
        auto fs = filesystems::getFileSystem(dateStrDir, nullptr);
        // First create the top level directory (date string of the query) with
        // TTL or other configs if set.
        filesystems::DirectoryOptions options;
        // Do not fail if the directory already exist because another process
        // may have already created the dateStrDir.
        options.failIfExists = false;
        auto config = SystemConfig::instance()->spillerDirectoryCreateConfig();
        if (!config.empty()) {
          options.values.emplace(
              filesystems::DirectoryOptions::kMakeDirectoryConfig.toString(),
              config);
        }
        fs->mkdir(dateStrDir, options);

        // After the parent directory is created,
        // then create the spill directory for the actual task.
        fs->mkdir(spillDir);
        return spillDir;
      });
}

// Keep outstanding Promises in RequestHandler's state itself.
//
// If the promise is not fulfilled yet, resetting promiseHolder will
// raise a "BrokenPromise" exception.  Even though we raise
// "BrokenPromise" Exception, it is mostly a nop because we call
// onFinalization() after we set 'requestExpired' to false, which
// prevents thenError() to generate any error responses.
// Since raising the exception releases all promise's resources
// including eventBase, it allows the server to stop the
// threads successfully and shutdown gracefully.
template <typename T>
void keepPromiseAlive(
    PromiseHolderPtr<T> promiseHolder,
    std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
  handlerState->runOnFinalization(
      [promiseHolder]() mutable { promiseHolder.reset(); });
}

std::unique_ptr<Result> createEmptyResult(long token) {
  auto result = std::make_unique<Result>();
  result->sequence = result->nextSequence = token;
  result->data = folly::IOBuf::create(0);
  result->complete = false;
  return result;
}

std::unique_ptr<Result> createCompleteResult(long token) {
  auto result = std::make_unique<Result>();
  result->sequence = result->nextSequence = token;
  result->data = folly::IOBuf::create(0);
  result->complete = true;
  return result;
}

void getData(
    PromiseHolderPtr<std::unique_ptr<Result>> promiseHolder,
    std::weak_ptr<http::CallbackRequestHandlerState> stateHolder,
    const TaskId& taskId,
    long destination,
    long token,
    protocol::DataSize maxSize,
    exec::OutputBufferManager& bufferManager) {
  if (promiseHolder == nullptr) {
    // promise/future is expired.
    return;
  }

  int64_t startMs = getCurrentTimeMs();
  auto bufferFound = bufferManager.getData(
      taskId,
      destination,
      maxSize.getValue(protocol::DataUnit::BYTE),
      token,
      [taskId = taskId, bufferId = destination, promiseHolder, startMs](
          std::vector<std::unique_ptr<folly::IOBuf>> pages,
          int64_t sequence,
          std::vector<int64_t> remainingBytes) mutable {
        bool complete = false;
        int64_t nextSequence = sequence;
        std::unique_ptr<folly::IOBuf> iobuf;
        int64_t bytes = 0;
        for (auto& page : pages) {
          if (page) {
            VELOX_CHECK(!complete, "Received data after end marker");
            if (!iobuf) {
              iobuf = std::move(page);
              bytes = iobuf->length();
            } else {
              auto next = std::move(page);
              bytes += next->length();
              iobuf->prev()->appendChain(std::move(next));
            }
            ++nextSequence;
          } else {
            complete = true;
          }
        }

        VLOG(1) << "Task " << taskId << ", buffer " << bufferId << ", sequence "
                << sequence << " Results size: " << bytes
                << ", page count: " << pages.size()
                << ", remaining: " << folly::join(',', remainingBytes)
                << ", complete: " << std::boolalpha << complete;

        auto result = std::make_unique<Result>();
        result->sequence = sequence;
        result->nextSequence = nextSequence;
        result->complete = complete;
        result->data = std::move(iobuf);
        result->remainingBytes = std::move(remainingBytes);

        promiseHolder->promise.setValue(std::move(result));

        RECORD_METRIC_VALUE(
            kCounterPartitionedOutputBufferGetDataLatencyMs,
            getCurrentTimeMs() - startMs);
      },
      [stateHolder]() {
        auto state = stateHolder.lock();
        if (state == nullptr) {
          return false;
        }
        return !state->requestExpired();
      });

  if (!bufferFound) {
    // Buffer was erased for current TaskId.
    VLOG(1) << "Task " << taskId << ", buffer " << destination << ", sequence "
            << token << ", buffer not found.";
    promiseHolder->promise.setValue(std::move(createEmptyResult(token)));
  }
}

// Presto-on-Spark is expected to specify all splits at once along with
// no-more-splits flag. Verify that all plan nodes that require splits
// have received splits and no-more-splits flag. This check helps
// prevent hard-to-debug query hangs caused by Velox Task waiting for
// splits that never arrive.
void checkSplitsForBatchTask(
    const velox::core::PlanNodePtr& planNode,
    const std::vector<protocol::TaskSource>& sources) {
  std::unordered_set<velox::core::PlanNodeId> splitNodeIds;
  velox::core::PlanNode::findFirstNode(
      planNode.get(), [&](const velox::core::PlanNode* node) {
        if (node->requiresSplits()) {
          splitNodeIds.insert(node->id());
        }
        return false;
      });

  for (const auto& source : sources) {
    VELOX_USER_CHECK(
        source.noMoreSplits,
        "Expected no-more-splits message for plan node {}",
        source.planNodeId);
    splitNodeIds.erase(source.planNodeId);
  }

  VELOX_USER_CHECK(
      splitNodeIds.empty(),
      "Expected all splits and no-more-splits message for all plan nodes: {}",
      folly::join(", ", splitNodeIds));
}

struct ZombieTaskStats {
  const std::string info;
  const long numExtraReferences;

  ZombieTaskStats(
      const std::shared_ptr<exec::Task>& task,
      long _numExtraReferences)
      : info(
            SystemConfig::instance()->logZombieTaskInfo() ? task->toString()
                                                          : task->taskId()),
        numExtraReferences(_numExtraReferences) {}
};

// Helper structure holding stats for 'zombie' tasks.
struct ZombieTaskStatsSet {
  size_t numRunning{0};
  size_t numFinished{0};
  size_t numCanceled{0};
  size_t numAborted{0};
  size_t numFailed{0};
  size_t numTotal{0};

  const size_t numSampleTasks;
  std::vector<ZombieTaskStats> tasks;

  ZombieTaskStatsSet()
      : numSampleTasks(SystemConfig::instance()->logNumZombieTasks()) {
    tasks.reserve(numSampleTasks);
  }

  void updateCounts(
      std::shared_ptr<exec::Task>& task,
      long numExtraReferences) {
    switch (task->state()) {
      case exec::TaskState::kRunning:
        ++numRunning;
        break;
      case exec::TaskState::kFinished:
        ++numFinished;
        break;
      case exec::TaskState::kCanceled:
        ++numCanceled;
        break;
      case exec::TaskState::kAborted:
        ++numAborted;
        break;
      case exec::TaskState::kFailed:
        ++numFailed;
        break;
      default:
        break;
    }
    if (tasks.size() < numSampleTasks) {
      tasks.emplace_back(task, numExtraReferences);
    }
  }

  void logZombieTaskStatus(const std::string& hangingClassName) {
    LOG(ERROR) << "There are " << numTotal << " zombie " << hangingClassName
               << " that satisfy cleanup conditions but could not be "
                  "cleaned up, because the "
               << hangingClassName
               << " are referenced by more than 1 owners. RUNNING["
               << numRunning << "] FINISHED[" << numFinished << "] CANCELED["
               << numCanceled << "] ABORTED[" << numAborted << "] FAILED["
               << numFailed << "]  Sample task IDs (shows only "
               << numSampleTasks << " IDs): " << std::endl;
    for (auto i = 0; i < tasks.size(); ++i) {
      LOG(ERROR) << "Zombie " << hangingClassName << " [" << i + 1 << "/"
                 << tasks.size()
                 << "]: Extra Refs: " << tasks[i].numExtraReferences << ", "
                 << tasks[i].info << std::endl;
    }
  }
};

// Add task to the task queue.
void enqueueTask(
    TaskQueue& taskQueue,
    std::shared_ptr<PrestoTask>& prestoTask) {
  auto execTask = prestoTask->task;
  if (execTask == nullptr) {
    return;
  }

  // If an entry exists with tasks for the same query, then add the task to it.
  for (auto& entry : taskQueue) {
    if (!entry.empty()) {
      if (auto queuedTask = entry[0].lock()) {
        auto queuedExecTask = queuedTask->task;
        if (queuedExecTask &&
            (queuedExecTask->queryCtx() == execTask->queryCtx())) {
          entry.emplace_back(prestoTask);
          return;
        }
      }
    }
  }
  // Otherwise create a new entry.
  taskQueue.push_back({prestoTask});
}
} // namespace

TaskManager::TaskManager(
    folly::Executor* driverExecutor,
    folly::Executor* httpSrvCpuExecutor,
    folly::Executor* spillerExecutor)
    : queryContextManager_(std::make_unique<QueryContextManager>(
          driverExecutor,
          spillerExecutor)),
      bufferManager_(velox::exec::OutputBufferManager::getInstanceRef()),
      httpSrvCpuExecutor_(httpSrvCpuExecutor) {
  VELOX_CHECK_NOT_NULL(bufferManager_, "invalid OutputBufferManager");
}

void TaskManager::setBaseUri(const std::string& baseUri) {
  baseUri_ = baseUri;
}

void TaskManager::setNodeId(const std::string& nodeId) {
  nodeId_ = nodeId;
}

void TaskManager::setBaseSpillDirectory(const std::string& baseSpillDirectory) {
  VELOX_CHECK(!baseSpillDirectory.empty());
  baseSpillDir_.withWLock(
      [&](auto& baseSpillDir) { baseSpillDir = baseSpillDirectory; });
}

std::string TaskManager::getBaseSpillDirectory() const {
  return baseSpillDir_.withRLock(
      [](const auto& baseSpillDir) { return baseSpillDir; });
}

bool TaskManager::emptyBaseSpillDirectory() const {
  return baseSpillDir_.withRLock(
      [](const auto& baseSpillDir) { return baseSpillDir.empty(); });
}

void TaskManager::setOldTaskCleanUpMs(int32_t oldTaskCleanUpMs) {
  VELOX_CHECK_GE(oldTaskCleanUpMs, 0);
  oldTaskCleanUpMs_ = oldTaskCleanUpMs;
}

TaskMap TaskManager::tasks() const {
  return taskMap_.withRLock([](const auto& tasks) { return tasks; });
}

const QueryContextManager* TaskManager::getQueryContextManager() const {
  return queryContextManager_.get();
}

void TaskManager::abortResults(const TaskId& taskId, long bufferId) {
  VLOG(1) << "TaskManager::abortResults " << taskId;

  bufferManager_->deleteResults(taskId, bufferId);
}

void TaskManager::acknowledgeResults(
    const TaskId& taskId,
    const long bufferId,
    long token) {
  VLOG(1) << "TaskManager::acknowledgeResults " << taskId << ", " << bufferId
          << ", " << token;
  bufferManager_->acknowledge(taskId, bufferId, token);
}

std::unique_ptr<TaskInfo> TaskManager::createOrUpdateErrorTask(
    const TaskId& taskId,
    const std::exception_ptr& exception,
    bool summarize,
    long startProcessCpuTime) {
  auto prestoTask = findOrCreateTask(taskId, startProcessCpuTime);
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateCoordinatorHeartbeatLocked();
    prestoTask->updateHeartbeatLocked();
    if (prestoTask->error == nullptr) {
      prestoTask->error = exception;
    }
    prestoTask->info.needsPlan = false;
  }

  auto info = prestoTask->updateInfo(summarize);
  return std::make_unique<TaskInfo>(info);
}

/*static*/ std::tuple<std::string, std::string>
TaskManager::buildTaskSpillDirectoryPath(
    const std::string& baseSpillPath,
    const std::string& nodeIp,
    const std::string& nodeId,
    const std::string& queryId,
    const protocol::TaskId& taskId,
    bool includeNodeInSpillPath) {
  // Generate 'YYYY-MM-DD' from the query ID, which starts with 'YYYYMMDD'.
  // In case query id is malformed (should not be the case in production) we
  // fall back to the predefined date.
  const std::string dateString = (queryId.size() >= 8)
      ? fmt::format(
            "{}-{}-{}",
            queryId.substr(0, 4),
            queryId.substr(4, 2),
            queryId.substr(6, 2))
      : "1970-01-01";

  std::string taskSpillDirPath;
  folly::toAppend(
      fmt::format("{}/presto_native/", baseSpillPath), &taskSpillDirPath);
  if (includeNodeInSpillPath) {
    folly::toAppend(fmt::format("{}_{}/", nodeIp, nodeId), &taskSpillDirPath);
  }

  std::string dateSpillDirPath = taskSpillDirPath;
  folly::toAppend(fmt::format("{}/", dateString), &dateSpillDirPath);

  folly::toAppend(
      fmt::format("{}/{}/{}/", dateString, queryId, taskId), &taskSpillDirPath);
  return std::make_tuple(
      std::move(taskSpillDirPath), std::move(dateSpillDirPath));
}

void TaskManager::getDataForResultRequests(
    const std::unordered_map<int64_t, std::shared_ptr<ResultRequest>>&
        resultRequests) {
  for (const auto& entry : resultRequests) {
    auto resultRequest = entry.second.get();

    VLOG(1) << "Processing pending result request for task "
            << resultRequest->taskId << ", buffer " << resultRequest->bufferId
            << ", sequence " << resultRequest->token;
    getData(
        resultRequest->promise.lock(),
        resultRequest->state,
        resultRequest->taskId,
        resultRequest->bufferId,
        resultRequest->token,
        resultRequest->maxSize,
        *bufferManager_);
  }
}

std::unique_ptr<protocol::TaskInfo> TaskManager::createOrUpdateTask(
    const protocol::TaskId& taskId,
    const protocol::TaskUpdateRequest& updateRequest,
    const velox::core::PlanFragment& planFragment,
    bool summarize,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    long startProcessCpuTime) {
  return createOrUpdateTaskImpl(
      taskId,
      planFragment,
      updateRequest.sources,
      updateRequest.outputIds,
      summarize,
      std::move(queryCtx),
      startProcessCpuTime);
}

std::unique_ptr<protocol::TaskInfo> TaskManager::createOrUpdateBatchTask(
    const protocol::TaskId& taskId,
    const protocol::BatchTaskUpdateRequest& batchUpdateRequest,
    const velox::core::PlanFragment& planFragment,
    bool summarize,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    long startProcessCpuTime) {
  auto updateRequest = batchUpdateRequest.taskUpdateRequest;

  checkSplitsForBatchTask(planFragment.planNode, updateRequest.sources);

  return createOrUpdateTaskImpl(
      taskId,
      planFragment,
      updateRequest.sources,
      updateRequest.outputIds,
      summarize,
      std::move(queryCtx),
      startProcessCpuTime);
}

std::unique_ptr<TaskInfo> TaskManager::createOrUpdateTaskImpl(
    const TaskId& taskId,
    const velox::core::PlanFragment& planFragment,
    const std::vector<protocol::TaskSource>& sources,
    const protocol::OutputBuffers& outputBuffers,
    bool summarize,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    long startProcessCpuTime) {
  std::shared_ptr<exec::Task> execTask;
  bool startTask = false;
  auto prestoTask = findOrCreateTask(taskId, startProcessCpuTime);
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateCoordinatorHeartbeatLocked();
    if (not prestoTask->task && planFragment.planNode) {
      // If the task is aborted, no need to do anything else.
      // This takes care of DELETE task message coming before CREATE task.
      if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
        return std::make_unique<TaskInfo>(
            prestoTask->updateInfoLocked(summarize));
      }

      // Uses a temp variable to store the created velox task to destroy it
      // under presto task lock if spill directory setup fails. Otherwise, the
      // concurrent task creation retry from the coordinator might see the
      // unexpected state in presto task left by the previously failed velox
      // task which hasn't been destroyed yet, such as the task pool in query's
      // root memory pool.
      auto newExecTask = exec::Task::create(
          taskId,
          planFragment,
          prestoTask->id.id(),
          std::move(queryCtx),
          exec::Task::ExecutionMode::kParallel,
          static_cast<exec::Consumer>(nullptr),
          prestoTask->id.stageId());
      // TODO: move spill directory creation inside velox task execution
      // whenever spilling is triggered. It will reduce the unnecessary file
      // operations on remote storage.
      const auto baseSpillDir = *(baseSpillDir_.rlock());
      maybeSetupTaskSpillDirectory(planFragment, *newExecTask, baseSpillDir);

      prestoTask->task = std::move(newExecTask);
      prestoTask->info.needsPlan = false;
      startTask = true;
    }
    execTask = prestoTask->task;
  }
  // Outside of prestoTask->mutex.
  VELOX_CHECK_NOT_NULL(
      execTask,
      "Task update received before setting a plan. The splits in "
      "this update could not be delivered for {}",
      taskId);
  std::unordered_map<int64_t, std::shared_ptr<ResultRequest>> resultRequests;
  PromiseHolderWeakPtr<std::unique_ptr<protocol::TaskStatus>> statusRequest;
  PromiseHolderWeakPtr<std::unique_ptr<protocol::TaskInfo>> infoRequest;

  bool startNextQueuedTask = false;
  std::unique_ptr<TaskInfo> ret;
  {
    // Create or update task can be called concurrently for the same task.
    // We need to lock here for allow only one to be executed at a time.
    // This is especially important for adding splits to the task.
    std::lock_guard<std::mutex> l(prestoTask->mutex);

    if (startTask) {
      maybeStartTaskLocked(prestoTask, startNextQueuedTask);

      resultRequests = std::move(prestoTask->resultRequests);
      statusRequest = prestoTask->statusRequest;
      infoRequest = prestoTask->infoRequest;
    }

    getDataForResultRequests(resultRequests);

    if (outputBuffers.type != protocol::BufferType::PARTITIONED &&
        !execTask->updateOutputBuffers(
            outputBuffers.buffers.size(), outputBuffers.noMoreBufferIds)) {
      VLOG(1) << "Failed to update output buffers for task: " << taskId;
    }

    for (const auto& source : sources) {
      // Add all splits from the source to the task.
      VLOG(1) << "Adding " << source.splits.size() << " splits to " << taskId
              << " for node " << source.planNodeId;
      // Keep track of the max sequence for this batch of splits.
      int64_t maxSplitSequenceId{-1};
      for (const auto& protocolSplit : source.splits) {
        auto split = toVeloxSplit(protocolSplit);
        if (split.hasConnectorSplit()) {
          maxSplitSequenceId =
              std::max(maxSplitSequenceId, protocolSplit.sequenceId);
          execTask->addSplitWithSequence(
              source.planNodeId, std::move(split), protocolSplit.sequenceId);
        }
      }
      // Update task's max split sequence id after all splits have been added.
      execTask->setMaxSplitSequenceId(source.planNodeId, maxSplitSequenceId);

      for (const auto& lifespan : source.noMoreSplitsForLifespan) {
        if (lifespan.isgroup) {
          LOG(INFO) << "No more splits for group " << lifespan.groupid
                    << " for " << taskId << " for node " << source.planNodeId;
          execTask->noMoreSplitsForGroup(source.planNodeId, lifespan.groupid);
        }
      }

      if (source.noMoreSplits) {
        LOG(INFO) << "No more splits for " << taskId << " for node "
                  << source.planNodeId;
        // If the task has not been started yet, we collect the plan node to
        // call 'no more splits' after the start.
        if (prestoTask->taskStarted) {
          execTask->noMoreSplits(source.planNodeId);
        } else {
          prestoTask->delayedNoMoreSplitsPlanNodes_.emplace(source.planNodeId);
        }
      }
    }

    // 'prestoTask' will exist by virtue of shared_ptr but may for example have
    // been aborted.
    auto info =
        prestoTask->updateInfoLocked(summarize); // Presto task is locked above.
    if (auto promiseHolder = infoRequest.lock()) {
      promiseHolder->promise.setValue(
          std::make_unique<protocol::TaskInfo>(info));
    }
    if (auto promiseHolder = statusRequest.lock()) {
      promiseHolder->promise.setValue(
          std::make_unique<protocol::TaskStatus>(info.taskStatus));
    }
    ret = std::make_unique<TaskInfo>(info);
  }

  if (startNextQueuedTask) {
    maybeStartNextQueuedTask();
  }

  return ret;
}

void TaskManager::maybeStartTaskLocked(
    std::shared_ptr<PrestoTask>& prestoTask,
    bool& startNextQueuedTask) {
  // Start the new task if the task queuing is disabled.
  // Also start it if some tasks from this query have already started.
  if (!SystemConfig::instance()->workerOverloadedTaskQueuingEnabled() ||
      getQueryContextManager()->queryHasStartedTasks(prestoTask->info.taskId)) {
    startTaskLocked(prestoTask);
    return;
  }

  if (serverOverloaded_) {
    // If server is overloaded, we don't start anything, but queue the new task.
    LOG(INFO) << "TASK QUEUE: Server is overloaded. Queueing task "
              << prestoTask->info.taskId;
    auto lockedTaskQueue = taskQueue_.wlock();
    enqueueTask(*lockedTaskQueue, prestoTask);
  } else {
    // If server is not overloaded, then we start the new task if the task queue
    // is empty, otherwise we queue the new task and start the first queued task
    // instead.
    {
      auto lockedTaskQueue = taskQueue_.wlock();
      if (!lockedTaskQueue->empty()) {
        LOG(INFO) << "TASK QUEUE: "
                     "Server is not overloaded, but "
                  << lockedTaskQueue->size()
                  << " queued queries detected. Queueing task "
                  << prestoTask->info.taskId;
        enqueueTask(*lockedTaskQueue, prestoTask);
        startNextQueuedTask = true;
      }
    }
    if (!startNextQueuedTask) {
      startTaskLocked(prestoTask);
    }
  }
}

void TaskManager::startTaskLocked(std::shared_ptr<PrestoTask>& prestoTask) {
  auto execTask = prestoTask->task;
  if (execTask == nullptr) {
    return;
  }

  getQueryContextManager()->setQueryHasStartedTasks(prestoTask->info.taskId);

  const uint32_t maxDrivers = execTask->queryCtx()->queryConfig().get<int32_t>(
      kMaxDriversPerTask.data(), SystemConfig::instance()->maxDriversPerTask());
  uint32_t concurrentLifespans =
      execTask->queryCtx()->queryConfig().get<int32_t>(
          kConcurrentLifespansPerTask.data(),
          SystemConfig::instance()->concurrentLifespansPerTask());
  // Zero concurrent lifespans means 'unlimited', but we still limit the
  // number to some reasonable one.
  if (concurrentLifespans == 0) {
    concurrentLifespans = kMaxConcurrentLifespans;
  }

  if (execTask->isGroupedExecution()) {
    LOG(INFO) << "Starting task " << prestoTask->info.taskId << " with "
              << maxDrivers << " max drivers and " << concurrentLifespans
              << " concurrent lifespans (grouped execution).";
  } else {
    LOG(INFO) << "Starting task " << prestoTask->info.taskId << " with "
              << maxDrivers << " max drivers.";
  }
  execTask->start(maxDrivers, concurrentLifespans);
  prestoTask->taskStarted = true;

  // Record the time we spent between task creation and start, which is the
  // planned (queued) time.
  const auto queuedTimeInMs =
      velox::getCurrentTimeMs() - prestoTask->createTimeMs;
  prestoTask->info.stats.queuedTimeInNanos = queuedTimeInMs * 1'000'000;
  RECORD_METRIC_VALUE(kCounterTaskPlannedTimeMs, queuedTimeInMs);
}

void TaskManager::maybeStartNextQueuedTask() {
  if (serverOverloaded_) {
    return;
  }

  // We will start all queued tasks from a single query.
  std::vector<std::shared_ptr<PrestoTask>> tasksToStart;

  // We run the loop here because some tasks might have failed or were aborted
  // or cancelled. Despite that we want to start at least one task.
  {
    auto lockedTaskQueue = taskQueue_.wlock();
    while (!lockedTaskQueue->empty()) {
      // Get the next entry.
      auto queuedTasks = std::move(lockedTaskQueue->front());
      lockedTaskQueue->pop_front();

      // Get all the still valid tasks from the entry.
      bool queryTasksAreGoodToStart{true};
      tasksToStart.clear();
      for (auto& queuedTask : queuedTasks) {
        auto taskToStart = queuedTask.lock();

        // Task is already gone or no Velox task (the latter will never happen).
        if (taskToStart == nullptr || taskToStart->task == nullptr) {
          LOG(WARNING) << "TASK QUEUE: Skipping null task in the queue.";
          queryTasksAreGoodToStart = false;
          continue;
        }

        // Sanity check.
        VELOX_CHECK(
            !taskToStart->taskStarted,
            "TASK QUEUE: "
            "The queued task must not be started, but it is already started");

        const auto taskState = taskToStart->taskState();
        // If the status is not 'planned' then the tasks were likely aborted.
        if (taskState != PrestoTaskState::kPlanned) {
          LOG(INFO) << "TASK QUEUE: Discarding (not starting) queued task "
                    << taskToStart->info.taskId << " because state is "
                    << prestoTaskStateString(taskState);
          queryTasksAreGoodToStart = false;
          continue;
        }

        tasksToStart.emplace_back(taskToStart);
      }

      if (queryTasksAreGoodToStart) {
        break;
      }
    }
  }

  for (auto& taskToStart : tasksToStart) {
    std::lock_guard<std::mutex> l(taskToStart->mutex);
    LOG(INFO) << "TASK QUEUE: Picking task to start from the queue: "
              << taskToStart->info.taskId;
    startTaskLocked(taskToStart);
    // Make sure we call 'no more splits' we might have received before the task
    // started.
    auto execTask = taskToStart->task;
    if (execTask != nullptr) {
      for (const auto& planNodeId :
           taskToStart->delayedNoMoreSplitsPlanNodes_) {
        execTask->noMoreSplits(planNodeId);
      }
      taskToStart->delayedNoMoreSplitsPlanNodes_.clear();
    }
  }
  const auto queuedTasksLeft = numQueuedTasks();
  if (queuedTasksLeft > 0) {
    LOG(INFO) << "TASK QUEUE: " << numQueuedTasks() << " queued tasks left";
  }
}

std::unique_ptr<TaskInfo>
TaskManager::deleteTask(const TaskId& taskId, bool /*abort*/, bool summarize) {
  LOG(INFO) << "Deleting task " << taskId;
  // Fast. non-blocking delete and cancel serialized on 'taskMap'.
  std::shared_ptr<facebook::presto::PrestoTask> prestoTask;

  taskMap_.withRLock([&](const auto& taskMap) {
    auto it = taskMap.find(taskId);
    if (it != taskMap.cend()) {
      prestoTask = it->second;
    }
  });

  if (prestoTask == nullptr) {
    VLOG(1) << "Task not found for delete: " << taskId;
    prestoTask = findOrCreateTask(taskId, 0);
  }

  std::lock_guard<std::mutex> l(prestoTask->mutex);
  prestoTask->updateHeartbeatLocked();
  prestoTask->updateCoordinatorHeartbeatLocked();
  auto execTask = prestoTask->task;
  if (execTask) {
    auto state = execTask->state();
    if (state == exec::TaskState::kRunning) {
      execTask->requestAbort();
    }
    prestoTask->info.stats.endTimeInMillis = velox::getCurrentTimeMs();
    prestoTask->updateInfoLocked(summarize);
  } else {
    // If task is not found than we observe DELETE message coming before
    // CREATE. In that case we create the task with ABORTED state, so we know
    // we don't need to do anything on CREATE message and can clean up the
    // cancelled task later.
    prestoTask->info.taskStatus.state = protocol::TaskState::ABORTED;
    return std::make_unique<TaskInfo>(prestoTask->info);
  }

  // Do not erase the finished/aborted tasks, because someone might still want
  // to get some results from them. Instead, we run a periodic task to clean up
  // the old finished/aborted tasks.
  if (prestoTask->info.taskStatus.state == protocol::TaskState::RUNNING) {
    prestoTask->info.taskStatus.state = protocol::TaskState::ABORTED;
  }

  return std::make_unique<TaskInfo>(prestoTask->info);
}

size_t TaskManager::cleanOldTasks() {
  const auto startTimeMs = getCurrentTimeMs();

  folly::F14FastSet<protocol::TaskId> taskIdsToClean;

  ZombieTaskStatsSet zombieVeloxTaskCounts;
  ZombieTaskStatsSet zombiePrestoTaskCounts;
  uint32_t numTasksWithStuckOperator{0};
  {
    // We copy task map locally to avoid locking task map for a potentially long
    // time. We also lock for 'read'.
    const TaskMap taskMap = *(taskMap_.rlock());

    for (const auto& [id, prestoTask] : taskMap) {
      if (prestoTask->hasStuckOperator) {
        ++numTasksWithStuckOperator;
      }

      bool eraseTask{false};
      if (prestoTask->task != nullptr) {
        if (prestoTask->task->state() != exec::TaskState::kRunning) {
          // Since the state is not running, we know the task has been
          // terminated. We use termination time instead of end time as the
          // former does not include time waiting for results to be consumed.
          if (prestoTask->task->timeSinceTerminationMs() >= oldTaskCleanUpMs_) {
            // Not running and old.
            eraseTask = true;
          }
        }
      } else {
        // Use heartbeat to determine the task's age.
        if (prestoTask->timeSinceLastHeartbeatMs() >= oldTaskCleanUpMs_) {
          eraseTask = true;
        }
      }

      // We assume 'not erase' is the 'most common' case.
      if (!eraseTask) {
        continue;
      }

      const auto prestoTaskRefCount = prestoTask.use_count();
      const auto taskRefCount = prestoTask->task.use_count();

      // Do not remove 'zombie' tasks (with outstanding references) from the
      // map. We use it to track the number of tasks. Note, since we copied the
      // task map, presto tasks should have an extra reference (2 from two
      // maps).
      if (prestoTaskRefCount > 2 || taskRefCount > 1) {
        auto& task = prestoTask->task;
        if (prestoTaskRefCount > 2) {
          ++zombiePrestoTaskCounts.numTotal;
          if (task != nullptr) {
            zombiePrestoTaskCounts.updateCounts(task, prestoTaskRefCount - 2);
          }
        }
        if (taskRefCount > 1) {
          ++zombieVeloxTaskCounts.numTotal;
          zombieVeloxTaskCounts.updateCounts(task, taskRefCount - 1);
        }
      } else {
        taskIdsToClean.emplace(id);
      }
    }

    cancelAbandonedTasksInternal(taskMap, oldTaskCleanUpMs_);
  }

  const auto elapsedMs = (getCurrentTimeMs() - startTimeMs);
  if (not taskIdsToClean.empty()) {
    std::vector<std::shared_ptr<PrestoTask>> tasksToDelete;
    tasksToDelete.reserve(taskIdsToClean.size());
    {
      // Remove tasks from the task map. We briefly lock for write here.
      auto writableTaskMap = taskMap_.wlock();
      for (const auto& taskId : taskIdsToClean) {
        tasksToDelete.push_back(std::move(writableTaskMap->at(taskId)));
        writableTaskMap->erase(taskId);
      }
    }
    LOG(INFO) << "cleanOldTasks: Cleaned " << taskIdsToClean.size()
              << " old task(s) in " << elapsedMs << " ms";
  } else if (elapsedMs > 1000) {
    // If we took more than 1 second to run this, something might be wrong.
    LOG(INFO) << "cleanOldTasks: Didn't clean any old task(s). Took "
              << elapsedMs << "ms";
  }

  if (zombieVeloxTaskCounts.numTotal > 0) {
    zombieVeloxTaskCounts.logZombieTaskStatus("Task");
  }
  if (zombiePrestoTaskCounts.numTotal > 0) {
    zombiePrestoTaskCounts.logZombieTaskStatus("PrestoTask");
  }
  RECORD_METRIC_VALUE(
      kCounterNumZombieVeloxTasks, zombieVeloxTaskCounts.numTotal);
  RECORD_METRIC_VALUE(
      kCounterNumZombiePrestoTasks, zombiePrestoTaskCounts.numTotal);
  RECORD_METRIC_VALUE(
      kCounterNumTasksWithStuckOperator, numTasksWithStuckOperator);
  return taskIdsToClean.size();
}

void TaskManager::cancelAbandonedTasks() {
  // We copy task map locally to avoid locking task map for a potentially long
  // time. We also lock for 'read'.
  const TaskMap taskMap = *(taskMap_.rlock());
  cancelAbandonedTasksInternal(taskMap, oldTaskCleanUpMs_);
}

folly::Future<std::unique_ptr<protocol::TaskInfo>> TaskManager::getTaskInfo(
    const TaskId& taskId,
    bool summarize,
    std::optional<protocol::TaskState> currentState,
    std::optional<protocol::Duration> maxWait,
    std::shared_ptr<http::CallbackRequestHandlerState> state) {
  auto [promise, future] =
      folly::makePromiseContract<std::unique_ptr<protocol::TaskInfo>>();
  auto prestoTask = findOrCreateTask(taskId);
  if (!currentState || !maxWait) {
    // Return current TaskInfo without waiting.
    promise.setValue(std::make_unique<protocol::TaskInfo>(
        prestoTask->updateInfo(summarize)));
    prestoTask->updateCoordinatorHeartbeat();
    return std::move(future).via(httpSrvCpuExecutor_);
  }

  uint64_t maxWaitMicros =
      std::max(1.0, maxWait.value().getValue(protocol::TimeUnit::MICROSECONDS));
  protocol::TaskInfo info;
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateHeartbeatLocked();
    prestoTask->updateCoordinatorHeartbeatLocked();
    if (!prestoTask->task) {
      auto promiseHolder =
          std::make_shared<PromiseHolder<std::unique_ptr<protocol::TaskInfo>>>(
              std::move(promise));
      keepPromiseAlive(promiseHolder, state);
      prestoTask->infoRequest = folly::to_weak_ptr(promiseHolder);

      return std::move(future)
          .via(httpSrvCpuExecutor_)
          .onTimeout(
              std::chrono::microseconds(maxWaitMicros),
              [prestoTask, summarize]() {
                return std::make_unique<protocol::TaskInfo>(
                    prestoTask->updateInfo(summarize));
              });
    }
    info = prestoTask->updateInfoLocked(summarize);
  }
  if (currentState.value() != info.taskStatus.state ||
      isFinalState(info.taskStatus.state)) {
    promise.setValue(std::make_unique<protocol::TaskInfo>(info));
    return std::move(future).via(httpSrvCpuExecutor_);
  }

  auto promiseHolder =
      std::make_shared<PromiseHolder<std::unique_ptr<protocol::TaskInfo>>>(
          std::move(promise));

  prestoTask->task->stateChangeFuture(maxWaitMicros)
      .via(httpSrvCpuExecutor_)
      .thenValue([promiseHolder, prestoTask, summarize](auto&& /*done*/) {
        promiseHolder->promise.setValue(std::make_unique<protocol::TaskInfo>(
            prestoTask->updateInfo(summarize)));
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [promiseHolder, prestoTask, summarize](const std::exception& /*e*/) {
            // We come here in the case of maxWait elapsed.
            promiseHolder->promise.setValue(
                std::make_unique<protocol::TaskInfo>(
                    prestoTask->updateInfo(summarize)));
          });
  return std::move(future).via(httpSrvCpuExecutor_);
}

folly::Future<std::unique_ptr<Result>> TaskManager::getResults(
    const TaskId& taskId,
    long destination,
    long token,
    protocol::DataSize maxSize,
    protocol::Duration maxWait,
    std::shared_ptr<http::CallbackRequestHandlerState> state) {
  uint64_t maxWaitMicros =
      std::max(1.0, maxWait.getValue(protocol::TimeUnit::MICROSECONDS));
  VLOG(1) << "TaskManager::getResults task:" << taskId
          << ", destination:" << destination << ", token:" << token;

  try {
    auto prestoTask = findOrCreateTask(taskId);

    // If the task is aborted or failed, then return an error.
    if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
      // respond with a delay to prevent request "bursts"
      return folly::futures::sleep(std::chrono::microseconds(maxWaitMicros))
          .via(httpSrvCpuExecutor_)
          .thenValue([token](auto&&) { return createEmptyResult(token); });
    }
    if (prestoTask->error != nullptr) {
      LOG(WARNING) << "Calling getResult() on a failed PrestoTask: " << taskId;
      // respond with a delay to prevent request "bursts"
      return folly::futures::sleep(std::chrono::microseconds(maxWaitMicros))
          .via(httpSrvCpuExecutor_)
          .thenValue([token](auto&&) { return createEmptyResult(token); });
    }

    auto [promise, future] =
        folly::makePromiseContract<std::unique_ptr<Result>>();

    auto promiseHolder =
        std::make_shared<PromiseHolder<std::unique_ptr<Result>>>(
            std::move(promise));

    // Error in fetching results or creating a task may prevent the promise from
    // being fulfilled leading to a BrokenPromise exception on promise
    // destruction. To avoid the BrokenPromise exception, fulfill the promise
    // with incomplete empty pages.
    promiseHolder->atDestruction(
        [token](folly::Promise<std::unique_ptr<Result>> p) {
          p.setValue(createEmptyResult(token));
        });

    auto timeoutFn = [token]() { return createEmptyResult(token); };

    for (;;) {
      if (prestoTask->taskStarted) {
        // If the task has finished, then send completion result.
        if (prestoTask->task->state() == exec::TaskState::kFinished) {
          promiseHolder->promise.setValue(createCompleteResult(token));
          return std::move(future).via(httpSrvCpuExecutor_);
        }
        // If task is not running let the request timeout. The task may have
        // failed at creation time and the coordinator hasn't yet caught up.
        if (prestoTask->task->state() == exec::TaskState::kRunning) {
          getData(
              promiseHolder,
              folly::to_weak_ptr(state),
              taskId,
              destination,
              token,
              maxSize,
              *bufferManager_);
        }
        return std::move(future)
            .via(httpSrvCpuExecutor_)
            .onTimeout(std::chrono::microseconds(maxWaitMicros), timeoutFn);
      }

      std::lock_guard<std::mutex> l(prestoTask->mutex);
      if (prestoTask->taskStarted) {
        continue;
      }
      // The task is not started yet, put the request
      VLOG(1) << "Queuing up result request for task " << taskId
              << ", destination " << destination << ", sequence " << token;

      keepPromiseAlive(promiseHolder, state);

      auto request = std::make_unique<ResultRequest>(
          folly::to_weak_ptr(promiseHolder),
          folly::to_weak_ptr(state),
          taskId,
          destination,
          token,
          maxSize);
      prestoTask->resultRequests.insert({destination, std::move(request)});
      return std::move(future)
          .via(httpSrvCpuExecutor_)
          .onTimeout(std::chrono::microseconds(maxWaitMicros), timeoutFn);
    }
  } catch (const velox::VeloxException& e) {
    return folly::makeSemiFuture<std::unique_ptr<Result>>(e).via(
        httpSrvCpuExecutor_);
  } catch (const std::exception& e) {
    return folly::makeSemiFuture<std::unique_ptr<Result>>(e).via(
        httpSrvCpuExecutor_);
  }
}

folly::Future<std::unique_ptr<protocol::TaskStatus>> TaskManager::getTaskStatus(
    const TaskId& taskId,
    std::optional<protocol::TaskState> currentState,
    std::optional<protocol::Duration> maxWait,
    std::shared_ptr<http::CallbackRequestHandlerState> state) {
  auto [promise, future] =
      folly::makePromiseContract<std::unique_ptr<protocol::TaskStatus>>();

  auto prestoTask = findOrCreateTask(taskId);

  if (!currentState || !maxWait) {
    // Return task's status immediately without waiting.
    prestoTask->updateCoordinatorHeartbeat();
    return std::make_unique<protocol::TaskStatus>(prestoTask->updateStatus());
  }

  uint64_t maxWaitMicros =
      std::max(1.0, maxWait.value().getValue(protocol::TimeUnit::MICROSECONDS));

  protocol::TaskStatus status;
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateCoordinatorHeartbeatLocked();
    if (!prestoTask->task) {
      auto promiseHolder = std::make_shared<
          PromiseHolder<std::unique_ptr<protocol::TaskStatus>>>(
          std::move(promise));

      keepPromiseAlive(promiseHolder, state);
      prestoTask->statusRequest = folly::to_weak_ptr(promiseHolder);
      return std::move(future)
          .via(httpSrvCpuExecutor_)
          .onTimeout(std::chrono::microseconds(maxWaitMicros), [prestoTask]() {
            return std::make_unique<protocol::TaskStatus>(
                prestoTask->updateStatus());
          });
    }

    status = prestoTask->updateStatusLocked();
  }

  if (currentState.value() != status.state || isFinalState(status.state)) {
    promise.setValue(std::make_unique<protocol::TaskStatus>(status));
    return std::move(future).via(httpSrvCpuExecutor_);
  }

  auto promiseHolder =
      std::make_shared<PromiseHolder<std::unique_ptr<protocol::TaskStatus>>>(
          std::move(promise));

  prestoTask->task->stateChangeFuture(maxWaitMicros)
      .via(httpSrvCpuExecutor_)
      .thenValue([promiseHolder, prestoTask](auto&& /*done*/) {
        promiseHolder->promise.setValue(
            std::make_unique<protocol::TaskStatus>(prestoTask->updateStatus()));
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [promiseHolder, prestoTask](std::exception const& /*e*/) {
            // We come here in the case of maxWait elapsed.
            promiseHolder->promise.setValue(
                std::make_unique<protocol::TaskStatus>(
                    prestoTask->updateStatus()));
          });
  return std::move(future).via(httpSrvCpuExecutor_);
}

void TaskManager::removeRemoteSource(
    const TaskId& taskId,
    const TaskId& remoteSourceTaskId) {
  VELOX_NYI();
}

std::shared_ptr<PrestoTask> TaskManager::findOrCreateTask(
    const TaskId& taskId,
    long startProcessCpuTime) {
  std::shared_ptr<PrestoTask> prestoTask;
  taskMap_.withRLock([&](const auto& taskMap) {
    auto it = taskMap.find(taskId);
    if (it != taskMap.end()) {
      prestoTask = it->second;
    }
  });

  if (prestoTask != nullptr) {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateHeartbeatLocked();
    ++prestoTask->info.taskStatus.version;
    return prestoTask;
  }

  prestoTask =
      std::make_shared<PrestoTask>(taskId, nodeId_, startProcessCpuTime);
  prestoTask->info.stats.createTimeInMillis = velox::getCurrentTimeMs();
  prestoTask->info.needsPlan = true;

  struct UuidSplit {
    int64_t lo;
    int64_t hi;
  };

  union UuidParse {
    boost::uuids::uuid uuid;
    UuidSplit split;
  };

  UuidParse uuid = {boost::uuids::random_generator()()};

  prestoTask->info.taskStatus.taskInstanceIdLeastSignificantBits =
      uuid.split.lo;
  prestoTask->info.taskStatus.taskInstanceIdMostSignificantBits = uuid.split.hi;

  prestoTask->info.taskStatus.state = protocol::TaskState::RUNNING;
  prestoTask->info.taskStatus.self =
      fmt::format("{}/v1/task/{}", baseUri_, taskId);
  prestoTask->updateHeartbeatLocked();
  ++prestoTask->info.taskStatus.version;

  taskMap_.withWLock([&](auto& taskMap) {
    if (taskMap.count(taskId) == 0) {
      taskMap[taskId] = prestoTask;
    } else {
      prestoTask = taskMap[taskId];
    }
  });
  return prestoTask;
}

std::string TaskManager::toString() const {
  std::stringstream out;
  auto taskMap = taskMap_.rlock();
  for (const auto& pair : *taskMap) {
    if (pair.second->task) {
      out << pair.second->task->toString() << std::endl;
    } else {
      out << exec::Task::shortId(pair.first) << " no task (" << pair.first
          << ")" << std::endl;
    }
  }
  out << bufferManager_->toString();
  return out.str();
}

velox::exec::Task::DriverCounts TaskManager::getDriverCounts() {
  const auto taskMap = *taskMap_.rlock();
  velox::exec::Task::DriverCounts ret;
  for (const auto& pair : taskMap) {
    if (pair.second->task != nullptr) {
      auto counts = pair.second->task->driverCounts();
      // TODO (spershin): Move add logic to velox::exec::Task::DriverCounts.
      ret.numQueuedDrivers += counts.numQueuedDrivers;
      ret.numOnThreadDrivers += counts.numOnThreadDrivers;
      ret.numSuspendedDrivers += counts.numSuspendedDrivers;
      for (const auto& it : counts.numBlockedDrivers) {
        ret.numBlockedDrivers[it.first] += it.second;
      }
    }
  }
  numQueuedDrivers_ = ret.numQueuedDrivers;
  return ret;
}

bool TaskManager::getStuckOpCalls(
    std::vector<std::string>& deadlockTasks,
    std::vector<velox::exec::Task::OpCallInfo>& stuckOpCalls) const {
  const auto thresholdDurationMs =
      SystemConfig::instance()->driverStuckOperatorThresholdMs();
  const auto thresholdCancelMs =
      SystemConfig::instance()
          ->driverCancelTasksWithStuckOperatorsThresholdMs();
  stuckOpCalls.clear();

  const std::chrono::milliseconds lockTimeoutMs(thresholdDurationMs);
  auto taskMap = taskMap_.rlock(lockTimeoutMs);
  if (!taskMap) {
    return false;
  }

  for (const auto& [id, prestoTask] : *taskMap) {
    if (prestoTask->task != nullptr) {
      const auto numPrevStuckOps = stuckOpCalls.size();
      if (!prestoTask->task->getLongRunningOpCalls(
              lockTimeoutMs, thresholdDurationMs, stuckOpCalls)) {
        deadlockTasks.push_back(id);
        continue;
      }
      // See if we need to cancel the Task - it should be running, the cancel
      // threshold should be valid and it should have at least one stuck driver
      // that was stuck for enough time.
      if (numPrevStuckOps < stuckOpCalls.size() && thresholdCancelMs != 0 &&
          prestoTask->task->isRunning()) {
        for (auto it = stuckOpCalls.begin() + numPrevStuckOps;
             it != stuckOpCalls.end();
             ++it) {
          if (it->durationMs >= thresholdCancelMs) {
            std::stringstream ss;
            ss << "Task " << id
               << " cancelled due to stuck operator: tid=" << it->tid
               << " opCall=" << it->opCall
               << " duration= " << velox::succinctMillis(it->durationMs);
            const std::string msg = ss.str();
            LOG(ERROR) << msg;
            prestoTask->task->setError(msg);
            RECORD_METRIC_VALUE(kCounterNumCancelledTasksByStuckDriver, 1);
            break;
          }
        }
      }
    }
  }
  return true;
}

int32_t TaskManager::yieldTasks(
    int32_t numTargetThreadsToYield,
    int32_t timeSliceMicros) {
  const auto taskMap = taskMap_.rlock();
  int32_t numYields = 0;
  uint64_t now = getCurrentTimeMicro();
  for (const auto& pair : *taskMap) {
    if (pair.second->task != nullptr) {
      numYields += pair.second->task->yieldIfDue(now - timeSliceMicros);
      if (numYields >= numTargetThreadsToYield) {
        return numYields;
      }
    }
  }
  return numYields;
}

std::array<size_t, 6> TaskManager::getTaskNumbers(size_t& numTasks) const {
  std::array<size_t, 6> res{0};
  auto taskMap = taskMap_.rlock();
  numTasks = 0;
  for (const auto& pair : *taskMap) {
    if (pair.second->task != nullptr) {
      const auto prestoTaskState = pair.second->taskState();
      ++res[static_cast<int>(prestoTaskState)];
      ++numTasks;
    }
  }
  return res;
}

size_t TaskManager::numQueuedTasks() const {
  size_t num = 0;
  auto lockedTaskQueue = taskQueue_.rlock();
  for (const auto& entry : *lockedTaskQueue) {
    num += entry.size();
  }
  return num;
}

int64_t TaskManager::getBytesProcessed() const {
  const auto taskMap = *taskMap_.rlock();
  int64_t totalCount = 0;
  for (const auto& pair : taskMap) {
    totalCount += pair.second->info.stats.processedInputDataSizeInBytes;
  }
  return totalCount;
}

void TaskManager::shutdown() {
  size_t numTasks;
  auto taskNumbers = getTaskNumbers(numTasks);
  size_t seconds = 0;
  while (taskNumbers[static_cast<int>(velox::exec::TaskState::kRunning)] > 0) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Waited (" << seconds
        << " seconds so far) for 'Running' tasks to complete. " << numTasks
        << " tasks left: " << PrestoTask::taskStatesToString(taskNumbers);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    cancelAbandonedTasks();
    taskNumbers = getTaskNumbers(numTasks);
    ++seconds;
  }

  taskMap_.withRLock([&](const TaskMap& taskMap) {
    for (auto it = taskMap.begin(); it != taskMap.end(); ++it) {
      const auto veloxTaskRefCount = it->second->task.use_count();
      if (veloxTaskRefCount > 1) {
        VELOX_CHECK_NOT_NULL(it->second->task);
        PRESTO_SHUTDOWN_LOG(WARNING)
            << "Velox task has pending reference on destruction: "
            << it->second->task->taskId();
        continue;
      }
      const auto prestoTaskRefCount = it->second.use_count();
      if (prestoTaskRefCount > 1) {
        PRESTO_SHUTDOWN_LOG(WARNING)
            << "Presto task has pending reference on destruction: "
            << it->second->id.toString();
      }
    }
  });
}

} // namespace facebook::presto
