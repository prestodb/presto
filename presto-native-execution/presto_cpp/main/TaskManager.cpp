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
#include "velox/exec/Exchange.h"

using namespace facebook::velox;

using facebook::presto::protocol::TaskId;
using facebook::presto::protocol::TaskInfo;

namespace facebook::presto {

// Unlimited concurrent lifespans is translated to this limit.
constexpr uint32_t kMaxConcurrentLifespans{16};

namespace {
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
  const auto taskSpillDirPath = TaskManager::buildTaskSpillDirectoryPath(
      baseSpillDirectory,
      nodeConfig->nodeInternalAddress(),
      nodeConfig->nodeId(),
      execTask.queryCtx()->queryId(),
      execTask.taskId(),
      includeNodeInSpillPath);
  execTask.setSpillDirectory(taskSpillDirPath, /*alreadyCreated=*/false);
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
          int64_t sequence) mutable {
        bool complete = pages.empty();
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
            nextSequence++;
          } else {
            complete = true;
          }
        }

        VLOG(1) << "Task " << taskId << ", buffer " << bufferId << ", sequence "
                << sequence << " Results size: " << bytes
                << ", page count: " << pages.size()
                << ", complete: " << std::boolalpha << complete;

        auto result = std::make_unique<Result>();
        result->sequence = sequence;
        result->nextSequence = nextSequence;
        result->complete = complete;
        result->data = std::move(iobuf);

        promiseHolder->promise.setValue(std::move(result));

        RECORD_METRIC_VALUE(
            kCounterPartitionedOutputBufferGetDataLatencyMs,
            getCurrentTimeMs() - startMs);
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
  const std::string taskId;
  const std::string taskInfo;

  explicit ZombieTaskStats(const std::shared_ptr<exec::Task>& task)
      : taskId(task->taskId()), taskInfo(task->toString()) {}

  std::string toString() const {
    return SystemConfig::instance()->logZombieTaskInfo() ? taskInfo : taskId;
  }
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

  void updateCounts(std::shared_ptr<exec::Task>& task) {
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
      tasks.emplace_back(task);
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
      LOG(ERROR) << "Zombie Task[" << i + 1 << "/" << tasks.size()
                 << "]: " << tasks[i].toString() << std::endl;
    }
  }
};
} // namespace

TaskManager::TaskManager(
    folly::Executor* driverExecutor,
    folly::Executor* httpSrvCpuExecutor,
    folly::Executor* spillerExecutor)
    : bufferManager_(velox::exec::OutputBufferManager::getInstance().lock()),
      queryContextManager_(std::make_unique<QueryContextManager>(
          driverExecutor,
          spillerExecutor)),
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
    long startProcessCpuTime) {
  auto prestoTask = findOrCreateTask(taskId, startProcessCpuTime);
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateHeartbeatLocked();
    if (prestoTask->error == nullptr) {
      prestoTask->error = exception;
    }
    prestoTask->info.needsPlan = false;
  }

  auto info = prestoTask->updateInfo();
  return std::make_unique<TaskInfo>(info);
}

/*static*/ std::string TaskManager::buildTaskSpillDirectoryPath(
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

  std::string path;
  folly::toAppend(fmt::format("{}/presto_native/", baseSpillPath), &path);
  if (includeNodeInSpillPath) {
    folly::toAppend(fmt::format("{}_{}/", nodeIp, nodeId), &path);
  }
  folly::toAppend(fmt::format("{}/{}/{}/", dateString, queryId, taskId), &path);
  return path;
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
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    long startProcessCpuTime) {
  return createOrUpdateTask(
      taskId,
      planFragment,
      updateRequest.sources,
      updateRequest.outputIds,
      queryCtx,
      startProcessCpuTime);
}

std::unique_ptr<protocol::TaskInfo> TaskManager::createOrUpdateBatchTask(
    const protocol::TaskId& taskId,
    const protocol::BatchTaskUpdateRequest& batchUpdateRequest,
    const velox::core::PlanFragment& planFragment,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    long startProcessCpuTime) {
  auto updateRequest = batchUpdateRequest.taskUpdateRequest;

  checkSplitsForBatchTask(planFragment.planNode, updateRequest.sources);

  return createOrUpdateTask(
      taskId,
      planFragment,
      updateRequest.sources,
      updateRequest.outputIds,
      std::move(queryCtx),
      startProcessCpuTime);
}

std::unique_ptr<TaskInfo> TaskManager::createOrUpdateTask(
    const TaskId& taskId,
    const velox::core::PlanFragment& planFragment,
    const std::vector<protocol::TaskSource>& sources,
    const protocol::OutputBuffers& outputBuffers,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    long startProcessCpuTime) {
  std::shared_ptr<exec::Task> execTask;
  bool startTask = false;
  auto prestoTask = findOrCreateTask(taskId, startProcessCpuTime);
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    if (not prestoTask->task && planFragment.planNode) {
      // If the task is aborted, no need to do anything else.
      // This takes care of DELETE task message coming before CREATE task.
      if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
        return std::make_unique<TaskInfo>(prestoTask->updateInfoLocked());
      }

      // Uses a temp variable to store the created velox task to destroy it
      // under presto task lock if spill directory setup fails. Otherwise, the
      // concurrent task creation retry from the coordinator might see the
      // unexpected state in presto task left by the previously failed velox
      // task which hasn't been destroyed yet, such as the task pool in query's
      // root memory pool.
      auto newExecTask = exec::Task::create(
          taskId, planFragment, prestoTask->id.id(), std::move(queryCtx));
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

  // Create or update task can be called concurrently for the same task.
  // We need to lock here for allow only one to be executed at a time.
  // This is especially important for adding splits to the task.
  std::lock_guard<std::mutex> l(prestoTask->mutex);

  if (startTask) {
    const uint32_t maxDrivers =
        execTask->queryCtx()->queryConfig().get<int32_t>(
            kMaxDriversPerTask.data(),
            SystemConfig::instance()->maxDriversPerTask());
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
      LOG(INFO) << "Starting task " << taskId << " with " << maxDrivers
                << " max drivers and " << concurrentLifespans
                << " concurrent lifespans (grouped execution).";
    } else {
      LOG(INFO) << "Starting task " << taskId << " with " << maxDrivers
                << " max drivers.";
    }
    execTask->start(maxDrivers, concurrentLifespans);

    prestoTask->taskStarted = true;
    resultRequests = std::move(prestoTask->resultRequests);
    statusRequest = prestoTask->statusRequest;
    infoRequest = prestoTask->infoRequest;
  }

  getDataForResultRequests(resultRequests);

  if (outputBuffers.type != protocol::BufferType::PARTITIONED &&
      !execTask->updateOutputBuffers(
          outputBuffers.buffers.size(), outputBuffers.noMoreBufferIds)) {
    LOG(WARNING) << "Failed to update output buffers for task: " << taskId;
  }

  for (const auto& source : sources) {
    // Add all splits from the source to the task.
    VLOG(1) << "Adding " << source.splits.size() << " splits to " << taskId
            << " for node " << source.planNodeId;
    // Keep track of the max sequence for this batch of splits.
    long maxSplitSequenceId{-1};
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
        LOG(INFO) << "No more splits for group " << lifespan.groupid << " for "
                  << taskId << " for node " << source.planNodeId;
        execTask->noMoreSplitsForGroup(source.planNodeId, lifespan.groupid);
      }
    }

    if (source.noMoreSplits) {
      LOG(INFO) << "No more splits for " << taskId << " for node "
                << source.planNodeId;
      execTask->noMoreSplits(source.planNodeId);
    }
  }

  // 'prestoTask' will exist by virtue of shared_ptr but may for example have
  // been aborted.
  auto info = prestoTask->updateInfoLocked(); // Presto task is locked above.
  if (auto promiseHolder = infoRequest.lock()) {
    promiseHolder->promise.setValue(std::make_unique<protocol::TaskInfo>(info));
  }
  if (auto promiseHolder = statusRequest.lock()) {
    promiseHolder->promise.setValue(
        std::make_unique<protocol::TaskStatus>(info.taskStatus));
  }
  return std::make_unique<TaskInfo>(info);
}

std::unique_ptr<TaskInfo> TaskManager::deleteTask(
    const TaskId& taskId,
    bool /*abort*/) {
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
  auto execTask = prestoTask->task;
  if (execTask) {
    auto state = execTask->state();
    if (state == exec::kRunning) {
      execTask->requestAbort();
    }
    prestoTask->info.stats.endTime =
        util::toISOTimestamp(velox::getCurrentTimeMs());
    prestoTask->updateInfoLocked();
  } else {
    // If task is not found than we observe DELETE message coming before
    // CREATE. In that case we create the task with ABORTED state, so we know
    // we don't need to do anything on CREATE message and can clean up the
    // cancelled task later.
    prestoTask->info.taskStatus.state = protocol::TaskState::ABORTED;
    return std::make_unique<TaskInfo>(prestoTask->info);
  }

  // Do not erase the finished/aborted tasks, because someone might still want
  // to get some results from them. Instead we run a periodic task to clean up
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
            zombiePrestoTaskCounts.updateCounts(task);
          }
        }
        if (taskRefCount > 1) {
          ++zombieVeloxTaskCounts.numTotal;
          zombieVeloxTaskCounts.updateCounts(task);
        }
      } else {
        taskIdsToClean.emplace(id);
      }
    }
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
    promise.setValue(
        std::make_unique<protocol::TaskInfo>(prestoTask->updateInfo()));
    return std::move(future).via(httpSrvCpuExecutor_);
  }

  uint64_t maxWaitMicros =
      std::max(1.0, maxWait.value().getValue(protocol::TimeUnit::MICROSECONDS));
  protocol::TaskInfo info;
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateHeartbeatLocked();
    if (!prestoTask->task) {
      auto promiseHolder =
          std::make_shared<PromiseHolder<std::unique_ptr<protocol::TaskInfo>>>(
              std::move(promise));
      keepPromiseAlive(promiseHolder, state);
      prestoTask->infoRequest = folly::to_weak_ptr(promiseHolder);

      return std::move(future)
          .via(httpSrvCpuExecutor_)
          .onTimeout(std::chrono::microseconds(maxWaitMicros), [prestoTask]() {
            return std::make_unique<protocol::TaskInfo>(
                prestoTask->updateInfo());
          });
    }
    info = prestoTask->updateInfoLocked();
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
      .thenValue([promiseHolder, prestoTask](auto&& /*done*/) {
        promiseHolder->promise.setValue(
            std::make_unique<protocol::TaskInfo>(prestoTask->updateInfo()));
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [promiseHolder, prestoTask](const std::exception& /*e*/) {
            // We come here in the case of maxWait elapsed.
            promiseHolder->promise.setValue(
                std::make_unique<protocol::TaskInfo>(prestoTask->updateInfo()));
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
  auto [promise, future] =
      folly::makePromiseContract<std::unique_ptr<Result>>();

  auto promiseHolder = std::make_shared<PromiseHolder<std::unique_ptr<Result>>>(
      std::move(promise));

  // Error in fetching results or creating a task may prevent the promise from
  // being fulfilled leading to a BrokenPromise exception on promise
  // destruction. To avoid the BrokenPromise exception, fulfill the promise
  // with incomplete empty pages.
  promiseHolder->atDestruction(
      [token](folly::Promise<std::unique_ptr<Result>> promise) {
        promise.setValue(createEmptyResult(token));
      });

  auto timeoutFn = [this, token]() { return createEmptyResult(token); };

  try {
    auto prestoTask = findOrCreateTask(taskId);

    // If the task is aborted or failed, then return an error.
    if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
      LOG(WARNING) << "Calling getResult() on a aborted task: " << taskId;
      promiseHolder->promise.setValue(createEmptyResult(token));
      return std::move(future).via(httpSrvCpuExecutor_);
    }
    if (prestoTask->error != nullptr) {
      LOG(WARNING) << "Calling getResult() on a failed PrestoTask: " << taskId;
      promiseHolder->promise.setValue(createEmptyResult(token));
      return std::move(future).via(httpSrvCpuExecutor_);
    }

    for (;;) {
      if (prestoTask->taskStarted) {
        // If the task has finished, then send completion result.
        if (prestoTask->task->state() == exec::kFinished) {
          promiseHolder->promise.setValue(createCompleteResult(token));
          return std::move(future).via(httpSrvCpuExecutor_);
        }
        // If task is not running let the request timeout. The task may have
        // failed at creation time and the coordinator hasn't yet caught up.
        if (prestoTask->task->state() == exec::kRunning) {
          getData(
              promiseHolder,
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

      auto request = std::make_unique<ResultRequest>();
      request->promise = folly::to_weak_ptr(promiseHolder);
      request->taskId = taskId;
      request->bufferId = destination;
      request->token = token;
      request->maxSize = maxSize;
      prestoTask->resultRequests.insert({destination, std::move(request)});
      return std::move(future)
          .via(httpSrvCpuExecutor_)
          .onTimeout(std::chrono::microseconds(maxWaitMicros), timeoutFn);
    }
  } catch (const velox::VeloxException& e) {
    promiseHolder->promise.setException(e);
    return std::move(future).via(httpSrvCpuExecutor_);
  } catch (const std::exception& e) {
    promiseHolder->promise.setException(e);
    return std::move(future).via(httpSrvCpuExecutor_);
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
    return std::make_unique<protocol::TaskStatus>(prestoTask->updateStatus());
  }

  uint64_t maxWaitMicros =
      std::max(1.0, maxWait.value().getValue(protocol::TimeUnit::MICROSECONDS));

  protocol::TaskStatus status;
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
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
  prestoTask->info.stats.createTime =
      util::toISOTimestamp(velox::getCurrentTimeMs());
  prestoTask->info.needsPlan = true;
  prestoTask->info.metadataUpdates.connectorId = "unused";

  struct UuidSplit {
    int64_t lo;
    int64_t hi;
  };

  union UuidParse {
    boost::uuids::uuid uuid;
    UuidSplit split;
  };

  UuidParse uuid;
  uuid.uuid = boost::uuids::random_generator()();

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

DriverCountStats TaskManager::getDriverCountStats() const {
  auto taskMap = taskMap_.rlock();
  DriverCountStats driverCountStats;
  for (const auto& pair : *taskMap) {
    if (pair.second->task != nullptr) {
      driverCountStats.numRunningDrivers +=
          pair.second->task->numRunningDrivers();
    }
  }
  driverCountStats.numBlockedDrivers =
      velox::exec::BlockingState::numBlockedDrivers();
  return driverCountStats;
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

std::array<size_t, 5> TaskManager::getTaskNumbers(size_t& numTasks) const {
  std::array<size_t, 5> res{0};
  auto taskMap = taskMap_.rlock();
  numTasks = 0;
  for (const auto& pair : *taskMap) {
    if (pair.second->task != nullptr) {
      ++res[pair.second->task->state()];
      ++numTasks;
    }
  }
  return res;
}

void TaskManager::shutdown() {
  size_t numTasks;
  auto taskNumbers = getTaskNumbers(numTasks);
  size_t seconds = 0;
  while (taskNumbers[velox::exec::TaskState::kRunning] > 0) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Waited (" << seconds
        << " seconds so far) for 'Running' tasks to complete. " << numTasks
        << " tasks left: " << PrestoTask::taskNumbersToString(taskNumbers);
    std::this_thread::sleep_for(std::chrono::seconds(1));
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
