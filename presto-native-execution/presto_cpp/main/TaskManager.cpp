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
#include "velox/type/tz/TimeZoneMap.h"

DEFINE_int32(
    old_task_ms,
    60'000, // 1 minute, by default.
    "Time (ms) since the task execution ended, when task is considered old.");

using namespace facebook::velox;

using facebook::presto::protocol::TaskId;
using facebook::presto::protocol::TaskInfo;

namespace facebook::presto {

// Unlimited concurrent lifespans is translated to this limit.
constexpr uint32_t kMaxConcurrentLifespans{16};

namespace {

// If spilling is enabled and the given Task can spill, then this helper
// generates the spilling directory path for the Task, creates that directory in
// the file system and sets the path to it to the Task.
static void maybeSetupTaskSpillDirectory(
    const core::PlanFragment& planFragment,
    exec::Task& execTask) {
  const auto baseSpillPath = SystemConfig::instance()->spillerSpillPath();
  if (!baseSpillPath.empty() &&
      planFragment.canSpill(execTask.queryCtx()->queryConfig())) {
    const auto taskSpillDirPath = TaskManager::buildTaskSpillDirectoryPath(
        baseSpillPath, execTask.queryCtx()->queryId(), execTask.taskId());
    execTask.setSpillDirectory(taskSpillDirPath);
    // Create folder for the task spilling.
    auto fileSystem =
        velox::filesystems::getFileSystem(taskSpillDirPath, nullptr);
    VELOX_CHECK_NOT_NULL(fileSystem, "File System is null!");
    fileSystem->mkdir(taskSpillDirPath);
  }
}

bool isFinalState(protocol::TaskState state) {
  switch (state) {
    case protocol::TaskState::FINISHED:
    case protocol::TaskState::FAILED:
    case protocol::TaskState::ABORTED:
    case protocol::TaskState::CANCELED:
      return true;
    default:
      return false;
  }
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
} // namespace

TaskManager::TaskManager()
    : bufferManager_(
          velox::exec::PartitionedOutputBufferManager::getInstance().lock()) {
  VELOX_CHECK_NOT_NULL(
      bufferManager_, "invalid PartitionedOutputBufferManager");
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

namespace {

std::unique_ptr<Result> createTimeOutResult(long token) {
  auto result = std::make_unique<Result>();
  result->sequence = result->nextSequence = token;
  result->data = folly::IOBuf::create(0);
  result->complete = false;
  return result;
}

void getData(
    PromiseHolderPtr<std::unique_ptr<Result>> promiseHolder,
    const TaskId& taskId,
    long bufferId,
    long token,
    protocol::DataSize maxSize,
    exec::PartitionedOutputBufferManager& bufferManager) {
  if (promiseHolder == nullptr) {
    // promise/future is expired.
    return;
  }

  int64_t startMs = getCurrentTimeMs();
  auto bufferFound = bufferManager.getData(
      taskId,
      bufferId,
      maxSize.getValue(protocol::DataUnit::BYTE),
      token,
      [taskId = taskId, bufferId = bufferId, promiseHolder, startMs](
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

        REPORT_ADD_STAT_VALUE(
            kCounterPartitionedOutputBufferGetDataLatencyMs,
            getCurrentTimeMs() - startMs);
      });

  if (!bufferFound) {
    // Buffer was erased for current TaskId.
    VLOG(1) << "Task " << taskId << ", buffer " << bufferId << ", sequence "
            << token << ", buffer not found.";
    promiseHolder->promise.setValue(std::move(createTimeOutResult(token)));
  }
}
} // namespace

std::unique_ptr<TaskInfo> TaskManager::createOrUpdateErrorTask(
    const TaskId& taskId,
    const std::exception_ptr& exception) {
  auto prestoTask = findOrCreateTask(taskId);
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
    const std::string& queryId,
    const protocol::TaskId& taskId) {
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

  std::stringstream ss;
  ss << baseSpillPath << "/";
  ss << dateString;
  ss << "/";
  // TODO(spershin): We will like need to use identity (from config?) in the
  // long run. Use 'presto_native' for now.
  ss << "presto_native/" << queryId << "/" << taskId << "/";
  return ss.str();
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

namespace {
std::unordered_map<std::string, std::string> toConfigs(
    const protocol::SessionRepresentation& session) {
  // Use base velox query config as the starting point and add Presto session
  // properties on top of it.
  auto configs = BaseVeloxQueryConfig::instance()->values();
  for (const auto& it : session.systemProperties) {
    configs[it.first] = it.second;
  }

  // If there's a timeZoneKey, convert to timezone name and add to the
  // configs. Throws if timeZoneKey can't be resolved.
  if (session.timeZoneKey != 0) {
    configs.emplace(
        velox::core::QueryConfig::kSessionTimezone,
        velox::util::getTimeZoneName(session.timeZoneKey));
  }
  return configs;
}

std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
toConnectorConfigs(const protocol::SessionRepresentation& session) {
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorConfigs;
  for (const auto& entry : session.catalogProperties) {
    connectorConfigs.insert(
        {entry.first,
         std::unordered_map<std::string, std::string>(
             entry.second.begin(), entry.second.end())});
  }

  return connectorConfigs;
}

/// Presto-on-Spark is expected to specify all splits at once along with
/// no-more-splits flag. Verify that all plan nodes that require splits
/// have received splits and no-more-splits flag. This check helps
/// prevent hard-to-debug query hangs caused by Velox Task waiting for
/// splits that never arrive.
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
} // namespace

std::unique_ptr<protocol::TaskInfo> TaskManager::createOrUpdateTask(
    const protocol::TaskId& taskId,
    const protocol::TaskUpdateRequest& updateRequest,
    const velox::core::PlanFragment& planFragment) {
  const auto& session = updateRequest.session;

  return createOrUpdateTask(
      taskId,
      planFragment,
      updateRequest.sources,
      updateRequest.outputIds,
      toConfigs(session),
      toConnectorConfigs(session));
}

std::unique_ptr<protocol::TaskInfo> TaskManager::createOrUpdateBatchTask(
    const protocol::TaskId& taskId,
    const protocol::BatchTaskUpdateRequest& batchUpdateRequest,
    const velox::core::PlanFragment& planFragment) {
  auto updateRequest = batchUpdateRequest.taskUpdateRequest;

  checkSplitsForBatchTask(planFragment.planNode, updateRequest.sources);

  const auto& session = updateRequest.session;

  return createOrUpdateTask(
      taskId,
      planFragment,
      updateRequest.sources,
      updateRequest.outputIds,
      toConfigs(session),
      toConnectorConfigs(session));
}

std::unique_ptr<TaskInfo> TaskManager::createOrUpdateTask(
    const TaskId& taskId,
    const velox::core::PlanFragment& planFragment,
    const std::vector<protocol::TaskSource>& sources,
    const protocol::OutputBuffers& outputBuffers,
    std::unordered_map<std::string, std::string>&& configStrings,
    std::unordered_map<
        std::string,
        std::unordered_map<std::string, std::string>>&&
        connectorConfigStrings) {
  std::shared_ptr<exec::Task> execTask;
  bool startTask = false;
  auto prestoTask = findOrCreateTask(taskId);
  {
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    if (not prestoTask->task && planFragment.planNode) {
      // If the task is aborted, no need to do anything else.
      // This takes care of DELETE task message coming before CREATE task.
      if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
        return std::make_unique<TaskInfo>(prestoTask->updateInfoLocked());
      }

      auto queryCtx = queryContextManager_.findOrCreateQueryCtx(
          taskId, std::move(configStrings), std::move(connectorConfigStrings));

      execTask = exec::Task::create(
          taskId, planFragment, prestoTask->id.id(), std::move(queryCtx));
      maybeSetupTaskSpillDirectory(planFragment, *execTask);

      prestoTask->task = execTask;
      prestoTask->info.needsPlan = false;
      startTask = true;
    } else {
      execTask = prestoTask->task;
    }
  }
  // outside of prestoTask->mutex.
  VELOX_CHECK(
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
    exec::Task::start(execTask, maxDrivers, concurrentLifespans);

    prestoTask->taskStarted = true;
    resultRequests = std::move(prestoTask->resultRequests);
    statusRequest = prestoTask->statusRequest;
    infoRequest = prestoTask->infoRequest;
  }

  getDataForResultRequests(resultRequests);

  if (outputBuffers.type == protocol::BufferType::BROADCAST &&
      !execTask->updateBroadcastOutputBuffers(
          outputBuffers.buffers.size(), outputBuffers.noMoreBufferIds)) {
    LOG(INFO) << "Failed to update broadcast buffers for task: " << taskId;
  }

  for (const auto& source : sources) {
    // Add all splits from the source to the task.
    LOG(INFO) << "Adding " << source.splits.size() << " splits to " << taskId
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
  auto taskMap = taskMap_.wlock();
  auto it = taskMap->find(taskId);
  if (it == taskMap->cend()) {
    VLOG(1) << "Task not found for delete: " << taskId;
    // If task is not found than we observe DELETE message coming before CREATE.
    // In that case we create the task with ABORTED state, so we know we don't
    // need to do anything on CREATE message and can clean up the cancelled task
    // later.
    auto prestoTask = findOrCreateTaskLocked(*taskMap, taskId);
    prestoTask->info.taskStatus.state = protocol::TaskState::ABORTED;
    return std::make_unique<TaskInfo>(prestoTask->info);
  }

  auto prestoTask = it->second;

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
  }

  // Do not erase the finished/aborted tasks, because someone might still want
  // to get some results from them. Instead we run a periodic task to clean up
  // the old finished/aborted tasks.
  if (prestoTask->info.taskStatus.state == protocol::TaskState::RUNNING) {
    prestoTask->info.taskStatus.state = protocol::TaskState::ABORTED;
  }

  return std::make_unique<TaskInfo>(prestoTask->info);
}

namespace {

// Helper structure holding stats for 'zombie' tasks.
struct ZombieTaskCounts {
  size_t numRunning{0};
  size_t numFinished{0};
  size_t numCanceled{0};
  size_t numAborted{0};
  size_t numFailed{0};
  size_t numTotal{0};

  const size_t numSampleTaskId{20};
  std::vector<std::string> taskIds;

  ZombieTaskCounts() {
    taskIds.reserve(numSampleTaskId);
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
    if (taskIds.size() < numSampleTaskId) {
      taskIds.emplace_back(task->taskId());
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
               << numSampleTaskId << " IDs): {" << folly::join(',', taskIds)
               << "}";
  }
};

}; // namespace

size_t TaskManager::cleanOldTasks() {
  const auto startTimeMs = getCurrentTimeMs();

  // We copy task map locally to avoid locking task map for a potentially long
  // time. We also lock for 'read'.
  TaskMap taskMap;
  folly::F14FastSet<protocol::TaskId> taskIdsToClean;
  { taskMap = *(taskMap_.rlock()); }

  ZombieTaskCounts zombieTaskCounts;
  ZombieTaskCounts zombiePrestoTaskCounts;
  for (auto it = taskMap.begin(); it != taskMap.end(); ++it) {
    bool eraseTask{false};
    if (it->second->task != nullptr) {
      if (it->second->task->state() != exec::TaskState::kRunning) {
        if (it->second->task->timeSinceEndMs() >= FLAGS_old_task_ms) {
          // Not running and old.
          eraseTask = true;
        }
      }
    } else {
      // Use heartbeat to determine the task's age.
      if (it->second->timeSinceLastHeartbeatMs() >= FLAGS_old_task_ms) {
        eraseTask = true;
      }
    }

    // We assume 'not erase' is the 'most common' case.
    if (not eraseTask) {
      continue;
    }

    auto prestoTaskRefCount = it->second.use_count();
    auto taskRefCount = it->second->task.use_count();

    // Do not remove 'zombie' tasks (with outstanding references) from the map.
    // We use it to track the number of tasks.
    // Note, since we copied the task map, presto tasks should have an extra
    // reference (2 from two maps).
    if (prestoTaskRefCount > 2 || taskRefCount > 1) {
      auto& task = it->second->task;
      if (prestoTaskRefCount > 2) {
        ++zombiePrestoTaskCounts.numTotal;
        if (task != nullptr) {
          zombiePrestoTaskCounts.updateCounts(task);
        }
      }
      if (taskRefCount > 1) {
        ++zombieTaskCounts.numTotal;
        zombieTaskCounts.updateCounts(task);
      }
    } else {
      taskIdsToClean.emplace(it->first);
    }
  }

  const auto elapsedMs = (getCurrentTimeMs() - startTimeMs);
  if (not taskIdsToClean.empty()) {
    {
      // Remove tasks from the task map. We briefly lock for write here.
      auto writableTaskMap = taskMap_.wlock();
      for (const auto& taskId : taskIdsToClean) {
        writableTaskMap->erase(taskId);
      }
    }
    LOG(INFO) << "cleanOldTasks: Cleaned " << taskIdsToClean.size()
              << " old task(s) in " << elapsedMs << "ms";
  } else if (elapsedMs > 1000) {
    // If we took more than 1 second to run this, something might be wrong.
    LOG(INFO) << "cleanOldTasks: Didn't clean any old task(s). Took "
              << elapsedMs << "ms";
  }

  if (zombieTaskCounts.numTotal > 0) {
    zombieTaskCounts.logZombieTaskStatus("Task");
  }
  if (zombiePrestoTaskCounts.numTotal > 0) {
    zombiePrestoTaskCounts.logZombieTaskStatus("PrestoTask");
  }
  REPORT_ADD_STAT_VALUE(kCounterNumZombieTasks, zombieTaskCounts.numTotal);
  REPORT_ADD_STAT_VALUE(
      kCounterNumZombiePrestoTasks, zombiePrestoTaskCounts.numTotal);
  return taskIdsToClean.size();
}

folly::Future<std::unique_ptr<protocol::TaskInfo>> TaskManager::getTaskInfo(
    const TaskId& taskId,
    bool summarize,
    std::optional<protocol::TaskState> currentState,
    std::optional<protocol::Duration> maxWait,
    std::shared_ptr<http::CallbackRequestHandlerState> state) {
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
  auto [promise, future] =
      folly::makePromiseContract<std::unique_ptr<protocol::TaskInfo>>();
  auto prestoTask = findOrCreateTask(taskId);
  if (!currentState || !maxWait) {
    // Return current TaskInfo without waiting.
    promise.setValue(
        std::make_unique<protocol::TaskInfo>(prestoTask->updateInfo()));
    return std::move(future).via(eventBase);
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

      return std::move(future).via(eventBase).onTimeout(
          std::chrono::microseconds(maxWaitMicros), [prestoTask]() {
            return std::make_unique<protocol::TaskInfo>(
                prestoTask->updateInfo());
          });
    }
    info = prestoTask->updateInfoLocked();
  }
  if (currentState.value() != info.taskStatus.state ||
      isFinalState(info.taskStatus.state)) {
    promise.setValue(std::make_unique<protocol::TaskInfo>(info));
    return std::move(future).via(eventBase);
  }

  auto promiseHolder =
      std::make_shared<PromiseHolder<std::unique_ptr<protocol::TaskInfo>>>(
          std::move(promise));

  prestoTask->task->stateChangeFuture(maxWaitMicros)
      .via(eventBase)
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
  return std::move(future).via(eventBase);
}

folly::Future<std::unique_ptr<Result>> TaskManager::getResults(
    const TaskId& taskId,
    long bufferId,
    long token,
    protocol::DataSize maxSize,
    protocol::Duration maxWait,
    std::shared_ptr<http::CallbackRequestHandlerState> state) {
  uint64_t maxWaitMicros =
      std::max(1.0, maxWait.getValue(protocol::TimeUnit::MICROSECONDS));
  VLOG(1) << "TaskManager::getResults " << taskId << ", " << bufferId << ", "
          << token;
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
        auto result = std::make_unique<Result>();
        result->sequence = token;
        result->nextSequence = token;
        result->complete = false;
        result->data = folly::IOBuf::copyBuffer("");
        promise.setValue(std::move(result));
      });

  auto timeoutFn = [this, token]() { return createTimeOutResult(token); };

  auto eventBase = folly::EventBaseManager::get()->getEventBase();
  try {
    auto prestoTask = findOrCreateTask(taskId);

    // If the task is aborted or failed, then return an error.
    if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
      VELOX_USER_FAIL("Calling getResult() on a aborted task: {}", taskId);
    }
    if (prestoTask->error != nullptr) {
      try {
        std::rethrow_exception(prestoTask->error);
      } catch (const VeloxException& e) {
        VELOX_USER_FAIL(
            "Calling getResult() on a failed PrestoTask: {}. PrestoTask failure reason: {}",
            taskId,
            e.what());
      } catch (const std::exception& e) {
        VELOX_USER_FAIL(
            "Calling getResult() on a failed PrestoTask: {}. PrestoTask failure reason: {}",
            taskId,
            e.what());
      } catch (...) {
        VELOX_USER_FAIL(
            "Calling getResult() on a failed PrestoTask: {}. PrestoTask failure reason: UNKNOWN",
            taskId);
      }
    }

    for (;;) {
      if (prestoTask->taskStarted) {
        // If task is not running let the request timeout. The task may have
        // failed at creation time and the coordinator hasn't yet caught up.
        if (prestoTask->task->state() == exec::kRunning) {
          getData(
              promiseHolder, taskId, bufferId, token, maxSize, *bufferManager_);
        }
        return std::move(future).via(eventBase).onTimeout(
            std::chrono::microseconds(maxWaitMicros), timeoutFn);
      }
      std::lock_guard<std::mutex> l(prestoTask->mutex);
      if (prestoTask->taskStarted) {
        continue;
      }
      // The task is not started yet, put the request
      VLOG(1) << "Queuing up result request for task " << taskId << ", buffer "
              << bufferId << ", sequence " << token;

      keepPromiseAlive(promiseHolder, state);

      auto request = std::make_unique<ResultRequest>();
      request->promise = folly::to_weak_ptr(promiseHolder);
      request->taskId = taskId;
      request->bufferId = bufferId;
      request->token = token;
      request->maxSize = maxSize;
      prestoTask->resultRequests.insert({bufferId, std::move(request)});
      return std::move(future).via(eventBase).onTimeout(
          std::chrono::microseconds(maxWaitMicros), timeoutFn);
    }
  } catch (const velox::VeloxException& e) {
    promiseHolder->promise.setException(e);
    return std::move(future).via(eventBase);
  } catch (const std::exception& e) {
    promiseHolder->promise.setException(e);
    return std::move(future).via(eventBase);
  }
}

folly::Future<std::unique_ptr<protocol::TaskStatus>> TaskManager::getTaskStatus(
    const TaskId& taskId,
    std::optional<protocol::TaskState> currentState,
    std::optional<protocol::Duration> maxWait,
    std::shared_ptr<http::CallbackRequestHandlerState> state) {
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
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
      return std::move(future).via(eventBase).onTimeout(
          std::chrono::microseconds(maxWaitMicros), [prestoTask]() {
            return std::make_unique<protocol::TaskStatus>(
                prestoTask->updateStatus());
          });
    }

    status = prestoTask->updateStatusLocked();
  }

  if (currentState.value() != status.state || isFinalState(status.state)) {
    promise.setValue(std::make_unique<protocol::TaskStatus>(status));
    return std::move(future).via(eventBase);
  }

  auto promiseHolder =
      std::make_shared<PromiseHolder<std::unique_ptr<protocol::TaskStatus>>>(
          std::move(promise));

  prestoTask->task->stateChangeFuture(maxWaitMicros)
      .via(eventBase)
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
  return std::move(future).via(eventBase);
}

void TaskManager::removeRemoteSource(
    const TaskId& taskId,
    const TaskId& remoteSourceTaskId) {}

std::shared_ptr<PrestoTask> TaskManager::findOrCreateTask(
    const TaskId& taskId) {
  auto taskMap = taskMap_.wlock();
  return findOrCreateTaskLocked(*taskMap, taskId);
}

std::shared_ptr<PrestoTask> TaskManager::findOrCreateTaskLocked(
    TaskMap& taskMap,
    const TaskId& taskId) {
  auto it = taskMap.find(taskId);
  if (it != taskMap.end()) {
    auto prestoTask = it->second;
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateHeartbeatLocked();
    ++prestoTask->info.taskStatus.version;
    return prestoTask;
  }

  auto prestoTask = std::make_shared<PrestoTask>(taskId, nodeId_);
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
  prestoTask->info.taskStatus.outputBufferUtilization = 1;
  prestoTask->updateHeartbeatLocked();
  ++prestoTask->info.taskStatus.version;

  taskMap[taskId] = prestoTask;
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

void TaskManager::waitForTasksToComplete() {
  size_t numTasks;
  auto taskNumbers = getTaskNumbers(numTasks);
  size_t seconds = 0;
  while (taskNumbers[velox::exec::TaskState::kRunning] > 0) {
    LOG(INFO) << "Waiting (" << seconds
              << " seconds so far) for 'Running' tasks to complete. "
              << numTasks << " tasks left: "
              << PrestoTask::taskNumbersToString(taskNumbers);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    taskNumbers = getTaskNumbers(numTasks);
    ++seconds;
  }
}

} // namespace facebook::presto
