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

#include "TaskManager.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <velox/core/PlanNode.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/types/PrestoToVeloxSplit.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Exchange.h"
#include "velox/serializers/PrestoSerializer.h"

DEFINE_int32(
    old_task_ms,
    60'000, // 1 minute, by default.
    "Time (ms) since the task execution ended, when task is considered old.");

using namespace facebook::velox;

using facebook::presto::protocol::TaskId;
using facebook::presto::protocol::TaskInfo;

namespace facebook::presto {

namespace {

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

TaskManager::TaskManager(
    std::unordered_map<std::string, std::string> properties,
    std::unordered_map<std::string, std::string> nodeProperties)
    : bufferManager_(
          velox::exec::PartitionedOutputBufferManager::getInstance().lock()),
      queryContextManager_(properties, nodeProperties),
      maxDriversPerTask_(SystemConfig::instance()->maxDriversPerTask()),
      concurrentLifespansPerTask_(
          SystemConfig::instance()->concurrentLifespansPerTask()) {
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

  bufferManager.getData(
      taskId,
      bufferId,
      maxSize.getValue(protocol::DataUnit::BYTE),
      token,
      [taskId = taskId, bufferId = bufferId, promiseHolder](
          std::vector<std::shared_ptr<exec::SerializedPage>>& pages,
          int64_t sequence) mutable {
        bool complete = pages.empty();
        int64_t nextSequence = sequence;
        std::unique_ptr<folly::IOBuf> iobuf;
        int64_t bytes = 0;
        for (auto& page : pages) {
          if (page) {
            VELOX_CHECK(!complete, "Received data after end marker");
            if (!iobuf) {
              iobuf = page->getIOBuf();
              bytes = iobuf->length();
            } else {
              auto next = page->getIOBuf();
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
      });
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
  // outside of prestoTask->mutex.
  prestoTask->taskStarted = true;

  auto info = prestoTask->updateInfo();
  return std::make_unique<TaskInfo>(info);
}

std::unique_ptr<TaskInfo> TaskManager::createOrUpdateTask(
    const TaskId& taskId,
    velox::core::PlanFragment planFragment,
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
  uint32_t maxDrivers;
  uint32_t concurrentLifespans;
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
      maxDrivers =
          queryCtx->get<int32_t>(kMaxDriversPerTask.data(), maxDriversPerTask_);
      concurrentLifespans = queryCtx->get<int32_t>(
          kConcurrentLifespansPerTask.data(), concurrentLifespansPerTask_);
      execTask = std::make_shared<exec::Task>(
          taskId, planFragment, 0, std::move(queryCtx));
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
    if (execTask->isGroupedExecution()) {
      LOG(INFO) << "Starting task " << taskId << " with " << maxDrivers
                << " max drivers and " << concurrentLifespans
                << " concurrent lifespans.";
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

  if (outputBuffers.type == protocol::BufferType::BROADCAST) {
    execTask->updateBroadcastOutputBuffers(
        outputBuffers.buffers.size(), outputBuffers.noMoreBufferIds);
  }

  for (const auto& source : sources) {
    // Add all splits from the source to the task.
    LOG(INFO) << "Adding " << source.splits.size() << " splits to " << taskId
              << " for " << source.planNodeId;
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
        execTask->noMoreSplitsForGroup(source.planNodeId, lifespan.groupid);
      }
    }

    if (source.noMoreSplits) {
      LOG(INFO) << "No more splits for " << taskId;
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

size_t TaskManager::cleanOldTasks() {
  size_t numCleanedTasks{0};

  auto taskMap = taskMap_.wlock();
  for (auto it = taskMap->begin(); it != taskMap->end();) {
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
      ++it;
    } else {
      VELOX_CHECK_EQ(
          it->second.use_count(),
          1,
          "PrestoTask still referenced by more than 1 owner");
      VELOX_CHECK_EQ(
          it->second->task.use_count(),
          1,
          "Task still referenced by more than 1 owner");
      it = taskMap->erase(it);
      ++numCleanedTasks;
    }
  }

  if (numCleanedTasks > 0) {
    LOG(INFO) << "Cleaned " << numCleanedTasks << " old task(s).";
  }
  return numCleanedTasks;
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
      .thenValue([promiseHolder, prestoTask](bool /*done*/) {
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
  auto eventBase = folly::EventBaseManager::get()->getEventBase();
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

  auto timeoutFn = [token]() {
    auto result = std::make_unique<Result>();
    result->sequence = result->nextSequence = token;
    result->data = folly::IOBuf::create(0);
    result->complete = false;
    return result;
  };

  try {
    auto prestoTask = findOrCreateTask(taskId);

    // If the task is aborted or failed, then return an error.
    if (prestoTask->info.taskStatus.state == protocol::TaskState::ABORTED) {
      VELOX_USER_FAIL("Calling getResult() on a aborted task: {}", taskId);
    }
    if (prestoTask->error != nullptr) {
      VELOX_USER_FAIL("Calling getResult() on a failed task: {}", taskId);
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
      .thenValue([promiseHolder, prestoTask](bool /*done*/) {
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
  if (it == taskMap.cend()) {
    auto prestoTask = std::make_shared<PrestoTask>(taskId);
    prestoTask->info.stats.createTime =
        util::toISOTimestamp(velox::getCurrentTimeMs());
    prestoTask->info.needsPlan = true;
    prestoTask->info.metadataUpdates.connectorId = "unused";

    struct uuid_split {
      int64_t lo;
      int64_t hi;
    };
    union uuid_parse {
      boost::uuids::uuid uuid;
      uuid_split split;
    };

    uuid_parse uuid;
    uuid.uuid = boost::uuids::random_generator()();

    prestoTask->info.taskStatus.taskInstanceIdLeastSignificantBits =
        uuid.split.lo;
    prestoTask->info.taskStatus.taskInstanceIdMostSignificantBits =
        uuid.split.hi;

    prestoTask->info.taskStatus.state = protocol::TaskState::RUNNING;
    prestoTask->info.taskStatus.self =
        fmt::format("{}/v1/task/{}", baseUri_, taskId);
    prestoTask->info.taskStatus.outputBufferUtilization = 1;
    prestoTask->updateHeartbeatLocked();
    ++prestoTask->info.taskStatus.version;

    taskMap[taskId] = prestoTask;
    return prestoTask;
  } else {
    auto prestoTask = it->second;
    std::lock_guard<std::mutex> l(prestoTask->mutex);
    prestoTask->updateHeartbeatLocked();
    ++prestoTask->info.taskStatus.version;
    return prestoTask;
  }
}

std::string TaskManager::toString() {
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

size_t TaskManager::getNumRunningDrivers() const {
  auto taskMap = taskMap_.rlock();
  size_t numDrivers = 0;
  for (const auto& pair : *taskMap) {
    if (pair.second->task != nullptr) {
      numDrivers += pair.second->task->numRunningDrivers();
    }
  }
  return numDrivers;
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

} // namespace facebook::presto
