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
#include "velox/exec/Task.h"
#include "velox/codegen/Codegen.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/CrossJoinBuild.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/LocalPlanner.h"
#include "velox/exec/Merge.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#if CODEGEN_ENABLED == 1
#include "velox/experimental/codegen/CodegenLogger.h"
#endif

namespace facebook::velox::exec {

Task::Task(
    const std::string& taskId,
    core::PlanFragment planFragment,
    int destination,
    std::shared_ptr<core::QueryCtx> queryCtx,
    Consumer consumer,
    std::function<void(std::exception_ptr)> onError)
    : Task{
          taskId,
          std::move(planFragment),
          destination,
          std::move(queryCtx),
          (consumer ? [c = std::move(consumer)]() { return c; }
                    : ConsumerSupplier{}),
          std::move(onError)} {}

Task::Task(
    const std::string& taskId,
    core::PlanFragment planFragment,
    int destination,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ConsumerSupplier consumerSupplier,
    std::function<void(std::exception_ptr)> onError)
    : taskId_(taskId),
      planFragment_(std::move(planFragment)),
      destination_(destination),
      queryCtx_(std::move(queryCtx)),
      consumerSupplier_(std::move(consumerSupplier)),
      onError_(onError),
      pool_(queryCtx_->pool()->addScopedChild("task_root")),
      bufferManager_(
          PartitionedOutputBufferManager::getInstance(queryCtx_->host())) {}

Task::~Task() {
  try {
    if (hasPartitionedOutput_) {
      if (auto bufferManager = bufferManager_.lock()) {
        bufferManager->removeTask(taskId_);
      }
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Caught exception in ~Task(): " << e.what();
  }
}

void Task::start(std::shared_ptr<Task> self, uint32_t maxDrivers) {
  VELOX_CHECK(self->drivers_.empty());
  {
    std::lock_guard<std::mutex> l(self->mutex_);
    self->taskStats_.executionStartTimeMs = getCurrentTimeMs();
  }

#if CODEGEN_ENABLED == 1
  const auto& config = self->queryCtx()->config();
  if (config.codegenEnabled() &&
      config.codegenConfigurationFilePath().length() != 0) {
    auto codegenLogger =
        std::make_shared<codegen::DefaultLogger>(self->taskId_);
    auto codegen = codegen::Codegen(codegenLogger);
    auto lazyLoading = config.codegenLazyLoading();
    codegen.initializeFromFile(
        config.codegenConfigurationFilePath(), lazyLoading);
    auto newPlanNode = codegen.compile(*(self->planFragment_.planNode));
    self->planFragment_.planNode =
        newPlanNode != nullptr ? newPlanNode : self->planFragment_.planNode;
  }
#endif

  LocalPlanner::plan(
      self->planFragment_.planNode,
      self->consumerSupplier(),
      &self->driverFactories_);

  for (auto& factory : self->driverFactories_) {
    self->numDrivers_ += std::min(factory->maxDrivers, maxDrivers);
  }

  const auto numDriverFactories = self->driverFactories_.size();
  self->taskStats_.pipelineStats.reserve(numDriverFactories);
  for (const auto& driverFactory : self->driverFactories_) {
    self->taskStats_.pipelineStats.emplace_back(
        driverFactory->inputDriver, driverFactory->outputDriver);
  }

  // Register self for possible memory recovery callback. Do this
  // after sizing 'drivers_' but before starting the
  // Drivers. 'drivers_' can be read by memory recovery or
  // cancellation while Drivers are being made, so the array should
  // have final size from the start.

  auto bufferManager = self->bufferManager_.lock();
  VELOX_CHECK_NOT_NULL(
      bufferManager,
      "Unable to initialize task. "
      "PartitionedOutputBufferManager was already destructed");

  std::vector<std::shared_ptr<Driver>> drivers;
  drivers.reserve(self->numDrivers_);
  for (auto pipeline = 0; pipeline < self->driverFactories_.size();
       ++pipeline) {
    auto& factory = self->driverFactories_[pipeline];
    auto numDrivers = std::min(factory->maxDrivers, maxDrivers);
    auto partitionedOutputNode = factory->needsPartitionedOutput();
    if (partitionedOutputNode) {
      VELOX_CHECK(
          !self->hasPartitionedOutput_,
          "Only one output pipeline per task is supported");
      self->hasPartitionedOutput_ = true;
      bufferManager->initializeTask(
          self,
          partitionedOutputNode->isBroadcast(),
          partitionedOutputNode->numPartitions(),
          numDrivers);
    }

    std::shared_ptr<ExchangeClient> exchangeClient = nullptr;
    if (factory->needsExchangeClient()) {
      exchangeClient = self->addExchangeClient();
    }

    auto exchangeId = factory->needsLocalExchangeSource();
    if (exchangeId.has_value()) {
      self->createLocalExchangeSources(exchangeId.value(), numDrivers);
    }

    self->addHashJoinBridges(factory->needsHashJoinBridges());
    self->addCrossJoinBridges(factory->needsCrossJoinBridges());

    for (int32_t i = 0; i < numDrivers; ++i) {
      drivers.push_back(factory->createDriver(
          std::make_unique<DriverCtx>(self, i, pipeline, numDrivers),
          exchangeClient,
          [self, maxDrivers](size_t i) {
            return i < self->driverFactories_.size()
                ? std::min(self->driverFactories_[i]->maxDrivers, maxDrivers)
                : 0;
          }));
      if (i == 0) {
        drivers.back()->initializeOperatorStats(
            self->taskStats_.pipelineStats[pipeline].operatorStats);
      }
    }
  }
  self->noMoreLocalExchangeProducers();
  // Set and start all Drivers together inside 'mutex_' so that
  // cancellations and pauses have well
  // defined timing. For example, do not pause and restart a task
  // while it is still adding Drivers.
  // If the given execuor is folly::InlineLikeExecutor (or it's child), since
  // the drivers will be executed synchronously on the same thread as the
  // current task, so we need release the lock to avoid the deadlock.
  std::unique_lock<std::mutex> l(self->mutex_);
  self->drivers_ = std::move(drivers);
  if (dynamic_cast<const folly::InlineLikeExecutor*>(
          self->queryCtx()->executor())) {
    l.unlock();
  }
  for (auto& driver : self->drivers_) {
    if (driver) {
      Driver::enqueue(driver);
    }
  }
}

// static
void Task::resume(std::shared_ptr<Task> self) {
  VELOX_CHECK(!self->exception_, "Cannot resume failed task");
  std::lock_guard<std::mutex> l(self->mutex_);
  // Setting pause requested must be atomic with the resuming so that
  // suspended sections do not go back on thread during resume.
  self->requestPauseLocked(false);
  for (auto& driver : self->drivers_) {
    if (driver) {
      if (driver->state().isSuspended) {
        // The Driver will come on thread in its own time as long as
        // the cancel flag is reset. This check needs to be inside 'mutex_'.
        continue;
      }
      if (driver->state().isEnqueued) {
        // A Driver can wait for a thread and there can be a
        // pause/resume during the wait. The Driver should not be
        // enqueued twice.
        continue;
      }
      VELOX_CHECK(!driver->isOnThread() && !driver->isTerminated());
      if (!driver->state().hasBlockingFuture) {
        // Do not continue a Driver that is blocked on external
        // event. The Driver gets enqueued by the promise realization.
        Driver::enqueue(driver);
      }
    }
  }
}

// static
void Task::removeDriver(std::shared_ptr<Task> self, Driver* driver) {
  std::lock_guard<std::mutex> taskLock(self->mutex_);
  for (auto& driverPtr : self->drivers_) {
    if (driverPtr.get() == driver) {
      driverPtr = nullptr;
      self->driverClosedLocked();
      return;
    }
  }
  VELOX_FAIL("Trying to delete a Driver twice from its Task");
}

void Task::setMaxSplitSequenceId(
    const core::PlanNodeId& planNodeId,
    long maxSequenceId) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(state_ == kRunning);

  auto& splitsState = splitsStates_[planNodeId];
  // We could have been sent an old split again, so only change max id, when the
  // new one is greater.
  splitsState.maxSequenceId =
      std::max(splitsState.maxSequenceId, maxSequenceId);
}

bool Task::addSplitWithSequence(
    const core::PlanNodeId& planNodeId,
    exec::Split&& split,
    long sequenceId) {
  std::unique_ptr<ContinuePromise> promise;
  bool added = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(state_ == kRunning);

    // The same split can be added again in some systems. The systems that want
    // 'one split processed once only' would use this method and duplicate
    // splits would be ignored.
    auto& splitsState = splitsStates_[planNodeId];
    if (sequenceId > splitsState.maxSequenceId) {
      promise = addSplitLocked(splitsState, std::move(split));
      added = true;
    }
  }
  if (promise) {
    promise->setValue(false);
  }
  return added;
}

void Task::addSplit(const core::PlanNodeId& planNodeId, exec::Split&& split) {
  std::unique_ptr<ContinuePromise> promise;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(state_ == kRunning);

    promise = addSplitLocked(splitsStates_[planNodeId], std::move(split));
  }
  if (promise) {
    promise->setValue(false);
  }
}

std::unique_ptr<ContinuePromise> Task::addSplitLocked(
    SplitsState& splitsState,
    exec::Split&& split) {
  ++taskStats_.numTotalSplits;
  ++taskStats_.numQueuedSplits;

  splitsState.splits.push_back(split);

  if (split.hasGroup()) {
    ++splitsState.groupSplits[split.groupId].numIncompleteSplits;
  }

  if (not splitsState.splitPromises.empty()) {
    auto promise = std::make_unique<ContinuePromise>(
        std::move(splitsState.splitPromises.back()));
    splitsState.splitPromises.pop_back();
    return promise;
  }
  return nullptr;
}

void Task::noMoreSplitsForGroup(
    const core::PlanNodeId& planNodeId,
    int32_t splitGroupId) {
  std::lock_guard<std::mutex> l(mutex_);

  auto& splitsState = splitsStates_[planNodeId];
  splitsState.groupSplits[splitGroupId].noMoreSplits = true;
  checkGroupSplitsCompleteLocked(
      splitsState.groupSplits,
      splitGroupId,
      splitsState.groupSplits.find(splitGroupId));
}

void Task::noMoreSplits(const core::PlanNodeId& planNodeId) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);

    auto& splitsState = splitsStates_[planNodeId];
    splitsState.noMoreSplits = true;
    promises = std::move(splitsState.splitPromises);
  }
  for (auto& promise : promises) {
    promise.setValue(false);
  }
}

bool Task::isAllSplitsFinishedLocked() {
  if (taskStats_.numFinishedSplits == taskStats_.numTotalSplits) {
    for (auto& it : splitsStates_) {
      if (not it.second.noMoreSplits) {
        return false;
      }
    }
    return true;
  }
  return false;
}

BlockingReason Task::getSplitOrFuture(
    int /*driverId*/,
    const core::PlanNodeId& planNodeId,
    exec::Split& split,
    ContinueFuture& future) {
  std::lock_guard<std::mutex> l(mutex_);

  auto& splitsState = splitsStates_[planNodeId];
  if (splitsState.splits.empty()) {
    if (splitsState.noMoreSplits) {
      return BlockingReason::kNotBlocked;
    }
    auto [splitPromise, splitFuture] = makeVeloxPromiseContract<bool>(
        fmt::format("Task::getSplitOrFuture {}", taskId_));
    future = std::move(splitFuture);
    splitsState.splitPromises.push_back(std::move(splitPromise));
    return BlockingReason::kWaitForSplit;
  }

  split = std::move(splitsState.splits.front());
  splitsState.splits.pop_front();

  --taskStats_.numQueuedSplits;
  ++taskStats_.numRunningSplits;

  if (taskStats_.firstSplitStartTimeMs == 0) {
    taskStats_.firstSplitStartTimeMs = getCurrentTimeMs();
  }
  taskStats_.lastSplitStartTimeMs = getCurrentTimeMs();

  return BlockingReason::kNotBlocked;
}

void Task::splitFinished(
    const core::PlanNodeId& planNodeId,
    int32_t splitGroupId) {
  std::lock_guard<std::mutex> l(mutex_);
  ++taskStats_.numFinishedSplits;
  --taskStats_.numRunningSplits;
  if (isAllSplitsFinishedLocked()) {
    taskStats_.executionEndTimeMs = getCurrentTimeMs();
  }
  // If bucketed group id for this split is valid, we want to check if this
  // group has been completed (no more running or queued splits).
  if (splitGroupId != -1) {
    auto& splitsState = splitsStates_[planNodeId];
    auto it = splitsState.groupSplits.find(splitGroupId);
    VELOX_DCHECK(
        it != splitsState.groupSplits.end(),
        "We have a finished split in group {}, which wasn't registered!",
        splitGroupId);
    if (it != splitsState.groupSplits.end()) {
      --it->second.numIncompleteSplits;
      VELOX_DCHECK_GE(
          it->second.numIncompleteSplits,
          0,
          "Number of incomplete splits in group {} is negative: {}!",
          splitGroupId,
          it->second.numIncompleteSplits);
      checkGroupSplitsCompleteLocked(splitsState.groupSplits, splitGroupId, it);
    }
  }
}

void Task::multipleSplitsFinished(int32_t numSplits) {
  std::lock_guard<std::mutex> l(mutex_);
  taskStats_.numFinishedSplits += numSplits;
  taskStats_.numRunningSplits -= numSplits;
}

void Task::checkGroupSplitsCompleteLocked(
    std::unordered_map<int32_t, GroupSplitsInfo>& mapGroupSplits,
    int32_t splitGroupId,
    std::unordered_map<int32_t, GroupSplitsInfo>::iterator it) {
  if (it->second.numIncompleteSplits == 0 and it->second.noMoreSplits) {
    mapGroupSplits.erase(it);
    taskStats_.completedSplitGroups.emplace(splitGroupId);
  }
}

void Task::updateBroadcastOutputBuffers(int numBuffers, bool noMoreBuffers) {
  auto bufferManager = bufferManager_.lock();
  VELOX_CHECK_NOT_NULL(
      bufferManager,
      "Unable to initialize task. "
      "PartitionedOutputBufferManager was already destructed");

  bufferManager->updateBroadcastOutputBuffers(
      taskId_, numBuffers, noMoreBuffers);
}

void Task::setAllOutputConsumed() {
  std::lock_guard<std::mutex> l(mutex_);
  partitionedOutputConsumed_ = true;
  if (!numDrivers_ && state_ == kRunning) {
    state_ = kFinished;
    taskStats_.endTimeMs = getCurrentTimeMs();
    stateChangedLocked();
  }
}

void Task::driverClosedLocked() {
  --numDrivers_;
  if ((numDrivers_ == 0) && (state_ == kRunning)) {
    if (taskStats_.executionEndTimeMs == 0) {
      // In case we haven't set executionEndTimeMs due to all splits depleted,
      // we set it here.
      // This can happen due to task error or task being cancelled.
      taskStats_.executionEndTimeMs = getCurrentTimeMs();
    }
    if (!hasPartitionedOutput_ || partitionedOutputConsumed_) {
      state_ = kFinished;
      stateChangedLocked();
    }
  }
}

std::shared_ptr<ExchangeClient> Task::addExchangeClient() {
  exchangeClients_.emplace_back(std::make_shared<ExchangeClient>(destination_));
  return exchangeClients_.back();
}

bool Task::allPeersFinished(
    const core::PlanNodeId& planNodeId,
    Driver* caller,
    ContinueFuture* future,
    std::vector<VeloxPromise<bool>>& promises,
    std::vector<std::shared_ptr<Driver>>& peers) {
  std::lock_guard<std::mutex> l(mutex_);
  if (exception_) {
    VELOX_FAIL("Task is terminating because of error: {}", errorMessage());
  }
  auto& state = barriers_[planNodeId];

  if (++state.numRequested == caller->driverCtx()->numDrivers) {
    peers = std::move(state.drivers);
    promises = std::move(state.promises);
    barriers_.erase(planNodeId);
    return true;
  }
  std::shared_ptr<Driver> callerShared;
  for (auto& driver : drivers_) {
    if (driver.get() == caller) {
      callerShared = driver;
      break;
    }
  }
  VELOX_CHECK(callerShared, "Caller of pipelineBarrier is not a valid Driver");
  state.drivers.push_back(callerShared);
  state.promises.emplace_back(
      fmt::format("Task::allPeersFinished {}", taskId_));
  *future = state.promises.back().getSemiFuture();

  return false;
}

void Task::addHashJoinBridges(
    const std::vector<core::PlanNodeId>& planNodeIds) {
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& planNodeId : planNodeIds) {
    bridges_.emplace(planNodeId, std::make_shared<HashJoinBridge>());
  }
}

void Task::addCrossJoinBridges(
    const std::vector<core::PlanNodeId>& planNodeIds) {
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& planNodeId : planNodeIds) {
    bridges_.emplace(planNodeId, std::make_shared<CrossJoinBridge>());
  }
}

std::shared_ptr<HashJoinBridge> Task::getHashJoinBridge(
    const core::PlanNodeId& planNodeId) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = bridges_.find(planNodeId);
  VELOX_CHECK(
      it != bridges_.end(),
      "Hash join bridge for plan node ID not found: {}",
      planNodeId);
  auto bridge = std::dynamic_pointer_cast<HashJoinBridge>(it->second);
  VELOX_CHECK_NOT_NULL(
      bridge,
      "Join bridge for plan node ID is not a hash join bridge: {}",
      planNodeId);
  return bridge;
}

std::shared_ptr<CrossJoinBridge> Task::getCrossJoinBridge(
    const core::PlanNodeId& planNodeId) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = bridges_.find(planNodeId);
  VELOX_CHECK(
      it != bridges_.end(),
      "Join bridge for plan node ID not found:{}",
      planNodeId);
  auto bridge = std::dynamic_pointer_cast<CrossJoinBridge>(it->second);
  VELOX_CHECK_NOT_NULL(
      bridge,
      "Join bridge for plan node ID is not a cross join bridge: {}",
      planNodeId);
  return bridge;
}

//  static
std::string Task::shortId(const std::string& id) {
  if (id.size() < 12) {
    return id;
  }
  const char* str = id.c_str();
  const char* dot = strchr(str, '.');
  if (!dot) {
    return id;
  }
  auto hash = std::hash<std::string_view>()(std::string_view(str, dot - str));
  return fmt::format("tk:{}", hash & 0xffff);
}

void Task::terminate(TaskState terminalState) {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (taskStats_.executionEndTimeMs == 0) {
      taskStats_.executionEndTimeMs = getCurrentTimeMs();
    }
    if (state_ != kRunning) {
      return;
    }
    state_ = terminalState;
  }
  requestTerminate();
  for (auto driver : drivers_) {
    // 'driver' is a  copy of the shared_ptr in
    // 'drivers_'. This is safe against a concurrent remove of the
    // Driver.
    if (driver) {
      driver->terminate();
    }
  }
  // We continue all Drivers waiting for splits or space in exchange buffers.
  if (hasPartitionedOutput_) {
    if (auto bufferManager = bufferManager_.lock()) {
      bufferManager->removeTask(taskId_);
    }
  }
  // Release reference to exchange client, so that it will close exchange
  // sources and prevent resending requests for data.
  std::vector<ContinuePromise> promises;
  std::unordered_map<std::string, std::shared_ptr<JoinBridge>> oldBridges;
  {
    std::lock_guard<std::mutex> l(mutex_);
    exchangeClients_.clear();
    oldBridges = std::move(bridges_);
    for (auto& pair : splitsStates_) {
      for (auto& promise : pair.second.splitPromises) {
        promises.push_back(std::move(promise));
      }
      pair.second.splitPromises.clear();
    }
  }
  for (auto& promise : promises) {
    promise.setValue(true);
  }
  for (auto& pair : oldBridges) {
    pair.second->cancel();
  }
  std::lock_guard<std::mutex> l(mutex_);
  stateChangedLocked();
}

void Task::addOperatorStats(OperatorStats& stats) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(
      stats.pipelineId >= 0 &&
      stats.pipelineId < taskStats_.pipelineStats.size());
  VELOX_CHECK(
      stats.operatorId >= 0 &&
      stats.operatorId <
          taskStats_.pipelineStats[stats.pipelineId].operatorStats.size());
  taskStats_.pipelineStats[stats.pipelineId]
      .operatorStats[stats.operatorId]
      .add(stats);
  stats.clear();
}

uint64_t Task::timeSinceStartMs() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (taskStats_.executionStartTimeMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - taskStats_.executionStartTimeMs;
}

uint64_t Task::timeSinceEndMs() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (taskStats_.executionEndTimeMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - taskStats_.executionEndTimeMs;
}

void Task::stateChangedLocked() {
  for (auto& promise : stateChangePromises_) {
    promise.setValue(true);
  }
  stateChangePromises_.clear();
}

ContinueFuture Task::stateChangeFuture(uint64_t maxWaitMicros) {
  std::lock_guard<std::mutex> l(mutex_);
  // If 'this' is running, the future is realized on timeout or when
  // this no longer is running.
  if (state_ != kRunning) {
    return ContinueFuture(true);
  }
  auto [promise, future] = makeVeloxPromiseContract<bool>(
      fmt::format("Task::stateChangeFuture {}", taskId_));
  stateChangePromises_.emplace_back(std::move(promise));
  if (maxWaitMicros) {
    return std::move(future).within(std::chrono::microseconds(maxWaitMicros));
  }
  return std::move(future);
}

std::string Task::toString() {
  std::stringstream out;
  out << "{Task " << shortId(taskId_) << " (" << taskId_ << ")";

  if (exception_) {
    out << "Error: " << errorMessage() << std::endl;
  }

  if (planFragment_.planNode) {
    out << "Plan: " << planFragment_.planNode->toString() << std::endl;
  }

  out << " drivers:\n";
  for (auto& driver : drivers_) {
    if (driver) {
      out << driver->toString() << std::endl;
    }
  }

  for (const auto& pair : splitsStates_) {
    const auto& splitState = pair.second;
    out << "Plan Node: " << pair.first << ": " << std::endl;

    out << splitState.splits.size() << " splits: ";
    int32_t counter = 0;
    for (const auto& split : splitState.splits) {
      out << split.toString() << " ";
      if (++counter > 4) {
        out << "...";
        break;
      }
    }
    out << std::endl;

    if (splitState.noMoreSplits) {
      out << "No more splits" << std::endl;
    }

    if (not splitState.splitPromises.empty()) {
      out << splitState.splitPromises.size() << " split promises" << std::endl;
    }
  }

  return out.str();
}

void Task::createLocalMergeSources(
    unsigned numSources,
    const std::shared_ptr<const RowType>& rowType,
    memory::MappedMemory* mappedMemory) {
  VELOX_CHECK(
      localMergeSources_.empty(),
      "Multiple local merges in a single task not supported");
  localMergeSources_.reserve(numSources);
  for (auto i = 0; i < numSources; ++i) {
    localMergeSources_.emplace_back(
        MergeSource::createLocalMergeSource(rowType, mappedMemory));
  }
}

void Task::createMergeJoinSource(const core::PlanNodeId& planNodeId) {
  VELOX_CHECK(
      mergeJoinSources_.find(planNodeId) == mergeJoinSources_.end(),
      "Merge join sources already exist: {}",
      planNodeId);

  mergeJoinSources_.insert({planNodeId, std::make_shared<MergeJoinSource>()});
}

std::shared_ptr<MergeJoinSource> Task::getMergeJoinSource(
    const core::PlanNodeId& planNodeId) {
  auto it = mergeJoinSources_.find(planNodeId);
  VELOX_CHECK(
      it != mergeJoinSources_.end(),
      "Merge join source for specified plan node doesn't exist: {}",
      planNodeId);
  return it->second;
}

void Task::createLocalExchangeSources(
    const core::PlanNodeId& planNodeId,
    int numPartitions) {
  VELOX_CHECK(
      localExchanges_.find(planNodeId) == localExchanges_.end(),
      "Local exchange already exists: {}",
      planNodeId);

  LocalExchange exchange;
  exchange.memoryManager = std::make_unique<LocalExchangeMemoryManager>(
      queryCtx_->config().maxLocalExchangeBufferSize());

  exchange.sources.reserve(numPartitions);
  for (auto i = 0; i < numPartitions; ++i) {
    exchange.sources.emplace_back(
        std::make_shared<LocalExchangeSource>(exchange.memoryManager.get(), i));
  }

  localExchanges_.insert({planNodeId, std::move(exchange)});
}

void Task::noMoreLocalExchangeProducers() {
  for (auto& exchange : localExchanges_) {
    for (auto& source : exchange.second.sources) {
      source->noMoreProducers();
    }
  }
}

std::shared_ptr<LocalExchangeSource> Task::getLocalExchangeSource(
    const core::PlanNodeId& planNodeId,
    int partition) {
  const auto& sources = getLocalExchangeSources(planNodeId);
  VELOX_CHECK_LT(
      partition,
      sources.size(),
      "Incorrect partition for local exchange {}",
      planNodeId);
  return sources[partition];
}

const std::vector<std::shared_ptr<LocalExchangeSource>>&
Task::getLocalExchangeSources(const core::PlanNodeId& planNodeId) {
  auto it = localExchanges_.find(planNodeId);
  VELOX_CHECK(
      it != localExchanges_.end(),
      "Incorrect local exchange ID: {}",
      planNodeId);
  return it->second.sources;
}

StopReason Task::enter(ThreadState& state) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(state.isEnqueued);
  state.isEnqueued = false;
  if (state.isTerminated) {
    return StopReason::kAlreadyTerminated;
  }
  if (state.isOnThread()) {
    return StopReason::kAlreadyOnThread;
  }
  auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
  }
  if (reason == StopReason::kNone) {
    ++numThreads_;
    state.setThread();
    state.hasBlockingFuture = false;
  }
  return reason;
}

StopReason Task::enterForTerminate(ThreadState& state) {
  std::lock_guard<std::mutex> l(mutex_);
  if (state.isOnThread() || state.isTerminated) {
    state.isTerminated = true;
    return StopReason::kAlreadyOnThread;
  }
  state.isTerminated = true;
  state.setThread();
  return StopReason::kTerminate;
}

StopReason Task::leave(ThreadState& state) {
  std::lock_guard<std::mutex> l(mutex_);
  if (--numThreads_ == 0) {
    finished();
  }
  state.clearThread();
  if (state.isTerminated) {
    return StopReason::kTerminate;
  }
  auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
  }
  return reason;
}

StopReason Task::enterSuspended(ThreadState& state) {
  VELOX_CHECK(!state.hasBlockingFuture);
  VELOX_CHECK(state.isOnThread());
  std::lock_guard<std::mutex> l(mutex_);
  if (state.isTerminated) {
    return StopReason::kAlreadyTerminated;
  }
  if (!state.isOnThread()) {
    return StopReason::kAlreadyTerminated;
  }
  auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
  }
  // A pause will not stop entering the suspended section. It will
  // just ack that the thread is no longer in inside the
  // CancelPool. The pause can wait at the exit of the suspended
  // section.
  if (reason == StopReason::kNone || reason == StopReason::kPause) {
    state.isSuspended = true;
    if (--numThreads_ == 0) {
      finished();
    }
  }
  return StopReason::kNone;
}

StopReason Task::leaveSuspended(ThreadState& state) {
  for (;;) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      ++numThreads_;
      state.isSuspended = false;
      if (state.isTerminated) {
        return StopReason::kAlreadyTerminated;
      }
      if (terminateRequested_) {
        state.isTerminated = true;
        return StopReason::kTerminate;
      }
      if (!pauseRequested_) {
        // For yield or anything but pause  we return here.
        return StopReason::kNone;
      }
      --numThreads_;
      state.isSuspended = true;
    }
    // If the pause flag is on when trying to reenter, sleep a while
    // outside of the mutex and recheck. This is rare and not time
    // critical. Can happen if memory interrupt sets pause while
    // already inside a suspended section for other reason, like
    // IO.
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // NOLINT
  }
}

folly::SemiFuture<bool> Task::finishFuture() {
  auto [promise, future] =
      makeVeloxPromiseContract<bool>("CancelPool::finishFuture");
  std::lock_guard<std::mutex> l(mutex_);
  if (numThreads_ == 0) {
    promise.setValue(true);
    return std::move(future);
  }
  finishPromises_.push_back(std::move(promise));
  return std::move(future);
}

} // namespace facebook::velox::exec
