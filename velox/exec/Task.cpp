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

namespace {
void collectSourcePlanNodeIds(
    const core::PlanNode* planNode,
    std::unordered_set<core::PlanNodeId>& allIds,
    std::unordered_set<core::PlanNodeId>& sourceIds) {
  bool ok = allIds.insert(planNode->id()).second;
  VELOX_USER_CHECK(
      ok,
      "Plan node IDs must be unique. Found duplicate ID: {}.",
      planNode->id());

  // Check if planNode is a leaf node in the plan tree. If so, it is a source
  // node and could use splits for processing.
  if (planNode->sources().empty()) {
    sourceIds.insert(planNode->id());
    return;
  }

  for (const auto& child : planNode->sources()) {
    collectSourcePlanNodeIds(child.get(), allIds, sourceIds);
  }
}

/// Returns a set of source (leaf) plan node IDs. Also, checks that plan node
/// IDs are unique and throws if encounters duplicates.
std::unordered_set<core::PlanNodeId> collectSourcePlanNodeIds(
    const std::shared_ptr<const core::PlanNode>& planNode) {
  std::unordered_set<core::PlanNodeId> allIds;
  std::unordered_set<core::PlanNodeId> sourceIds;
  collectSourcePlanNodeIds(planNode.get(), allIds, sourceIds);
  return sourceIds;
}

} // namespace

// The number of splits groups we run concurrently.
// TODO(spershin): We need to expose this as some kind of setting/parameter.
static constexpr uint32_t kNumConcurrentSplitGroups = 1;

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
      sourcePlanNodeIds_(collectSourcePlanNodeIds(planFragment_.planNode)),
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

velox::memory::MemoryPool* FOLLY_NONNULL Task::addDriverPool() {
  childPools_.push_back(pool_->addScopedChild("driver_root"));
  auto* driverPool = childPools_.back().get();
  auto parentTracker = pool_->getMemoryUsageTracker();
  if (parentTracker) {
    driverPool->setMemoryUsageTracker(parentTracker->addChild());
  }

  return driverPool;
}

velox::memory::MemoryPool* FOLLY_NONNULL
Task::addOperatorPool(velox::memory::MemoryPool* FOLLY_NONNULL driverPool) {
  childPools_.push_back(driverPool->addScopedChild("operator_ctx"));
  return childPools_.back().get();
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
    if (auto newPlanNode = codegen.compile(*(self->planFragment_.planNode))) {
      self->planFragment_.planNode = newPlanNode;
    }
  }
#endif

  // Here we create driver factories.
  LocalPlanner::plan(
      self->planFragment_,
      self->consumerSupplier(),
      &self->driverFactories_,
      maxDrivers);

  // Keep one exchange client per pipeline (NULL if not used).
  const auto numPipelines = self->driverFactories_.size();
  self->exchangeClients_.resize(numPipelines);

  // For grouped execution we need one split group state per split group,
  // otherwise just one.
  const uint32_t numSplitGroups =
      std::max(1, self->planFragment_.numSplitGroups);
  self->splitGroupStates_.resize(numSplitGroups);

  // For each pipeline we have a corresponding driver factory.
  // Here we count how many drivers in total we need and we also create create
  // pipeline stats.
  for (auto& factory : self->driverFactories_) {
    self->numDriversPerSplitGroup_ += factory->numDrivers;
    self->numTotalDrivers_ += factory->numTotalDrivers;
    self->taskStats_.pipelineStats.emplace_back(
        factory->inputDriver, factory->outputDriver);
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

  // In this loop we prepare the pipelines: buffer manager, exchange clients,
  // local exchanges and join bridges.
  for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
    auto& factory = self->driverFactories_[pipeline];

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
          factory->numTotalDrivers);
    }

    if (factory->needsExchangeClient()) {
      self->exchangeClients_[pipeline] =
          std::make_shared<ExchangeClient>(self->destination_);
    }

    auto exchangeId = factory->needsLocalExchangeSource();
    if (exchangeId.has_value()) {
      self->createLocalExchangeSources(exchangeId.value(), factory->numDrivers);
    }

    self->addHashJoinBridges(factory->needsHashJoinBridges());
    self->addCrossJoinBridges(factory->needsCrossJoinBridges());
  }

  // In this loop we create the drivers we are going to run for this task.
  // Note, that for grouped execution (when we have split groups) we will create
  // only a portion of drivers here to tackle first N split groups. As drivers
  // finish, we'll create more of them to tackle the remaining split groups,
  // until we have worked through all split groups.
  const uint32_t initialSplitGroups =
      std::min(numSplitGroups, kNumConcurrentSplitGroups);
  std::vector<std::shared_ptr<Driver>> drivers;
  drivers.reserve(self->numDriversPerSplitGroup_ * initialSplitGroups);
  for (size_t i = 0; i < initialSplitGroups; ++i) {
    self->createDrivers(drivers, self);
  }

  // Initialize operator stats using the 1st driver of each operator.
  size_t driverIndex{0};
  for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
    auto& factory = self->driverFactories_[pipeline];
    drivers[driverIndex]->initializeOperatorStats(
        self->taskStats_.pipelineStats[pipeline].operatorStats);
    driverIndex += factory->numDrivers;
  }

  // Set and start all Drivers together inside 'mutex_' so that
  // cancellations and pauses have well
  // defined timing. For example, do not pause and restart a task
  // while it is still adding Drivers.
  // If the given executor is folly::InlineLikeExecutor (or it's child), since
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
      ++self->numRunningDrivers_;
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

void Task::createDrivers(
    std::vector<std::shared_ptr<Driver>>& out,
    std::shared_ptr<Task>& self) {
  auto& splitGroupState = self->splitGroupStates_[nextSplitGroupId_];
  const auto numPipelines = driverFactories_.size();
  for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
    auto& factory = driverFactories_[pipeline];
    const uint32_t driverIdOffset = factory->numDrivers * nextSplitGroupId_;
    for (uint32_t partitionId = 0; partitionId < factory->numDrivers;
         ++partitionId) {
      out.emplace_back(factory->createDriver(
          std::make_unique<DriverCtx>(
              self,
              driverIdOffset + partitionId,
              pipeline,
              nextSplitGroupId_,
              partitionId),
          self->exchangeClients_[pipeline],
          [self](size_t i) {
            return i < self->driverFactories_.size()
                ? self->driverFactories_[i]->numTotalDrivers
                : 0;
          }));
      ++splitGroupState.activeDrivers;
    }
  }
  noMoreLocalExchangeProducers(nextSplitGroupId_);
  ++self->nextSplitGroupId_;
}

// static
void Task::removeDriver(std::shared_ptr<Task> self, Driver* driver) {
  std::lock_guard<std::mutex> taskLock(self->mutex_);
  for (auto& driverPtr : self->drivers_) {
    if (driverPtr.get() != driver) {
      continue;
    }

    // Mark the closure of another driver for its split group (even in ungrouped
    // execution mode).
    auto& splitGroupState =
        self->splitGroupStates_[driver->driverCtx()->splitGroupId];
    --splitGroupState.activeDrivers;

    // Release the driver, note that after this 'driver' is invalid.
    driverPtr = nullptr;
    self->driverClosedLocked();

    if (self->isGroupedExecution()) {
      // Check if a split group is finished.
      if (splitGroupState.activeDrivers == 0) {
        splitGroupState.clear();

        // Create a bunch of drivers for the next split, if there is one.
        if (self->state_ == TaskState::kRunning &&
            self->nextSplitGroupId_ < self->planFragment_.numSplitGroups) {
          std::vector<std::shared_ptr<Driver>> drivers;
          drivers.reserve(self->numDriversPerSplitGroup_);
          self->createDrivers(drivers, self);
          // Move created drivers into the vacant spots in 'drivers_' and
          // enqueue them.
          size_t i = 0;
          for (auto& newDriverPtr : drivers) {
            while (self->drivers_[i] != nullptr) {
              VELOX_CHECK_LT(i, self->drivers_.size());
              ++i;
            }
            auto& targetPtr = self->drivers_[i];
            targetPtr = std::move(newDriverPtr);
            if (targetPtr) {
              ++self->numRunningDrivers_;
              Driver::enqueue(targetPtr);
            }
          }
        }
      }
    }

    return;
  }
  LOG(WARNING) << "Trying to remove a Driver twice from its Task";
}

void Task::setMaxSplitSequenceId(
    const core::PlanNodeId& planNodeId,
    long maxSequenceId) {
  checkPlanNodeIdForSplit(planNodeId);

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
  checkPlanNodeIdForSplit(planNodeId);
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
  checkPlanNodeIdForSplit(planNodeId);
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

void Task::checkPlanNodeIdForSplit(const core::PlanNodeId& id) const {
  VELOX_USER_CHECK(
      sourcePlanNodeIds_.find(id) != sourcePlanNodeIds_.end(),
      "Splits can be associated only with source plan nodes. Plan node ID {} doesn't refer to a source node.",
      id);
}

std::unique_ptr<ContinuePromise> Task::addSplitLocked(
    SplitsState& splitsState,
    exec::Split&& split) {
  ++taskStats_.numTotalSplits;
  ++taskStats_.numQueuedSplits;

  if (not isGroupedExecution()) {
    VELOX_DCHECK(
        not split.hasGroup(), "Got split group for ungrouped execution!");
    return addSplitToStoreLocked(splitsState.splitsStore, std::move(split));
  } else {
    VELOX_CHECK(split.hasGroup(), "Missing split group for grouped execution!");
    const auto splitGroupId = split.groupId; // Avoid eval order c++ warning.
    return addSplitToStoreLocked(
        groupSplitsStoreSafe(splitsState, splitGroupId), std::move(split));
  }
}

std::unique_ptr<ContinuePromise> Task::addSplitToStoreLocked(
    SplitsStore& splitsStore,
    exec::Split&& split) {
  splitsStore.splits.push_back(split);
  if (not splitsStore.splitPromises.empty()) {
    auto promise = std::make_unique<ContinuePromise>(
        std::move(splitsStore.splitPromises.back()));
    splitsStore.splitPromises.pop_back();
    return promise;
  }
  return nullptr;
}

void Task::noMoreSplitsForGroup(
    const core::PlanNodeId& planNodeId,
    int32_t splitGroupId) {
  checkPlanNodeIdForSplit(planNodeId);
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);

    auto& splitsState = splitsStates_[planNodeId];
    auto& splitsStore = groupSplitsStoreSafe(splitsState, splitGroupId);
    splitsStore.noMoreSplits = true;
    checkGroupSplitsCompleteLocked(splitGroupId, splitsStore);
    promises = std::move(splitsStore.splitPromises);
  }
  for (auto& promise : promises) {
    promise.setValue(false);
  }
}

void Task::noMoreSplits(const core::PlanNodeId& planNodeId) {
  checkPlanNodeIdForSplit(planNodeId);
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);

    auto& splitsStore = splitsStates_[planNodeId].splitsStore;
    splitsStore.noMoreSplits = true;
    promises = std::move(splitsStore.splitPromises);
  }
  for (auto& promise : promises) {
    promise.setValue(false);
  }
}

bool Task::isAllSplitsFinishedLocked() {
  if (taskStats_.numFinishedSplits == taskStats_.numTotalSplits) {
    for (const auto& it : splitsStates_) {
      if (not it.second.splitsStore.noMoreSplits) {
        return false;
      }
    }
    return true;
  }
  return false;
}

BlockingReason Task::getSplitOrFuture(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId,
    exec::Split& split,
    ContinueFuture& future) {
  std::lock_guard<std::mutex> l(mutex_);

  auto& splitsState = splitsStates_[planNodeId];

  if (not isGroupedExecution()) {
    return getSplitOrFutureLocked(splitsState.splitsStore, split, future);
  } else {
    // Driver id allows a single driver to only access splits of a single group.
    return getSplitOrFutureLocked(
        groupSplitsStoreSafe(splitsState, splitGroupId), split, future);
  }
}

BlockingReason Task::getSplitOrFutureLocked(
    SplitsStore& splitsStore,
    exec::Split& split,
    ContinueFuture& future) {
  if (splitsStore.splits.empty()) {
    if (splitsStore.noMoreSplits) {
      return BlockingReason::kNotBlocked;
    }
    auto [splitPromise, splitFuture] = makeVeloxPromiseContract<bool>(
        fmt::format("Task::getSplitOrFuture {}", taskId_));
    future = std::move(splitFuture);
    splitsStore.splitPromises.push_back(std::move(splitPromise));
    return BlockingReason::kWaitForSplit;
  }

  split = std::move(splitsStore.splits.front());
  splitsStore.splits.pop_front();

  --taskStats_.numQueuedSplits;
  ++taskStats_.numRunningSplits;
  taskStats_.lastSplitStartTimeMs = getCurrentTimeMs();
  if (taskStats_.firstSplitStartTimeMs == 0) {
    taskStats_.firstSplitStartTimeMs = taskStats_.lastSplitStartTimeMs;
  }

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

  if (not isGroupedExecution()) {
    VELOX_DCHECK(
        splitGroupId == -1, "Got split group for ungrouped execution!");
  } else {
    VELOX_CHECK(
        splitGroupId >= 0, "Missing split group for grouped execution!");
    auto& splitsState = splitsStates_[planNodeId];
    VELOX_DCHECK_LT(
        splitGroupId,
        splitsState.groupSplitsStores(planFragment_.numSplitGroups).size(),
        "Split group id is greater than number of split groups!");
    checkGroupSplitsCompleteLocked(
        splitGroupId, groupSplitsStoreSafe(splitsState, splitGroupId));
  }
}

void Task::multipleSplitsFinished(int32_t numSplits) {
  std::lock_guard<std::mutex> l(mutex_);
  taskStats_.numFinishedSplits += numSplits;
  taskStats_.numRunningSplits -= numSplits;
}

void Task::checkGroupSplitsCompleteLocked(
    int32_t splitGroupId,
    const SplitsStore& splitsStore) {
  if (splitsStore.splits.empty() and splitsStore.noMoreSplits) {
    taskStats_.completedSplitGroups.emplace(splitGroupId);
  }
}

bool Task::isGroupedExecution() const {
  return planFragment_.isGroupedExecution();
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
  if ((numFinishedDrivers_ == numTotalDrivers_) && state_ == kRunning) {
    state_ = kFinished;
    taskStats_.endTimeMs = getCurrentTimeMs();
    stateChangedLocked();
  }
}

void Task::driverClosedLocked() {
  --numRunningDrivers_;
  ++numFinishedDrivers_;
  if ((numFinishedDrivers_ == numTotalDrivers_) && (state_ == kRunning)) {
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

  const auto numPeers =
      driverFactories_[caller->driverCtx()->pipelineId]->numDrivers;
  if (++state.numRequested == numPeers) {
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
    for (auto& splitGroupState : splitGroupStates_) {
      splitGroupState.bridges.emplace(
          planNodeId, std::make_shared<HashJoinBridge>());
    }
  }
}

void Task::addCrossJoinBridges(
    const std::vector<core::PlanNodeId>& planNodeIds) {
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& planNodeId : planNodeIds) {
    for (auto& splitGroupState : splitGroupStates_) {
      splitGroupState.bridges.emplace(
          planNodeId, std::make_shared<CrossJoinBridge>());
    }
  }
}

std::shared_ptr<HashJoinBridge> Task::getHashJoinBridge(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return getJoinBridgeInternal<HashJoinBridge>(splitGroupId, planNodeId);
}

std::shared_ptr<CrossJoinBridge> Task::getCrossJoinBridge(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return getJoinBridgeInternal<CrossJoinBridge>(splitGroupId, planNodeId);
}

template <class TBridgeType>
std::shared_ptr<TBridgeType> Task::getJoinBridgeInternal(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  std::lock_guard<std::mutex> l(mutex_);
  checkSplitGroupIndex(splitGroupId, "Get join bridge");
  const auto& splitGroupState = splitGroupStates_[splitGroupId];

  auto it = splitGroupState.bridges.find(planNodeId);
  VELOX_CHECK(
      it != splitGroupState.bridges.end(),
      "Join bridge for plan node ID not found: {}",
      planNodeId);
  auto bridge = std::dynamic_pointer_cast<TBridgeType>(it->second);
  VELOX_CHECK_NOT_NULL(
      bridge,
      "Join bridge for plan node ID is of the wrong type: {}",
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

/// Moves split promises from one vector to another.
static void movePromisesOut(
    std::vector<VeloxPromise<bool>>& from,
    std::vector<VeloxPromise<bool>>& to) {
  for (auto& promise : from) {
    to.push_back(std::move(promise));
  }
  from.clear();
}

void Task::terminate(TaskState terminalState) {
  std::vector<std::shared_ptr<Driver>> offThreadDrivers;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (taskStats_.executionEndTimeMs == 0) {
      taskStats_.executionEndTimeMs = getCurrentTimeMs();
    }
    if (state_ != kRunning) {
      return;
    }
    state_ = terminalState;
    // Drivers that are on thread will see this at latest when they go off
    // thread.
    terminateRequested_ = true;
    for (auto& driver : drivers_) {
      if (driver) {
        if (enterForTerminateLocked(driver->state()) ==
            StopReason::kTerminate) {
          offThreadDrivers.push_back(std::move(driver));
          driverClosedLocked();
        }
      }
    }
  }

  // Get the stats and free the resources of Drivers that were not on
  // thread.
  for (auto& driver : offThreadDrivers) {
    driver->closeByTask();
  }

  // We continue all Drivers waiting for promises known to the
  // Task. The Drivers are now detached from Task and therefore will
  // not go on thread. The reference in the future callback is
  // typically the last one.
  if (hasPartitionedOutput_) {
    if (auto bufferManager = bufferManager_.lock()) {
      bufferManager->removeTask(taskId_);
    }
  }
  // Release reference to exchange client, so that it will close exchange
  // sources and prevent resending requests for data.
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<JoinBridge>> oldBridges;
  {
    std::lock_guard<std::mutex> l(mutex_);
    exchangeClients_.clear();

    // Collect all the join bridges to clear them.
    for (auto& splitGroupState : splitGroupStates_) {
      for (auto& pair : splitGroupState.bridges) {
        oldBridges.emplace_back(std::move(pair.second));
      }
      splitGroupState.bridges.clear();
    }

    // Collect all outstanding split promises from all splits state structures.
    for (auto& pair : splitsStates_) {
      movePromisesOut(pair.second.splitsStore.splitPromises, promises);
      for (auto& groupSplitsStore :
           pair.second.groupSplitsStores(planFragment_.numSplitGroups)) {
        movePromisesOut(groupSplitsStore.splitPromises, promises);
      }
    }
  }

  for (auto& promise : promises) {
    promise.setValue(true);
  }
  for (auto& bridge : oldBridges) {
    bridge->cancel();
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
    const auto& splitStore = pair.second.splitsStore;
    out << "Plan Node: " << pair.first << ": " << std::endl;

    out << splitStore.splits.size() << " splits: ";
    int32_t counter = 0;
    for (const auto& split : splitStore.splits) {
      out << split.toString() << " ";
      if (++counter > 4) {
        out << "...";
        break;
      }
    }
    out << std::endl;

    if (splitStore.noMoreSplits) {
      out << "No more splits" << std::endl;
    }

    if (not splitStore.splitPromises.empty()) {
      out << splitStore.splitPromises.size() << " split promises" << std::endl;
    }
  }

  return out.str();
}

void Task::createLocalMergeSources(
    uint32_t splitGroupId,
    unsigned numSources,
    const std::shared_ptr<const RowType>& rowType,
    memory::MappedMemory* mappedMemory) {
  checkSplitGroupIndex(splitGroupId, "Create local merge sources");
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  VELOX_CHECK(
      splitGroupState.localMergeSources.empty(),
      "Multiple local merges in a single task not supported");
  splitGroupState.localMergeSources.reserve(numSources);
  for (auto i = 0; i < numSources; ++i) {
    splitGroupState.localMergeSources.emplace_back(
        MergeSource::createLocalMergeSource(rowType, mappedMemory));
  }
}

std::shared_ptr<MergeSource> Task::getLocalMergeSource(
    uint32_t splitGroupId,
    int sourceId) {
  checkSplitGroupIndex(splitGroupId, "Get local merge source");
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  VELOX_CHECK_LT(
      sourceId,
      splitGroupState.localMergeSources.size(),
      "Incorrect source id ");
  return splitGroupState.localMergeSources[sourceId];
}

void Task::createMergeJoinSource(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  checkSplitGroupIndex(splitGroupId, "Create merge join source");
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  VELOX_CHECK(
      splitGroupState.mergeJoinSources.find(planNodeId) ==
          splitGroupState.mergeJoinSources.end(),
      "Merge join sources already exist: {}",
      planNodeId);

  splitGroupState.mergeJoinSources.insert(
      {planNodeId, std::make_shared<MergeJoinSource>()});
}

std::shared_ptr<MergeJoinSource> Task::getMergeJoinSource(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  checkSplitGroupIndex(splitGroupId, "Get merge join source");
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  auto it = splitGroupState.mergeJoinSources.find(planNodeId);
  VELOX_CHECK(
      it != splitGroupState.mergeJoinSources.end(),
      "Merge join source for specified plan node doesn't exist: {}",
      planNodeId);
  return it->second;
}

void Task::createLocalExchangeSources(
    const core::PlanNodeId& planNodeId,
    int numPartitions) {
  for (auto& splitGroupState : splitGroupStates_) {
    VELOX_CHECK(
        splitGroupState.localExchanges.find(planNodeId) ==
            splitGroupState.localExchanges.end(),
        "Local exchange already exists: {}",
        planNodeId);

    // TODO(spershin): Should we have one memory manager for all local exchanges
    //  in all split groups?
    LocalExchange exchange;
    exchange.memoryManager = std::make_unique<LocalExchangeMemoryManager>(
        queryCtx_->config().maxLocalExchangeBufferSize());

    exchange.sources.reserve(numPartitions);
    for (auto i = 0; i < numPartitions; ++i) {
      exchange.sources.emplace_back(std::make_shared<LocalExchangeSource>(
          exchange.memoryManager.get(), i));
    }

    splitGroupState.localExchanges.insert({planNodeId, std::move(exchange)});
  }
}

void Task::noMoreLocalExchangeProducers(uint32_t splitGroupId) {
  checkSplitGroupIndex(splitGroupId, "No more local exchange producers");
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  for (auto& exchange : splitGroupState.localExchanges) {
    for (auto& source : exchange.second.sources) {
      source->noMoreProducers();
    }
  }
}

std::shared_ptr<LocalExchangeSource> Task::getLocalExchangeSource(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId,
    int partition) {
  const auto& sources = getLocalExchangeSources(splitGroupId, planNodeId);
  VELOX_CHECK_LT(
      partition,
      sources.size(),
      "Incorrect partition for local exchange {}",
      planNodeId);
  return sources[partition];
}

const std::vector<std::shared_ptr<LocalExchangeSource>>&
Task::getLocalExchangeSources(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  checkSplitGroupIndex(splitGroupId, "No more local exchange producers");
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  auto it = splitGroupState.localExchanges.find(planNodeId);
  VELOX_CHECK(
      it != splitGroupState.localExchanges.end(),
      "Incorrect local exchange ID: {}",
      planNodeId);
  return it->second.sources;
}

void Task::setError(const std::exception_ptr& exception) {
  bool isFirstError = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (state_ != kRunning) {
      return;
    }
    if (!exception_) {
      exception_ = exception;
      isFirstError = true;
    }
  }
  if (isFirstError) {
    terminate(kFailed);
  }
  if (isFirstError && onError_) {
    onError_(exception_);
  }
}

void Task::setError(const std::string& message) {
  // The only way to acquire an std::exception_ptr is via throw and
  // std::current_exception().
  try {
    throw std::runtime_error(message);
  } catch (const std::runtime_error& e) {
    setError(std::current_exception());
  }
}

std::string Task::errorMessage() {
  if (!exception_) {
    return "";
  }
  std::string message;
  try {
    std::rethrow_exception(exception_);
  } catch (const std::exception& e) {
    message = e.what();
  }
  return message;
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

StopReason Task::enterForTerminateLocked(ThreadState& state) {
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

StopReason Task::shouldStop() {
  if (terminateRequested_) {
    return StopReason::kTerminate;
  }
  if (pauseRequested_) {
    return StopReason::kPause;
  }
  if (toYield_) {
    std::lock_guard<std::mutex> l(mutex_);
    return shouldStopLocked();
  }
  return StopReason::kNone;
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

void Task::finished() {
  for (auto& promise : finishPromises_) {
    promise.setValue(true);
  }
  finishPromises_.clear();
}

StopReason Task::shouldStopLocked() {
  if (terminateRequested_) {
    return StopReason::kTerminate;
  }
  if (pauseRequested_) {
    return StopReason::kPause;
  }
  if (toYield_) {
    --toYield_;
    return StopReason::kYield;
  }
  return StopReason::kNone;
}

void Task::checkSplitGroupIndex(uint32_t splitGroupId, const char* context) {
  VELOX_DCHECK_LT(
      splitGroupId,
      splitGroupStates_.size(),
      "Invalid split group {} while executing {}",
      splitGroupId,
      context);
}

} // namespace facebook::velox::exec
