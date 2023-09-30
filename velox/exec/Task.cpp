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
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>

#include "velox/codegen/Codegen.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/LocalPlanner.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Merge.h"
#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/Task.h"
#if CODEGEN_ENABLED == 1
#include "velox/experimental/codegen/CodegenLogger.h"
#endif
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

namespace {
// RAII helper class to satisfy given promises and notify listeners of an event
// connected to the promises outside of the mutex that guards the promises.
// Inactive on creation. Must be activated explicitly by calling 'activate'.
class EventCompletionNotifier {
 public:
  /// Calls notify() if it hasn't been called yet.
  ~EventCompletionNotifier() {
    notify();
  }

  // Activates the notifier and provides a callback to invoke and promises to
  // satisfy on destruction or a call to 'notify'.
  void activate(
      std::vector<ContinuePromise> promises,
      std::function<void()> callback = nullptr) {
    active_ = true;
    callback_ = callback;
    promises_ = std::move(promises);
  }

  // Satisfies the promises passed to 'activate' and invokes the callback.
  // Does nothing if 'activate' hasn't been called or 'notify' has been
  // called already.
  void notify() {
    if (active_) {
      for (auto& promise : promises_) {
        promise.setValue();
      }
      promises_.clear();

      if (callback_) {
        callback_();
      }

      active_ = false;
    }
  }

 private:
  bool active_{false};
  std::function<void()> callback_{nullptr};
  std::vector<ContinuePromise> promises_;
};

folly::Synchronized<std::vector<std::shared_ptr<TaskListener>>>& listeners() {
  static folly::Synchronized<std::vector<std::shared_ptr<TaskListener>>>
      kListeners;
  return kListeners;
}

std::string errorMessageImpl(const std::exception_ptr& exception) {
  if (!exception) {
    return "";
  }
  std::string message;
  try {
    std::rethrow_exception(exception);
  } catch (const std::exception& e) {
    message = e.what();
  } catch (...) {
    message = "<Unknown exception type>";
  }
  return message;
}

// Add 'running time' metrics from CpuWallTiming structures to have them
// available aggregated per thread.
void addRunningTimeOperatorMetrics(exec::OperatorStats& op) {
  op.runtimeStats["runningAddInputWallNanos"] =
      RuntimeMetric(op.addInputTiming.wallNanos, RuntimeCounter::Unit::kNanos);
  op.runtimeStats["runningGetOutputWallNanos"] =
      RuntimeMetric(op.getOutputTiming.wallNanos, RuntimeCounter::Unit::kNanos);
  op.runtimeStats["runningFinishWallNanos"] =
      RuntimeMetric(op.finishTiming.wallNanos, RuntimeCounter::Unit::kNanos);
}

void buildSplitStates(
    const core::PlanNode* planNode,
    std::unordered_set<core::PlanNodeId>& allIds,
    std::unordered_map<core::PlanNodeId, SplitsState>& splitStateMap) {
  bool ok = allIds.insert(planNode->id()).second;
  VELOX_USER_CHECK(
      ok,
      "Plan node IDs must be unique. Found duplicate ID: {}.",
      planNode->id());

  // Check if planNode is a leaf node in the plan tree. If so, it is a source
  // node and may use splits for processing.
  if (planNode->sources().empty()) {
    // Not all leaf nodes require splits. ValuesNode doesn't. Check if this plan
    // node requires splits.
    if (planNode->requiresSplits()) {
      splitStateMap[planNode->id()];
    }
    return;
  }

  for (const auto& child : planNode->sources()) {
    buildSplitStates(child.get(), allIds, splitStateMap);
  }
}

// Returns a map of ids of source (leaf) plan nodes expecting splits.
// SplitsState structures are initialized to blank states. Also, checks that
// plan node IDs are unique and throws if encounters duplicates.
std::unordered_map<core::PlanNodeId, SplitsState> buildSplitStates(
    const std::shared_ptr<const core::PlanNode>& planNode) {
  std::unordered_set<core::PlanNodeId> allIds;
  std::unordered_map<core::PlanNodeId, SplitsState> splitStateMap;
  buildSplitStates(planNode.get(), allIds, splitStateMap);
  return splitStateMap;
}

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

// Returns true if an operator is a hash join operator given 'operatorType'.
bool isHashJoinOperator(const std::string& operatorType) {
  return (operatorType == "HashBuild") || (operatorType == "HashProbe");
}
} // namespace

std::string taskStateString(TaskState state) {
  switch (state) {
    case TaskState::kRunning:
      return "Running";
    case TaskState::kFinished:
      return "Finished";
    case TaskState::kCanceled:
      return "Canceled";
    case TaskState::kAborted:
      return "Aborted";
    case TaskState::kFailed:
      return "Failed";
    default:
      return fmt::format("UNKNOWN[{}]", static_cast<int>(state));
  }
}

std::atomic<uint64_t> Task::numCreatedTasks_ = 0;
std::atomic<uint64_t> Task::numDeletedTasks_ = 0;

bool registerTaskListener(std::shared_ptr<TaskListener> listener) {
  return listeners().withWLock([&](auto& listeners) {
    for (const auto& existingListener : listeners) {
      if (existingListener == listener) {
        // Listener already registered. Do not register again.
        return false;
      }
    }
    listeners.push_back(std::move(listener));
    return true;
  });
}

bool unregisterTaskListener(const std::shared_ptr<TaskListener>& listener) {
  return listeners().withWLock([&](auto& listeners) {
    for (auto it = listeners.begin(); it != listeners.end(); ++it) {
      if ((*it) == listener) {
        listeners.erase(it);
        return true;
      }
    }

    // Listener not found.
    return false;
  });
}

// static.
std::shared_ptr<Task> Task::create(
    const std::string& taskId,
    core::PlanFragment planFragment,
    int destination,
    std::shared_ptr<core::QueryCtx> queryCtx,
    Consumer consumer,
    std::function<void(std::exception_ptr)> onError) {
  return Task::create(
      taskId,
      std::move(planFragment),
      destination,
      std::move(queryCtx),
      (consumer ? [c = std::move(consumer)]() { return c; }
                : ConsumerSupplier{}),
      std::move(onError));
}

std::shared_ptr<Task> Task::create(
    const std::string& taskId,
    core::PlanFragment planFragment,
    int destination,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ConsumerSupplier consumerSupplier,
    std::function<void(std::exception_ptr)> onError) {
  auto task = std::shared_ptr<Task>(new Task(
      taskId,
      std::move(planFragment),
      destination,
      std::move(queryCtx),
      std::move(consumerSupplier),
      std::move(onError)));
  task->initTaskPool();
  return task;
}

Task::Task(
    const std::string& taskId,
    core::PlanFragment planFragment,
    int destination,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ConsumerSupplier consumerSupplier,
    std::function<void(std::exception_ptr)> onError)
    : uuid_{makeUuid()},
      taskId_(taskId),
      planFragment_(std::move(planFragment)),
      destination_(destination),
      queryCtx_(std::move(queryCtx)),
      consumerSupplier_(std::move(consumerSupplier)),
      onError_(onError),
      splitsStates_(buildSplitStates(planFragment_.planNode)),
      bufferManager_(PartitionedOutputBufferManager::getInstance()) {}

Task::~Task() {
  try {
    if (hasPartitionedOutput()) {
      if (auto bufferManager = bufferManager_.lock()) {
        bufferManager->removeTask(taskId_);
      }
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Caught exception in Task " << taskId()
                 << " destructor: " << e.what();
  }

  removeSpillDirectoryIfExists();
}

uint64_t Task::timeSinceStartMsLocked() const {
  if (taskStats_.executionStartTimeMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - taskStats_.executionStartTimeMs;
}

SplitsState& Task::getPlanNodeSplitsStateLocked(
    const core::PlanNodeId& planNodeId) {
  auto it = splitsStates_.find(planNodeId);
  if (FOLLY_LIKELY(it != splitsStates_.end())) {
    return it->second;
  }

  VELOX_USER_FAIL(
      "Splits can be associated only with leaf plan nodes which require splits."
      " Plan node ID {} doesn't refer to such plan node.",
      planNodeId);
}

bool Task::allNodesReceivedNoMoreSplitsMessageLocked() const {
  for (const auto& it : splitsStates_) {
    if (not it.second.noMoreSplits) {
      return false;
    }
  }
  return true;
}

void Task::removeSpillDirectoryIfExists() {
  if (!spillDirectory_.empty()) {
    try {
      auto fs = filesystems::getFileSystem(spillDirectory_, nullptr);
      fs->rmdir(spillDirectory_);
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to remove spill directory '" << spillDirectory_
                 << "' for Task " << taskId() << ": " << e.what();
    }
  }
}

void Task::initTaskPool() {
  VELOX_CHECK_NULL(pool_);
  pool_ = queryCtx_->pool()->addAggregateChild(
      fmt::format("task.{}", taskId_.c_str()), createTaskReclaimer());
}

velox::memory::MemoryPool* Task::getOrAddNodePool(
    const core::PlanNodeId& planNodeId,
    bool isHashJoinNode) {
  if (nodePools_.count(planNodeId) == 1) {
    return nodePools_[planNodeId];
  }
  childPools_.push_back(pool_->addAggregateChild(
      fmt::format("node.{}", planNodeId), createNodeReclaimer(isHashJoinNode)));
  auto* nodePool = childPools_.back().get();
  nodePools_[planNodeId] = nodePool;
  return nodePool;
}

std::unique_ptr<memory::MemoryReclaimer> Task::createNodeReclaimer(
    bool isHashJoinNode) const {
  if (pool()->reclaimer() == nullptr) {
    return nullptr;
  }
  // Sets memory reclaimer for the parent node memory pool on the first child
  // operator construction which has set memory reclaimer.
  return isHashJoinNode ? HashJoinMemoryReclaimer::create()
                        : exec::MemoryReclaimer::create();
}

std::unique_ptr<memory::MemoryReclaimer> Task::createExchangeClientReclaimer()
    const {
  if (pool()->reclaimer() == nullptr) {
    return nullptr;
  }
  return exec::MemoryReclaimer::create();
}

std::unique_ptr<memory::MemoryReclaimer> Task::createTaskReclaimer() {
  // We shall only create the task memory reclaimer once on task memory pool
  // creation.
  VELOX_CHECK_NULL(pool_);
  if (queryCtx_->pool()->reclaimer() == nullptr) {
    return nullptr;
  }
  return Task::MemoryReclaimer::create(shared_from_this());
}

velox::memory::MemoryPool* Task::addOperatorPool(
    const core::PlanNodeId& planNodeId,
    int pipelineId,
    uint32_t driverId,
    const std::string& operatorType) {
  auto* nodePool =
      getOrAddNodePool(planNodeId, isHashJoinOperator(operatorType));
  childPools_.push_back(nodePool->addLeafChild(fmt::format(
      "op.{}.{}.{}.{}", planNodeId, pipelineId, driverId, operatorType)));
  return childPools_.back().get();
}

velox::memory::MemoryPool* Task::addConnectorPoolLocked(
    const core::PlanNodeId& planNodeId,
    int pipelineId,
    uint32_t driverId,
    const std::string& operatorType,
    const std::string& connectorId) {
  auto* nodePool = getOrAddNodePool(planNodeId);
  childPools_.push_back(nodePool->addAggregateChild(fmt::format(
      "op.{}.{}.{}.{}.{}",
      planNodeId,
      pipelineId,
      driverId,
      operatorType,
      connectorId)));
  return childPools_.back().get();
}

velox::memory::MemoryPool* Task::addMergeSourcePool(
    const core::PlanNodeId& planNodeId,
    uint32_t pipelineId,
    uint32_t sourceId) {
  std::lock_guard<std::mutex> l(mutex_);
  auto* nodePool = getOrAddNodePool(planNodeId);
  childPools_.push_back(nodePool->addLeafChild(
      fmt::format(
          "mergeExchangeClient.{}.{}.{}", planNodeId, pipelineId, sourceId),
      true,
      createExchangeClientReclaimer()));
  return childPools_.back().get();
}

velox::memory::MemoryPool* Task::addExchangeClientPool(
    const core::PlanNodeId& planNodeId,
    uint32_t pipelineId) {
  auto* nodePool = getOrAddNodePool(planNodeId);
  childPools_.push_back(nodePool->addLeafChild(
      fmt::format("exchangeClient.{}.{}", planNodeId, pipelineId),
      true,
      createExchangeClientReclaimer()));
  return childPools_.back().get();
}

bool Task::supportsSingleThreadedExecution() const {
  if (consumerSupplier_) {
    return false;
  }

  std::vector<std::unique_ptr<DriverFactory>> driverFactories;
  LocalPlanner::plan(
      planFragment_, nullptr, &driverFactories, queryCtx_->queryConfig(), 1);

  for (const auto& factory : driverFactories) {
    if (!factory->supportsSingleThreadedExecution()) {
      return false;
    }
  }

  return true;
}

RowVectorPtr Task::next(ContinueFuture* future) {
  // NOTE: Task::next() is single-threaded execution so locking is not required
  // to access Task object.
  VELOX_CHECK_EQ(
      core::ExecutionStrategy::kUngrouped,
      planFragment_.executionStrategy,
      "Single-threaded execution supports only ungrouped execution");

  if (!splitsStates_.empty()) {
    for (const auto& it : splitsStates_) {
      VELOX_CHECK(
          it.second.noMoreSplits,
          "Single-threaded execution requires all splits to be added before "
          "calling Task::next().");
    }
  }

  VELOX_CHECK_EQ(state_, kRunning, "Task has already finished processing.");

  // On first call, create the drivers.
  if (driverFactories_.empty()) {
    VELOX_CHECK_NULL(
        consumerSupplier_,
        "Single-threaded execution doesn't support delivering results to a "
        "callback");

    LocalPlanner::plan(
        planFragment_, nullptr, &driverFactories_, queryCtx_->queryConfig(), 1);
    exchangeClients_.resize(driverFactories_.size());

    // In Task::next() we always assume ungrouped execution.
    for (const auto& factory : driverFactories_) {
      VELOX_CHECK(factory->supportsSingleThreadedExecution());
      numDriversUngrouped_ += factory->numDrivers;
      numTotalDrivers_ += factory->numTotalDrivers;
      taskStats_.pipelineStats.emplace_back(
          factory->inputDriver, factory->outputDriver);
    }

    // Create drivers.
    auto self = shared_from_this();
    std::vector<std::shared_ptr<Driver>> drivers;
    drivers.reserve(numDriversUngrouped_);
    createSplitGroupStateLocked(kUngroupedGroupId);
    createDriversLocked(self, kUngroupedGroupId, drivers);
    if (self->pool_->stats().currentBytes != 0) {
      VELOX_FAIL(
          "Unexpected memory pool allocations during task[{}] driver initialization: {}",
          self->taskId_,
          self->pool_->treeMemoryUsage());
    }

    drivers_ = std::move(drivers);
  }

  // Run drivers one at a time. If a driver blocks, continue running the other
  // drivers. Running other drivers is expected to unblock some or all blocked
  // drivers.
  const auto numDrivers = drivers_.size();

  std::vector<ContinueFuture> futures;
  futures.resize(numDrivers);

  for (;;) {
    int runnableDrivers = 0;
    int blockedDrivers = 0;
    for (auto i = 0; i < numDrivers; ++i) {
      if (drivers_[i] == nullptr) {
        // This driver has finished processing.
        continue;
      }

      if (!futures[i].isReady()) {
        // This driver is still blocked.
        ++blockedDrivers;
        continue;
      }

      ++runnableDrivers;

      std::shared_ptr<BlockingState> blockingState;
      auto result = drivers_[i]->next(blockingState);
      if (result) {
        return result;
      }

      if (blockingState) {
        futures[i] = blockingState->future();
      }

      if (error()) {
        std::rethrow_exception(error());
      }
    }

    if (runnableDrivers == 0) {
      if (blockedDrivers > 0) {
        if (!future) {
          VELOX_FAIL(
              "Cannot make progress as all remaining drivers are blocked and user are not expected to wait.");
        } else {
          std::vector<ContinueFuture> notReadyFutures;
          for (auto& continueFuture : futures) {
            if (!continueFuture.isReady()) {
              notReadyFutures.emplace_back(std::move(continueFuture));
            }
          }
          *future = folly::collectAll(std::move(notReadyFutures)).unit();
        }
      }
      return nullptr;
    }
  }
}

// static
void Task::start(
    std::shared_ptr<Task> self,
    uint32_t maxDrivers,
    uint32_t concurrentSplitGroups) {
  facebook::velox::process::ThreadDebugInfo threadDebugInfo{
      self->queryCtx()->queryId(), self->taskId_, nullptr};
  facebook::velox::process::ScopedThreadDebugInfo scopedInfo(threadDebugInfo);
  try {
    VELOX_CHECK_GE(
        maxDrivers,
        1,
        "maxDrivers parameter must be greater then or equal to 1");
    VELOX_CHECK_GE(
        concurrentSplitGroups,
        1,
        "concurrentSplitGroups parameter must be greater then or equal to 1");

    uint32_t numPipelines;
    {
      std::unique_lock<std::mutex> l(self->mutex_);
      VELOX_CHECK(self->drivers_.empty());
      self->concurrentSplitGroups_ = concurrentSplitGroups;
      self->taskStats_.executionStartTimeMs = getCurrentTimeMs();

#if CODEGEN_ENABLED == 1
      const auto& config = self->queryCtx()->queryConfig();
      if (config.codegenEnabled() &&
          config.codegenConfigurationFilePath().length() != 0) {
        auto codegenLogger =
            std::make_shared<codegen::DefaultLogger>(self->taskId_);
        auto codegen = codegen::Codegen(codegenLogger);
        auto lazyLoading = config.codegenLazyLoading();
        codegen.initializeFromFile(
            config.codegenConfigurationFilePath(), lazyLoading);
        if (auto newPlanNode =
                codegen.compile(*(self->planFragment_.planNode))) {
          self->planFragment_.planNode = newPlanNode;
        }
      }
#endif

      // Here we create driver factories.
      LocalPlanner::plan(
          self->planFragment_,
          self->consumerSupplier(),
          &self->driverFactories_,
          self->queryCtx_->queryConfig(),
          maxDrivers);

      // Keep one exchange client per pipeline (NULL if not used).
      numPipelines = self->driverFactories_.size();
      self->exchangeClients_.resize(numPipelines);

      // For each pipeline we have a corresponding driver factory.
      // Here we count how many drivers in total we need and create
      // pipeline stats.
      for (auto& factory : self->driverFactories_) {
        if (factory->groupedExecution) {
          self->numDriversPerSplitGroup_ += factory->numDrivers;
        } else {
          self->numDriversUngrouped_ += factory->numDrivers;
        }
        self->numTotalDrivers_ += factory->numTotalDrivers;
        self->taskStats_.pipelineStats.emplace_back(
            factory->inputDriver, factory->outputDriver);
      }

      self->validateGroupedExecutionLeafNodes();
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

    // In this loop we prepare the global state of pipelines: partitioned output
    // buffer and exchange client(s).
    for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
      auto& factory = self->driverFactories_[pipeline];

      if (auto partitionedOutputNode = factory->needsPartitionedOutput()) {
        VELOX_CHECK(
            !self->hasPartitionedOutput(),
            "Only one output pipeline per task is supported");
        self->numDriversInPartitionedOutput_ = factory->numDrivers;
        self->groupedPartitionedOutput_ = factory->groupedExecution;
        const auto totalOutputDrivers = factory->groupedExecution
            ? factory->numDrivers * self->planFragment_.numSplitGroups
            : factory->numDrivers;
        bufferManager->initializeTask(
            self,
            partitionedOutputNode->kind(),
            partitionedOutputNode->numPartitions(),
            totalOutputDrivers);
      }

      // NOTE: MergeExchangeNode doesn't use the exchange client created here to
      // fetch data from the merge source but only uses it to send abortResults
      // to the merge source of the split which is added after the task has
      // failed. Correspondingly, MergeExchangeNode creates one exchange client
      // for each merge source to fetch data as we can't mix the data from
      // different sources for merging.
      if (auto exchangeNodeId = factory->needsExchangeClient()) {
        self->createExchangeClient(pipeline, exchangeNodeId.value());
      }
    }

    std::unique_lock<std::mutex> l(self->mutex_);

    // Preallocate a bunch of slots for max concurrent grouped execution
    // drivers, if needed.
    if (self->numDriversPerSplitGroup_ > 0) {
      self->drivers_.resize(
          self->numDriversPerSplitGroup_ * self->concurrentSplitGroups_);
    }

    // We create the drivers running pipelines in ungrouped execution mode
    // first.
    if (self->numDriversUngrouped_ > 0) {
      // Create the drivers we are going to run for this task.
      std::vector<std::shared_ptr<Driver>> drivers;
      drivers.reserve(self->numDriversUngrouped_);
      self->createSplitGroupStateLocked(kUngroupedGroupId);
      self->createDriversLocked(self, kUngroupedGroupId, drivers);
      if (self->pool_->stats().currentBytes != 0) {
        VELOX_FAIL(
            "Unexpected memory pool allocations during task[{}] driver initialization: {}",
            self->taskId_,
            self->pool_->treeMemoryUsage());
      }

      // Prevent the connecting structures from being cleaned up before all
      // split groups are finished during the grouped execution mode.
      if (self->isGroupedExecution()) {
        self->splitGroupStates_[kUngroupedGroupId].mixedExecutionMode = true;
      }

      // We might have first slots taken for grouped execution drivers, so need
      // to append the ungrouped execution drivers afterwards in that case.
      if (self->drivers_.empty()) {
        self->drivers_ = std::move(drivers);
      } else {
        self->drivers_.reserve(
            self->drivers_.size() + self->numDriversUngrouped_);
        for (auto& driver : drivers) {
          self->drivers_.emplace_back(std::move(driver));
        }
      }

      // Set and start all Drivers together inside 'mutex_' so that
      // cancellations and pauses have well defined timing. For example, do not
      // pause and restart a task while it is still adding Drivers. If the given
      // executor is folly::InlineLikeExecutor (or it's child), since the
      // drivers will be executed synchronously on the same thread as the
      // current task, so we need release the lock to avoid the deadlock.
      if (dynamic_cast<const folly::InlineLikeExecutor*>(
              self->queryCtx()->executor())) {
        l.unlock();
      }
      // We might have first slots taken for grouped execution drivers, so need
      // only to enqueue the ungrouped execution drivers.
      for (auto it = self->drivers_.end() - self->numDriversUngrouped_;
           it != self->drivers_.end();
           ++it) {
        if (*it) {
          ++self->numRunningDrivers_;
          Driver::enqueue(*it);
        }
      }
    }

    // As some splits for grouped execution could have been added before the
    // task start, ensure we start running drivers for them.
    if (self->numDriversPerSplitGroup_ > 0) {
      if (!l.owns_lock()) {
        l.lock();
      }
      self->ensureSplitGroupsAreBeingProcessedLocked(self);
    }

  } catch (const std::exception& e) {
    self->setError(std::current_exception());
    throw;
  }
}

// static
void Task::resume(std::shared_ptr<Task> self) {
  std::vector<std::shared_ptr<Driver>> offThreadDrivers;
  {
    std::lock_guard<std::mutex> l(self->mutex_);
    // Setting pause requested must be atomic with the resuming so that
    // suspended sections do not go back on thread during resume.
    self->pauseRequested_ = false;
    if (!self->exception_) {
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
    } else {
      // NOTE: no need to resume task execution if the task has been terminated.
      // But we need to close the drivers which are off threads as task
      // terminate code path skips closing the off thread drivers if the task
      // has been requested pause and leave the task resume path to handle. If
      // a task has been paused, then there might be concurrent memory
      // arbitration thread to reclaim the memory resource from the off thread
      // driver operators.
      for (auto& driver : self->drivers_) {
        if (driver == nullptr) {
          continue;
        }
        if (driver->isOnThread()) {
          VELOX_CHECK(driver->isTerminated());
          continue;
        }
        if (driver->isTerminated()) {
          continue;
        }
        driver->state().isTerminated = true;
        driver->state().setThread();
        self->driverClosedLocked();
        offThreadDrivers.push_back(std::move(driver));
      }
    }
  }

  // Get the stats and free the resources of Drivers that were not on thread.
  for (auto& driver : offThreadDrivers) {
    driver->closeByTask();
  }
}

void Task::validateGroupedExecutionLeafNodes() {
  if (isGroupedExecution()) {
    VELOX_USER_CHECK(
        !planFragment_.groupedExecutionLeafNodeIds.empty(),
        "groupedExecutionLeafNodeIds must not be empty in "
        "grouped execution mode");
    // Check that each node designated as the grouped execution leaf node
    // existing in a pipeline that will run grouped execution.
    for (const auto& leafNodeId : planFragment_.groupedExecutionLeafNodeIds) {
      bool found{false};
      for (auto& factory : driverFactories_) {
        if (leafNodeId == factory->leafNodeId()) {
          VELOX_USER_CHECK(
              factory->inputDriver,
              "Grouped execution leaf node {} not found "
              "or it is not a leaf node",
              leafNodeId);
          found = true;
          break;
        }
      }
      VELOX_USER_CHECK(
          found,
          "Grouped execution leaf node {} is not a leaf node in "
          "any pipeline",
          leafNodeId);
    }
  } else {
    VELOX_USER_CHECK(
        planFragment_.groupedExecutionLeafNodeIds.empty(),
        "groupedExecutionLeafNodeIds must be empty in "
        "ungrouped execution mode");
  }
}

void Task::createSplitGroupStateLocked(uint32_t splitGroupId) {
  const bool groupedExecutionDrivers = (splitGroupId != kUngroupedGroupId);
  // In this loop we prepare per split group pipelines structures:
  // local exchanges and join bridges.
  const auto numPipelines = driverFactories_.size();
  for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
    auto& factory = driverFactories_[pipeline];
    // We either create states for grouped execution or ungrouped.
    if (factory->groupedExecution != groupedExecutionDrivers) {
      continue;
    }

    auto exchangeId = factory->needsLocalExchange();
    if (exchangeId.has_value()) {
      createLocalExchangeQueuesLocked(
          splitGroupId, exchangeId.value(), factory->numDrivers);
    }

    addHashJoinBridgesLocked(splitGroupId, factory->needsHashJoinBridges());
    addNestedLoopJoinBridgesLocked(
        splitGroupId, factory->needsNestedLoopJoinBridges());
    addCustomJoinBridgesLocked(splitGroupId, factory->planNodes);
  }
}

void Task::createDriversLocked(
    std::shared_ptr<Task>& self,
    uint32_t splitGroupId,
    std::vector<std::shared_ptr<Driver>>& out) {
  TestValue::adjust("facebook::velox::exec::Task::createDriversLocked", this);
  const bool groupedExecutionDrivers = (splitGroupId != kUngroupedGroupId);
  auto& splitGroupState = self->splitGroupStates_[splitGroupId];
  const auto numPipelines = driverFactories_.size();

  for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
    auto& factory = driverFactories_[pipeline];
    // We either create drivers for grouped execution or ungrouped.
    if (factory->groupedExecution != groupedExecutionDrivers) {
      continue;
    }

    // In each pipeline we start drivers id from zero or, in case of grouped
    // execution, from the split group id.
    const uint32_t driverIdOffset =
        factory->numDrivers * (groupedExecutionDrivers ? splitGroupId : 0);
    for (uint32_t partitionId = 0; partitionId < factory->numDrivers;
         ++partitionId) {
      out.emplace_back(factory->createDriver(
          std::make_unique<DriverCtx>(
              self,
              driverIdOffset + partitionId,
              pipeline,
              splitGroupId,
              partitionId),
          getExchangeClientLocked(pipeline),
          [self](size_t i) {
            return i < self->driverFactories_.size()
                ? self->driverFactories_[i]->numTotalDrivers
                : 0;
          }));
      ++splitGroupState.numRunningDrivers;
    }
  }
  noMoreLocalExchangeProducers(splitGroupId);
  if (groupedExecutionDrivers) {
    ++numRunningSplitGroups_;
  }

  // Initialize operator stats using the 1st driver of each operator.
  // We create drivers for grouped and ungrouped execution separately, so we
  // need to track down initialization of operator stats separately as well.
  if ((groupedExecutionDrivers & !initializedGroupedOpStats_) ||
      (!groupedExecutionDrivers & !initializedUngroupedOpStats_)) {
    (groupedExecutionDrivers ? initializedGroupedOpStats_
                             : initializedUngroupedOpStats_) = true;
    size_t firstPipelineDriverIndex{0};
    for (auto pipeline = 0; pipeline < numPipelines; ++pipeline) {
      auto& factory = driverFactories_[pipeline];
      if (factory->groupedExecution == groupedExecutionDrivers) {
        out[firstPipelineDriverIndex]->initializeOperatorStats(
            taskStats_.pipelineStats[pipeline].operatorStats);
        firstPipelineDriverIndex += factory->numDrivers;
      }
    }
  }

  // Start all the join bridges before we start driver execution.
  for (auto& bridgeEntry : splitGroupState.bridges) {
    bridgeEntry.second->start();
  }

  // Start all the spill groups before we start the driver execution.
  for (auto& coordinatorEntry : splitGroupState.spillOperatorGroups) {
    coordinatorEntry.second->start();
  }
}

// static
void Task::removeDriver(std::shared_ptr<Task> self, Driver* driver) {
  bool foundDriver = false;
  bool allFinished = true;
  EventCompletionNotifier stateChangeNotifier;
  {
    std::lock_guard<std::mutex> taskLock(self->mutex_);
    for (auto& driverPtr : self->drivers_) {
      if (driverPtr.get() != driver) {
        continue;
      }

      // Mark the closure of another driver for its split group (even in
      // ungrouped execution mode).
      const auto splitGroupId = driver->driverCtx()->splitGroupId;
      auto& splitGroupState = self->splitGroupStates_[splitGroupId];
      --splitGroupState.numRunningDrivers;

      auto pipelineId = driver->driverCtx()->pipelineId;

      if (self->isOutputPipeline(pipelineId)) {
        ++splitGroupState.numFinishedOutputDrivers;
      }

      // Release the driver, note that after this 'driver' is invalid.
      driverPtr = nullptr;
      self->driverClosedLocked();

      allFinished = self->checkIfFinishedLocked();

      // Check if a split group is finished.
      if (splitGroupState.numRunningDrivers == 0) {
        if (splitGroupId != kUngroupedGroupId) {
          --self->numRunningSplitGroups_;
          self->taskStats_.completedSplitGroups.emplace(splitGroupId);
          stateChangeNotifier.activate(std::move(self->stateChangePromises_));
          splitGroupState.clear();
          self->ensureSplitGroupsAreBeingProcessedLocked(self);
        } else {
          splitGroupState.clear();
        }
      }
      foundDriver = true;
      break;
    }

    if (self->numFinishedDrivers_ == self->numTotalDrivers_) {
      LOG(INFO) << "All drivers (" << self->numFinishedDrivers_
                << ") finished for task " << self->taskId()
                << " after running for " << self->timeSinceStartMsLocked()
                << " ms.";
    }
  }
  stateChangeNotifier.notify();

  if (!foundDriver) {
    LOG(WARNING) << "Trying to remove a Driver twice from its Task";
  }

  if (allFinished) {
    self->terminate(TaskState::kFinished);
  }
}

void Task::ensureSplitGroupsAreBeingProcessedLocked(
    std::shared_ptr<Task>& self) {
  // Only try creating more drivers if we are running.
  if (not isRunningLocked() or (numDriversPerSplitGroup_ == 0)) {
    return;
  }

  while (numRunningSplitGroups_ < concurrentSplitGroups_ and
         not queuedSplitGroups_.empty()) {
    const uint32_t splitGroupId = queuedSplitGroups_.front();
    queuedSplitGroups_.pop();

    std::vector<std::shared_ptr<Driver>> drivers;
    drivers.reserve(numDriversPerSplitGroup_);
    createSplitGroupStateLocked(splitGroupId);
    createDriversLocked(self, splitGroupId, drivers);
    // Move created drivers into the vacant spots in 'drivers_' and enqueue
    // them. We have vacant spots, because we initially allocate enough items in
    // the vector and keep null pointers for completed drivers.
    size_t i = 0;
    for (auto& newDriverPtr : drivers) {
      while (drivers_[i] != nullptr) {
        VELOX_CHECK_LT(i, drivers_.size());
        ++i;
      }
      auto& targetPtr = drivers_[i];
      targetPtr = std::move(newDriverPtr);
      if (targetPtr) {
        ++numRunningDrivers_;
        Driver::enqueue(targetPtr);
      }
    }
  }
}

void Task::setMaxSplitSequenceId(
    const core::PlanNodeId& planNodeId,
    long maxSequenceId) {
  std::lock_guard<std::mutex> l(mutex_);
  if (isRunningLocked()) {
    auto& splitsState = getPlanNodeSplitsStateLocked(planNodeId);
    // We could have been sent an old split again, so only change max id, when
    // the new one is greater.
    splitsState.maxSequenceId =
        std::max(splitsState.maxSequenceId, maxSequenceId);
  }
}

bool Task::addSplitWithSequence(
    const core::PlanNodeId& planNodeId,
    exec::Split&& split,
    long sequenceId) {
  std::unique_ptr<ContinuePromise> promise;
  bool added = false;
  bool isTaskRunning;
  {
    std::lock_guard<std::mutex> l(mutex_);
    isTaskRunning = isRunningLocked();
    if (isTaskRunning) {
      // The same split can be added again in some systems. The systems that
      // want 'one split processed once only' would use this method and
      // duplicate splits would be ignored.
      auto& splitsState = getPlanNodeSplitsStateLocked(planNodeId);
      if (sequenceId > splitsState.maxSequenceId) {
        promise = addSplitLocked(splitsState, std::move(split));
        added = true;
      }
    }
  }

  if (promise) {
    promise->setValue();
  }

  if (!isTaskRunning) {
    addRemoteSplit(planNodeId, split);
  }

  return added;
}

void Task::addSplit(const core::PlanNodeId& planNodeId, exec::Split&& split) {
  bool isTaskRunning;
  std::unique_ptr<ContinuePromise> promise;
  {
    std::lock_guard<std::mutex> l(mutex_);
    isTaskRunning = isRunningLocked();
    if (isTaskRunning) {
      promise = addSplitLocked(
          getPlanNodeSplitsStateLocked(planNodeId), std::move(split));
    }
  }

  if (promise) {
    promise->setValue();
  }

  if (!isTaskRunning) {
    // Safe because 'split' is moved away above only if 'isTaskRunning'.
    addRemoteSplit(planNodeId, split);
  }
}

void Task::addRemoteSplit(
    const core::PlanNodeId& planNodeId,
    const exec::Split& split) {
  if (split.hasConnectorSplit()) {
    if (exchangeClientByPlanNode_.count(planNodeId)) {
      auto remoteSplit =
          std::dynamic_pointer_cast<RemoteConnectorSplit>(split.connectorSplit);
      VELOX_CHECK(remoteSplit, "Wrong type of split");
      exchangeClientByPlanNode_[planNodeId]->addRemoteTaskId(
          remoteSplit->taskId);
    }
  }
}

std::unique_ptr<ContinuePromise> Task::addSplitLocked(
    SplitsState& splitsState,
    exec::Split&& split) {
  ++taskStats_.numTotalSplits;
  ++taskStats_.numQueuedSplits;

  if (split.connectorSplit) {
    VELOX_CHECK_NULL(split.connectorSplit->dataSource);
  }

  if (!split.hasGroup()) {
    return addSplitToStoreLocked(
        splitsState.groupSplitsStores[kUngroupedGroupId], std::move(split));
  }

  const auto splitGroupId = split.groupId;
  // If this is the 1st split from this group, add the split group to queue.
  // Also add that split group to the set of 'seen' split groups.
  if (seenSplitGroups_.find(splitGroupId) == seenSplitGroups_.end()) {
    seenSplitGroups_.emplace(splitGroupId);
    queuedSplitGroups_.push(splitGroupId);
    auto self = shared_from_this();
    // We might have some free driver slots to process this split group.
    ensureSplitGroupsAreBeingProcessedLocked(self);
  }
  return addSplitToStoreLocked(
      splitsState.groupSplitsStores[splitGroupId], std::move(split));
}

std::unique_ptr<ContinuePromise> Task::addSplitToStoreLocked(
    SplitsStore& splitsStore,
    exec::Split&& split) {
  splitsStore.splits.push_back(split);
  if (splitsStore.splitPromises.empty()) {
    return nullptr;
  }
  auto promise = std::make_unique<ContinuePromise>(
      std::move(splitsStore.splitPromises.back()));
  splitsStore.splitPromises.pop_back();
  return promise;
}

void Task::noMoreSplitsForGroup(
    const core::PlanNodeId& planNodeId,
    int32_t splitGroupId) {
  std::vector<ContinuePromise> promises;
  EventCompletionNotifier stateChangeNotifier;
  {
    std::lock_guard<std::mutex> l(mutex_);

    auto& splitsState = getPlanNodeSplitsStateLocked(planNodeId);
    auto& splitsStore = splitsState.groupSplitsStores[splitGroupId];
    splitsStore.noMoreSplits = true;
    promises = std::move(splitsStore.splitPromises);

    // There were no splits in this group, hence, no active drivers. Mark the
    // group complete.
    if (seenSplitGroups_.count(splitGroupId) == 0) {
      taskStats_.completedSplitGroups.insert(splitGroupId);
      stateChangeNotifier.activate(std::move(stateChangePromises_));
    }
  }
  stateChangeNotifier.notify();
  for (auto& promise : promises) {
    promise.setValue();
  }
}

void Task::noMoreSplits(const core::PlanNodeId& planNodeId) {
  std::vector<ContinuePromise> splitPromises;
  bool allFinished;
  std::shared_ptr<ExchangeClient> exchangeClient;
  {
    std::lock_guard<std::mutex> l(mutex_);

    // Global 'no more splits' for a plan node comes in case of ungrouped
    // execution when no more splits will arrive. For grouped execution it
    // comes when no more split groups will arrive for that plan node.
    auto& splitsState = getPlanNodeSplitsStateLocked(planNodeId);
    splitsState.noMoreSplits = true;
    if (not splitsState.groupSplitsStores.empty()) {
      // Mark all split stores as 'no more splits'.
      for (auto& it : splitsState.groupSplitsStores) {
        it.second.noMoreSplits = true;
        splitPromises = std::move(it.second.splitPromises);
      }
    } else if (!planFragment_.leafNodeRunsGroupedExecution(planNodeId)) {
      // During ungrouped execution, in the unlikely case there are no split
      // stores (this means there were no splits at all), we create one.
      splitsState.groupSplitsStores.emplace(
          kUngroupedGroupId, SplitsStore{{}, true, {}});
    }

    allFinished = checkNoMoreSplitGroupsLocked();

    if (!isRunningLocked()) {
      exchangeClient = getExchangeClientLocked(planNodeId);
    }
  }

  for (auto& promise : splitPromises) {
    promise.setValue();
  }

  if (exchangeClient != nullptr) {
    exchangeClient->noMoreRemoteTasks();
  }

  if (allFinished) {
    terminate(kFinished);
  }
}

bool Task::checkNoMoreSplitGroupsLocked() {
  if (isUngroupedExecution()) {
    return false;
  }

  // For grouped execution, when all plan nodes have 'no more splits' coming,
  // we should review the total number of drivers, which initially is set to
  // process all split groups, but in reality workers share split groups and
  // each worker processes only a part of them, meaning much less than all.
  if (allNodesReceivedNoMoreSplitsMessageLocked()) {
    numTotalDrivers_ = seenSplitGroups_.size() * numDriversPerSplitGroup_ +
        numDriversUngrouped_;
    if (groupedPartitionedOutput_) {
      auto bufferManager = bufferManager_.lock();
      bufferManager->updateNumDrivers(
          taskId(), numDriversInPartitionedOutput_ * seenSplitGroups_.size());
    }

    return checkIfFinishedLocked();
  }

  return false;
}

bool Task::isAllSplitsFinishedLocked() {
  return (taskStats_.numFinishedSplits == taskStats_.numTotalSplits) &&
      allNodesReceivedNoMoreSplitsMessageLocked();
}

BlockingReason Task::getSplitOrFuture(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId,
    exec::Split& split,
    ContinueFuture& future,
    int32_t maxPreloadSplits,
    std::function<void(std::shared_ptr<connector::ConnectorSplit>)> preload) {
  std::lock_guard<std::mutex> l(mutex_);
  return getSplitOrFutureLocked(
      getPlanNodeSplitsStateLocked(planNodeId).groupSplitsStores[splitGroupId],
      split,
      future,
      maxPreloadSplits,
      preload);
}

BlockingReason Task::getSplitOrFutureLocked(
    SplitsStore& splitsStore,
    exec::Split& split,
    ContinueFuture& future,
    int32_t maxPreloadSplits,
    std::function<void(std::shared_ptr<connector::ConnectorSplit>)> preload) {
  if (splitsStore.splits.empty()) {
    if (splitsStore.noMoreSplits) {
      return BlockingReason::kNotBlocked;
    }
    auto [splitPromise, splitFuture] = makeVeloxContinuePromiseContract(
        fmt::format("Task::getSplitOrFuture {}", taskId_));
    future = std::move(splitFuture);
    splitsStore.splitPromises.push_back(std::move(splitPromise));
    return BlockingReason::kWaitForSplit;
  }

  split = getSplitLocked(splitsStore, maxPreloadSplits, preload);
  return BlockingReason::kNotBlocked;
}

exec::Split Task::getSplitLocked(
    SplitsStore& splitsStore,
    int32_t maxPreloadSplits,
    std::function<void(std::shared_ptr<connector::ConnectorSplit>)> preload) {
  int32_t readySplitIndex = -1;
  if (maxPreloadSplits) {
    for (auto i = 0; i < splitsStore.splits.size() && i < maxPreloadSplits;
         ++i) {
      auto& split = splitsStore.splits[i].connectorSplit;
      if (!split->dataSource) {
        // Initializes split->dataSource
        preload(split);
      } else if (readySplitIndex == -1 && split->dataSource->hasValue()) {
        readySplitIndex = i;
      }
    }
  }
  if (readySplitIndex == -1) {
    readySplitIndex = 0;
  }
  assert(!splitsStore.splits.empty());
  auto split = std::move(splitsStore.splits[readySplitIndex]);
  splitsStore.splits.erase(splitsStore.splits.begin() + readySplitIndex);

  --taskStats_.numQueuedSplits;
  ++taskStats_.numRunningSplits;
  taskStats_.lastSplitStartTimeMs = getCurrentTimeMs();
  if (taskStats_.firstSplitStartTimeMs == 0) {
    taskStats_.firstSplitStartTimeMs = taskStats_.lastSplitStartTimeMs;
  }

  return split;
}

void Task::splitFinished() {
  std::lock_guard<std::mutex> l(mutex_);
  ++taskStats_.numFinishedSplits;
  --taskStats_.numRunningSplits;
  if (isAllSplitsFinishedLocked()) {
    taskStats_.executionEndTimeMs = getCurrentTimeMs();
  }
}

void Task::multipleSplitsFinished(int32_t numSplits) {
  std::lock_guard<std::mutex> l(mutex_);
  taskStats_.numFinishedSplits += numSplits;
  taskStats_.numRunningSplits -= numSplits;
  if (isAllSplitsFinishedLocked()) {
    taskStats_.executionEndTimeMs = getCurrentTimeMs();
  }
}

bool Task::isGroupedExecution() const {
  return planFragment_.isGroupedExecution();
}

bool Task::isUngroupedExecution() const {
  return not isGroupedExecution();
}

bool Task::isRunning() const {
  std::lock_guard<std::mutex> l(mutex_);
  return (state_ == TaskState::kRunning);
}

bool Task::isFinished() const {
  std::lock_guard<std::mutex> l(mutex_);
  return (state_ == TaskState::kFinished);
}

bool Task::isRunningLocked() const {
  return (state_ == TaskState::kRunning);
}

bool Task::isFinishedLocked() const {
  return (state_ == TaskState::kFinished);
}

bool Task::updateOutputBuffers(int numBuffers, bool noMoreBuffers) {
  auto bufferManager = bufferManager_.lock();
  VELOX_CHECK_NOT_NULL(
      bufferManager,
      "Unable to initialize task. "
      "PartitionedOutputBufferManager was already destructed");
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (noMoreOutputBuffers_) {
      // Ignore messages received after no-more-buffers message.
      return false;
    }
    if (noMoreBuffers) {
      noMoreOutputBuffers_ = true;
    }
  }
  return bufferManager->updateOutputBuffers(taskId_, numBuffers, noMoreBuffers);
}

int Task::getOutputPipelineId() const {
  for (auto i = 0; i < driverFactories_.size(); ++i) {
    if (driverFactories_[i]->outputDriver) {
      return i;
    }
  }

  VELOX_FAIL("Output pipeline not found");
}

void Task::setAllOutputConsumed() {
  bool allFinished;
  {
    std::lock_guard<std::mutex> l(mutex_);
    partitionedOutputConsumed_ = true;
    allFinished = checkIfFinishedLocked();
  }

  if (allFinished) {
    terminate(TaskState::kFinished);
  }
}

void Task::driverClosedLocked() {
  if (isRunningLocked()) {
    --numRunningDrivers_;
  }
  ++numFinishedDrivers_;
}

bool Task::checkIfFinishedLocked() {
  if (!isRunningLocked()) {
    return false;
  }

  // TODO Add support for terminating processing early in grouped execution.
  bool allFinished = numFinishedDrivers_ == numTotalDrivers_;
  if (!allFinished && isUngroupedExecution()) {
    auto outputPipelineId = getOutputPipelineId();
    if (splitGroupStates_[kUngroupedGroupId].numFinishedOutputDrivers ==
        numDrivers(outputPipelineId)) {
      allFinished = true;

      if (taskStats_.executionEndTimeMs == 0) {
        // In case we haven't set executionEndTimeMs due to all splits
        // depleted, we set it here. This can happen due to task error or task
        // being cancelled.
        taskStats_.executionEndTimeMs = getCurrentTimeMs();
      }
    }
  }

  if (allFinished) {
    if ((not hasPartitionedOutput()) || partitionedOutputConsumed_) {
      taskStats_.endTimeMs = getCurrentTimeMs();
      return true;
    }
  }

  return false;
}

std::vector<Operator*> Task::findPeerOperators(
    int pipelineId,
    Operator* caller) {
  std::vector<Operator*> peers;
  const auto operatorId = caller->operatorId();
  const auto& operatorType = caller->operatorType();
  std::lock_guard<std::mutex> l(mutex_);
  for (auto& driver : drivers_) {
    if (driver == nullptr) {
      continue;
    }
    if (driver->driverCtx()->pipelineId != pipelineId) {
      continue;
    }
    Operator* peer = driver->findOperator(operatorId);
    VELOX_CHECK_EQ(peer->operatorType(), operatorType);
    peers.push_back(peer);
  }
  return peers;
}

bool Task::allPeersFinished(
    const core::PlanNodeId& planNodeId,
    Driver* caller,
    ContinueFuture* future,
    std::vector<ContinuePromise>& promises,
    std::vector<std::shared_ptr<Driver>>& peers) {
  std::lock_guard<std::mutex> l(mutex_);
  if (exception_) {
    VELOX_FAIL(
        "Task is terminating because of error: {}",
        errorMessageImpl(exception_));
  }
  const auto splitGroupId = caller->driverCtx()->splitGroupId;
  auto& barriers = splitGroupStates_[splitGroupId].barriers;
  auto& state = barriers[planNodeId];

  const auto numPeers = numDrivers(caller->driverCtx()->pipelineId);
  if (++state.numRequested == numPeers) {
    peers = std::move(state.drivers);
    promises = std::move(state.allPeersFinishedPromises);
    barriers.erase(planNodeId);
    return true;
  }
  std::shared_ptr<Driver> callerShared;
  for (auto& driver : drivers_) {
    if (driver.get() == caller) {
      callerShared = driver;
      break;
    }
  }
  VELOX_CHECK_NOT_NULL(
      callerShared, "Caller of Task::allPeersFinished is not a valid Driver");
  // NOTE: we only set promise for the driver caller if it wants to wait for all
  // the peers to finish.
  if (future != nullptr) {
    state.drivers.push_back(callerShared);
    state.allPeersFinishedPromises.emplace_back(
        fmt::format("Task::allPeersFinished {}", taskId_));
    *future = state.allPeersFinishedPromises.back().getSemiFuture();
  }
  return false;
}

void Task::addHashJoinBridgesLocked(
    uint32_t splitGroupId,
    const std::vector<core::PlanNodeId>& planNodeIds) {
  auto& splitGroupState = splitGroupStates_[splitGroupId];
  for (const auto& planNodeId : planNodeIds) {
    splitGroupState.bridges.emplace(
        planNodeId, std::make_shared<HashJoinBridge>());
    splitGroupState.spillOperatorGroups.emplace(
        planNodeId,
        std::make_unique<SpillOperatorGroup>(
            taskId_, splitGroupId, planNodeId));
  }
}

void Task::addCustomJoinBridgesLocked(
    uint32_t splitGroupId,
    const std::vector<core::PlanNodePtr>& planNodes) {
  auto& splitGroupState = splitGroupStates_[splitGroupId];
  for (const auto& planNode : planNodes) {
    if (auto joinBridge = Operator::joinBridgeFromPlanNode(planNode)) {
      splitGroupState.bridges.emplace(planNode->id(), std::move(joinBridge));
      return;
    }
  }
}

std::shared_ptr<JoinBridge> Task::getCustomJoinBridge(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return getJoinBridgeInternal<JoinBridge>(splitGroupId, planNodeId);
}

void Task::addNestedLoopJoinBridgesLocked(
    uint32_t splitGroupId,
    const std::vector<core::PlanNodeId>& planNodeIds) {
  auto& splitGroupState = splitGroupStates_[splitGroupId];
  for (const auto& planNodeId : planNodeIds) {
    splitGroupState.bridges.emplace(
        planNodeId, std::make_shared<NestedLoopJoinBridge>());
  }
}

std::shared_ptr<HashJoinBridge> Task::getHashJoinBridge(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return getJoinBridgeInternal<HashJoinBridge>(splitGroupId, planNodeId);
}

std::shared_ptr<HashJoinBridge> Task::getHashJoinBridgeLocked(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return getJoinBridgeInternalLocked<HashJoinBridge>(splitGroupId, planNodeId);
}

std::shared_ptr<NestedLoopJoinBridge> Task::getNestedLoopJoinBridge(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return getJoinBridgeInternal<NestedLoopJoinBridge>(splitGroupId, planNodeId);
}

template <class TBridgeType>
std::shared_ptr<TBridgeType> Task::getJoinBridgeInternal(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  std::lock_guard<std::mutex> l(mutex_);
  return getJoinBridgeInternalLocked<TBridgeType>(splitGroupId, planNodeId);
}

template <class TBridgeType>
std::shared_ptr<TBridgeType> Task::getJoinBridgeInternalLocked(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  const auto& splitGroupState = splitGroupStates_[splitGroupId];

  auto it = splitGroupState.bridges.find(planNodeId);
  if (it == splitGroupState.bridges.end()) {
    // We might be looking for a bridge between grouped and ungrouped execution.
    // It will belong to the 'ungrouped' state.
    if (isGroupedExecution() && splitGroupId != kUngroupedGroupId) {
      return getJoinBridgeInternalLocked<TBridgeType>(
          kUngroupedGroupId, planNodeId);
    }
  }
  VELOX_CHECK(
      it != splitGroupState.bridges.end(),
      "Join bridge for plan node ID {} not found for group {}, task {}",
      planNodeId,
      splitGroupId,
      taskId());

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
    std::vector<ContinuePromise>& from,
    std::vector<ContinuePromise>& to) {
  for (auto& promise : from) {
    to.push_back(std::move(promise));
  }
  from.clear();
}

ContinueFuture Task::terminate(TaskState terminalState) {
  std::vector<std::shared_ptr<Driver>> offThreadDrivers;
  EventCompletionNotifier taskCompletionNotifier;
  EventCompletionNotifier stateChangeNotifier;
  std::vector<std::shared_ptr<ExchangeClient>> exchangeClients;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (taskStats_.executionEndTimeMs == 0) {
      taskStats_.executionEndTimeMs = getCurrentTimeMs();
    }
    if (not isRunningLocked()) {
      return makeFinishFutureLocked("Task::terminate");
    }
    state_ = terminalState;
    if (state_ == TaskState::kCanceled || state_ == TaskState::kAborted) {
      try {
        VELOX_FAIL(
            state_ == TaskState::kCanceled ? "Cancelled"
                                           : "Aborted for external error");
      } catch (const std::exception& e) {
        exception_ = std::current_exception();
      }
    }

    LOG(INFO) << "Terminating task " << taskId() << " with state "
              << taskStateString(state_) << " after running for "
              << timeSinceStartMsLocked() << " ms.";

    taskCompletionNotifier.activate(
        std::move(taskCompletionPromises_), [&]() { onTaskCompletion(); });
    stateChangeNotifier.activate(std::move(stateChangePromises_));

    // Update the total number of drivers if we were cancelled.
    numTotalDrivers_ = seenSplitGroups_.size() * numDriversPerSplitGroup_ +
        numDriversUngrouped_;
    // Drivers that are on thread will see this at latest when they go off
    // thread.
    terminateRequested_ = true;
    // The drivers that are on thread will go off thread in time and
    // 'numRunningDrivers_' is cleared here so that this is 0 right
    // after terminate as tests expect.
    numRunningDrivers_ = 0;
    for (auto& driver : drivers_) {
      if (driver) {
        if (enterForTerminateLocked(driver->state()) ==
            StopReason::kTerminate) {
          offThreadDrivers.push_back(std::move(driver));
          driverClosedLocked();
        }
      }
    }
    exchangeClients.swap(exchangeClients_);
  }

  taskCompletionNotifier.notify();
  stateChangeNotifier.notify();

  // Get the stats and free the resources of Drivers that were not on
  // thread.
  for (auto& driver : offThreadDrivers) {
    driver->closeByTask();
  }

  // We continue all Drivers waiting for promises known to the
  // Task. The Drivers are now detached from Task and therefore will
  // not go on thread. The reference in the future callback is
  // typically the last one.
  if (hasPartitionedOutput()) {
    if (auto bufferManager = bufferManager_.lock()) {
      bufferManager->removeTask(taskId_);
    }
  }

  for (auto& exchangeClient : exchangeClients) {
    if (exchangeClient != nullptr) {
      exchangeClient->close();
    }
  }

  // Release reference to exchange client, so that it will close exchange
  // sources and prevent resending requests for data.
  exchangeClients.clear();

  std::vector<ContinuePromise> splitPromises;
  std::vector<std::shared_ptr<JoinBridge>> oldBridges;
  std::vector<SplitGroupState> splitGroupStates;
  std::
      unordered_map<core::PlanNodeId, std::pair<std::vector<exec::Split>, bool>>
          remainingRemoteSplits;
  {
    std::lock_guard<std::mutex> l(mutex_);
    // Collect all the join bridges to clear them.
    for (auto& splitGroupState : splitGroupStates_) {
      for (auto& pair : splitGroupState.second.bridges) {
        oldBridges.emplace_back(std::move(pair.second));
      }
      splitGroupStates.push_back(std::move(splitGroupState.second));
    }

    // Collect all outstanding split promises from all splits state structures.
    for (auto& pair : splitsStates_) {
      auto& splitState = pair.second;
      for (auto& it : pair.second.groupSplitsStores) {
        movePromisesOut(it.second.splitPromises, splitPromises);
      }

      // Process remaining remote splits.
      if (getExchangeClientLocked(pair.first) != nullptr) {
        std::vector<exec::Split> splits;
        for (auto& [groupId, store] : splitState.groupSplitsStores) {
          while (!store.splits.empty()) {
            splits.emplace_back(getSplitLocked(store, 0, nullptr));
          }
        }
        if (!splits.empty()) {
          remainingRemoteSplits.emplace(
              pair.first,
              std::make_pair(std::move(splits), splitState.noMoreSplits));
        }
      }
    }
  }

  TestValue::adjust("facebook::velox::exec::Task::terminate", this);

  for (auto& [planNodeId, splits] : remainingRemoteSplits) {
    auto client = getExchangeClient(planNodeId);
    for (auto& split : splits.first) {
      addRemoteSplit(planNodeId, split);
    }
    if (splits.second) {
      client->noMoreRemoteTasks();
    }
  }

  for (auto& splitGroupState : splitGroupStates) {
    splitGroupState.clear();
  }

  for (auto& promise : splitPromises) {
    promise.setValue();
  }

  for (auto& bridge : oldBridges) {
    bridge->cancel();
  }

  return makeFinishFuture("Task::terminate");
}

ContinueFuture Task::makeFinishFutureLocked(const char* comment) {
  auto [promise, future] = makeVeloxContinuePromiseContract(comment);

  if (numThreads_ == 0) {
    promise.setValue();
    return std::move(future);
  }
  threadFinishPromises_.push_back(std::move(promise));
  return std::move(future);
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
  aggregateOperatorRuntimeStats(stats.runtimeStats);
  addRunningTimeOperatorMetrics(stats);
  taskStats_.pipelineStats[stats.pipelineId]
      .operatorStats[stats.operatorId]
      .add(stats);
}

TaskStats Task::taskStats() const {
  std::lock_guard<std::mutex> l(mutex_);

  // 'taskStats_' contains task stats plus stats for the completed drivers
  // (their operators).
  TaskStats taskStats = taskStats_;

  taskStats.numTotalDrivers = drivers_.size();

  // Add stats of the drivers (their operators) that are still running.
  for (const auto& driver : drivers_) {
    // Driver can be null.
    if (driver == nullptr) {
      ++taskStats.numCompletedDrivers;
      continue;
    }

    for (auto& op : driver->operators()) {
      auto statsCopy = op->stats(false);
      aggregateOperatorRuntimeStats(statsCopy.runtimeStats);
      addRunningTimeOperatorMetrics(statsCopy);
      taskStats.pipelineStats[statsCopy.pipelineId]
          .operatorStats[statsCopy.operatorId]
          .add(statsCopy);
    }
    if (driver->isOnThread()) {
      ++taskStats.numRunningDrivers;
    } else if (driver->isTerminated()) {
      ++taskStats.numTerminatedDrivers;
    } else {
      ++taskStats.numBlockedDrivers[driver->blockingReason()];
    }
  }

  auto bufferManager = bufferManager_.lock();
  taskStats.outputBufferUtilization = bufferManager->getUtilization(taskId_);
  taskStats.outputBufferOverutilized = bufferManager->isOverutilized(taskId_);

  return taskStats;
}

uint64_t Task::timeSinceStartMs() const {
  std::lock_guard<std::mutex> l(mutex_);
  return timeSinceStartMsLocked();
}

uint64_t Task::timeSinceEndMs() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (taskStats_.executionEndTimeMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - taskStats_.executionEndTimeMs;
}

void Task::onTaskCompletion() {
  listeners().withRLock([&](auto& listeners) {
    if (listeners.empty()) {
      return;
    }

    TaskStats stats;
    TaskState state;
    std::exception_ptr exception;
    {
      std::lock_guard<std::mutex> l(mutex_);
      stats = taskStats_;
      state = state_;
      exception = exception_;
    }

    for (auto& listener : listeners) {
      listener->onTaskCompletion(uuid_, taskId_, state, exception, stats);
    }
  });
}

ContinueFuture Task::stateChangeFuture(uint64_t maxWaitMicros) {
  std::lock_guard<std::mutex> l(mutex_);
  // If 'this' is running, the future is realized on timeout or when
  // this no longer is running.
  if (not isRunningLocked()) {
    return ContinueFuture();
  }
  auto [promise, future] = makeVeloxContinuePromiseContract(
      fmt::format("Task::stateChangeFuture {}", taskId_));
  stateChangePromises_.emplace_back(std::move(promise));
  if (maxWaitMicros > 0) {
    return std::move(future).within(std::chrono::microseconds(maxWaitMicros));
  }
  return std::move(future);
}

ContinueFuture Task::taskCompletionFuture(uint64_t maxWaitMicros) {
  std::lock_guard<std::mutex> l(mutex_);
  // If 'this' is running, the future is realized on timeout or when
  // this no longer is running.
  if (not isRunningLocked()) {
    return makeFinishFutureLocked(
        fmt::format("Task::taskCompletionFuture {}", taskId_).data());
  }
  auto [promise, future] = makeVeloxContinuePromiseContract(
      fmt::format("Task::taskCompletionFuture {}", taskId_));
  taskCompletionPromises_.emplace_back(std::move(promise));
  if (maxWaitMicros > 0) {
    return std::move(future).within(std::chrono::microseconds(maxWaitMicros));
  }
  return std::move(future);
}

std::string Task::toString() const {
  std::lock_guard<std::mutex> l(mutex_);
  std::stringstream out;
  out << "{Task " << shortId(taskId_) << " (" << taskId_ << ")";

  if (exception_) {
    out << "Error: " << errorMessageLocked() << std::endl;
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

  return out.str();
}

std::string Task::toJsonString() const {
  std::lock_guard<std::mutex> l(mutex_);
  folly::dynamic obj = folly::dynamic::object;
  obj["shortId"] = shortId(taskId_);
  obj["id"] = taskId_;
  obj["state"] = taskStateString(state_);
  obj["numRunningDrivers"] = numRunningDrivers_;
  obj["numTotalDrivers_"] = numTotalDrivers_;
  obj["numFinishedDrivers"] = numFinishedDrivers_;
  obj["numDriversPerSplitGroup"] = numDriversPerSplitGroup_;
  obj["numDriversUngrouped"] = numDriversUngrouped_;
  obj["groupedPartitionedOutput"] = groupedPartitionedOutput_;
  obj["concurrentSplitGroups"] = concurrentSplitGroups_;
  obj["numRunningSplitGroups"] = numRunningSplitGroups_;
  obj["numDriversUngrouped"] = numDriversUngrouped_;
  obj["partitionedOutputConsumed"] = partitionedOutputConsumed_;
  obj["noMoreOutputBuffers"] = noMoreOutputBuffers_;
  obj["numThreads"] = numThreads_;
  obj["onThreadSince"] = std::to_string(onThreadSince_);
  obj["terminateRequested_"] = std::to_string(terminateRequested_);

  if (exception_) {
    obj["exception"] = errorMessageLocked();
  }

  if (planFragment_.planNode) {
    obj["plan"] = planFragment_.planNode->toString();
  }

  folly::dynamic driverObj = folly::dynamic::object;
  int index = 0;
  for (auto& driver : drivers_) {
    driverObj[std::to_string(index++)] =
        driver ? driver->toJsonString() : "null";
  }
  obj["drivers"] = driverObj;

  if (auto buffers = bufferManager_.lock()) {
    if (auto buffer = buffers->getBufferIfExists(taskId_)) {
      obj["buffer"] = buffer->toString();
    }
  }

  folly::dynamic exchangeClients = folly::dynamic::object;
  for (const auto& [id, client] : exchangeClientByPlanNode_) {
    exchangeClients[id] = client->toJsonString();
  }
  obj["exchangeClientByPlanNode"] = exchangeClients;

  return folly::toPrettyJson(obj);
}

std::shared_ptr<MergeSource> Task::addLocalMergeSource(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId,
    const RowTypePtr& rowType) {
  auto source = MergeSource::createLocalMergeSource();
  splitGroupStates_[splitGroupId].localMergeSources[planNodeId].push_back(
      source);
  return source;
}

const std::vector<std::shared_ptr<MergeSource>>& Task::getLocalMergeSources(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  return splitGroupStates_[splitGroupId].localMergeSources[planNodeId];
}

void Task::createMergeJoinSource(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
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
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  auto it = splitGroupState.mergeJoinSources.find(planNodeId);
  VELOX_CHECK(
      it != splitGroupState.mergeJoinSources.end(),
      "Merge join source for specified plan node doesn't exist: {}",
      planNodeId);
  return it->second;
}

void Task::createLocalExchangeQueuesLocked(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId,
    int numPartitions) {
  auto& splitGroupState = splitGroupStates_[splitGroupId];
  VELOX_CHECK(
      splitGroupState.localExchanges.find(planNodeId) ==
          splitGroupState.localExchanges.end(),
      "Local exchange already exists: {}",
      planNodeId);

  // TODO(spershin): Should we have one memory manager for all local exchanges
  //  in all split groups?
  LocalExchangeState exchange;
  exchange.memoryManager = std::make_shared<LocalExchangeMemoryManager>(
      queryCtx_->queryConfig().maxLocalExchangeBufferSize());

  exchange.queues.reserve(numPartitions);
  for (auto i = 0; i < numPartitions; ++i) {
    exchange.queues.emplace_back(
        std::make_shared<LocalExchangeQueue>(exchange.memoryManager, i));
  }

  splitGroupState.localExchanges.insert({planNodeId, std::move(exchange)});
}

void Task::noMoreLocalExchangeProducers(uint32_t splitGroupId) {
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  for (auto& exchange : splitGroupState.localExchanges) {
    for (auto& queue : exchange.second.queues) {
      queue->noMoreProducers();
    }
  }
}

std::shared_ptr<LocalExchangeQueue> Task::getLocalExchangeQueue(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId,
    int partition) {
  const auto& queues = getLocalExchangeQueues(splitGroupId, planNodeId);
  VELOX_CHECK_LT(
      partition,
      queues.size(),
      "Incorrect partition for local exchange {}",
      planNodeId);
  return queues[partition];
}

const std::vector<std::shared_ptr<LocalExchangeQueue>>&
Task::getLocalExchangeQueues(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  auto& splitGroupState = splitGroupStates_[splitGroupId];

  auto it = splitGroupState.localExchanges.find(planNodeId);
  VELOX_CHECK(
      it != splitGroupState.localExchanges.end(),
      "Incorrect local exchange ID {} for group {}, task {}",
      planNodeId,
      splitGroupId,
      taskId());
  return it->second.queues;
}

void Task::setError(const std::exception_ptr& exception) {
  TestValue::adjust("facebook::velox::exec::Task::setError", this);
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (not isRunningLocked()) {
      return;
    }
    if (exception_ != nullptr) {
      return;
    }
    exception_ = exception;
  }
  terminate(TaskState::kFailed);
  if (onError_) {
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

std::string Task::errorMessageLocked() const {
  return errorMessageImpl(exception_);
}

std::string Task::errorMessage() const {
  std::lock_guard<std::mutex> l(mutex_);
  return errorMessageLocked();
}

StopReason Task::enter(ThreadState& state, uint64_t nowMicros) {
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
    if (numThreads_ == 1) {
      onThreadSince_ = nowMicros;
    }
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
  if (pauseRequested_) {
    // NOTE: if the task has been requested to pause, then we let the task
    // resume code path to close these off thread drivers.
    return StopReason::kPause;
  }
  state.isTerminated = true;
  state.setThread();
  return StopReason::kTerminate;
}

void Task::leave(
    ThreadState& state,
    const std::function<void(StopReason)>& driverCb) {
  std::vector<ContinuePromise> threadFinishPromises;
  auto guard = folly::makeGuard([&]() {
    for (auto& promise : threadFinishPromises) {
      promise.setValue();
    }
  });
  StopReason reason;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!state.isTerminated) {
      reason = shouldStopLocked();
      if (reason == StopReason::kTerminate) {
        state.isTerminated = true;
      }
    } else {
      reason = StopReason::kTerminate;
    }
    if ((reason != StopReason::kTerminate) || (driverCb == nullptr)) {
      if (--numThreads_ == 0) {
        threadFinishPromises = allThreadsFinishedLocked();
      }
      state.clearThread();
      return;
    }
  }

  VELOX_CHECK_EQ(reason, StopReason::kTerminate);
  VELOX_CHECK_NOT_NULL(driverCb);
  // Call 'driverCb' before goes off the driver thread. 'driverCb' will close
  // the driver and remove it from the task.
  driverCb(reason);

  std::lock_guard<std::mutex> l(mutex_);
  if (--numThreads_ == 0) {
    threadFinishPromises = allThreadsFinishedLocked();
  }
  state.clearThread();
}

StopReason Task::enterSuspended(ThreadState& state) {
  VELOX_CHECK(!state.hasBlockingFuture);
  VELOX_CHECK(state.isOnThread());

  std::vector<ContinuePromise> threadFinishPromises;
  auto guard = folly::makeGuard([&]() {
    for (auto& promise : threadFinishPromises) {
      promise.setValue();
    }
  });
  std::lock_guard<std::mutex> l(mutex_);
  if (state.isTerminated) {
    return StopReason::kAlreadyTerminated;
  }
  const auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
    return StopReason::kTerminate;
  }
  // A pause will not stop entering the suspended section. It will just ack that
  // the thread is no longer inside the driver executor pool.
  VELOX_CHECK(
      reason == StopReason::kNone || reason == StopReason::kPause ||
          reason == StopReason::kYield,
      "Unexpected stop reason on suspension: {}",
      reason);
  state.isSuspended = true;
  if (--numThreads_ == 0) {
    threadFinishPromises = allThreadsFinishedLocked();
  }
  return StopReason::kNone;
}

StopReason Task::leaveSuspended(ThreadState& state) {
  VELOX_CHECK(!state.hasBlockingFuture);
  VELOX_CHECK(state.isOnThread());

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
        // For yield or anything but pause we return here.
        return StopReason::kNone;
      }
      --numThreads_;
      state.isSuspended = true;
    }
    // If the pause flag is on when trying to reenter, sleep a while outside of
    // the mutex and recheck. This is rare and not time critical. Can happen if
    // memory interrupt sets pause while already inside a suspended section for
    // other reason, like IO.
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

int32_t Task::yieldIfDue(uint64_t startTimeMicros) {
  if (onThreadSince_ < startTimeMicros) {
    std::lock_guard<std::mutex> l(mutex_);
    // Reread inside the mutex
    if (onThreadSince_ < startTimeMicros && numThreads_ && !toYield_ &&
        !terminateRequested_ && !pauseRequested_) {
      toYield_ = numThreads_;
      return numThreads_;
    }
  }
  return 0;
}

std::vector<ContinuePromise> Task::allThreadsFinishedLocked() {
  std::vector<ContinuePromise> threadFinishPromises;
  threadFinishPromises.swap(threadFinishPromises_);
  return threadFinishPromises;
}

StopReason Task::shouldStopLocked() {
  if (terminateRequested_) {
    return StopReason::kTerminate;
  }
  if (pauseRequested_) {
    return StopReason::kPause;
  }
  if (toYield_ > 0) {
    --toYield_;
    return StopReason::kYield;
  }
  return StopReason::kNone;
}

ContinueFuture Task::requestPause() {
  std::lock_guard<std::mutex> l(mutex_);
  TestValue::adjust("facebook::velox::exec::Task::requestPauseLocked", this);
  pauseRequested_ = true;
  return makeFinishFutureLocked("Task::requestPause");
}

void Task::createExchangeClient(
    int32_t pipelineId,
    const core::PlanNodeId& planNodeId) {
  std::unique_lock<std::mutex> l(mutex_);
  VELOX_CHECK_NULL(
      getExchangeClientLocked(pipelineId),
      "Exchange client has been created at pipeline: {} for planNode: {}",
      pipelineId,
      planNodeId);
  VELOX_CHECK_NULL(
      getExchangeClientLocked(planNodeId),
      "Exchange client has been created for planNode: {}",
      planNodeId);
  // Low-water mark for filling the exchange queue is 1/2 of the per worker
  // buffer size of the producers.
  exchangeClients_[pipelineId] = std::make_shared<ExchangeClient>(
      taskId_,
      destination_,
      addExchangeClientPool(planNodeId, pipelineId),
      queryCtx()->queryConfig().maxExchangeBufferSize());
  exchangeClientByPlanNode_.emplace(planNodeId, exchangeClients_[pipelineId]);
}

std::shared_ptr<ExchangeClient> Task::getExchangeClientLocked(
    const core::PlanNodeId& planNodeId) const {
  auto it = exchangeClientByPlanNode_.find(planNodeId);
  if (it == exchangeClientByPlanNode_.end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<ExchangeClient> Task::getExchangeClientLocked(
    int32_t pipelineId) const {
  VELOX_CHECK_LT(pipelineId, exchangeClients_.size());
  return exchangeClients_[pipelineId];
}

std::shared_ptr<SpillOperatorGroup> Task::getSpillOperatorGroupLocked(
    uint32_t splitGroupId,
    const core::PlanNodeId& planNodeId) {
  auto& groups = splitGroupStates_[splitGroupId].spillOperatorGroups;
  auto it = groups.find(planNodeId);
  VELOX_CHECK(it != groups.end(), "Split group is not set {}", splitGroupId);
  auto group = it->second;
  VELOX_CHECK_NOT_NULL(
      group,
      "Spill group for plan node ID {} is not set in split group {}",
      planNodeId,
      splitGroupId);
  return group;
}

void Task::testingVisitDrivers(const std::function<void(Driver*)>& callback) {
  std::lock_guard<std::mutex> l(mutex_);
  for (int i = 0; i < drivers_.size(); ++i) {
    if (drivers_[i] != nullptr) {
      callback(drivers_[i].get());
    }
  }
}

std::unique_ptr<memory::MemoryReclaimer> Task::MemoryReclaimer::create(
    const std::shared_ptr<Task>& task) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new Task::MemoryReclaimer(task));
}

uint64_t Task::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes) {
  auto task = ensureTask();
  if (FOLLY_UNLIKELY(task == nullptr)) {
    return 0;
  }
  VELOX_CHECK_EQ(task->pool()->name(), pool->name());
  task->requestPause().wait();
  auto guard = folly::makeGuard([&]() {
    try {
      Task::resume(task);
    } catch (const VeloxRuntimeError& exception) {
      LOG(WARNING) << "Failed to resume task " << task->taskId_
                   << " after memory reclamation: " << exception.message();
    }
  });
  // Don't reclaim from a cancelled task as it will terminate soon.
  if (task->isCancelled()) {
    return 0;
  }
  return memory::MemoryReclaimer::reclaim(pool, targetBytes);
}

void Task::MemoryReclaimer::abort(
    memory::MemoryPool* pool,
    const std::exception_ptr& error) {
  auto task = ensureTask();
  if (FOLLY_UNLIKELY(task == nullptr)) {
    return;
  }
  VELOX_CHECK_EQ(task->pool()->name(), pool->name());
  task->setError(error);
  // Set timeout to zero to infinite wait until task completes.
  task->taskCompletionFuture(0).wait();
  memory::MemoryReclaimer::abort(pool, error);
}

} // namespace facebook::velox::exec
