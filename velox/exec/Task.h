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
#pragma once
#include <limits>
#include "velox/core/QueryCtx.h"
#include "velox/exec/Driver.h"
#include "velox/exec/LocalPartition.h"
#include "velox/exec/MergeSource.h"
#include "velox/exec/Split.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class PartitionedOutputBufferManager;

// Corresponds to Presto TaskState, needed for reporting query completion.
enum TaskState { kRunning, kFinished, kCanceled, kAborted, kFailed };

struct PipelineStats {
  // Cumulative OperatorStats for finished Drivers. The subscript is the
  // operator id, which is the initial ordinal position of the
  // operator in the DriverFactory.
  std::vector<OperatorStats> operatorStats;

  // True if contains the source node for the task.
  bool inputPipeline;

  // True if contains the sync node for the task.
  bool outputPipeline;

  PipelineStats(bool _inputPipeline, bool _outputPipeline)
      : inputPipeline{_inputPipeline}, outputPipeline{_outputPipeline} {}
};

struct TaskStats {
  int32_t numTotalSplits{0};
  int32_t numFinishedSplits{0};
  int32_t numRunningSplits{0};
  int32_t numQueuedSplits{0};
  std::unordered_set<int32_t> completedSplitGroups;

  // The subscript is given by each Operator's
  // DriverCtx::pipelineId. This is a sum total reflecting fully
  // processed Splits for Drivers of this pipeline.
  std::vector<PipelineStats> pipelineStats;

  // Epoch time (ms) when task starts to run
  uint64_t executionStartTimeMs{0};

  // Epoch time (ms) when last split is processed. For some tasks there might be
  // some additional time to send buffered results before the task finishes.
  uint64_t executionEndTimeMs{0};

  // Epoch time (ms) when first split is fetched from the task by an operator.
  uint64_t firstSplitStartTimeMs{0};

  // Epoch time (ms) when last split is fetched from the task by an operator.
  uint64_t lastSplitStartTimeMs{0};

  // Epoch time (ms) when the task completed, e.g. all splits were processed and
  // results have been consumed.
  uint64_t endTimeMs{0};
};

class JoinBridge;
class HashJoinBridge;
class CrossJoinBridge;

class Task {
 public:
  Task(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      Consumer consumer = nullptr,
      std::function<void(std::exception_ptr)> onError = nullptr)
      : Task{
            taskId,
            std::move(planNode),
            destination,
            std::move(queryCtx),
            (consumer ? [c = std::move(consumer)]() { return c; }
                      : ConsumerSupplier{}),
            std::move(onError)} {}

  Task(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ConsumerSupplier consumerSupplier,
      std::function<void(std::exception_ptr)> onError = nullptr);

  ~Task();

  const std::string& taskId() const {
    return taskId_;
  }

  velox::memory::MemoryPool* FOLLY_NONNULL addDriverPool() {
    childPools_.push_back(pool_->addScopedChild("driver_root"));
    auto* driverPool = childPools_.back().get();
    auto parentTracker = pool_->getMemoryUsageTracker();
    if (parentTracker) {
      driverPool->setMemoryUsageTracker(parentTracker->addChild());
    }

    return driverPool;
  }

  velox::memory::MemoryPool* FOLLY_NONNULL
  addOperatorPool(velox::memory::MemoryPool* FOLLY_NONNULL driverPool) {
    childPools_.push_back(driverPool->addScopedChild("operator_ctx"));
    return childPools_.back().get();
  }

  static void start(std::shared_ptr<Task> self, uint32_t maxDrivers);

  // Resumes execution of 'self' after a successful pause. All 'drivers_' must
  // be off-thread and there must be no 'exception_'
  static void resume(std::shared_ptr<Task> self);

  // Removes driver from the set of drivers in 'self'. The task will be kept
  // alive by 'self'. 'self' going out of scope may cause the Task to
  // be freed. This happens if a cancelled task is decoupled from the
  // task manager and threads are left to finish themselves.
  static void removeDriver(
      std::shared_ptr<Task> self,
      Driver* FOLLY_NONNULL instance);

  // Sets the (so far) max split sequence id, so all splits with sequence id
  // equal or below that, will be ignored in the 'addSplitWithSequence' call.
  // Note, that 'addSplitWithSequence' does not update max split sequence id.
  void setMaxSplitSequenceId(
      const core::PlanNodeId& planNodeId,
      long maxSequenceId);

  // Adds split for a source operator corresponding to plan node with
  // specified ID.
  // It requires sequential id of the split and, when that id is NOT greater
  // than the current max split sequence id, the split is discarded as a
  // duplicate.
  // Note, that this method does NOT update max split sequence id.
  // Returns true if split was added, false if it was ignored.
  bool addSplitWithSequence(
      const core::PlanNodeId& planNodeId,
      exec::Split&& split,
      long sequenceId);

  // Adds split for a source operator corresponding to plan node with
  // specified ID. Does not require sequential id.
  void addSplit(const core::PlanNodeId& planNodeId, exec::Split&& split);

  // We mark that for the given group there would be no more splits coming.
  void noMoreSplitsForGroup(
      const core::PlanNodeId& planNodeId,
      int32_t splitGroupId);

  // Signals that there are no more splits for the source operator
  // corresponding to plan node with specified ID.
  void noMoreSplits(const core::PlanNodeId& planNodeId);

  // Returns a split for the source operator corresponding to plan node with
  // specified ID. If there are no splits and no-more-splits signal has been
  // received, sets split to null and returns kNotBlocked. Otherwise, returns
  // kWaitForSplit and sets a future that will complete when split becomes
  // available or no-more-splits signal is received.
  BlockingReason getSplitOrFuture(
      const core::PlanNodeId& planNodeId,
      exec::Split& split,
      ContinueFuture& future);

  void splitFinished(const core::PlanNodeId& planNodeId, int32_t splitGroupId);

  void multipleSplitsFinished(int32_t numSplits);

  void updateBroadcastOutputBuffers(int numBuffers, bool noMoreBuffers);

  void createLocalMergeSources(
      unsigned numSources,
      const std::shared_ptr<const RowType>& rowType,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory);

  std::shared_ptr<MergeSource> getLocalMergeSource(int sourceId) {
    VELOX_CHECK_LT(sourceId, localMergeSources_.size(), "Incorrect source id ");
    return localMergeSources_[sourceId];
  }

  void createMergeJoinSource(const core::PlanNodeId& planNodeId);

  std::shared_ptr<MergeJoinSource> getMergeJoinSource(
      const core::PlanNodeId& planNodeId);

  void createLocalExchangeSources(
      const core::PlanNodeId& planNodeId,
      int numPartitions);

  void noMoreLocalExchangeProducers();

  std::shared_ptr<LocalExchangeSource> getLocalExchangeSource(
      const core::PlanNodeId& planNodeId,
      int partition);

  const std::vector<std::shared_ptr<LocalExchangeSource>>&
  getLocalExchangeSources(const core::PlanNodeId& planNodeId);

  std::exception_ptr error() const {
    return exception_;
  }

  void setError(std::exception_ptr exception) {
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

  void setError(const std::string& message) {
    // The only way to acquire an std::exception_ptr is via throw and
    // std::current_exception().
    try {
      throw std::runtime_error(message);
    } catch (const std::runtime_error& e) {
      setError(std::current_exception());
    }
  }

  std::string errorMessage() {
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

  std::shared_ptr<core::QueryCtx> queryCtx() const {
    return queryCtx_;
  }

  ConsumerSupplier consumerSupplier() {
    return consumerSupplier_;
  }

  // Synchronizes completion of an Operator across Drivers of 'this'.
  // 'planNodeId' identifies the Operator within all
  // Operators/pipelines of 'this'.  Each Operator instance calls this
  // once. All but the last get a false return value and 'future' is
  // set to a future the caller should block on. At this point the
  // caller should go off thread as in any blocking situation.  The
  // last to call gets a true return value and 'peers' is set to all
  // Drivers except 'caller'. 'promises' coresponds pairwise to
  // 'peers'. Realizing the promise will continue the peer. This
  // effects a synchronization barrier between Drivers of a pipeline
  // inside one worker. This is used for example for multithreaded
  // hash join build to ensure all build threads are completed before
  // allowing the probe pipeline to proceed. Throws a cancelled error
  // if 'this' is in an error state.
  bool allPeersFinished(
      const core::PlanNodeId& planNodeId,
      Driver* FOLLY_NONNULL caller,
      ContinueFuture* FOLLY_NONNULL future,
      std::vector<VeloxPromise<bool>>& promises,
      std::vector<std::shared_ptr<Driver>>& peers);

  // Adds HashJoinBridge's for all the specified plan node IDs.
  void addHashJoinBridges(const std::vector<core::PlanNodeId>& planNodeIds);

  // Adds CrossJoinBridge's for all the specified plan node IDs.
  void addCrossJoinBridges(const std::vector<core::PlanNodeId>& planNodeIds);

  // Returns a HashJoinBridge for 'planNodeId'. This is used for synchronizing
  // start of probe with completion of build for a join that has a
  // separate probe and build. 'id' is the PlanNodeId shared between
  // the probe and build Operators of the join.
  std::shared_ptr<HashJoinBridge> getHashJoinBridge(
      const core::PlanNodeId& planNodeId);

  // Returns a CrossJoinBridge for 'planNodeId'.
  std::shared_ptr<CrossJoinBridge> getCrossJoinBridge(
      const core::PlanNodeId& planNodeId);

  // Sets the CancelPool of the QueryCtx to a terminate requested
  // state and frees all resources of Drivers that are not presently
  // on thread. Unblocks all waiting Drivers, e.g. Drivers waiting for
  // free space in outgoing buffers or new splits. Sets the state to
  // 'terminalState', which should be kCanceled for cancellation by users,
  // kFailed for errors and kAborted for termination due to failure in
  // some other Task.
  void terminate(TaskState terminalState);

  // Transitions this to kFinished state if all Drivers are
  // finished. Otherwise sets a flag so that the last Driver to finish
  // will transition the state.
  void setAllOutputConsumed();

  // Adds 'stats' to the cumulative total stats for the operator in
  // the Task stats. Clears 'stats'.
  void addOperatorStats(OperatorStats& stats);

  // Returns by copy as other threads might be updating the structure.
  TaskStats taskStats() const {
    std::lock_guard<std::mutex> l(mutex_);
    return taskStats_;
  }

  /// Returns time (ms) since the task execution started.
  /// Returns zero, if not started.
  uint64_t timeSinceStartMs() const;

  /// Returns time (ms) since the task execution ended.
  /// Returns zero, if not finished.
  uint64_t timeSinceEndMs() const;

  // Convenience function for shortening a Presto taskId. To be used
  // in debugging messages and listings.
  static std::string shortId(const std::string& id);

  std::string toString();

  TaskState state() const {
    std::lock_guard<std::mutex> l(mutex_);
    return state_;
  }

  // Returns a future which is realized when 'this' is no longer in
  // running state. If 'this' is not in running state at the time of
  // call, the future is immediately realized. The future is realized
  // with an exception after maxWaitMicros. A zero max wait means no
  // timeout.
  ContinueFuture stateChangeFuture(uint64_t maxWaitMicros);

  int32_t numDrivers() const {
    return numDrivers_;
  }

  velox::memory::MemoryPool* FOLLY_NONNULL pool() const {
    return pool_.get();
  }

  const core::CancelPoolPtr& cancelPool() const {
    return cancelPool_;
  }

  // Returns the Driver running on the current thread or nullptr if the current
  // thread is not running a Driver of 'this'.
  Driver* FOLLY_NULLABLE thisDriver() const;

 private:
  struct BarrierState {
    int32_t numRequested;
    std::vector<std::shared_ptr<Driver>> drivers;
    std::vector<VeloxPromise<bool>> promises;
  };

  // Map for bucketed group id -> group splits info.
  struct GroupSplitsInfo {
    int32_t numIncompleteSplits{0};
    bool noMoreSplits{false};
  };

  // Structure contains the current info on splits.
  struct SplitsState {
    // Arrived (added), but not distributed yet, splits.
    std::deque<exec::Split> splits;

    // Blocking promises given out when out of splits to distribute.
    std::vector<VeloxPromise<bool>> splitPromises;

    // Singnal, that no more splits will arrive.
    bool noMoreSplits{false};

    // For splits, coming with group ids, we keep track of them.
    std::unordered_map<int32_t, GroupSplitsInfo> groupSplits;

    // Keep the max added split's sequence id to deduplicate incoming splits.
    long maxSequenceId{std::numeric_limits<long>::min()};

    // We need these due to having promises in the structure.
    SplitsState() = default;
    SplitsState(SplitsState const&) = delete;
    SplitsState& operator=(SplitsState const&) = delete;
  };

  void driverClosed();

  std::shared_ptr<ExchangeClient> addExchangeClient();

  void stateChangedLocked();

  // Returns true if all splits are finished processing and there are no more
  // splits coming for the task.
  bool isAllSplitsFinishedLocked();

  void checkGroupSplitsCompleteLocked(
      std::unordered_map<int32_t, GroupSplitsInfo>& mapGroupSplits,
      int32_t splitGroupId,
      std::unordered_map<int32_t, GroupSplitsInfo>::iterator it);

  void addSplitLocked(SplitsState& splitsState, exec::Split&& split);

  const std::string taskId_;
  std::shared_ptr<const core::PlanNode> planNode_;
  const int destination_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  // True if produces output via PartitionedOutputBufferManager.
  bool hasPartitionedOutput_ = false;
  // Set to true by PartitionedOutputBufferManager when all output is
  // acknowledged. If this happens before Drivers are at end, the last
  // Driver to finish will set state_ to kFinished. If Drivers have
  // finished then setting this to true will also set state_ to
  // kFinished.
  bool partitionedOutputConsumed_ = false;

  // Exchange clients. One per pipeline / source.
  std::vector<std::shared_ptr<ExchangeClient>> exchangeClients_;

  // Set if terminated by an error. This is the first error reported
  // by any of the instances.
  std::exception_ptr exception_ = nullptr;
  mutable std::mutex mutex_;

  ConsumerSupplier consumerSupplier_;
  std::function<void(std::exception_ptr)> onError_;

  std::vector<std::unique_ptr<DriverFactory>> driverFactories_;
  std::vector<std::shared_ptr<Driver>> drivers_;
  int32_t numDrivers_ = 0;
  TaskState state_ = kRunning;

  // We store separate splits state for each plan node.
  std::unordered_map<core::PlanNodeId, SplitsState> splitsStates_;

  // Holds states for pipelineBarrier(). Guarded by
  // 'mutex_'.
  std::unordered_map<std::string, BarrierState> barriers_;
  // Map from the plan node id of the join to the corresponding JoinBridge.
  // Guarded by 'mutex_'.
  std::unordered_map<std::string, std::shared_ptr<JoinBridge>> bridges_;

  std::vector<VeloxPromise<bool>> stateChangePromises_;

  TaskStats taskStats_;
  std::unique_ptr<velox::memory::MemoryPool> pool_;

  // Keep driver and operator memory pools alive for the duration of the task to
  // allow for sharing vectors across drivers without copy.
  std::vector<std::unique_ptr<velox::memory::MemoryPool>> childPools_;

  std::vector<std::shared_ptr<MergeSource>> localMergeSources_;

  std::unordered_map<core::PlanNodeId, std::shared_ptr<MergeJoinSource>>
      mergeJoinSources_;

  struct LocalExchange {
    std::unique_ptr<LocalExchangeMemoryManager> memoryManager;
    std::vector<std::shared_ptr<LocalExchangeSource>> sources;
  };

  /// Map of local exchanges keyed on LocalPartition plan node ID.
  std::unordered_map<core::PlanNodeId, LocalExchange> localExchanges_;

  core::CancelPoolPtr cancelPool_{std::make_shared<core::CancelPool>()};
  std::weak_ptr<PartitionedOutputBufferManager> bufferManager_;
};

} // namespace facebook::velox::exec
