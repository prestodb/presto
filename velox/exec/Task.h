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
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Driver.h"
#include "velox/exec/LocalPartition.h"
#include "velox/exec/MergeSource.h"
#include "velox/exec/Split.h"
#include "velox/exec/TaskStructs.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class PartitionedOutputBufferManager;

class HashJoinBridge;
class CrossJoinBridge;

using ContinuePromise = VeloxPromise<bool>;

class Task {
 public:
  Task(
      const std::string& taskId,
      core::PlanFragment planFragment,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      Consumer consumer = nullptr,
      std::function<void(std::exception_ptr)> onError = nullptr);

  Task(
      const std::string& taskId,
      core::PlanFragment planFragment,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ConsumerSupplier consumerSupplier,
      std::function<void(std::exception_ptr)> onError = nullptr);

  ~Task();

  const std::string& taskId() const {
    return taskId_;
  }

  velox::memory::MemoryPool* FOLLY_NONNULL addDriverPool();

  velox::memory::MemoryPool* FOLLY_NONNULL
  addOperatorPool(velox::memory::MemoryPool* FOLLY_NONNULL driverPool);

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
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId,
      exec::Split& split,
      ContinueFuture& future);

  void splitFinished(const core::PlanNodeId& planNodeId, int32_t splitGroupId);

  void multipleSplitsFinished(int32_t numSplits);

  void updateBroadcastOutputBuffers(int numBuffers, bool noMoreBuffers);

  bool isGroupedExecution() const;

  void createLocalMergeSources(
      uint32_t splitGroupId,
      unsigned numSources,
      const std::shared_ptr<const RowType>& rowType,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory);

  std::shared_ptr<MergeSource> getLocalMergeSource(
      uint32_t splitGroupId,
      int sourceId);

  void createMergeJoinSource(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  std::shared_ptr<MergeJoinSource> getMergeJoinSource(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  void createLocalExchangeSources(
      const core::PlanNodeId& planNodeId,
      int numPartitions);

  void noMoreLocalExchangeProducers(uint32_t splitGroupId);

  std::shared_ptr<LocalExchangeSource> getLocalExchangeSource(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId,
      int partition);

  const std::vector<std::shared_ptr<LocalExchangeSource>>&
  getLocalExchangeSources(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  std::exception_ptr error() const {
    return exception_;
  }

  void setError(const std::exception_ptr& exception);

  void setError(const std::string& message);

  std::string errorMessage();

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
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  // Returns a CrossJoinBridge for 'planNodeId'.
  std::shared_ptr<CrossJoinBridge> getCrossJoinBridge(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  // Sets this to a terminate requested
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

  /// Returns the number of running drivers.
  uint32_t numRunningDrivers() const {
    std::lock_guard<std::mutex> taskLock(mutex_);
    return numRunningDrivers_;
  }

  /// Returns the total number of drivers the task needs to run.
  uint32_t numTotalDrivers() const {
    return numTotalDrivers_;
  }

  /// Returns the number of finished drivers so far.
  uint32_t numFinishedDrivers() const {
    std::lock_guard<std::mutex> taskLock(mutex_);
    return numFinishedDrivers_;
  }

  velox::memory::MemoryPool* FOLLY_NONNULL pool() const {
    return pool_.get();
  }

  // Returns kNone if no pause or terminate is requested. The thread count is
  // incremented if kNone is returned. If something else is returned the
  // calling thread should unwind and return itself to its pool.
  StopReason enter(ThreadState& state);

  // Sets the state to terminated. Returns kAlreadyOnThread if the
  // Driver is running. In this case, the Driver will free resources
  // and the caller should not do anything. Returns kTerminate if the
  // Driver was not on thread. When this happens, the Driver is on the
  // caller thread wit isTerminated set and the caller is responsible
  // for freeing resources.
  StopReason enterForTerminate(ThreadState& state);

  // Marks that the Driver is not on thread. If no more Drivers in the
  // CancelPool are on thread, this realizes any finishFutures. The
  // Driver may go off thread because of hasBlockingFuture or pause
  // requested or terminate requested. The return value indicates the
  // reason. If kTerminate is returned, the isTerminated flag is set.
  StopReason leave(ThreadState& state);

  // Enters a suspended section where the caller stays on thread but
  // is not accounted as being on the thread.  Returns kNone if no
  // terminate is requested. The thread count is decremented if kNone
  // is returned. If thread count goes to zero, waiting promises are
  // realized. If kNone is not returned the calling thread should
  // unwind and return itself to its pool.
  StopReason enterSuspended(ThreadState& state);

  StopReason leaveSuspended(ThreadState& state);

  // Returns a stop reason without synchronization. If the stop reason
  // is yield, then atomically decrements the count of threads that
  // are to yield.
  StopReason shouldStop();

  void requestPause(bool pause) {
    std::lock_guard<std::mutex> l(mutex_);
    requestPauseLocked(pause);
  }

  void requestPauseLocked(bool pause) {
    pauseRequested_ = pause;
  }

  void requestTerminate() {
    std::lock_guard<std::mutex> l(mutex_);
    terminateRequested_ = true;
  }

  void requestYield() {
    std::lock_guard<std::mutex> l(mutex_);
    toYield_ = numThreads_;
  }

  // Once 'pauseRequested_' is set, it will not be cleared until
  // task::resume(). It is therefore OK to read it without a mutex
  // from a thread that this flag concerns.
  bool pauseRequested() const {
    return pauseRequested_;
  }

  // Returns a future that is completed when all threads have acknowledged
  // terminate or pause. If the future is realized there is no running activity
  // on behalf of threads that have entered 'this'.
  folly::SemiFuture<bool> finishFuture();

  std::mutex& mutex() {
    return mutex_;
  }

 private:
  template <class TBridgeType>
  std::shared_ptr<TBridgeType> getJoinBridgeInternal(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  /// Retrieve a split or split future from the given split store structure.
  BlockingReason getSplitOrFutureLocked(
      SplitsStore& splitsStore,
      exec::Split& split,
      ContinueFuture& future);

  /// Creates a bunch of drivers for the 'nextSplitGroupId_' and advances the
  /// latter.
  void createDrivers(
      std::vector<std::shared_ptr<Driver>>& out,
      std::shared_ptr<Task>& self);

  /// Safely returns reference to the group splits store, ensuring before
  /// accessing it that it is created.
  inline SplitsStore& groupSplitsStoreSafe(
      SplitsState& splitsState,
      uint32_t splitGroupId) {
    return splitsState.groupSplitsStores(
        planFragment_.numSplitGroups)[splitGroupId];
  }

  void driverClosedLocked();

  void stateChangedLocked();

  // Returns true if all splits are finished processing and there are no more
  // splits coming for the task.
  bool isAllSplitsFinishedLocked();

  /// See if we need to register a split group as completed.
  void checkGroupSplitsCompleteLocked(
      int32_t splitGroupId,
      const SplitsStore& splitsStore);

  std::unique_ptr<ContinuePromise> addSplitLocked(
      SplitsState& splitsState,
      exec::Split&& split);

  std::unique_ptr<ContinuePromise> addSplitToStoreLocked(
      SplitsStore& splitsStore,
      exec::Split&& split);

  void finished();

  StopReason shouldStopLocked();

  void checkSplitGroupIndex(
      uint32_t splitGroupId,
      const char* FOLLY_NONNULL context);

 private:
  const std::string taskId_;
  core::PlanFragment planFragment_;
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

  /// Exchange clients. One per pipeline / source.
  /// Null for pipelines, which don't need it.
  std::vector<std::shared_ptr<ExchangeClient>> exchangeClients_;

  // Set if terminated by an error. This is the first error reported
  // by any of the instances.
  std::exception_ptr exception_ = nullptr;
  mutable std::mutex mutex_;

  ConsumerSupplier consumerSupplier_;
  std::function<void(std::exception_ptr)> onError_;

  std::vector<std::unique_ptr<DriverFactory>> driverFactories_;
  std::vector<std::shared_ptr<Driver>> drivers_;
  /// The total number of running drivers in all pipelines.
  /// This number changes over time as drivers finish their work and maybe new
  /// get created.
  uint32_t numRunningDrivers_{0};
  /// The total number of drivers we need to run in all pipelines. In normal
  /// execution it is the sum of number of drivers for all pipelines. In grouped
  /// execution we multiply that by the number of split groups.
  uint32_t numTotalDrivers_{0};
  /// The number of completed drivers so far.
  /// This number increases over time as drivers finish their work.
  /// We use this number to detect when the Task is completed.
  uint32_t numFinishedDrivers_{0};
  /// Reflects number of drivers required to process single split group during
  /// grouped execution or the whole plan fragment during normal execution.
  uint32_t numDriversPerSplitGroup_{0};
  /// Used during grouped execution, indicates the next split group to process.
  uint32_t nextSplitGroupId_{0};

  TaskState state_ = kRunning;

  /// Stores separate splits state for each plan node.
  std::unordered_map<core::PlanNodeId, SplitsState> splitsStates_;

  // Holds states for pipelineBarrier(). Guarded by 'mutex_'.
  std::unordered_map<std::string, BarrierState> barriers_;

  std::vector<VeloxPromise<bool>> stateChangePromises_;

  TaskStats taskStats_;
  std::unique_ptr<velox::memory::MemoryPool> pool_;

  // Keep driver and operator memory pools alive for the duration of the task to
  // allow for sharing vectors across drivers without copy.
  std::vector<std::unique_ptr<velox::memory::MemoryPool>> childPools_;

  /// Stores inter-operator state (exchange, bridges) per split group.
  /// During ungrouped execution we use the 1st entry in this vector.
  std::vector<SplitGroupState> splitGroupStates_;

  std::weak_ptr<PartitionedOutputBufferManager> bufferManager_;

  // Thread counts and cancellation -related state.
  //
  // Some of the below are declared atomic for tsan because they are
  // sometimes tested outside of 'mutex_' for a value of 0/false,
  // which is safe to access without acquiring 'nutex_'.Thread counts
  // and promises are guarded by 'mutex_'
  std::atomic<bool> pauseRequested_{false};
  std::atomic<bool> terminateRequested_{false};
  std::atomic<int32_t> toYield_ = 0;
  int32_t numThreads_ = 0;
  std::vector<VeloxPromise<bool>> finishPromises_;
};

} // namespace facebook::velox::exec
