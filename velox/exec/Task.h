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
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/MergeSource.h"
#include "velox/exec/Split.h"
#include "velox/exec/TaskStats.h"
#include "velox/exec/TaskStructs.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class OutputBufferManager;

class HashJoinBridge;
class NestedLoopJoinBridge;

using ConnectorSplitPreloadFunc =
    std::function<void(const std::shared_ptr<connector::ConnectorSplit>&)>;

class Task : public std::enable_shared_from_this<Task> {
 public:
  /// Threading mode the task is executed.
  enum class ExecutionMode {
    /// Mode that executes the query serially (single-threaded) on the calling
    /// thread. Task is executed via the Task::next() API.
    kSerial,
    /// Mode that executes the query in parallel (multi-threaded) using provided
    /// executor. Task is executed via the Task::start() API that starts up
    /// multiple driver threads and manages their lifecycle.
    kParallel,
  };

  /// Creates a task to execute a plan fragment, but doesn't start execution
  /// until Task::start() method is called.
  /// @param taskId Unique task identifier.
  /// @param planFragment Plan fragment.
  /// @param destination Partition number if task is expected to receive data
  /// for a particular partition from a set of upstream tasks participating in a
  /// distributed execution. Used to initialize an ExchangeClient. Ignored if
  /// plan fragment doesn't have an ExchangeNode.
  /// @param queryCtx Query context containing MemoryPool and MemoryAllocator
  /// instances to use for memory allocations during execution, executor to
  /// schedule operators on, and session properties.
  /// @param mode Execution mode for this task. The task can be executed in
  /// Serial and Parallel mode.
  /// @param consumer Optional factory function to get callbacks to pass the
  /// results of the execution. In a parallel execution mode, results from each
  /// thread are passed on to a separate consumer.
  /// @param onError Optional callback to receive an exception if task
  /// execution fails.
  static std::shared_ptr<Task> create(
      const std::string& taskId,
      core::PlanFragment planFragment,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode,
      Consumer consumer = nullptr,
      std::function<void(std::exception_ptr)> onError = nullptr);

  static std::shared_ptr<Task> create(
      const std::string& taskId,
      core::PlanFragment planFragment,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode,
      ConsumerSupplier consumerSupplier,
      std::function<void(std::exception_ptr)> onError = nullptr);

  /// Convenience function for shortening a Presto taskId. To be used
  /// in debugging messages and listings.
  static std::string shortId(const std::string& id);

  ~Task();

  /// Specify directory to which data will be spilled if spilling is enabled and
  /// required. Set 'alreadyCreated' to true if the directory has already been
  /// created by the caller.
  void setSpillDirectory(
      const std::string& spillDirectory,
      bool alreadyCreated = true) {
    spillDirectory_ = spillDirectory;
    spillDirectoryCreated_ = alreadyCreated;
  }

  std::string toString() const;

  folly::dynamic toJson() const;

  folly::dynamic toShortJson() const;

  /// Returns universally unique identifier of the task.
  const std::string& uuid() const {
    return uuid_;
  }

  /// Returns task ID specified in the constructor.
  const std::string& taskId() const {
    return taskId_;
  }

  const int destination() const {
    return destination_;
  }

  /// Configured cpu slice time limit for drivers. 0 (meaning slicing/yield
  /// disabled) when task is under serial mode.
  uint64_t driverCpuTimeSliceLimitMs() const;

  /// Returns QueryCtx specified in the constructor.
  const std::shared_ptr<core::QueryCtx>& queryCtx() const {
    return queryCtx_;
  }

  /// Returns MemoryPool used to allocate memory during execution. This instance
  /// is a child of the MemoryPool passed in the constructor.
  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  /// Returns ConsumerSupplier passed in the constructor.
  ConsumerSupplier consumerSupplier() const {
    return consumerSupplier_;
  }

  bool isGroupedExecution() const;

  bool isUngroupedExecution() const;

  /// Returns true if this task has ungrouped execution split under grouped
  /// execution mode.
  ///
  /// NOTE: calls this function after task has been started as the number of
  /// ungrouped drivers is set during task startup.
  bool hasMixedExecutionGroup() const;

  /// Starts executing the plan fragment specified in the constructor. If leaf
  /// nodes require splits (e.g. TableScan, Exchange, etc.), these splits can be
  /// added before or after calling start().
  ///
  /// @param maxDrivers Maximum number of drivers / threads used to run a single
  /// pipeline. Some pipelines are running in fewer threads if they contain plan
  /// nodes that do not support parallelism (e.g. final limit) or their source
  /// nodes require splits and there are not enough of these.
  /// @param concurrentSplitGroups In grouped execution, maximum number of
  /// splits groups processed concurrently.
  void start(uint32_t maxDrivers, uint32_t concurrentSplitGroups = 1);

  /// If this returns true, this Task supports the single-threaded execution API
  /// next().
  bool supportsSingleThreadedExecution() const;

  /// Single-threaded execution API. Runs the query and returns results one
  /// batch at a time. Returns nullptr if query evaluation is finished and no
  /// more data will be produced. Throws an exception if query execution
  /// failed.
  ///
  /// This API is available for query plans that do not use
  /// PartitionedOutputNode and LocalPartitionNode plan nodes.
  ///
  /// The caller is required to add all the necessary splits, and signal
  /// no-more-splits before calling 'next' for the first time.
  ///
  /// If no `future` is provided, the operators in the pipeline are not allowed
  /// to block for external events, but can block waiting for data to be
  /// produced by a different pipeline of the same task.
  ///
  /// If `future` is provided, the operators in the pipeline can block
  /// externally. When any operators are blocked externally, the function lets
  /// all non-blocked operators to run until completion, returns nullptr and
  /// updates `future`. `future` is realized when the operators are no longer
  /// blocked. Caller thread is responsible to wait for future before calling
  /// `next` again.
  RowVectorPtr next(ContinueFuture* future = nullptr);

  /// Resumes execution of 'self' after a successful pause. All 'drivers_' must
  /// be off-thread and there must be no 'exception_'
  static void resume(std::shared_ptr<Task> self);

  /// Sets the (so far) max split sequence id, so all splits with sequence id
  /// equal or below that, will be ignored in the 'addSplitWithSequence' call.
  /// Note, that 'addSplitWithSequence' does not update max split sequence id
  /// and the operation is silently ignored if Task is not running.
  void setMaxSplitSequenceId(
      const core::PlanNodeId& planNodeId,
      long maxSequenceId);

  /// Adds split for a source operator corresponding to plan node with
  /// specified ID.
  /// It requires sequential id of the split and, when that id is NOT greater
  /// than the current max split sequence id, the split is discarded as a
  /// duplicate.
  /// Note, that this method does NOT update max split sequence id.
  /// Returns true if split was added, false if it was ignored.
  /// Note that, the operation is silently ignored if Task is not running.
  bool addSplitWithSequence(
      const core::PlanNodeId& planNodeId,
      exec::Split&& split,
      long sequenceId);

  /// Adds split for a source operator corresponding to plan node with
  /// specified ID. Does not require sequential id.
  /// Note that, the operation is silently ignored if Task is not running.
  void addSplit(const core::PlanNodeId& planNodeId, exec::Split&& split);

  /// We mark that for the given group there would be no more splits coming.
  void noMoreSplitsForGroup(
      const core::PlanNodeId& planNodeId,
      int32_t splitGroupId);

  /// Signals that there are no more splits for the source operator
  /// corresponding to plan node with specified ID.
  void noMoreSplits(const core::PlanNodeId& planNodeId);

  /// Updates the total number of output buffers to broadcast or arbitrarily
  /// distribute the results of the execution to. Used when plan tree ends with
  /// a PartitionedOutputNode with broadcast of arbitrary output type.
  /// @param numBuffers Number of output buffers. Must not decrease on
  /// subsequent calls.
  /// @param noMoreBuffers A flag indicating that numBuffers is the final number
  /// of buffers. No more calls are expected after the call with noMoreBuffers
  /// == true, but occasionally the caller might resend it, so calls
  /// received after a call with noMoreBuffers == true are ignored.
  /// @return true if update was successful.
  ///         false if noMoreBuffers was previously set to true.
  ///         false if buffer was not found for a given task.
  bool updateOutputBuffers(int numBuffers, bool noMoreBuffers);

  /// Returns true if state is 'running'.
  bool isRunning() const;

  /// Returns true if state is 'finished'.
  bool isFinished() const;

  /// Returns current state of execution.
  TaskState state() const {
    std::lock_guard<std::timed_mutex> l(mutex_);
    return state_;
  }

  /// Returns a future which is realized when the task's state has changed and
  /// the Task is ready to report some progress (such as split group finished or
  /// task is completed).
  /// If the task is not in running state at the time of call, the future is
  /// immediately realized. The future is realized with an exception after
  /// maxWaitMicros. A zero max wait means no timeout.
  ContinueFuture stateChangeFuture(uint64_t maxWaitMicros);

  /// Returns a future which is realized when the task is no longer in
  /// running state.
  /// If the task is not in running state at the time of call, the future is
  /// immediately realized.
  ContinueFuture taskCompletionFuture();

  /// Returns a future which is realized when the task has been deleted, and all
  /// the background activities have finished at that point.
  ContinueFuture taskDeletionFuture();

  /// Returns task execution error or nullptr if no error occurred.
  std::exception_ptr error() const {
    std::lock_guard<std::timed_mutex> l(mutex_);
    return exception_;
  }

  /// Returns task execution error message or empty string if not error
  /// occurred.
  std::string errorMessage() const;

  /// Returns Task Stats by copy as other threads might be updating the
  /// structure.
  TaskStats taskStats() const;

  /// Information about an operator call that helps debugging stuck calls.
  struct OpCallInfo {
    size_t durationMs;
    /// Thread id of where the operator got stuck.
    int32_t tid;
    int32_t opId;
    std::string taskId;
    /// Call in the format of "<operatorType>.<nodeId>::<operatorMethod>".
    std::string opCall;
  };

  /// Collect long running operator calls across all drivers in this task.
  /// Return false when the lock cannot be taken within the timeout, in that
  /// case the result is not populated.  Return true if everything works well.
  bool getLongRunningOpCalls(
      std::chrono::nanoseconds lockTimeout,
      size_t thresholdDurationMs,
      std::vector<OpCallInfo>& out) const;

  /// Returns time (ms) since the task execution started or zero, if not
  /// started.
  uint64_t timeSinceStartMs() const;

  /// Returns time (ms) since the task execution ended or zero, if not finished.
  uint64_t timeSinceEndMs() const;

  /// Returns time (ms) since the task was terminated or zero, if not terminated
  /// yet.
  uint64_t timeSinceTerminationMs() const;

  /// Returns the total number of drivers in the output pipeline, e.g. the
  /// pipeline that produces the results.
  uint32_t numOutputDrivers() const {
    return numDrivers(getOutputPipelineId());
  }

  /// Stores the number of drivers in various states of execution.
  struct DriverCounts {
    uint32_t numQueuedDrivers{0};
    uint32_t numOnThreadDrivers{0};
    uint32_t numSuspendedDrivers{0};
    std::unordered_map<BlockingReason, uint64_t> numBlockedDrivers;
  };

  /// Returns the number of drivers in various states of execution.
  DriverCounts driverCounts() const;

  /// Returns the number of running drivers.
  uint32_t numRunningDrivers() const {
    std::lock_guard<std::timed_mutex> taskLock(mutex_);
    return numRunningDrivers_;
  }

  /// Returns the total number of drivers the task needs to run.
  uint32_t numTotalDrivers() const {
    std::lock_guard<std::timed_mutex> taskLock(mutex_);
    return numTotalDrivers_;
  }

  /// Returns the number of finished drivers so far.
  uint32_t numFinishedDrivers() const {
    std::lock_guard<std::timed_mutex> taskLock(mutex_);
    return numFinishedDrivers_;
  }

  const std::vector<std::weak_ptr<Driver>>& testingDriversClosedByTask() const {
    return driversClosedByTask_;
  }

  /// Internal public methods. These methods are intended to be used by internal
  /// library components (Driver, Operator, etc.) and should not be called by
  /// the library users.

  /// Creates new instance of MemoryPool for an operator, stores it in the task
  /// to ensure lifetime and returns a raw pointer. Not thread safe, e.g. must
  /// be called from the Operator's constructor.
  velox::memory::MemoryPool* addOperatorPool(
      const core::PlanNodeId& planNodeId,
      uint32_t splitGroupId,
      int pipelineId,
      uint32_t driverId,
      const std::string& operatorType);

  /// Creates new instance of MemoryPool with aggregate kind for the connector
  /// use, stores it in the task to ensure lifetime and returns a raw pointer.
  /// Not thread safe, e.g. must be called from the Operator's constructor.
  velox::memory::MemoryPool* addConnectorPoolLocked(
      const core::PlanNodeId& planNodeId,
      int pipelineId,
      uint32_t driverId,
      const std::string& operatorType,
      const std::string& connectorId);

  /// Creates new instance of MemoryPool for a merge source in a
  /// MergeExchangeNode, stores it in the task to ensure lifetime and returns a
  /// raw pointer.
  velox::memory::MemoryPool* addMergeSourcePool(
      const core::PlanNodeId& planNodeId,
      uint32_t pipelineId,
      uint32_t sourceId);

  /// Removes driver from the set of drivers in 'self'. The task will be kept
  /// alive by 'self'. 'self' going out of scope may cause the Task to
  /// be freed. This happens if a cancelled task is decoupled from the
  /// task manager and threads are left to finish themselves.
  static void removeDriver(std::shared_ptr<Task> self, Driver* instance);

  /// Returns a split for the source operator corresponding to plan
  /// node with specified ID. If there are no splits and no-more-splits
  /// signal has been received, sets split to null and returns
  /// kNotBlocked. Otherwise, returns kWaitForSplit and sets a future
  /// that will complete when split becomes available or no-more-splits
  /// signal is received. If 'maxPreloadSplits' is given, ensures that
  /// so many of splits at the head of the queue are preloading. If
  /// they are not, calls preload on them to start preload.
  BlockingReason getSplitOrFuture(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId,
      exec::Split& split,
      ContinueFuture& future,
      int32_t maxPreloadSplits = 0,
      const ConnectorSplitPreloadFunc& preload = nullptr);

  void splitFinished(bool fromTableScan, int64_t splitWeight);

  void multipleSplitsFinished(
      bool fromTableScan,
      int32_t numSplits,
      int64_t splitsWeight);

  /// Adds a MergeSource for the specified splitGroupId and planNodeId.
  std::shared_ptr<MergeSource> addLocalMergeSource(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId,
      const RowTypePtr& rowType);

  /// Returns all MergeSource's for the specified splitGroupId and planNodeId.
  const std::vector<std::shared_ptr<MergeSource>>& getLocalMergeSources(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  void createMergeJoinSource(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  std::shared_ptr<MergeJoinSource> getMergeJoinSource(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  void createLocalExchangeQueuesLocked(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId,
      int numPartitions);

  void noMoreLocalExchangeProducers(uint32_t splitGroupId);

  std::shared_ptr<LocalExchangeQueue> getLocalExchangeQueue(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId,
      int partition);

  const std::vector<std::shared_ptr<LocalExchangeQueue>>&
  getLocalExchangeQueues(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  void setError(const std::exception_ptr& exception);

  void setError(const std::string& message);

  /// Returns all the peer operators of the 'caller' operator from a given
  /// 'pipelindId' in this task.
  std::vector<Operator*> findPeerOperators(int pipelineId, Operator* caller);

  /// Synchronizes completion of an Operator across Drivers of 'this'.
  /// 'planNodeId' identifies the Operator within all Operators/pipelines of
  /// 'this'.  Each Operator instance calls this once. All but the last get a
  /// false return value and 'future' is set to a future the caller should block
  /// on. At this point the caller should go off thread as in any blocking
  /// situation.  The last to call gets a true return value and 'peers' is set
  /// to all Drivers except 'caller'. 'promises' corresponds pairwise to
  /// 'peers'. Realizing the promise will continue the peer. This effects a
  /// synchronization barrier between Drivers of a pipeline inside one worker.
  /// This is used for example for multithreaded hash join build to ensure all
  /// build threads are completed before allowing the probe pipeline to proceed.
  /// Throws a cancelled error if 'this' is in an error state.
  ///
  /// NOTE: if 'future' is null, then the caller doesn't intend to wait for the
  /// other peers to finish. The function won't set its promise and record it in
  /// peers. This is used in scenario that the caller only needs to know whether
  /// it is the last one to reach the barrier.
  ///
  /// NOTE: The last peer (the one that got 'true' returned and a bunch of
  /// promises) is responsible for promises' fulfillment even in case of an
  /// exception!
  bool allPeersFinished(
      const core::PlanNodeId& planNodeId,
      Driver* caller,
      ContinueFuture* future,
      std::vector<ContinuePromise>& promises,
      std::vector<std::shared_ptr<Driver>>& peers);

  /// Adds HashJoinBridge's for all the specified plan node IDs.
  void addHashJoinBridgesLocked(
      uint32_t splitGroupId,
      const std::vector<core::PlanNodeId>& planNodeIds);

  /// Adds NestedLoopJoinBridge's for all the specified plan node IDs.
  void addNestedLoopJoinBridgesLocked(
      uint32_t splitGroupId,
      const std::vector<core::PlanNodeId>& planNodeIds);

  /// Adds custom join bridges for all the specified plan nodes.
  void addCustomJoinBridgesLocked(
      uint32_t splitGroupId,
      const std::vector<core::PlanNodePtr>& planNodes);

  /// Returns a HashJoinBridge for 'planNodeId'. This is used for synchronizing
  /// start of probe with completion of build for a join that has a
  /// separate probe and build. 'id' is the PlanNodeId shared between
  /// the probe and build Operators of the join.
  std::shared_ptr<HashJoinBridge> getHashJoinBridge(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  std::shared_ptr<HashJoinBridge> getHashJoinBridgeLocked(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  /// Returns a NestedLoopJoinBridge for 'planNodeId'.
  std::shared_ptr<NestedLoopJoinBridge> getNestedLoopJoinBridge(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  /// Returns a custom join bridge for 'planNodeId'.
  std::shared_ptr<JoinBridge> getCustomJoinBridge(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  /// Transitions this to kFinished state if all Drivers are
  /// finished. Otherwise sets a flag so that the last Driver to finish
  /// will transition the state.
  void setAllOutputConsumed();

  /// Adds 'stats' to the cumulative total stats for the operator in the Task
  /// stats. Called from Drivers upon their closure.
  void addOperatorStats(OperatorStats& stats);

  /// Adds per driver statistics.  Called from Drivers upon their closure.
  void addDriverStats(int pipelineId, DriverStats stats);

  /// Returns kNone if no pause or terminate is requested. The thread count is
  /// incremented if kNone is returned. If something else is returned the
  /// calling thread should unwind and return itself to its pool. If 'this' goes
  /// from no threads running to one thread running, sets 'onThreadSince_' to
  /// 'nowMicros'.
  StopReason enter(ThreadState& state, uint64_t nowMicros = 0);

  /// Sets the state to terminated. Returns kAlreadyOnThread if the
  /// Driver is running. In this case, the Driver will free resources
  /// and the caller should not do anything. Returns kTerminate if the
  /// Driver was not on thread. When this happens, the Driver is on the
  /// caller thread wit isTerminated set and the caller is responsible
  /// for freeing resources.
  StopReason enterForTerminateLocked(ThreadState& state);

  /// Marks that the Driver is not on thread. If no more Drivers in the
  /// CancelPool are on thread, this realizes threadFinishFutures_. These allow
  /// syncing with pause or termination. The Driver may go off thread because of
  /// hasBlockingFuture or pause requested or terminate requested. The
  /// return value indicates the reason. If kTerminate is returned, the
  /// isTerminated flag is set. 'driverCb' is called to close the driver before
  /// it goes off thread if the task has been terminated. It ensures that the
  /// driver close operation is always executed on driver thread. This helps to
  /// avoid the race condition between driver close and operator abort
  /// operations.
  void leave(
      ThreadState& state,
      const std::function<void(StopReason)>& driverCb);

  /// Enters a suspended section where the caller stays on thread but
  /// is not accounted as being on the thread.  Returns kNone if no
  /// terminate is requested. The thread count is decremented if kNone
  /// is returned. If thread count goes to zero, waiting promises are
  /// realized. If kNone is not returned the calling thread should
  /// unwind and return itself to its pool.
  StopReason enterSuspended(ThreadState& state);

  StopReason leaveSuspended(ThreadState& state);

  /// Returns a stop reason without synchronization. If the stop reason
  /// is yield, then atomically decrements the count of threads that
  /// are to yield.
  StopReason shouldStop();

  /// Returns true if Driver or async executor threads for 'this'
  /// should silently stop and drop any results that may be
  /// pending. This is like shouldStop() but can be called multiple
  /// times since not affect a yield counter.
  bool isCancelled() const {
    return terminateRequested_;
  }

  /// Requests the Task to stop activity.  The returned future is
  /// realized when all running threads have stopped running. Activity
  /// can be resumed with resume() after the future is realized.
  ContinueFuture requestPause();

  /// Requests activity of 'this' to stop. The returned future will be
  /// realized when the last thread stops running for 'this'. This is used to
  /// mark cancellation by the user.
  ContinueFuture requestCancel() {
    return terminate(kCanceled);
  }

  /// Like requestCancel but sets end state to kAborted. This is for stopping
  /// Tasks due to failures of other parts of the query.
  ContinueFuture requestAbort() {
    return terminate(kAborted);
  }

  void requestYield() {
    std::lock_guard<std::timed_mutex> l(mutex_);
    toYield_ = numThreads_;
  }

  /// Requests yield if 'this' is running and has had at least one Driver on
  /// thread since before 'startTimeMicros'. Returns the number of threads in
  /// 'this' at the time of requesting yield. Returns 0 if yield not requested.
  int32_t yieldIfDue(uint64_t startTimeMicros);

  /// Once 'pauseRequested_' is set, it will not be cleared until
  /// task::resume(). It is therefore OK to read it without a mutex
  /// from a thread that this flag concerns.
  bool pauseRequested() const {
    return pauseRequested_;
  }

  std::timed_mutex& mutex() {
    return mutex_;
  }

  /// Returns the number of concurrent drivers in the pipeline of 'driver'.
  int32_t numDrivers(Driver* driver) {
    return driverFactories_[driver->driverCtx()->pipelineId]->numDrivers;
  }

  /// Returns the number of created and deleted tasks since the velox engine
  /// starts running so far.
  static uint64_t numCreatedTasks() {
    return numCreatedTasks_;
  }

  static uint64_t numDeletedTasks() {
    return numDeletedTasks_;
  }

  const std::string& spillDirectory() const {
    return spillDirectory_;
  }

  /// Returns the spill directory path. Ensures that the spill directory is
  /// created before returning. Is thread safe. Returns an empty string if
  /// either the spill directory is not specified during task creation or the
  /// folder could not be created.
  const std::string& getOrCreateSpillDirectory();

  /// True if produces output via OutputBufferManager.
  bool hasPartitionedOutput() const {
    return numDriversInPartitionedOutput_ > 0;
  }

  void testingIncrementThreads() {
    std::lock_guard l(mutex_);
    ++numThreads_;
  }

  /// Invoked to run provided 'callback' on each alive driver of the task.
  void testingVisitDrivers(const std::function<void(Driver*)>& callback);

  /// Invoked to finish the task for test purpose.
  void testingFinish() {
    terminate(TaskState::kFinished).wait();
  }

 private:
  Task(
      const std::string& taskId,
      core::PlanFragment planFragment,
      int destination,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode,
      ConsumerSupplier consumerSupplier,
      std::function<void(std::exception_ptr)> onError = nullptr);

  // Consistency check of the task execution to make sure the execution mode
  // stays the same.
  void checkExecutionMode(ExecutionMode mode);

  // Creates driver factories.
  void createDriverFactoriesLocked(uint32_t maxDrivers);

  // Creates the output buffer in partitioned output buffer manager if needed.
  void initializePartitionOutput();

  // Creates and starts drivers.
  void createAndStartDrivers(uint32_t concurrentSplitGroups);

  // Creates a bunch of drivers for the given split group.
  std::vector<std::shared_ptr<Driver>> createDriversLocked(
      uint32_t splitGroupId);

  // Returns time (ms) since the task execution started or zero, if not started.
  uint64_t timeSinceStartMsLocked() const;

  // Returns reference to the SplitsState structure for the specified plan node
  // id. Throws if not found, meaning that plan node does not expect splits.
  SplitsState& getPlanNodeSplitsStateLocked(const core::PlanNodeId& planNodeId);

  // Validate that the supplied grouped execution leaf nodes make sense.
  void validateGroupedExecutionLeafNodes();

  // Returns true if all nodes expecting splits have received 'no more splits'
  // message.
  bool allNodesReceivedNoMoreSplitsMessageLocked() const;

  // Remove the spill directory, if the Task was creating it for potential
  // spilling.
  void removeSpillDirectoryIfExists();

  // Invoked to initialize the memory pool for this task on creation.
  void initTaskPool();

  // Creates new instance of memory pool for a plan node, stores it in the task
  // to ensure lifetime and returns a raw pointer.
  memory::MemoryPool* getOrAddNodePool(const core::PlanNodeId& planNodeId);

  // Similar to getOrAddNodePool but creates the memory pool instance for a hash
  // join plan node. If 'splitGroupId' is not kUngroupedGroupId, it specifies
  // the split group id under the grouped execution mode.
  memory::MemoryPool* getOrAddJoinNodePool(
      const core::PlanNodeId& planNodeId,
      uint32_t splitGroupId);

  // Creates a memory reclaimer instance for a plan node if the task memory
  // pool has set memory reclaimer. If 'isHashJoinNode' is true, it creates a
  // customized instance for hash join plan node, otherwise creates a default
  // memory reclaimer.
  std::unique_ptr<memory::MemoryReclaimer> createNodeReclaimer(
      bool isHashJoinNode) const;

  // Creates a memory reclaimer instance for an exchange client if the task
  // memory pool has set memory reclaimer. We don't support to reclaim memory
  // from an exchange client, and the customized reclaimer is used to handle
  // memory arbitration request initiated under the driver execution context.
  std::unique_ptr<memory::MemoryReclaimer> createExchangeClientReclaimer()
      const;

  // Creates a memory reclaimer instance for this task. If the query memory
  // pool doesn't set memory reclaimer, then the function simply returns null.
  // Otherwise, it creates a customized memory reclaimer for this task.
  std::unique_ptr<memory::MemoryReclaimer> createTaskReclaimer();

  // Creates new instance of MemoryPool for the exchange client of an
  // ExchangeNode in a pipeline, stores it in the task to ensure lifetime and
  // returns a raw pointer.
  velox::memory::MemoryPool* addExchangeClientPool(
      const core::PlanNodeId& planNodeId,
      uint32_t pipelineId);

  // Invoked to remove this task from the output buffer manager if it has set
  // output buffer.
  void maybeRemoveFromOutputBufferManager();

  // Returns task execution error message or empty string if not error
  // occurred. This should only be called inside mutex_ protection.
  std::string errorMessageLocked() const;

  folly::dynamic toShortJsonLocked() const;

  class MemoryReclaimer : public exec::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        const std::shared_ptr<Task>& task);

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

    void abort(memory::MemoryPool* pool, const std::exception_ptr& error)
        override;

   private:
    explicit MemoryReclaimer(const std::shared_ptr<Task>& task) : task_(task) {
      VELOX_CHECK_NOT_NULL(task);
    }

    // Gets the shared pointer to the driver to ensure its liveness during the
    // memory reclaim operation.
    //
    // NOTE: a task's memory pool might outlive the task itself.
    std::shared_ptr<Task> ensureTask() const {
      return task_.lock();
    }

    uint64_t reclaimTask(
        const std::shared_ptr<Task>& task,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats);

    std::weak_ptr<Task> task_;
  };

  // Counts the number of created tasks which is incremented on each task
  // creation.
  static std::atomic<uint64_t> numCreatedTasks_;

  // Counts the number of deleted tasks which is incremented on each task
  // destruction.
  static std::atomic<uint64_t> numDeletedTasks_;

  static void taskCreated() {
    ++numCreatedTasks_;
  }

  static void taskDeleted() {
    ++numDeletedTasks_;
  }

  /// Returns true if state is 'running'.
  bool isRunningLocked() const;

  /// Returns true if state is 'finished'.
  bool isFinishedLocked() const;

  template <class TBridgeType>
  std::shared_ptr<TBridgeType> getJoinBridgeInternal(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  template <class TBridgeType>
  std::shared_ptr<TBridgeType> getJoinBridgeInternalLocked(
      uint32_t splitGroupId,
      const core::PlanNodeId& planNodeId);

  /// Add remote split to ExchangeClient for the specified plan node. Used to
  /// close remote sources that are added after the task completed early.
  void addRemoteSplit(
      const core::PlanNodeId& planNodeId,
      const exec::Split& split);

  /// Retrieve a split or split future from the given split store structure.
  BlockingReason getSplitOrFutureLocked(
      bool forTableScan,
      SplitsStore& splitsStore,
      exec::Split& split,
      ContinueFuture& future,
      int32_t maxPreloadSplits,
      const ConnectorSplitPreloadFunc& preload);

  /// Returns next split from the store. The caller must ensure the store is not
  /// empty.
  exec::Split getSplitLocked(
      bool forTableScan,
      SplitsStore& splitsStore,
      int32_t maxPreloadSplits,
      const ConnectorSplitPreloadFunc& preload);

  // Creates for the given split group and fills up the 'SplitGroupState'
  // structure, which stores inter-operator state (local exchange, bridges).
  void createSplitGroupStateLocked(uint32_t splitGroupId);

  // Checks if we have splits in a split group that haven't been processed yet
  // and have capacity in terms of number of concurrent split groups being
  // processed. If yes, creates split group state and Drivers and runs them.
  void ensureSplitGroupsAreBeingProcessedLocked();

  void driverClosedLocked();

  // Returns true if Task is in kRunning state, but all output drivers finished
  // processing and all output has been consumed. In other words, returns true
  // if task should transition to kFinished state.
  //
  // In case of grouped execution, checks that all drivers, not just output
  // drivers finished processing.
  bool checkIfFinishedLocked();

  // Check if we have no more split groups coming and adjust the total number
  // of drivers if more split groups coming. Returns true if Task is in
  // kRunning state, but no more split groups are commit and all drivers
  // finished processing and all output has been consumed. In other words,
  // returns true if task should transition to kFinished state.
  bool checkNoMoreSplitGroupsLocked();

  // Notifies listeners that the task is now complete.
  void onTaskCompletion();

  // Returns true if all splits are finished processing and there are no more
  // splits coming for the task.
  bool isAllSplitsFinishedLocked();

  std::unique_ptr<ContinuePromise> addSplitLocked(
      SplitsState& splitsState,
      exec::Split&& split);

  std::unique_ptr<ContinuePromise> addSplitToStoreLocked(
      SplitsStore& splitsStore,
      exec::Split&& split);

  // Invoked when all the driver threads are off thread. The function returns
  // 'threadFinishPromises_' to fulfill.
  std::vector<ContinuePromise> allThreadsFinishedLocked();

  StopReason shouldStopLocked();

  // Sets this to a terminal requested state and frees all resources
  // of Drivers that are not presently on thread. Unblocks all waiting
  // Drivers, e.g.  Drivers waiting for free space in outgoing buffers
  // or new splits. Sets the state to 'terminalState', which should be
  // kCanceled for cancellation by users, kFailed for errors and
  // kAborted for termination due to failure in some other Task. The
  // returned future is realized when all threads running for 'this'
  // have finished.
  ContinueFuture terminate(TaskState terminalState);

  // Returns a future that is realized when there are no more threads
  // executing for 'this'. 'comment' is used as a debugging label on
  // the promise/future pair.
  ContinueFuture makeFinishFuture(const char* comment) {
    std::lock_guard<std::timed_mutex> l(mutex_);
    return makeFinishFutureLocked(comment);
  }

  ContinueFuture makeFinishFutureLocked(const char* comment);

  bool isOutputPipeline(int pipelineId) const {
    return driverFactories_[pipelineId]->outputDriver;
  }

  uint32_t numDrivers(int pipelineId) const {
    return driverFactories_[pipelineId]->numDrivers;
  }

  int getOutputPipelineId() const;

  // Create an exchange client for the specified exchange plan node at a given
  // pipeline.
  void createExchangeClientLocked(
      int32_t pipelineId,
      const core::PlanNodeId& planNodeId);

  // Get a shared reference to the exchange client with the specified exchange
  // plan node 'planNodeId'. The function returns null if there is no client
  // created for 'planNodeId' in 'exchangeClientByPlanNode_'.
  std::shared_ptr<ExchangeClient> getExchangeClient(
      const core::PlanNodeId& planNodeId) const {
    std::lock_guard<std::timed_mutex> l(mutex_);
    return getExchangeClientLocked(planNodeId);
  }

  std::shared_ptr<ExchangeClient> getExchangeClientLocked(
      const core::PlanNodeId& planNodeId) const;

  // Get a shared reference to the exchange client with the specified
  // 'pipelineId'. The function returns null if there is no client created for
  // 'pipelineId' set in 'exchangeClients_'.
  std::shared_ptr<ExchangeClient> getExchangeClientLocked(
      int32_t pipelineId) const;

  // The helper class used to maintain 'numCreatedTasks_' and 'numDeletedTasks_'
  // on task construction and destruction.
  class TaskCounter {
   public:
    TaskCounter() {
      Task::taskCreated();
    }
    ~TaskCounter() {
      Task::taskDeleted();
    }
  };
  friend class Task::TaskCounter;

  // NOTE: keep 'taskCount_' the first member so that it will be the first
  // constructed member and the last destructed one. The purpose is to make
  // 'numCreatedTasks_' and 'numDeletedTasks_' counting more robust to the
  // timing race condition when used in scenarios such as waiting for all the
  // tasks to be destructed in test.
  const TaskCounter taskCounter_;

  // Universally unique identifier of the task. Used to identify the task when
  // calling TaskListener.
  const std::string uuid_;

  // Application specific task ID specified at construction time. May not be
  // unique or universally unique.
  const std::string taskId_;
  core::PlanFragment planFragment_;
  const int destination_;
  const std::shared_ptr<core::QueryCtx> queryCtx_;

  // The execution mode of the task. It is enforced that a task can only be
  // executed in a single mode throughout its lifetime
  const ExecutionMode mode_;

  // Root MemoryPool for this Task. All member variables that hold references
  // to pool_ must be defined after pool_, childPools_.
  std::shared_ptr<memory::MemoryPool> pool_;

  // Keep driver and operator memory pools alive for the duration of the task
  // to allow for sharing vectors across drivers without copy.
  std::vector<std::shared_ptr<memory::MemoryPool>> childPools_;

  // The map from plan node id to the corresponding memory pool object's raw
  // pointer.
  //
  // NOTE: 'childPools_' holds the ownerships of node memory pools.
  std::unordered_map<std::string, memory::MemoryPool*> nodePools_;

  // Set to true by OutputBufferManager when all output is
  // acknowledged. If this happens before Drivers are at end, the last
  // Driver to finish will set state_ to kFinished. If Drivers have
  // finished then setting this to true will also set state_ to
  // kFinished.
  bool partitionedOutputConsumed_ = false;

  // Set if terminated by an error. This is the first error reported
  // by any of the instances.
  std::exception_ptr exception_ = nullptr;
  mutable std::timed_mutex mutex_;

  // Exchange clients. One per pipeline / source. Null for pipelines, which
  // don't need it.
  //
  // NOTE: there can be only one exchange client for a given pipeline ID, and
  // the exchange clients are also referenced by 'exchangeClientByPlanNode_'.
  // Hence, exchange clients can be indexed either by pipeline ID or by plan
  // node ID.
  std::vector<std::shared_ptr<ExchangeClient>> exchangeClients_;

  // Exchange clients keyed by the corresponding Exchange plan node ID. Used to
  // process remaining remote splits after the task has completed early.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<ExchangeClient>>
      exchangeClientByPlanNode_;

  ConsumerSupplier consumerSupplier_;

  // The function that is executed when the task encounters its first error,
  // that is, serError() is called for the first time.
  std::function<void(std::exception_ptr)> onError_;

  std::vector<std::unique_ptr<DriverFactory>> driverFactories_;
  std::vector<std::shared_ptr<Driver>> drivers_;
  // When Drivers are closed by the Task, there is a chance that race and/or
  // bugs can cause such Drivers to be held forever, in turn holding a pointer
  // to the Task making it a zombie Tasks. This vector is used to keep track of
  // such drivers to assist debugging zombie Tasks.
  std::vector<std::weak_ptr<Driver>> driversClosedByTask_;
  // The total number of running drivers in all pipelines.
  // This number changes over time as drivers finish their work and maybe new
  // get created.
  uint32_t numRunningDrivers_{0};
  // The total number of drivers we need to run in all pipelines. It is sum of
  // the Ungrouped Execution drivers plus the number of Grouped Execution
  // drivers per split group times the number of split groups. In practice for
  // tasks with Grouped Execution the final number would be much less (roughly
  // divided by the number of workers), so this will be adjusted in the end of
  // task's work.
  uint32_t numTotalDrivers_{0};
  // The number of completed drivers so far. This number increases over time as
  // drivers finish their work. We use this number to detect when the Task is
  // completed.
  uint32_t numFinishedDrivers_{0};
  // Reflects number of drivers required to process single split group during
  // grouped execution. Zero for a completely ungrouped execution.
  uint32_t numDriversPerSplitGroup_{0};
  // Reflects number of drivers required to run ungrouped execution in the
  // fragment. Zero for a completely grouped execution.
  uint32_t numDriversUngrouped_{0};
  // Number of drivers running in the pipeline hosting the Partitioned Output
  // (in a single split group). We use it to recalculate the number of producing
  // drivers at the end during the Grouped Execution mode.
  uint32_t numDriversInPartitionedOutput_{0};
  // True if the pipeline hosting the Partitioned Output runs in the Grouped
  // Execution mode. In this case we will need to update the number of output
  // drivers in the end. False otherwise.
  bool groupedPartitionedOutput_{false};
  // The number of splits groups we run concurrently.
  uint32_t concurrentSplitGroups_{1};

  // Have we already initialized stats of operators in the drivers for Grouped
  // Execution?
  bool initializedGroupedOpStats_{false};
  // Have we already initialized stats of operators in the drivers for Ungrouped
  // Execution?
  bool initializedUngroupedOpStats_{false};

  // How many splits groups we are processing at the moment. Used to control
  // split group concurrency. Ungrouped Split Group is not included here.
  uint32_t numRunningSplitGroups_{0};
  // Split groups for which we have received at least one split - meaning our
  // task is to process these. This set only grows. Used to deduplicate split
  // groups for different nodes and to determine how many split groups we to
  // process in total.
  std::unordered_set<uint32_t> seenSplitGroups_;
  // Split groups for which we have received splits but haven't started
  // processing. It grows with arrival of the 1st split of a previously not seen
  // split group and depletes with creating new sets of drivers to process
  // queued split groups.
  std::queue<uint32_t> queuedSplitGroups_;

  TaskState state_ = TaskState::kRunning;

  // Stores splits state structure for each plan node. At construction populated
  // with all leaf plan nodes that require splits. Afterwards accessed with
  // getPlanNodeSplitsStateLocked() to ensure we only manage splits of the plan
  // nodes that expect splits.
  std::unordered_map<core::PlanNodeId, SplitsState> splitsStates_;

  // Promises that are fulfilled when the task is completed (terminated).
  std::vector<ContinuePromise> taskCompletionPromises_;

  // Promises that are fulfilled when the task is destroyed.
  std::vector<ContinuePromise> taskDeletionPromises_;

  // Promises that are fulfilled when the task's state has changed and ready to
  // report some progress (such as split group finished or task is completed).
  std::vector<ContinuePromise> stateChangePromises_;

  TaskStats taskStats_;

  // Stores inter-operator state (exchange, bridges) per split group. During
  // ungrouped execution we use the [0] entry in this vector.
  std::unordered_map<uint32_t, SplitGroupState> splitGroupStates_;

  std::weak_ptr<OutputBufferManager> bufferManager_;

  // Boolean indicating that we have already received no-more-output-buffers
  // message. Subsequent messages will be ignored.
  bool noMoreOutputBuffers_{false};

  // Thread counts and cancellation -related state.
  //
  // Some variables below are declared atomic for tsan because they are
  // sometimes tested outside 'mutex_' for a value of 0/false,
  // which is safe to access without acquiring 'mutex_'.Thread counts
  // and promises are guarded by 'mutex_'
  std::atomic<bool> pauseRequested_{false};
  std::atomic<bool> terminateRequested_{false};
  std::atomic<int32_t> toYield_ = 0;
  int32_t numThreads_ = 0;
  // Microsecond real time when 'this' last went from no threads to
  // one thread running. Used to decide if continuous run should be
  // interrupted by yieldIfDue().
  tsan_atomic<uint64_t> onThreadSince_{0};
  // Promises for the futures returned to callers of requestPause() or
  // terminate(). They are fulfilled when the last thread stops
  // running for 'this'.
  std::vector<ContinuePromise> threadFinishPromises_;

  // Base spill directory for this task.
  std::string spillDirectory_;

  // Mutex to ensure only the first caller thread of 'getOrCreateSpillDirectory'
  // creates the directory.
  mutable std::mutex spillDirCreateMutex_;

  // Indicates whether the spill directory has been created.
  std::atomic<bool> spillDirectoryCreated_{false};

  // Stores unconsumed preloading splits to ensure they are closed promptly.
  folly::F14FastSet<std::shared_ptr<connector::ConnectorSplit>>
      preloadingSplits_;
};

/// Listener invoked on task completion.
class TaskListener {
 public:
  virtual ~TaskListener() = default;

  /// Called on task completion. Provides the information about success or
  /// failure as well as runtime statistics about task execution.
  virtual void onTaskCompletion(
      const std::string& taskUuid,
      const std::string& taskId,
      TaskState state,
      std::exception_ptr error,
      TaskStats stats) = 0;
};

/// Register a listener to be invoked on task completion. Returns true if
/// listener was successfully registered, false if listener is already
/// registered.
bool registerTaskListener(std::shared_ptr<TaskListener> listener);

/// Unregister a listener registered earlier. Returns true if listener was
/// unregistered successfuly, false if listener was not found.
bool unregisterTaskListener(const std::shared_ptr<TaskListener>& listener);

std::string executionModeString(Task::ExecutionMode mode);

std::ostream& operator<<(std::ostream& out, Task::ExecutionMode mode);

} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::Task::ExecutionMode>
    : formatter<std::string> {
  auto format(
      facebook::velox::exec::Task::ExecutionMode m,
      format_context& ctx) {
    return formatter<std::string>::format(
        facebook::velox::exec::executionModeString(m), ctx);
  }
};
