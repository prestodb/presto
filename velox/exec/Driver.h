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
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/portability/SysSyscall.h>

#include "velox/common/future/VeloxPromise.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"

namespace facebook::velox::exec {

class Driver;
class ExchangeClient;
class Operator;
struct OperatorStats;
class Task;

enum class StopReason {
  // Keep running.
  kNone,
  // Go off thread and do not schedule more activity.
  kPause,
  // Stop and free all. This is returned once and the thread that gets
  // this value is responsible for freeing the state associated with
  // the thread. Other threads will get kAlreadyTerminated after the
  // first thread has received kTerminate.
  kTerminate,
  kAlreadyTerminated,
  // Go off thread and then enqueue to the back of the runnable queue.
  kYield,
  // Must wait for external events.
  kBlock,
  // No more data to produce.
  kAtEnd,
  kAlreadyOnThread
};

// Represents a Driver's state. This is used for cancellation, forcing
// release of and for waiting for memory. The fields are serialized on
// the mutex of the Driver's Task.
//
// The Driver goes through the following states:
// Not on thread. It is created and has not started. All flags are false.
//
// Enqueued - The Driver is added to an executor but does not yet have a thread.
// isEnqueued is true. Next states are terminated or on thread.
//
// On thread - 'thread' is set to the thread that is running the Driver. Next
// states are blocked, terminated, suspended, enqueued.
//
//  Blocked - The Driver is not on thread and is waiting for an external event.
//  Next states are terminated, enqueued.
//
// Suspended - The Driver is on thread, 'thread' and 'isSuspended' are set. The
// thread does not manipulate the Driver's state and is suspended as in waiting
// for memory or out of process IO. This is different from Blocked in that here
// we keep the stack so that when the wait is over the control stack is not
// lost. Next states are on thread or terminated.
//
//  Terminated - 'isTerminated' is set. The Driver cannot run after this and
// the state is final.
//
// CancelPool  allows terminating or pausing a set of Drivers. The Task API
// allows starting or resuming Drivers. When terminate is requested the request
// is successful when all Drivers are off thread, blocked or suspended. When
// pause is requested, we have success when all Drivers are either enqueued,
// suspended, off thread or blocked.
struct ThreadState {
  // The thread currently running this.
  std::atomic<std::thread::id> thread{};
  // The tid of 'thread'. Allows finding the thread in a debugger.
  std::atomic<int32_t> tid{0};
  // True if queued on an executor but not on thread.
  std::atomic<bool> isEnqueued{false};
  // True if being terminated or already terminated.
  std::atomic<bool> isTerminated{false};
  // True if there is a future outstanding that will schedule this on an
  // executor thread when some promise is realized.
  bool hasBlockingFuture{false};
  // True if on thread but in a section waiting for RPC or memory
  // strategy decision. The thread is not supposed to access its
  // memory, which a third party can revoke while the thread is in
  // this state.
  bool isSuspended{false};

  bool isOnThread() const {
    return thread != std::thread::id();
  }

  void setThread() {
    thread = std::this_thread::get_id();
#if !defined(__APPLE__)
    // This is a debugging feature disabled on the Mac since syscall
    // is deprecated on that platform.
    tid = syscall(FOLLY_SYS_gettid);
#endif
  }

  void clearThread() {
    thread = std::thread::id(); // no thread.
    tid = 0;
  }
};

enum class BlockingReason {
  kNotBlocked,
  kWaitForConsumer,
  kWaitForSplit,
  kWaitForExchange,
  kWaitForJoinBuild,
  kWaitForMemory
};

std::string blockingReasonToString(BlockingReason reason);

using ContinueFuture = folly::SemiFuture<bool>;

class BlockingState {
 public:
  BlockingState(
      std::shared_ptr<Driver> driver,
      ContinueFuture&& future,
      Operator* FOLLY_NONNULL op,
      BlockingReason reason);

  static void setResume(std::shared_ptr<BlockingState> state);

  Operator* FOLLY_NONNULL op() {
    return operator_;
  }

  BlockingReason reason() {
    return reason_;
  }

 private:
  std::shared_ptr<Driver> driver_;
  ContinueFuture future_;
  Operator* FOLLY_NONNULL operator_;
  BlockingReason reason_;
  uint64_t sinceMicros_;
};

struct DriverCtx {
  std::shared_ptr<Task> task;
  std::unique_ptr<core::ExecCtx> execCtx;
  std::unique_ptr<connector::ExpressionEvaluator> expressionEvaluator;
  const int driverId;
  const int pipelineId;
  /// Id of the split group this driver should process in case of grouped
  /// execution, zero otherwise.
  const uint32_t splitGroupId;
  /// Id of the partition to use by this driver. For local exchange, for
  /// instance.
  const uint32_t partitionId;
  Driver* FOLLY_NONNULL driver;

  explicit DriverCtx(
      std::shared_ptr<Task> _task,
      int _driverId,
      int _pipelineId,
      uint32_t _splitGroupId,
      uint32_t _partitionId);

  velox::memory::MemoryPool* FOLLY_NONNULL addOperatorPool();

  // Makes an extract of QueryCtx for use in a connector. 'planNodeId'
  // is the id of the calling TableScan. This and the task id identify
  // the scan for column access tracking.
  std::unique_ptr<connector::ConnectorQueryCtx> createConnectorQueryCtx(
      const std::string& connectorId,
      const std::string& planNodeId) const;
};

class Driver {
 public:
  Driver(
      std::unique_ptr<DriverCtx> driverCtx,
      std::vector<std::unique_ptr<Operator>>&& operators);

  ~Driver();

  static void run(std::shared_ptr<Driver> self);

  static void enqueue(std::shared_ptr<Driver> instance);

  bool isOnThread() const {
    return state_.isOnThread();
  }

  bool isTerminated() const {
    return state_.isTerminated;
  }

  std::string label() const;

  ThreadState& state() {
    return state_;
  }

  void initializeOperatorStats(std::vector<OperatorStats>& stats);

  void addStatsToTask();

  // Returns true if all operators between the source and 'aggregation' are
  // order-preserving and do not increase cardinality.
  bool mayPushdownAggregation(Operator* FOLLY_NONNULL aggregation) const;

  // Returns a subset of channels for which there are operators upstream from
  // filterSource that accept dynamically generated filters.
  std::unordered_set<ChannelIndex> canPushdownFilters(
      Operator* FOLLY_NONNULL filterSource,
      const std::vector<ChannelIndex>& channels) const;

  // Returns the Operator with 'planNodeId.' or nullptr if not
  // found. For example, hash join probe accesses the corresponding
  // build by id.
  Operator* FOLLY_NULLABLE findOperator(std::string_view planNodeId) const;

  void setError(std::exception_ptr exception);

  std::string toString();

  DriverCtx* FOLLY_NONNULL driverCtx() const {
    return ctx_.get();
  }

  std::shared_ptr<Task> task() const {
    return task_;
  }

  // Updates the stats in 'task_' and frees resources. Only called by Task for
  // closing non-running Drivers.
  void closeByTask();

 private:
  void enqueueInternal();

  StopReason runInternal(
      std::shared_ptr<Driver>& self,
      std::shared_ptr<BlockingState>* FOLLY_NONNULL blockingState);

  void close();

  // Push down dynamic filters produced by the operator at the specified
  // position in the pipeline.
  void pushdownFilters(int operatorIndex);

  std::unique_ptr<DriverCtx> ctx_;
  std::shared_ptr<Task> task_;

  // Set via Task_ and serialized by 'task_'s mutex.
  ThreadState state_;

  // Timer used to track down the time we are sitting in the driver queue.
  size_t queueTimeStartMicros_{0};
  // Index of the current operator to run (or the 1st one if we haven't started
  // yet). Used to determine which operator's queueTime we should update.
  size_t curOpIndex_{0};

  std::vector<std::unique_ptr<Operator>> operators_;

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
};

using OperatorSupplier = std::function<std::unique_ptr<Operator>(
    int32_t operatorId,
    DriverCtx* FOLLY_NONNULL ctx)>;

using Consumer =
    std::function<BlockingReason(RowVectorPtr, ContinueFuture* FOLLY_NULLABLE)>;
using ConsumerSupplier = std::function<Consumer()>;

struct DriverFactory {
  std::vector<std::shared_ptr<const core::PlanNode>> planNodes;
  /// Function that will generate the final operator of a driver being
  /// constructed.
  OperatorSupplier consumerSupplier;
  /// Maximum number of drivers that can be run concurrently in this pipeline.
  uint32_t maxDrivers;
  /// Number of drivers that will be run concurrently in this pipeline for one
  /// split group (during grouped execution) or for the whole task (ungrouped
  /// execution).
  uint32_t numDrivers;
  /// Total number of drivers in this pipeline we expect to be run. In case of
  /// grouped execution it is 'numDrivers' * 'numSplitGroups', otherwise it is
  /// 'numDrivers'.
  uint32_t numTotalDrivers;
  /// The (local) node that will consume results supplied by this pipeline.
  /// Can be null. We use that to determine the max drivers.
  std::shared_ptr<const core::PlanNode> consumerNode;

  // True if 'planNodes' contains a source node for the task, e.g. TableScan or
  // Exchange.
  bool inputDriver{false};
  // True if 'planNodes' contains a sync node for the task, e.g.
  // PartitionedOutput.
  bool outputDriver{false};

  std::shared_ptr<Driver> createDriver(
      std::unique_ptr<DriverCtx> ctx,
      std::shared_ptr<ExchangeClient> exchangeClient,
      std::function<int(int pipelineId)> numDrivers);

  std::shared_ptr<const core::PartitionedOutputNode> needsPartitionedOutput() {
    VELOX_CHECK(!planNodes.empty());
    if (auto partitionedOutputNode =
            std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
                planNodes.back())) {
      return partitionedOutputNode;
    }
    return nullptr;
  }

  bool needsExchangeClient() const {
    VELOX_CHECK(!planNodes.empty());
    if (auto exchangeNode = std::dynamic_pointer_cast<const core::ExchangeNode>(
            planNodes.front())) {
      return true;
    }
    return false;
  }

  /// Returns LocalPartition plan node ID if the pipeline gets data from a local
  /// exchange.
  std::optional<core::PlanNodeId> needsLocalExchangeSource() const {
    VELOX_CHECK(!planNodes.empty());
    if (auto exchangeNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                planNodes.front())) {
      return exchangeNode->id();
    }
    return std::nullopt;
  }

  /// Returns plan node IDs of all HashJoinNode's in the pipeline.
  std::vector<core::PlanNodeId> needsHashJoinBridges() const {
    std::vector<core::PlanNodeId> planNodeIds;
    for (const auto& planNode : planNodes) {
      if (auto joinNode =
              std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
    return planNodeIds;
  }

  /// Returns plan node IDs of all CrossJoinNode's in the pipeline.
  std::vector<core::PlanNodeId> needsCrossJoinBridges() const {
    std::vector<core::PlanNodeId> joinNodeIds;
    for (const auto& planNode : planNodes) {
      if (auto joinNode =
              std::dynamic_pointer_cast<const core::CrossJoinNode>(planNode)) {
        joinNodeIds.emplace_back(joinNode->id());
      }
    }

    return joinNodeIds;
  }
};

// Begins and ends a section where a thread is running but not
// counted in its Task. Using this, a Driver thread can for
// example stop its own Task. For arbitrating memory overbooking,
// the contending threads go suspended and each in turn enters a
// global critical section. When running the arbitration strategy, a
// thread can stop and restart Tasks, including its own. When a Task
// is stopped, its drivers are blocked or suspended and the strategy thread
// can alter the Task's memory including spilling or killing the whole Task.
// Other threads waiting to run the arbitration, are in a suspended state
// which also means that they are instantaneously killable or spillable.
class SuspendedSection {
 public:
  explicit SuspendedSection(Driver* FOLLY_NONNULL driver);
  ~SuspendedSection();

 private:
  Driver* FOLLY_NONNULL driver_;
};

} // namespace facebook::velox::exec
