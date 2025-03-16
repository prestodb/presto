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

#include <memory>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/portability/SysSyscall.h>

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/base/TraceConfig.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"

namespace facebook::velox::exec {

class Driver;
class ExchangeClient;
class Operator;
struct OperatorStats;
class Task;

enum class StopReason {
  /// Keep running.
  kNone,
  /// Go off thread and do not schedule more activity.
  kPause,
  /// Stop and free all. This is returned once and the thread that gets this
  /// value is responsible for freeing the state associated with the thread.
  /// Other threads will get kAlreadyTerminated after the first thread has
  /// received kTerminate.
  kTerminate,
  kAlreadyTerminated,
  /// Go off thread and then enqueue to the back of the runnable queue.
  kYield,
  /// Must wait for external events.
  kBlock,
  /// No more data to produce.
  kAtEnd,
  kAlreadyOnThread
};

std::string stopReasonString(StopReason reason);

std::ostream& operator<<(std::ostream& out, const StopReason& reason);

struct DriverStats {
  static constexpr const char* kTotalPauseTime = "totalDriverPauseWallNanos";
  static constexpr const char* kTotalOffThreadTime =
      "totalDriverOffThreadWallNanos";

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;
};

/// Represents a Driver's state. This is used for cancellation, forcing
/// release of and for waiting for memory. The fields are serialized on
/// the mutex of the Driver's Task.
///
/// The Driver goes through the following states:
/// Not on thread. It is created and has not started. All flags are false.
///
/// Enqueued - The Driver is added to an executor but does not yet have a
/// thread. isEnqueued is true. Next states are terminated or on thread.
///
/// On thread - 'thread' is set to the thread that is running the Driver. Next
/// states are blocked, terminated, suspended, enqueued.
///
///  Blocked - The Driver is not on thread and is waiting for an external event.
///  Next states are terminated, enqueued.
///
/// Suspended - The Driver is on thread, 'thread' and 'isSuspended' are set. The
/// thread does not manipulate the Driver's state and is suspended as in waiting
/// for memory or out of process IO. This is different from Blocked in that here
/// we keep the stack so that when the wait is over the control stack is not
/// lost. Next states are on thread or terminated.
///
///  Terminated - 'isTerminated' is set. The Driver cannot run after this and
/// the state is final.
///
/// CancelPool  allows terminating or pausing a set of Drivers. The Task API
/// allows starting or resuming Drivers. When terminate is requested the request
/// is successful when all Drivers are off thread, blocked or suspended. When
/// pause is requested, we have success when all Drivers are either enqueued,
/// suspended, off thread or blocked.
struct ThreadState {
  /// The thread currently running this.
  std::atomic<std::thread::id> thread{std::thread::id()};
  /// The tid of 'thread'. Allows finding the thread in a debugger.
  std::atomic<int32_t> tid{0};
  /// True if queued on an executor but not on thread.
  std::atomic<bool> isEnqueued{false};
  /// True if being terminated or already terminated.
  std::atomic<bool> isTerminated{false};
  /// True if there is a future outstanding that will schedule this on an
  /// executor thread when some promise is realized.
  bool hasBlockingFuture{false};
  /// The number of suspension requests on a on-thread driver. If > 0, this
  /// driver thread is in a (recursive) section waiting for RPC or memory
  /// strategy decision. The thread is not supposed to access its memory, which
  /// a third party can revoke while the thread is in this state.
  std::atomic<uint32_t> numSuspensions{0};
  /// The start execution time on thread in milliseconds. It is reset when the
  /// driver goes off thread. This is used to track the time that a driver has
  /// continuously run on a thread for per-driver cpu time slice enforcement.
  size_t startExecTimeMs{0};
  /// The end execution time on thread in milliseconds. It is set when the
  /// driver goes off thread and reset when the driver gets on a thread.
  size_t endExecTimeMs{0};
  /// Total time the driver in pause.
  uint64_t totalPauseTimeMs{0};
  /// Total off thread time (including blocked time and pause time).
  uint64_t totalOffThreadTimeMs{0};

  bool isOnThread() const {
    return thread != std::thread::id();
  }

  void setThread() {
    thread = std::this_thread::get_id();
    startExecTimeMs = getCurrentTimeMs();
    if (endExecTimeMs != 0) {
      totalOffThreadTimeMs += startExecTimeMs - endExecTimeMs;
      endExecTimeMs = 0;
    }
#if !defined(__APPLE__)
    // This is a debugging feature disabled on the Mac since syscall
    // is deprecated on that platform.
    tid = syscall(FOLLY_SYS_gettid);
#endif
  }

  void clearThread() {
    thread = std::thread::id(); // no thread.
    endExecTimeMs = getCurrentTimeMs();
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricDriverExecTimeMs, (endExecTimeMs - startExecTimeMs));
    startExecTimeMs = 0;
    tid = 0;
  }

  /// Returns the driver execution time on thread. Returns zero if the driver
  /// is currently not running on thread.
  size_t execTimeMs() const {
    if (startExecTimeMs == 0) {
      VELOX_CHECK(!isOnThread());
      return 0;
    }
    return getCurrentTimeMs() - startExecTimeMs;
  }

  bool suspended() const {
    return numSuspensions > 0;
  }

  folly::dynamic toJson() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["onThread"] = std::to_string(isOnThread());
    obj["tid"] = tid.load();
    obj["isTerminated"] = isTerminated.load();
    obj["isEnqueued"] = isEnqueued.load();
    obj["hasBlockingFuture"] = hasBlockingFuture;
    obj["isSuspended"] = suspended();
    obj["startExecTime"] = startExecTimeMs;
    return obj;
  }
};

enum class BlockingReason {
  kNotBlocked,
  kWaitForConsumer,
  kWaitForSplit,
  /// Some operators can get blocked due to the producer(s) (they are
  /// currently waiting data from) not having anything produced. Used by
  /// LocalExchange, LocalMergeExchange, Exchange and MergeExchange operators.
  kWaitForProducer,
  kWaitForJoinBuild,
  /// For a build operator, it is blocked waiting for the probe operators to
  /// finish probing before build the next hash table from one of the
  /// previously spilled partition data. For a probe operator, it is blocked
  /// waiting for all its peer probe operators to finish probing before
  /// notifying the build operators to build the next hash table from the
  /// previously spilled data.
  kWaitForJoinProbe,
  /// Used by MergeJoin operator, indicating that it was blocked by the right
  /// side input being unavailable.
  kWaitForMergeJoinRightSide,
  kWaitForMemory,
  kWaitForConnector,
  /// Some operators (like Table Scan) may run long loops and can 'voluntarily'
  /// exit them because Task requested to yield or stop or after a certain time.
  /// This is the blocking reason used in such cases.
  kYield,
  /// Operator is blocked waiting for its associated query memory arbitration to
  /// finish.
  kWaitForArbitration,
  /// For a table scan operator, it is blocked waiting for the scan controller
  /// to increase the number of table scan processing threads to start
  /// processing.
  kWaitForScanScaleUp,
  /// Used by IndexLookupJoin operator, indicating that it was blocked by the
  /// async index lookup.
  kWaitForIndexLookup,
};

std::string blockingReasonToString(BlockingReason reason);

class BlockingState {
 public:
  BlockingState(
      std::shared_ptr<Driver> driver,
      ContinueFuture&& future,
      Operator* op,
      BlockingReason reason);

  ~BlockingState() {
    numBlockedDrivers_--;
  }

  static void setResume(std::shared_ptr<BlockingState> state);

  Operator* op() {
    return operator_;
  }

  BlockingReason reason() {
    return reason_;
  }

  /// Moves out the blocking future stored inside. Can be called only once.
  /// Used in serial execution mode.
  ContinueFuture future() {
    return std::move(future_);
  }

  /// Returns total number of drivers process wide that are currently in
  /// blocked state.
  static uint64_t numBlockedDrivers() {
    return numBlockedDrivers_;
  }

 private:
  std::shared_ptr<Driver> driver_;
  ContinueFuture future_;
  Operator* operator_;
  BlockingReason reason_;
  uint64_t sinceUs_;

  static std::atomic_uint64_t numBlockedDrivers_;
};

/// Special group id to reflect the ungrouped execution.
constexpr uint32_t kUngroupedGroupId{std::numeric_limits<uint32_t>::max()};

struct DriverCtx {
  const int driverId;
  const int pipelineId;
  /// Id of the split group this driver should process in case of grouped
  /// execution, kUngroupedGroupId otherwise.
  const uint32_t splitGroupId;
  /// Id of the partition to use by this driver. For local exchange, for
  /// instance.
  const uint32_t partitionId;

  std::shared_ptr<Task> task;
  Driver* driver{nullptr};
  facebook::velox::process::ThreadDebugInfo threadDebugInfo;
  /// Tracks the traced operator ids. It is also used to avoid tracing the
  /// auxiliary operator such as the aggregation operator used by the table
  /// writer to generate the columns stats.
  std::unordered_map<int32_t, std::string> tracedOperatorMap;

  DriverCtx(
      std::shared_ptr<Task> _task,
      int _driverId,
      int _pipelineId,
      uint32_t _splitGroupId,
      uint32_t _partitionId);

  const core::QueryConfig& queryConfig() const;

  const std::optional<trace::TraceConfig>& traceConfig() const;

  velox::memory::MemoryPool* addOperatorPool(
      const core::PlanNodeId& planNodeId,
      const std::string& operatorType);

  /// Builds the spill config for the operator with specified 'operatorId'.
  std::optional<common::SpillConfig> makeSpillConfig(int32_t operatorId) const;

  common::PrefixSortConfig prefixSortConfig() const {
    return common::PrefixSortConfig{
        queryConfig().prefixSortNormalizedKeyMaxBytes(),
        queryConfig().prefixSortMinRows(),
        queryConfig().prefixSortMaxStringPrefixLength()};
  }
};

constexpr const char* kOpMethodNone = "";
constexpr const char* kOpMethodIsBlocked = "isBlocked";
constexpr const char* kOpMethodNeedsInput = "needsInput";
constexpr const char* kOpMethodGetOutput = "getOutput";
constexpr const char* kOpMethodAddInput = "addInput";
constexpr const char* kOpMethodNoMoreInput = "noMoreInput";
constexpr const char* kOpMethodIsFinished = "isFinished";

/// Same as the structure below, but does not have atomic members.
/// Used to return the status from the struct with atomics.
struct OpCallStatusRaw {
  /// Time (ms) when the operator call started.
  size_t timeStartMs{0};
  /// Id of the operator, method of which is currently running. It is index into
  /// the vector of Driver's operators.
  int32_t opId{0};
  /// Method of the operator, which is currently running.
  const char* method{kOpMethodNone};

  bool empty() const {
    return timeStartMs == 0;
  }

  static std::string formatCall(Operator* op, const char* operatorMethod);
  size_t callDuration() const;
};

/// Structure holds the information about the current operator call the driver
/// is in. Can be used to detect deadlocks and otherwise blocked calls.
/// If timeStartMs is zero, then we aren't in an operator call.
struct OpCallStatus {
  OpCallStatus() {}

  /// The status accessor.
  OpCallStatusRaw operator()() const {
    return OpCallStatusRaw{timeStartMs, opId, method};
  }

  void start(int32_t operatorId, const char* operatorMethod);
  void stop();

 private:
  /// Time (ms) when the operator call started.
  std::atomic_size_t timeStartMs{0};
  /// Id of the operator, method of which is currently running. It is index into
  /// the vector of Driver's operators.
  std::atomic_int32_t opId{0};
  /// Method of the operator, which is currently running.
  std::atomic<const char*> method{kOpMethodNone};
};

class Driver : public std::enable_shared_from_this<Driver> {
 public:
  static void enqueue(std::shared_ptr<Driver> instance);

  /// Run the pipeline until it produces a batch of data or gets blocked.
  /// Return the data produced or nullptr if pipeline finished processing and
  /// will not produce more data. Return nullptr and set 'future' if
  /// pipeline got blocked. 'blockingOp' and 'blockingReason' are set if the
  /// driver is blocked by an operator.
  ///
  /// This API supports execution of a Task synchronously in the caller's
  /// thread. The caller must use either this API or 'enqueue', but not both.
  /// When using 'enqueue', the last operator in the pipeline (sink) must not
  /// return any data from Operator::getOutput(). When using 'next', the last
  /// operator must produce data that will be returned to caller.
  RowVectorPtr next(
      ContinueFuture* future,
      Operator*& blockingOp,
      BlockingReason& blockingReason);

  /// Invoked to initialize the operators from this driver once on its first
  /// execution.
  void initializeOperators();

  bool isOnThread() const {
    return state_.isOnThread();
  }

  /// Returns the time in ms since this driver started execution on thread. The
  /// function returns zero if this driver is off-thread.
  uint64_t execTimeMs() const {
    return state_.execTimeMs();
  }

  bool isTerminated() const {
    return state_.isTerminated;
  }

  std::string label() const;

  ThreadState& state() {
    return state_;
  }

  /// Returns true if this driver is running on thread and has exceeded the cpu
  /// time slice limit if set.
  bool shouldYield() const;

  /// Checks if the associated query is under memory arbitration or not. The
  /// function returns true if it is and set future which is fulfilled when the
  /// memory arbitration finishes.
  bool checkUnderArbitration(ContinueFuture* future);

  void initializeOperatorStats(std::vector<OperatorStats>& stats);

  /// Close operators and add operator stats to the task.
  void closeOperators();

  /// Returns true if all operators between the source and 'aggregation' are
  /// order-preserving and do not increase cardinality.
  bool mayPushdownAggregation(Operator* aggregation) const;

  /// Returns a subset of channels for which there are operators upstream from
  /// filterSource that accept dynamically generated filters.
  std::unordered_set<column_index_t> canPushdownFilters(
      const Operator* filterSource,
      const std::vector<column_index_t>& channels) const;

  /// Returns the Operator with 'planNodeId' or nullptr if not found. For
  /// example, hash join probe accesses the corresponding build by id.
  Operator* findOperator(std::string_view planNodeId) const;

  /// Returns the Operator with 'operatorId' (basically by index) or throws if
  /// not found.
  Operator* findOperator(int32_t operatorId) const;

  /// Returns the Operator with 'operatorId' (basically by index) or nullptr if
  /// not found.
  Operator* findOperatorNoThrow(int32_t operatorId) const;

  /// Returns a list of all operators.
  std::vector<Operator*> operators() const;

  std::string toString() const;

  folly::dynamic toJson() const;

  OpCallStatusRaw opCallStatus() const {
    return opCallStatus_();
  }

  DriverCtx* driverCtx() const {
    return ctx_.get();
  }

  const std::shared_ptr<Task>& task() const {
    return ctx_->task;
  }

  /// Updates the stats in Task and frees resources. Only called by Task for
  /// closing non-running Drivers.
  void closeByTask();

  BlockingReason blockingReason() const {
    return blockingReason_;
  }

  /// Returns the process-wide number of driver cpu yields.
  static std::atomic_uint64_t& yieldCount();

  static std::shared_ptr<Driver> testingCreate(
      std::unique_ptr<DriverCtx> ctx = nullptr) {
    auto driver = new Driver();
    if (ctx != nullptr) {
      ctx->driver = driver;
      driver->ctx_ = std::move(ctx);
    }
    return std::shared_ptr<Driver>(driver);
  }

 private:
  // Ensures that the thread is removed from its Task's thread count on exit.
  class CancelGuard {
   public:
    CancelGuard(
        const std::shared_ptr<Driver>& driver,
        Task* task,
        ThreadState* state,
        std::function<void(StopReason)> onTerminate)
        : driver_(driver),
          task_(task),
          state_(state),
          onTerminate_(std::move(onTerminate)) {}

    void notThrown() {
      isThrow_ = false;
    }

    ~CancelGuard();

   private:
    std::shared_ptr<Driver> const driver_;
    Task* const task_;
    ThreadState* const state_;
    const std::function<void(StopReason reason)> onTerminate_;

    bool isThrow_{true};
  };

  Driver() = default;

  // Invoked to record the driver cpu yield count.
  static void recordYieldCount();

  void init(
      std::unique_ptr<DriverCtx> driverCtx,
      std::vector<std::unique_ptr<Operator>> operators);

  void enqueueInternal();

  static void run(std::shared_ptr<Driver> self);

  StopReason runInternal(
      std::shared_ptr<Driver>& self,
      std::shared_ptr<BlockingState>& blockingState,
      RowVectorPtr& result);

  void updateStats();

  void close();

  // Push down dynamic filters produced by the operator at the specified
  // position in the pipeline.
  void pushdownFilters(int operatorIndex);

  using TimingMemberPtr = CpuWallTiming OperatorStats::*;
  template <typename Func>
  void withDeltaCpuWallTimer(
      Operator* op,
      TimingMemberPtr opTimingMember,
      Func&& opFunction);

  // Adjusts 'timing' by removing the lazy load wall time, CPU time, and input
  // bytes accrued since last time timing information was recorded for 'op'. The
  // accrued lazy load times are credited to the source operator of 'this'. The
  // per-operator runtimeStats for lazy load are left in place to reflect which
  // operator triggered the load but these do not bias the op's timing.
  CpuWallTiming processLazyIoStats(Operator& op, const CpuWallTiming& timing);

  inline void validateOperatorOutputResult(
      const RowVectorPtr& result,
      const Operator& op);

  inline StopReason blockDriver(
      const std::shared_ptr<Driver>& self,
      size_t blockedOperatorId,
      ContinueFuture&& future,
      std::shared_ptr<BlockingState>& blockingState,
      CancelGuard& guard);

  std::unique_ptr<DriverCtx> ctx_;

  // If not zero, specifies the driver cpu time slice.
  size_t cpuSliceMs_{0};

  bool operatorsInitialized_{false};

  std::atomic_bool closed_{false};

  OpCallStatus opCallStatus_;

  // Set via Task and serialized by Task's mutex.
  ThreadState state_;

  // Timer used to track down the time we are sitting in the driver queue.
  size_t queueTimeStartUs_{0};
  // Id (index in the vector) of the current operator to run (or the 1st one if
  // we haven't started yet). Used to determine which operator's queueTime we
  // should update.
  size_t curOperatorId_{0};

  std::vector<std::unique_ptr<Operator>> operators_;

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  size_t blockedOperatorId_{0};

  bool trackOperatorCpuUsage_;

  // Indicates that a DriverAdapter can rearrange Operators. Set to false at end
  // of DriverFactory::createDriver().
  bool isAdaptable_{true};

  friend struct DriverFactory;
};

using OperatorSupplier = std::function<
    std::unique_ptr<Operator>(int32_t operatorId, DriverCtx* ctx)>;

using Consumer = std::function<BlockingReason(RowVectorPtr, ContinueFuture*)>;
using ConsumerSupplier = std::function<Consumer()>;

struct DriverFactory;
using AdaptDriverFunction =
    std::function<bool(const DriverFactory& factory, Driver& driver)>;

struct DriverAdapter {
  std::string label;
  std::function<void(const core::PlanFragment&)> inspect;
  AdaptDriverFunction adapt;
};

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
  /// True if the drivers in this pipeline use grouped execution strategy.
  bool groupedExecution{false};
  /// True if 'planNodes' contains a source node for the task, e.g. TableScan
  /// or Exchange.
  bool inputDriver{false};
  /// True if 'planNodes' contains a sync node for the task, e.g.
  /// PartitionedOutput.
  bool outputDriver{false};
  /// Contains node ids for which Hash Join Bridges connect ungrouped
  /// execution and grouped execution and must be created in ungrouped
  /// execution pipeline and skipped in grouped execution pipeline.
  folly::F14FastSet<core::PlanNodeId> mixedExecutionModeHashJoinNodeIds;
  /// Same as 'mixedExecutionModeHashJoinNodeIds' but for Nested Loop Joins.
  folly::F14FastSet<core::PlanNodeId> mixedExecutionModeNestedLoopJoinNodeIds;

  std::shared_ptr<Driver> createDriver(
      std::unique_ptr<DriverCtx> ctx,
      std::shared_ptr<ExchangeClient> exchangeClient,
      std::function<int(int pipelineId)> numDrivers);

  /// Replaces operators at indices 'begin' to 'end - 1' with
  /// 'replaceWith, in the Driver being created.  Sets operator ids to be
  /// consecutive after the replace. May only be called from inside a
  /// DriverAdapter. Returns the replaced Operators.
  std::vector<std::unique_ptr<Operator>> replaceOperators(
      Driver& driver,
      int32_t begin,
      int32_t end,
      std::vector<std::unique_ptr<Operator>> replaceWith) const;

  static void registerAdapter(DriverAdapter adapter);

  bool supportsSerialExecution() const {
    return !needsPartitionedOutput() && !needsExchangeClient();
  }

  const core::PlanNodeId& leafNodeId() const {
    VELOX_CHECK(!planNodes.empty());
    return planNodes.front()->id();
  }

  const core::PlanNodeId& outputNodeId() const {
    VELOX_CHECK(!planNodes.empty());
    return planNodes.back()->id();
  }

  std::shared_ptr<const core::PartitionedOutputNode> needsPartitionedOutput()
      const {
    VELOX_CHECK(!planNodes.empty());
    if (auto partitionedOutputNode =
            std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
                planNodes.back())) {
      return partitionedOutputNode;
    }
    return nullptr;
  }

  /// Returns Exchange plan node ID if the pipeline receives data from an
  /// exchange.
  std::optional<core::PlanNodeId> needsExchangeClient() const {
    VELOX_CHECK(!planNodes.empty());
    const auto& leafNode = planNodes.front();
    if (leafNode->requiresExchangeClient()) {
      return leafNode->id();
    }
    return std::nullopt;
  }

  /// Returns true if the pipeline gets data from a local exchange. The function
  /// sets plan node in 'planNode'.
  bool needsLocalExchange(core::PlanNodePtr& planNode) const {
    VELOX_CHECK(!planNodes.empty());
    if (auto exchangeNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                planNodes.front())) {
      planNode = exchangeNode;
      return true;
    }
    return false;
  }

  /// Returns true if the pipeline gets data from a table scan. The function
  /// sets plan node id in 'planNodeId'.
  bool needsTableScan(core::PlanNodeId& planNodeId) const {
    VELOX_CHECK(!planNodes.empty());
    if (auto scanNode = std::dynamic_pointer_cast<const core::TableScanNode>(
            planNodes.front())) {
      planNodeId = scanNode->id();
      return true;
    }
    return false;
  }

  /// Returns plan node IDs for which Hash Join Bridges must be created based
  /// on this pipeline.
  std::vector<core::PlanNodeId> needsHashJoinBridges() const;

  /// Returns plan node IDs for which Nested Loop Join Bridges must be created
  /// based on this pipeline.
  std::vector<core::PlanNodeId> needsNestedLoopJoinBridges() const;

  static std::vector<DriverAdapter> adapters;
};

/// Provides the execution context of a driver thread. This is set to a
/// per-thread local variable if the running thread is a driver thread.
class DriverThreadContext {
 public:
  explicit DriverThreadContext(const DriverCtx* driverCtx)
      : driverCtx_(driverCtx) {}

  const DriverCtx* driverCtx() const {
    VELOX_CHECK_NOT_NULL(driverCtx_);
    return driverCtx_;
  }

 private:
  const DriverCtx* driverCtx_;
};

/// Object used to set/restore the driver thread context when driver execution
/// starts/leaves the driver thread.
class ScopedDriverThreadContext {
 public:
  explicit ScopedDriverThreadContext(const DriverCtx* driverCtx);
  explicit ScopedDriverThreadContext(
      const DriverThreadContext* _driverThreadCtx);
  ~ScopedDriverThreadContext();

 private:
  DriverThreadContext* const savedDriverThreadCtx_{nullptr};
  DriverThreadContext currentDriverThreadCtx_;
};

/// Returns the driver thread context set by a per-thread local variable if the
/// current running thread is a driver thread.
DriverThreadContext* driverThreadContext();

} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::BlockingReason>
    : formatter<std::string> {
  auto format(facebook::velox::exec::BlockingReason b, format_context& ctx)
      const {
    return formatter<std::string>::format(
        facebook::velox::exec::blockingReasonToString(b), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::exec::StopReason>
    : formatter<std::string> {
  auto format(facebook::velox::exec::StopReason s, format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::exec::stopReasonString(s), ctx);
  }
};
