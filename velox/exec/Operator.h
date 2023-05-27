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
#include <folly/Synchronized.h>
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/Spiller.h"
#include "velox/type/Filter.h"

namespace facebook::velox::exec {

// Represents a column that is copied from input to output, possibly
// with cardinality change, i.e. values removed or duplicated.
struct IdentityProjection {
  IdentityProjection(
      column_index_t _inputChannel,
      column_index_t _outputChannel)
      : inputChannel(_inputChannel), outputChannel(_outputChannel) {}

  const column_index_t inputChannel;
  const column_index_t outputChannel;
};

struct MemoryStats {
  uint64_t userMemoryReservation{0};
  uint64_t revocableMemoryReservation{0};
  uint64_t systemMemoryReservation{0};
  uint64_t peakUserMemoryReservation{0};
  uint64_t peakSystemMemoryReservation{0};
  uint64_t peakTotalMemoryReservation{0};
  uint64_t numMemoryAllocations{0};

  void update(memory::MemoryPool* pool) {
    const memory::MemoryPool::Stats stats = pool->stats();
    userMemoryReservation = stats.currentBytes;
    systemMemoryReservation = 0;
    peakUserMemoryReservation = stats.peakBytes;
    peakSystemMemoryReservation = 0;
    peakTotalMemoryReservation = stats.peakBytes;
    numMemoryAllocations = stats.numAllocs;
  }

  void add(const MemoryStats& other) {
    userMemoryReservation += other.userMemoryReservation;
    revocableMemoryReservation += other.revocableMemoryReservation;
    systemMemoryReservation += other.systemMemoryReservation;
    peakUserMemoryReservation =
        std::max(peakUserMemoryReservation, other.peakUserMemoryReservation);
    peakSystemMemoryReservation = std::max(
        peakSystemMemoryReservation, other.peakSystemMemoryReservation);
    peakTotalMemoryReservation =
        std::max(peakTotalMemoryReservation, other.peakTotalMemoryReservation);
    numMemoryAllocations += other.numMemoryAllocations;
  }

  void clear() {
    userMemoryReservation = 0;
    revocableMemoryReservation = 0;
    systemMemoryReservation = 0;
    peakUserMemoryReservation = 0;
    peakSystemMemoryReservation = 0;
    peakTotalMemoryReservation = 0;
    numMemoryAllocations = 0;
  }
};

struct OperatorStats {
  /// Initial ordinal position in the operator's pipeline.
  int32_t operatorId = 0;
  int32_t pipelineId = 0;
  core::PlanNodeId planNodeId;

  /// Name for reporting. We use Presto compatible names set at
  /// construction of the Operator where applicable.
  std::string operatorType;

  /// Number of splits (or chunks of work). Split can be a part of data file to
  /// read.
  int64_t numSplits{0};

  /// Bytes read from raw source, e.g. compressed file or network connection.
  uint64_t rawInputBytes = 0;
  uint64_t rawInputPositions = 0;

  CpuWallTiming addInputTiming;

  /// Bytes of input in terms of retained size of input vectors.
  uint64_t inputBytes = 0;
  uint64_t inputPositions = 0;

  /// Number of input batches / vectors. Allows to compute an average batch
  /// size.
  uint64_t inputVectors = 0;

  CpuWallTiming getOutputTiming;

  /// Bytes of output in terms of retained size of vectors.
  uint64_t outputBytes = 0;
  uint64_t outputPositions = 0;

  /// Number of output batches / vectors. Allows to compute an average batch
  /// size.
  uint64_t outputVectors = 0;

  uint64_t physicalWrittenBytes = 0;

  uint64_t blockedWallNanos = 0;

  CpuWallTiming finishTiming;

  MemoryStats memoryStats;

  // Total bytes written for spilling.
  uint64_t spilledBytes{0};

  // Total rows written for spilling.
  uint64_t spilledRows{0};

  // Total spilled partitions.
  uint32_t spilledPartitions{0};

  // Total current spilled files.
  uint32_t spilledFiles{0};

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;

  int numDrivers = 0;

  OperatorStats(
      int32_t _operatorId,
      int32_t _pipelineId,
      std::string _planNodeId,
      std::string _operatorType)
      : operatorId(_operatorId),
        pipelineId(_pipelineId),
        planNodeId(std::move(_planNodeId)),
        operatorType(std::move(_operatorType)) {}

  void addInputVector(uint64_t bytes, uint64_t positions) {
    inputBytes += bytes;
    inputPositions += positions;
    inputVectors += 1;
  }

  void addOutputVector(uint64_t bytes, uint64_t positions) {
    outputBytes += bytes;
    outputPositions += positions;
    outputVectors += 1;
  }

  void addRuntimeStat(const std::string& name, const RuntimeCounter& value);
  void add(const OperatorStats& other);
  void clear();
};

class OperatorCtx {
 public:
  OperatorCtx(
      DriverCtx* driverCtx,
      const core::PlanNodeId& planNodeId,
      int32_t operatorId,
      const std::string& operatorType = "");

  const std::shared_ptr<Task>& task() const {
    return driverCtx_->task;
  }

  const std::string& taskId() const;

  Driver* driver() const {
    return driverCtx_->driver;
  }

  DriverCtx* driverCtx() const {
    return driverCtx_;
  }

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  const core::PlanNodeId& planNodeId() const {
    return planNodeId_;
  }

  const int32_t operatorId() const {
    return operatorId_;
  }

  const std::string& operatorType() const {
    return operatorType_;
  }

  core::ExecCtx* execCtx() const;

  /// Makes an extract of QueryCtx for use in a connector. 'planNodeId'
  /// is the id of the calling TableScan. This and the task id identify the scan
  /// for column access tracking. 'connectorPool' is an aggregate memory pool
  /// for connector use.
  std::shared_ptr<connector::ConnectorQueryCtx> createConnectorQueryCtx(
      const std::string& connectorId,
      const std::string& planNodeId,
      memory::MemoryPool* connectorPool) const;

 private:
  DriverCtx* const driverCtx_;
  const core::PlanNodeId planNodeId_;
  const int32_t operatorId_;
  const std::string operatorType_;
  velox::memory::MemoryPool* const pool_;

  // These members are created on demand.
  mutable std::unique_ptr<core::ExecCtx> execCtx_;
};

// Query operator
class Operator : public BaseRuntimeStatWriter {
 public:
  // Factory class for mapping a user-registered PlanNode into the corresponding
  // Operator.
  class PlanNodeTranslator {
   public:
    virtual ~PlanNodeTranslator() = default;

    // Translates plan node to operator. Returns nullptr if the plan node cannot
    // be handled by this factory.
    virtual std::unique_ptr<Operator>
    toOperator(DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
      return nullptr;
    }

    // An overloaded method that should be called when the operator needs an
    // ExchangeClient.
    virtual std::unique_ptr<Operator> toOperator(
        DriverCtx* ctx,
        int32_t id,
        const core::PlanNodePtr& node,
        std::shared_ptr<ExchangeClient> exchangeClient) {
      return nullptr;
    }

    // Translates plan node to join bridge. Returns nullptr if the plan node
    // cannot be handled by this factory.
    virtual std::unique_ptr<JoinBridge> toJoinBridge(
        const core::PlanNodePtr& /* node */) {
      return nullptr;
    }

    // Translates plan node to operator supplier. Returns nullptr if the plan
    // node cannot be handled by this factory.
    virtual OperatorSupplier toOperatorSupplier(
        const core::PlanNodePtr& /* node */) {
      return nullptr;
    }

    // Returns max driver count for the plan node. Returns std::nullopt if the
    // plan node cannot be handled by this factory.
    virtual std::optional<uint32_t> maxDrivers(
        const core::PlanNodePtr& /* node */) {
      return std::nullopt;
    }
  };

  /// 'operatorId' is the initial index of the 'this' in the Driver's list of
  /// Operators. This is used as in index into OperatorStats arrays in the Task.
  /// 'planNodeId' is a query-level unique identifier of the PlanNode to which
  /// 'this' corresponds. 'operatorType' is a label for use in stats. If
  /// 'canSpill' is true, then disk spilling is allowed for this operator.
  Operator(
      DriverCtx* driverCtx,
      RowTypePtr outputType,
      int32_t operatorId,
      std::string planNodeId,
      std::string operatorType,
      std::optional<Spiller::Config> spillConfig = std::nullopt);

  virtual ~Operator() = default;

  // Returns true if 'this' can accept input. Not used if operator is a source
  // operator, e.g. the first operator in the pipeline.
  virtual bool needsInput() const = 0;

  // Adds input. Not used if operator is a source operator, e.g. the first
  // operator in the pipeline.
  // @param input Non-empty input vector.
  virtual void addInput(RowVectorPtr input) = 0;

  // Informs 'this' that addInput will no longer be called. This means
  // that any partial state kept by 'this' should be returned by
  // the next call(s) to getOutput. Not used if operator is a source operator,
  // e.g. the first operator in the pipeline.
  virtual void noMoreInput() {
    noMoreInput_ = true;
  }

  // Returns a RowVector with the result columns. Returns nullptr if
  // no more output can be produced without more input or if blocked
  // for outside causes. isBlocked distinguishes between the
  // cases. Sink operator, e.g. the last operator in the pipeline, must return
  // nullptr and pass results to the consumer through a custom mechanism.
  // @return nullptr or a non-empty output vector.
  virtual RowVectorPtr getOutput() = 0;

  // Returns kNotBlocked if 'this' is not prevented from
  // advancing. Otherwise, returns a reason and sets 'future' to a
  // future that will be realized when the reason is no longer present.
  // The caller must wait for the `future` to complete before making
  // another call.
  virtual BlockingReason isBlocked(ContinueFuture* future) = 0;

  // Returns true if completely finished processing and no more output will be
  // produced. Some operators may finish early before receiving all input and
  // noMoreInput() message. For example, Limit operator finishes as soon as it
  // receives specified number of rows and HashProbe finishes early if the build
  // side is empty.
  virtual bool isFinished() = 0;

  // Returns single-column dynamically generated filters to be pushed down to
  // upstream operators. Used to push down filters on join keys from broadcast
  // hash join into probe-side table scan. Can also be used to push down TopN
  // cutoff.
  virtual const std::
      unordered_map<column_index_t, std::shared_ptr<common::Filter>>&
      getDynamicFilters() const {
    return dynamicFilters_;
  }

  // Clears dynamically generated filters. Called after filters were pushed
  // down.
  virtual void clearDynamicFilters() {
    dynamicFilters_.clear();
  }

  // Returns true if this operator would accept a filter dynamically generated
  // by a downstream operator.
  virtual bool canAddDynamicFilter() const {
    return false;
  }

  // Adds a filter dynamically generated by a downstream operator. Called only
  // if canAddFilter() returns true.
  virtual void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) {
    VELOX_UNSUPPORTED(
        "This operator doesn't support dynamic filter pushdown: {}",
        toString());
  }

  // Returns a list of identify projections, e.g. columns that are projected
  // as-is possibly after applying a filter.
  const std::vector<IdentityProjection>& identityProjections() const {
    return identityProjections_;
  }

  // Frees all resources associated with 'this'. No other methods
  // should be called after this.
  virtual void close() {
    input_ = nullptr;
    results_.clear();
    // Release the unused memory reservation on close.
    operatorCtx_->pool()->release();
  }

  // Returns true if 'this' never has more output rows than input rows.
  virtual bool isFilter() const {
    return false;
  }

  virtual bool preservesOrder() const {
    return false;
  }

  /// Returns copy of operator stats. If 'clear' is true, the function also
  /// clears the operator stats after retrieval.
  OperatorStats stats(bool clear);

  /// Add a single runtime stat to the operator stats under the write lock.
  /// This member overrides BaseRuntimeStatWriter's member.
  void addRuntimeStat(const std::string& name, const RuntimeCounter& value)
      override {
    stats_.wlock()->addRuntimeStat(name, value);
  }

  /// Returns reference to the operator stats synchronized object to gain bulck
  /// read/write access to the stats.
  folly::Synchronized<OperatorStats>& stats() {
    return stats_;
  }

  void recordBlockingTime(uint64_t start, BlockingReason reason);

  virtual std::string toString() const;

  velox::memory::MemoryPool* pool() const {
    return operatorCtx_->pool();
  }

  /// Returns true if the operator is reclaimable. Currently, we only support
  /// to reclaim memory from a spillable operator.
  FOLLY_ALWAYS_INLINE virtual bool canReclaim() const {
    return canSpill();
  }

  /// Returns how many bytes is reclaimable from this operator. The function
  /// returns true if this operator is reclaimable, and returns the estimated
  /// reclaimable bytes.
  virtual bool reclaimableBytes(uint64_t& reclaimableBytes) const {
    const bool reclaimable = canReclaim();
    reclaimableBytes = reclaimable ? pool()->reservedBytes() : 0;
    return reclaimable;
  }

  /// Invoked by the memory arbitrator to reclaim memory from this operator with
  /// specified reclaim target bytes. If 'targetBytes' is zero, then it tries to
  /// reclaim all the reclaimable memory from this operator.
  ///
  /// NOTE: this method doesn't return the actually freed memory bytes. The
  /// caller need to claim the actually freed memory space by shrinking the
  /// associated root memory pool's capacity accordingly.
  virtual void reclaim(uint64_t targetBytes) {}

  const core::PlanNodeId& planNodeId() const {
    return operatorCtx_->planNodeId();
  }

  const int32_t operatorId() const {
    return operatorCtx_->operatorId();
  }

  const std::string& operatorType() const {
    return operatorCtx_->operatorType();
  }

  /// Registers 'translator' for mapping user defined PlanNode subclass
  /// instances to user-defined Operators.
  static void registerOperator(std::unique_ptr<PlanNodeTranslator> translator);

  /// Calls all the registered PlanNodeTranslators on 'planNode' and returns the
  /// result of the first one that returns non-nullptr or nullptr if all return
  /// nullptr. exchangeClient is not-null only when
  /// planNode->requiresExchangeClient() is true.
  static std::unique_ptr<Operator> fromPlanNode(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& planNode,
      std::shared_ptr<ExchangeClient> exchangeClient = nullptr);

  /// Calls all the registered PlanNodeTranslators on 'planNode' and returns the
  /// result of the first one that returns non-nullptr or nullptr if all return
  /// nullptr.
  static std::unique_ptr<JoinBridge> joinBridgeFromPlanNode(
      const core::PlanNodePtr& planNode);

  /// Calls all the registered PlanNodeTranslators on 'planNode' and returns the
  /// result of the first one that returns non-nullptr or nullptr if all return
  /// nullptr.
  static OperatorSupplier operatorSupplierFromPlanNode(
      const core::PlanNodePtr& planNode);

  /// Calls `maxDrivers` on all the registered PlanNodeTranslators and returns
  /// the first one that is not std::nullopt or std::nullopt otherwise.
  static std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& planNode);

  /// Returns the operator context of this operator. This method is only used
  /// for test.
  const OperatorCtx* testingOperatorCtx() const {
    return operatorCtx_.get();
  }

 protected:
  static std::vector<std::unique_ptr<PlanNodeTranslator>>& translators();
  friend class NonReclaimableSection;

  class MemoryReclaimer : public memory::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        DriverCtx* driverCtx,
        Operator* op);

    void enterArbitration() override;

    void leaveArbitration() noexcept override;

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(memory::MemoryPool* pool, uint64_t targetBytes) override;

    void abort(memory::MemoryPool* pool) override;

   private:
    MemoryReclaimer(const std::shared_ptr<Driver>& driver, Operator* op)
        : driver_(driver), op_(op) {
      VELOX_CHECK_NOT_NULL(op_);
    }

    // Gets the shared pointer to the associated driver to ensure the liveness
    // of the operator during the memory reclaim operation.
    //
    // NOTE: an operator's memory pool can outlive its operator.
    std::shared_ptr<Driver> ensureDriver() const {
      return driver_.lock();
    }

    const std::weak_ptr<Driver> driver_;
    Operator* const op_;
  };

  /// The scoped object to mark a reclaimable operator is under non-reclaimable
  /// execution section. This prevents the memory arbitrator from reclaiming
  /// memory from the operator if it happens to be suspended for memory
  /// arbitration processing.
  class NonReclaimableSection {
   public:
    explicit NonReclaimableSection(Operator* op) : op_(op) {
      VELOX_CHECK(!op_->nonReclaimableSection_);
      op_->nonReclaimableSection_ = true;
    }

    ~NonReclaimableSection() {
      VELOX_CHECK(op_->nonReclaimableSection_);
      op_->nonReclaimableSection_ = false;
    }

   private:
    Operator* const op_;
  };

  /// Invoked to setup memory reclaimer for this operator's memory pool if its
  /// parent node memory pool has set the reclaimer.
  void maybeSetReclaimer();

  /// Returns true if this is a spillable operator and has configured spilling.
  FOLLY_ALWAYS_INLINE bool canSpill() const {
    return spillConfig_.has_value();
  }

  /// Creates output vector from 'input_' and 'results_' according to
  /// 'identityProjections_' and 'resultProjections_'.
  RowVectorPtr fillOutput(vector_size_t size, BufferPtr mapping);

  /// Returns the number of rows for the output batch. This uses averageRowSize
  /// to calculate how many rows fit in preferredOutputBatchBytes. It caps the
  /// number of rows at 10K and returns at least one row. The averageRowSize
  /// must not be negative. If the averageRowSize is 0 which is not advised,
  /// returns maxOutputBatchRows. If the averageRowSize is not given, returns
  /// preferredOutputBatchRows.
  uint32_t outputBatchRows(
      std::optional<uint64_t> averageRowSize = std::nullopt) const;

  const std::unique_ptr<OperatorCtx> operatorCtx_;
  const RowTypePtr outputType_;
  // Contains the disk spilling related configs if spilling is enabled (e.g.
  // the fs dir path to store spill files), otherwise null.
  const std::optional<Spiller::Config> spillConfig_;

  folly::Synchronized<OperatorStats> stats_;

  /// Indicates if an operator is under a non-reclaimable execution section.
  /// This prevents the memory arbitrator from reclaiming memory from this
  /// operator if it happens to be suspended for memory arbitration processing.
  /// This only applies to a reclaimable operator.
  tsan_atomic<bool> nonReclaimableSection_{false};

  /// Holds the last data from addInput until it is processed. Reset after the
  /// input is processed.
  RowVectorPtr input_;

  bool noMoreInput_ = false;
  std::vector<IdentityProjection> identityProjections_;
  std::vector<VectorPtr> results_;

  /// Maps between index in results_ and index in output RowVector.
  std::vector<IdentityProjection> resultProjections_;

  /// True if the input and output rows have exactly the same fields, i.e. one
  /// could copy directly from input to output if no cardinality change.
  bool isIdentityProjection_ = false;

  std::unordered_map<column_index_t, std::shared_ptr<common::Filter>>
      dynamicFilters_;
};

/// Given a row type returns indices for the specified subset of columns.
std::vector<column_index_t> toChannels(
    const RowTypePtr& rowType,
    const std::vector<core::TypedExprPtr>& exprs);

column_index_t exprToChannel(const core::ITypedExpr* expr, const TypePtr& type);

/// Given a source output type and target input type we return the indices of
/// the target input columns in the source output type.
/// The target output type is used to determine if the projection is identity.
/// An empty indices vector is returned when projection is identity.
std::vector<column_index_t> calculateOutputChannels(
    const RowTypePtr& sourceOutputType,
    const RowTypePtr& targetInputType,
    const RowTypePtr& targetOutputType);

// A first operator in a Driver, e.g. table scan or exchange client.
class SourceOperator : public Operator {
 public:
  SourceOperator(
      DriverCtx* driverCtx,
      RowTypePtr outputType,
      int32_t operatorId,
      const std::string& planNodeId,
      const std::string& operatorType)
      : Operator(
            driverCtx,
            std::move(outputType),
            operatorId,
            planNodeId,
            operatorType) {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /* unused */) override {
    VELOX_FAIL("SourceOperator does not support addInput()");
  }

  void noMoreInput() override {
    VELOX_FAIL("SourceOperator does not support noMoreInput()");
  }
};

} // namespace facebook::velox::exec
