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
#include "velox/common/time/CpuWallTimer.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/type/Filter.h"

namespace facebook::velox::exec {

// Represents a column that is copied from input to output, possibly
// with cardinality change, i.e. values removed or duplicated.
struct IdentityProjection {
  IdentityProjection(ChannelIndex _inputChannel, ChannelIndex _outputChannel)
      : inputChannel(_inputChannel), outputChannel(_outputChannel) {}

  const ChannelIndex inputChannel;
  const ChannelIndex outputChannel;
};

struct MemoryStats {
  uint64_t userMemoryReservation = {};
  uint64_t revocableMemoryReservation = {};
  uint64_t systemMemoryReservation = {};
  uint64_t peakUserMemoryReservation = {};
  uint64_t peakSystemMemoryReservation = {};
  uint64_t peakTotalMemoryReservation = {};

  void update(const std::shared_ptr<memory::MemoryUsageTracker>& tracker) {
    if (!tracker) {
      return;
    }
    userMemoryReservation = tracker->getCurrentUserBytes();
    systemMemoryReservation = tracker->getCurrentSystemBytes();
    peakUserMemoryReservation = tracker->getPeakUserBytes();
    peakSystemMemoryReservation = tracker->getPeakSystemBytes();
    peakTotalMemoryReservation = tracker->getPeakTotalBytes();
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
  }

  void clear() {
    userMemoryReservation = 0;
    revocableMemoryReservation = 0;
    systemMemoryReservation = 0;
    peakUserMemoryReservation = 0;
    peakSystemMemoryReservation = 0;
    peakTotalMemoryReservation = 0;
  }
};

struct RuntimeMetric {
  int64_t sum{0};
  int64_t count{0};
  int64_t min{std::numeric_limits<int64_t>::max()};
  int64_t max{std::numeric_limits<int64_t>::min()};

  void addValue(int64_t value) {
    sum += value;
    count++;
    min = std::min(min, value);
    max = std::max(max, value);
  }

  void merge(const RuntimeMetric& other) {
    sum += other.sum;
    count += other.count;
    min = std::min(min, other.min);
    max = std::max(max, other.max);
  }
};

struct OperatorStats {
  // Initial ordinal position in the operator's pipeline.
  int32_t operatorId = 0;
  int32_t pipelineId = 0;
  core::PlanNodeId planNodeId;
  // Name for reporting. We use Presto compatible names set at
  // construction of the Operator where applicable.
  std::string operatorType;

  // Number of splits (or chunks of work). Split can be a part of data file to
  // read.
  int64_t numSplits{0};

  // Bytes read from raw source, e.g. compressed file or network connection.
  uint64_t rawInputBytes = 0;
  uint64_t rawInputPositions = 0;

  CpuWallTiming addInputTiming;
  // Bytes of input in terms of retained size of input vectors.
  uint64_t inputBytes = 0;
  uint64_t inputPositions = 0;

  CpuWallTiming getOutputTiming;
  // Bytes of output in terms of retained size of vectors.
  uint64_t outputBytes = 0;
  uint64_t outputPositions = 0;

  uint64_t physicalWrittenBytes = 0;

  uint64_t blockedWallNanos = 0;

  CpuWallTiming finishTiming;

  MemoryStats memoryStats;

  std::unordered_map<std::string, RuntimeMetric> runtimeStats;

  OperatorStats(
      int32_t _operatorId,
      int32_t _pipelineId,
      std::string _planNodeId,
      std::string _operatorType)
      : operatorId(_operatorId),
        pipelineId(_pipelineId),
        planNodeId(std::move(_planNodeId)),
        operatorType(std::move(_operatorType)) {}

  void addRuntimeStat(const std::string& name, int64_t value) {
    runtimeStats[name].addValue(value);
  }

  void add(const OperatorStats& other);
  void clear();
};

class OperatorCtx {
 public:
  explicit OperatorCtx(DriverCtx* driverCtx);

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

  memory::MappedMemory* mappedMemory() const;

  core::ExecCtx* execCtx() const;

  // Makes an extract of QueryCtx for use in a connector. 'planNodeId'
  // is the id of the calling TableScan. This and the task id identify
  // the scan for column access tracking.
  std::unique_ptr<connector::ConnectorQueryCtx> createConnectorQueryCtx(
      const std::string& connectorId,
      const std::string& planNodeId) const;

 private:
  DriverCtx* driverCtx_;
  velox::memory::MemoryPool* pool_;

  // These members are created on demand.
  mutable memory::MappedMemory* mappedMemory_{nullptr};
  mutable std::unique_ptr<core::ExecCtx> execCtx_;
  mutable std::unique_ptr<connector::ExpressionEvaluator> expressionEvaluator_;
};

// Query operator
class Operator {
 public:
  // Factory class for mapping a user-registered PlanNode into the corresponding
  // Operator.
  class PlanNodeTranslator {
   public:
    virtual ~PlanNodeTranslator() = default;

    // Translates plan node to operator. Returns nullptr if the plan node cannot
    // be handled by this factory.
    virtual std::unique_ptr<Operator> translate(
        DriverCtx* ctx,
        int32_t id,
        const std::shared_ptr<const core::PlanNode>& node) = 0;

    // Returns max driver count for the plan node. Returns std::nullopt if the
    // plan node cannot be handled by this factory.
    virtual std::optional<uint32_t> maxDrivers(
        const std::shared_ptr<const core::PlanNode>& /* node */) {
      return std::nullopt;
    }
  };

  // 'operatorId' is the initial index of the 'this' in the Driver's
  // list of Operators. This is used as in index into OperatorStats
  // arrays in the Task. 'planNodeId' is a query-level unique
  // identifier of the PlanNode to which 'this'
  // corresponds. 'operatorType' is a label for use in stats.
  Operator(
      DriverCtx* driverCtx,
      std::shared_ptr<const RowType> outputType,
      int32_t operatorId,
      std::string planNodeId,
      std::string operatorType)
      : operatorCtx_(std::make_unique<OperatorCtx>(driverCtx)),
        stats_(
            operatorId,
            driverCtx->pipelineId,
            std::move(planNodeId),
            std::move(operatorType)),
        outputType_(std::move(outputType)) {}

  virtual ~Operator() = default;

  // Returns true if 'this' can accept input. Not used if operator is a source
  // operator, e.g. the first operator in the pipeline.
  virtual bool needsInput() const = 0;

  // Adds input. Not used if operator is a source operator, e.g. the first
  // operator in the pipeline.
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
      unordered_map<ChannelIndex, std::shared_ptr<common::Filter>>&
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
      ChannelIndex /*outputChannel*/,
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
    output_ = nullptr;
    results_.clear();
  }

  // Returns true if 'this' never has more output rows than input rows.
  virtual bool isFilter() const {
    return false;
  }

  virtual bool preservesOrder() const {
    return false;
  }

  OperatorStats& stats() {
    return stats_;
  }

  void recordBlockingTime(uint64_t start);

  virtual std::string toString();

  velox::memory::MemoryPool* pool() {
    return operatorCtx_->pool();
  }

  const core::PlanNodeId& planNodeId() const {
    return stats_.planNodeId;
  }

  // Registers 'translator' for mapping user defined PlanNode subclass instances
  // to user-defined Operators.
  static void registerOperator(std::unique_ptr<PlanNodeTranslator> translator);

  // Calls all the registered PlanNodeTranslators on 'planNode' and
  // returns the result of the first one that returns non-nullptr
  // or nullptr if all return nullptr.
  static std::unique_ptr<Operator> fromPlanNode(
      DriverCtx* ctx,
      int32_t id,
      const std::shared_ptr<const core::PlanNode>& planNode);

  // Calls `maxDrivers` on all the registered PlanNodeTranslators and returns
  // the first one that is not std::nullopt or std::nullopt otherwise.
  static std::optional<uint32_t> maxDrivers(
      const std::shared_ptr<const core::PlanNode>& planNode);

 protected:
  static std::vector<std::unique_ptr<PlanNodeTranslator>>& translators();

  // Clears the columns of 'output_' that are projected from
  // 'input_'. This should be done when preparing to produce a next
  // batch of output to drop any lingering references to row
  // number mappings or input vectors. In this way input vectors do
  // not have to be copied and will be singly referenced by their
  // producer.
  void clearIdentityProjectedOutput();

  // Returns a previously used result vector if it exists and is suitable for
  // reuse, nullptr otherwise.
  VectorPtr getResultVector(ChannelIndex index);

  // Fills 'result' with a vector for each input of
  // 'resultProjection_'. These are recycled from 'output_' if
  // suitable for reuse.
  void getResultVectors(std::vector<VectorPtr>* result);

  // Copies 'input_' and 'results_' into 'output_' according to
  // 'identityProjections_' and 'resultProjections_'.
  RowVectorPtr fillOutput(vector_size_t size, BufferPtr mapping);

  // Drops references to identity projected columns from 'output_' and
  // clears 'input_'. The producer will see its vectors as singly
  // referenced.
  void inputProcessed();

  std::unique_ptr<OperatorCtx> operatorCtx_;
  OperatorStats stats_;
  const std::shared_ptr<const RowType> outputType_;

  // Holds the last data from addInput until it is processed. Reset after the
  // input is processed.
  RowVectorPtr input_;

  // Holds the last data returned by getOutput. References vectors
  // from 'input_' and from 'results_'. Reused if singly referenced.
  RowVectorPtr output_;

  bool noMoreInput_ = false;
  std::vector<IdentityProjection> identityProjections_;
  std::vector<VectorPtr> results_;

  // Maps between index in results_ and index in output RowVector.
  std::vector<IdentityProjection> resultProjections_;

  // True if the input and output rows have exactly the same fields,
  // i.e. one could copy directly from input to output if no
  // cardinality change.
  bool isIdentityProjection_ = false;

  std::unordered_map<ChannelIndex, std::shared_ptr<common::Filter>>
      dynamicFilters_;
};

constexpr ChannelIndex kConstantChannel =
    std::numeric_limits<ChannelIndex>::max();

/// Given a row type returns indices for the specified subset of columns.
std::vector<ChannelIndex> toChannels(
    const RowTypePtr& rowType,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        fields);

ChannelIndex exprToChannel(const core::ITypedExpr* expr, const TypePtr& type);

/// Given an input type and output type that contains a subset of the input type
/// columns possibly in different order returns the indices of the output
/// columns in the input type.
std::vector<ChannelIndex> calculateOutputChannels(
    const RowTypePtr& inputType,
    const RowTypePtr& outputType);

// A first operator in a Driver, e.g. table scan or exchange client.
class SourceOperator : public Operator {
 public:
  SourceOperator(
      DriverCtx* driverCtx,
      std::shared_ptr<const RowType> outputType,
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

// Concrete class implementing the base runtime stats writer. Wraps around
// operator pointer to be called at any time to updated runtime stats.
// Used for reporting IO wall time from lazy vectors, for example.
class OperatorRuntimeStatWriter : public BaseRuntimeStatWriter {
 public:
  explicit OperatorRuntimeStatWriter(Operator* op) : operator_{op} {}

  void addRuntimeStat(const std::string& name, int64_t value) override {
    if (operator_) {
      operator_->stats().addRuntimeStat(name, value);
    }
  }

 private:
  Operator* operator_;
};

} // namespace facebook::velox::exec
