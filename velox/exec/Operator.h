/*
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

namespace facebook::velox::exec {

class ExchangeClient;
class LocalMerge;

using OperationTimer = CpuWallTimer;
using OperationTiming = CpuWallTiming;

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
  // TODO: Populate revocableMemoryReservation as well.
  uint64_t revocableMemoryReservation = {};
  uint64_t systemMemoryReservation = {};
  uint64_t peakUserMemoryReservation = {};
  uint64_t peakSystemMemoryReservation = {};
  uint64_t peakTotalMemoryReservation = {};

  void update(std::shared_ptr<memory::MemoryUsageTracker> tracker) {
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

  OperationTiming addInputTiming;
  // Bytes of input in terms of retained size of input vectors.
  uint64_t inputBytes = 0;
  uint64_t inputPositions = 0;

  OperationTiming getOutputTiming;
  // Bytes of output in terms of retained size of vectors.
  uint64_t outputBytes = 0;
  uint64_t outputPositions = 0;

  uint64_t physicalWrittenBytes = 0;

  uint64_t blockedWallNanos = 0;

  OperationTiming finishTiming;

  MemoryStats memoryStats;

  OperatorStats(
      int32_t _operatorId,
      int32_t _pipelineId,
      const std::string& _planNodeId,
      const std::string& _operatorType)
      : operatorId(_operatorId),
        pipelineId(_pipelineId),
        planNodeId(_planNodeId),
        operatorType(_operatorType) {}

  void add(const OperatorStats& other);
  void clear();
};

class OperatorCtx {
 public:
  explicit OperatorCtx(DriverCtx* driverCtx)
      : driverCtx_(driverCtx), pool_(driverCtx_->addOperatorUserPool()) {}

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  velox::memory::MemoryPool* systemPool() const {
    if (!systemMemPool_) {
      systemMemPool_ = driverCtx_->addOperatorSystemPool(pool_);
    }
    return systemMemPool_;
  }

  memory::MappedMemory* mappedMemory() const;

  std::shared_ptr<Task> task() const {
    return driverCtx_->task;
  }

  const std::string& taskId() const;

  core::ExecCtx* execCtx() const {
    return driverCtx_->execCtx.get();
  }

  Driver* driver() const {
    return driverCtx_->driver;
  }

  DriverCtx* driverCtx() const {
    return driverCtx_;
  }

 private:
  DriverCtx* driverCtx_;
  velox::memory::MemoryPool* pool_;

  // These members are created on demand.
  mutable velox::memory::MemoryPool* systemMemPool_{nullptr};
  mutable std::shared_ptr<memory::MappedMemory> mappedMemory_;
};

// Query operator
class Operator {
 public:
  // 'operatorId' is the initial index of the 'this' in the Driver's
  // list of Operators. This is used as in index into OperatorStats
  // arrays in the Task. 'planNodeId' is a query-level unique
  // identifier of the PlanNode to which 'this'
  // corresponds. 'operatorType' is a label for use in stats.
  Operator(
      DriverCtx* driverCtx,
      std::shared_ptr<const RowType> outputType,
      int32_t operatorId,
      const std::string& planNodeId,
      const std::string& operatorType)
      : operatorCtx_(std::make_unique<OperatorCtx>(driverCtx)),
        stats_(operatorId, driverCtx->pipelineId, planNodeId, operatorType),
        outputType_(outputType) {}

  virtual ~Operator() {}

  // Returns true if can accept input.
  virtual bool needsInput() const = 0;

  // Adds input.
  virtual void addInput(RowVectorPtr input) = 0;

  // Returns a RowVector with the result columns. Returns nullptr if
  // no more output can be produced without more input or if blocked
  // for outside cause. isBlocked distinguishes between the
  // cases.
  virtual RowVectorPtr getOutput() = 0;

  // Informs 'this' that addInput will no longer be called. This means
  // that any partial state kept by 'this' should be returned by
  // the next call(s) to getOutput.
  virtual void finish() {
    isFinishing_ = true;
  }

  // Returns kNotBlocked if 'this' is not prevented from
  // advancing. Otherwise returns a reason and sets 'future' to a
  // future that will be realized when the reason is no longer present.
  // The caller must wait for the `future` to complete before making
  // another call.
  virtual BlockingReason isBlocked(ContinueFuture* future) = 0;

  virtual bool isFinishing() {
    return isFinishing_;
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

 protected:
  // Clears the columns of 'output_' that are projected from
  // 'input_'. This should be done when preparing to produce a next
  // batch of output so as to drop any lingering references to row
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

  // Tests if 'numProcessedRows_' equals to the length of input_ and clears
  // outstanding references to input_ if done. Returns true if getOutput
  // should return nullptr.
  bool allInputProcessed();

  // Drops references to identity projected columns from 'output_' and
  // clears 'input_'. The producer will see its vectors as singly
  // referenced.
  void inputProcessed();

  void setProcessedRows(vector_size_t numRows) {
    numProcessedInputRows_ = numRows;
  }

  std::unique_ptr<OperatorCtx> operatorCtx_;
  OperatorStats stats_;

  // Holds the last data from addInput until it is processed. Reset after the
  // input is processed.
  RowVectorPtr input_;

  // Holds the last data returned by getOutput. References vectors
  // from 'input_' and from 'results_'. Reused if singly referenced.
  RowVectorPtr output_;

  vector_size_t numProcessedInputRows_ = 0;
  bool isFinishing_ = false;
  std::shared_ptr<const RowType> outputType_;
  std::vector<IdentityProjection> identityProjections_;
  std::vector<VectorPtr> results_;

  // Maps between index in results_ and index in output RowVector.
  std::vector<IdentityProjection> resultProjections_;

  // True if the input and output rows have exactly the same fields,
  // i.e. one could copy directly from input to output if no
  // cardinality change.
  bool isIdentityProjection_ = false;
};

constexpr ChannelIndex kConstantChannel =
    std::numeric_limits<ChannelIndex>::max();

/// Given a row type returns indices for the specified subset of columns.
std::vector<ChannelIndex> toChannels(
    const RowTypePtr& rowType,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        fields);

ChannelIndex exprToChannel(const core::ITypedExpr* expr, TypePtr type);

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
      : Operator(driverCtx, outputType, operatorId, planNodeId, operatorType) {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /* unused */) override {
    VELOX_CHECK(false, "SourceOperator does not support addInput()");
  }
};

} // namespace facebook::velox::exec
