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

#include "presto_cpp/main/operators/MaterializedOutputBuffer.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/row/CompactRow.h"

namespace facebook::presto::operators {

class MaterializedOutputNode : public velox::core::PlanNode {
 public:
  MaterializedOutputNode(
      const velox::core::PlanNodeId& id,
      std::vector<velox::core::TypedExprPtr> keys,
      int numPartitions,
      velox::RowTypePtr outputType,
      std::shared_ptr<const velox::core::PartitionFunctionSpec>
          partitionFunctionSpec,
      velox::core::PlanNodePtr source,
      std::shared_ptr<MaterializedOutputBuffer> buffer)
      : velox::core::PlanNode(id),
        numPartitions_(numPartitions),
        keys_(std::move(keys)),
        outputType_(std::move(outputType)),
        partitionFunctionSpec_(std::move(partitionFunctionSpec)),
        sources_{std::move(source)},
        buffer_(std::move(buffer)) {}

  std::string_view name() const override {
    return "MaterializedOutput";
  }

  int numPartitions() const {
    return numPartitions_;
  }

  const auto& keys() const {
    return keys_;
  }

  MaterializedOutputBuffer* buffer() const {
    return buffer_.get();
  }

  const auto& partitionFunctionSpec() const {
    return partitionFunctionSpec_;
  }

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << numPartitions_;
  }

  const int numPartitions_;
  const std::vector<velox::core::TypedExprPtr> keys_;
  const velox::RowTypePtr outputType_;
  const std::shared_ptr<const velox::core::PartitionFunctionSpec>
      partitionFunctionSpec_;
  const std::vector<velox::core::PlanNodePtr> sources_;
  const std::shared_ptr<MaterializedOutputBuffer> buffer_;
};

class MaterializedOutput : public velox::exec::Operator {
 public:
  MaterializedOutput(
      int32_t operatorId,
      velox::exec::DriverCtx* ctx,
      const std::shared_ptr<const MaterializedOutputNode>& planNode);

  void addInput(velox::RowVectorPtr input) override;

  velox::RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !finished_ &&
        blockingReason_ == velox::exec::BlockingReason::kNotBlocked;
  }

  void noMoreInput() override;

  velox::exec::BlockingReason isBlocked(velox::ContinueFuture* future) override;

  bool isFinished() override;

  void close() override;

 private:
  // Project input columns and lazy-load all children.
  void initializeInput(velox::RowVectorPtr input);

  // Flush remaining data and coordinate with peer drivers. Called from
  // noMoreInput() and close(). Idempotent via finished_ guard.
  void finish();

  // Partition input, serialize into flat buffer, and flush if threshold
  // reached.
  void flushBatch();

  // Assign partition IDs for all rows using the partition function.
  void computePartitions(const velox::RowVector& rawInput, int32_t numRows);

  // Serialize all rows into the flat buffer using CompactRow. Dispatches
  // to fixed-width or variable-width path based on schema.
  void serializeRows(velox::row::CompactRow& compactRow, int32_t numRows);

  // Fixed-width path: batch serialize with vectorized offsets and single
  // memset.
  void serializeFixedWidthRows(
      velox::row::CompactRow& compactRow,
      int32_t numRows);

  // Variable-width path: row-at-a-time serialize with per-row size computation.
  void serializeVariableWidthRows(
      velox::row::CompactRow& compactRow,
      int32_t numRows);

  // Grow the flat buffer if needed to accommodate additionalBytes.
  void ensureFlatBufferCapacity(int64_t additionalBytes);

  // Build a contiguous IOBuf with RowGroupHeader + TRowSize-framed CompactRow
  // data for the given row indices belonging to one partition.
  std::unique_ptr<folly::IOBuf> buildRowGroup(
      const std::vector<int32_t>& rowIndices);

  // Immutable config — declaration order must match constructor init order.
  const int32_t numDestinations_;
  const std::vector<velox::column_index_t> outputChannels_;
  std::unique_ptr<velox::core::PartitionFunction> partitionFunction_;
  MaterializedOutputBuffer* const buffer_;

  // Flush threshold: clamp(numPartitions * kDefaultAvgRowSize, 1MB, 10MB).
  int64_t targetSizeInBytes_;

  // Fixed row size for all-fixed-width schemas (avoids per-row rowSize()).
  std::optional<int32_t> fixedRowSize_;

  // Operator state.
  velox::exec::BlockingReason blockingReason_{
      velox::exec::BlockingReason::kNotBlocked};
  velox::ContinueFuture future_;
  bool finished_{false};

  // Reusable per-batch buffers.
  velox::RowVectorPtr output_;
  std::vector<uint32_t> partitions_;

  // Flat buffer model — rows are serialized into a pool-tracked
  // contiguous buffer with parallel arrays tracking offsets, sizes,
  // and partition IDs. flushBatch() groups by partition and creates
  // per-partition pages.
  int32_t rowCount_{0};
  int64_t flatBufferSize_{0};
  velox::BufferPtr flatBuffer_;
  std::vector<int64_t> rowOffsets_;
  std::vector<int32_t> rowSizes_;
  std::vector<uint32_t> rowPartitions_;
};

class MaterializedOutputTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};

} // namespace facebook::presto::operators
