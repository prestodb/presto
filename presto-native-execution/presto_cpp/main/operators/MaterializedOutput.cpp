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
#include "presto_cpp/main/operators/MaterializedOutput.h"

#include <cstring>

#include <folly/io/IOBuf.h>
#include "presto_cpp/main/common/Configs.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/RowSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::operators {

namespace {
constexpr int64_t kMinTargetSize = 1L << 20; // 1MB
constexpr int64_t kDefaultMaxTargetSize = 16L << 20; // 16MB
constexpr int64_t kDefaultAvgRowSize = 1024; // 1KB

core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}
} // namespace

folly::dynamic MaterializedOutputNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["numPartitions"] = numPartitions_;
  obj["keys"] = ISerializable::serialize(keys_);
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  obj["outputType"] = outputType_->serialize();
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

core::PlanNodePtr MaterializedOutputNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto keys = ISerializable::deserialize<std::vector<core::ITypedExpr>>(
      obj["keys"], context);

  std::vector<core::TypedExprPtr> keyPtrs;
  keyPtrs.reserve(keys.size());
  for (auto& key : keys) {
    keyPtrs.emplace_back(
        std::shared_ptr<const core::ITypedExpr>(std::move(key)));
  }

  auto source = ISerializable::deserialize<std::vector<core::PlanNode>>(
      obj["sources"], context)[0];

  auto partitionFunctionSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context);

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

  // Buffer cannot be deserialized — it is set externally by the plan
  // converter.
  return std::make_shared<MaterializedOutputNode>(
      deserializePlanNodeId(obj),
      std::move(keyPtrs),
      static_cast<int>(obj["numPartitions"].asInt()),
      std::move(outputType),
      std::move(partitionFunctionSpec),
      std::move(source),
      std::shared_ptr<MaterializedOutputBuffer>{});
}

MaterializedOutput::MaterializedOutput(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const MaterializedOutputNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "MaterializedOutput"),
      numDestinations_(planNode->numPartitions()),
      outputChannels_(calculateOutputChannels(
          planNode->sources()[0]->outputType(),
          planNode->outputType(),
          planNode->outputType())),
      partitionFunction_(
          numDestinations_ == 1 ? nullptr
                                : planNode->partitionFunctionSpec()->create(
                                      numDestinations_,
                                      /*localExchange=*/false)),
      buffer_(planNode->buffer()),
      targetSizeInBytes_(
          std::clamp(
              static_cast<int64_t>(numDestinations_) * kDefaultAvgRowSize,
              kMinTargetSize,
              SystemConfig::instance()
                  ->exchangeMaterializationPartitioningRowBatchBufferSize())),
      fixedRowSize_(
          row::CompactRow::fixedRowSize(
              std::dynamic_pointer_cast<const RowType>(
                  planNode->outputType()))) {
  VELOX_CHECK_GT(numDestinations_, 0);
  VELOX_CHECK_NOT_NULL(buffer_);
}

void MaterializedOutput::initializeInput(RowVectorPtr input) {
  if (outputType_->size() == 0) {
    // Empty output type: query only cares about row counts, not column values.
    output_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        nullptr,
        input->size(),
        std::vector<VectorPtr>{});
  } else if (outputChannels_.empty()) {
    // No projection — pass all columns through.
    output_ = std::move(input);
  } else {
    // Project / reorder columns from input to outputType. Mirrors
    // PartitionedOutput::initializeInput so serialized bytes match the
    // OLD PartitionAndSerialize+ShuffleWrite path for the same plan.
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputChannels_.size());
    for (auto i : outputChannels_) {
      outputColumns.push_back(input->childAt(i));
    }
    output_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        /*nulls=*/nullptr,
        input->size(),
        std::move(outputColumns));
  }

  // Lazy load all input columns.
  for (auto i = 0; i < output_->childrenSize(); ++i) {
    output_->childAt(i)->loadedVector();
  }
}

void MaterializedOutput::computePartitions(
    const RowVector& rawInput,
    int32_t numRows) {
  partitions_.resize(numRows);
  if (numDestinations_ == 1) {
    std::fill(partitions_.begin(), partitions_.end(), 0);
  } else {
    auto singlePartition = partitionFunction_->partition(rawInput, partitions_);
    if (singlePartition.has_value()) {
      std::fill(
          partitions_.begin(), partitions_.end(), singlePartition.value());
    }
  }
}

void MaterializedOutput::serializeFixedWidthRows(
    row::CompactRow& compactRow,
    int32_t numRows) {
  const auto startRow = rowCount_;
  const auto fixedSize = fixedRowSize_.value();
  const int64_t batchBytes = static_cast<int64_t>(numRows) * fixedSize;

  rowSizes_.resize(startRow + numRows);
  std::fill(rowSizes_.begin() + startRow, rowSizes_.end(), fixedSize);

  ensureFlatBufferCapacity(batchBytes);

  rowOffsets_.resize(startRow + numRows);
  rowPartitions_.resize(startRow + numRows);

  std::vector<size_t> bufferOffsets(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    rowOffsets_[startRow + i] = flatBufferSize_;
    rowPartitions_[startRow + i] = partitions_[i];
    bufferOffsets[i] = static_cast<size_t>(flatBufferSize_);
    flatBufferSize_ += fixedSize;
  }
  // Zero the buffer region for null-bits handling.
  std::memset(
      flatBuffer_->asMutable<char>() + rowOffsets_[startRow], 0, batchBytes);
  compactRow.serialize(
      0, numRows, bufferOffsets.data(), flatBuffer_->asMutable<char>());

  rowCount_ = startRow + numRows;
}

void MaterializedOutput::serializeVariableWidthRows(
    row::CompactRow& compactRow,
    int32_t numRows) {
  const auto startRow = rowCount_;

  rowSizes_.resize(startRow + numRows);
  int64_t batchBytes = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto size = compactRow.rowSize(i);
    rowSizes_[startRow + i] = size;
    batchBytes += size;
  }

  ensureFlatBufferCapacity(batchBytes);

  rowOffsets_.resize(startRow + numRows);
  rowPartitions_.resize(startRow + numRows);

  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto size = rowSizes_[startRow + i];
    rowOffsets_[startRow + i] = flatBufferSize_;
    rowPartitions_[startRow + i] = partitions_[i];
    // Zero for null-bits handling.
    std::memset(flatBuffer_->asMutable<char>() + flatBufferSize_, 0, size);
    compactRow.serialize(i, flatBuffer_->asMutable<char>() + flatBufferSize_);
    flatBufferSize_ += size;
  }

  rowCount_ = startRow + numRows;
}

void MaterializedOutput::ensureFlatBufferCapacity(int64_t additionalBytes) {
  const auto requiredSize = flatBufferSize_ + additionalBytes;
  const auto currentCapacity = flatBuffer_ ? flatBuffer_->capacity() : 0;
  if (requiredSize > static_cast<int64_t>(currentCapacity)) {
    const auto newSize =
        std::max(requiredSize, static_cast<int64_t>(currentCapacity) * 2);
    if (!flatBuffer_) {
      flatBuffer_ = velox::AlignedBuffer::allocate<char>(newSize, pool());
    } else {
      velox::AlignedBuffer::reallocate<char>(&flatBuffer_, newSize);
    }
  }
}

void MaterializedOutput::serializeRows(
    row::CompactRow& compactRow,
    int32_t numRows) {
  if (fixedRowSize_.has_value()) {
    serializeFixedWidthRows(compactRow, numRows);
  } else {
    serializeVariableWidthRows(compactRow, numRows);
  }
}

void MaterializedOutput::addInput(RowVectorPtr input) {
  // Save a reference to the raw input before initializeInput() projects it.
  // The partition function's key channels are set up relative to inputType
  // (the plan node's input schema). We must partition on the raw input, not
  // the projected output, to ensure key channel indices resolve correctly.
  auto rawInput = input;
  initializeInput(std::move(input));
  VELOX_CHECK_NOT_NULL(output_);

  const auto numRows = output_->size();
  if (numRows == 0) {
    output_.reset();
    return;
  }

  computePartitions(*rawInput, numRows);

  row::CompactRow compactRow(output_);
  serializeRows(compactRow, numRows);

  output_.reset();

  if (flatBufferSize_ >= targetSizeInBytes_) {
    flushBatch();
  }
}

std::unique_ptr<folly::IOBuf> MaterializedOutput::buildRowGroup(
    const std::vector<int32_t>& rowIndices) {
  using TRowSize = serializer::TRowSize;
  const auto kHeaderSize = serializer::detail::RowGroupHeader::size();

  int64_t rowDataBytes = 0;
  for (auto idx : rowIndices) {
    rowDataBytes += sizeof(TRowSize) + rowSizes_[idx];
  }
  const int64_t totalBytes = kHeaderSize + rowDataBytes;

  auto iobuf = buffer_->allocateTrackedIOBuf(totalBytes);
  auto* dest = iobuf->writableData();

  serializer::detail::RowGroupHeader header;
  header.uncompressedSize = static_cast<int32_t>(rowDataBytes);
  header.compressedSize = static_cast<int32_t>(rowDataBytes);
  header.compressed = false;
  header.write(reinterpret_cast<char*>(dest));
  dest += kHeaderSize;

  for (auto idx : rowIndices) {
    const TRowSize rowSize =
        folly::Endian::big(static_cast<TRowSize>(rowSizes_[idx]));
    std::memcpy(dest, &rowSize, sizeof(TRowSize));
    dest += sizeof(TRowSize);
    std::memcpy(
        dest,
        flatBuffer_->asMutable<char>() + rowOffsets_[idx],
        rowSizes_[idx]);
    dest += rowSizes_[idx];
  }
  iobuf->append(totalBytes);
  return iobuf;
}

void MaterializedOutput::flushBatch() {
  if (rowCount_ == 0) {
    return;
  }

  // Build per-partition row lists in a single O(rows) pass.
  std::vector<std::vector<int32_t>> partitionRows(numDestinations_);
  for (int32_t i = 0; i < rowCount_; ++i) {
    partitionRows[rowPartitions_[i]].push_back(i);
  }

  for (int32_t partition = 0; partition < numDestinations_; ++partition) {
    const auto& rows = partitionRows[partition];
    if (rows.empty()) {
      continue;
    }

    auto iobuf = buildRowGroup(rows);

    // Enqueue always accepts the data — backpressure is advisory. If the
    // buffer is full, enqueue returns true and populates a future. We record
    // the future but continue flushing remaining partitions. The driver
    // suspends on the next isBlocked() call, not mid-loop. The overshoot
    // per flush is bounded by targetSizeInBytes_ (1-16MB).
    ContinueFuture future;
    if (buffer_->enqueue(partition, std::move(iobuf), &future)) {
      blockingReason_ = BlockingReason::kWaitForConsumer;
      future_ = std::move(future);
    }
  }

  // Reset accumulated state.
  rowOffsets_.clear();
  rowSizes_.clear();
  rowPartitions_.clear();
  rowCount_ = 0;
  flatBufferSize_ = 0;
}

RowVectorPtr MaterializedOutput::getOutput() {
  return nullptr;
}

void MaterializedOutput::noMoreInput() {
  Operator::noMoreInput();
  finish();
}

BlockingReason MaterializedOutput::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    auto reason = blockingReason_;
    blockingReason_ = BlockingReason::kNotBlocked;
    return reason;
  }
  return BlockingReason::kNotBlocked;
}

bool MaterializedOutput::isFinished() {
  return finished_;
}

void MaterializedOutput::close() {
  finish();
  Operator::close();
}

void MaterializedOutput::finish() {
  if (finished_) {
    return;
  }
  finished_ = true;
  flushBatch();

  // Use Velox's allPeersFinished barrier — returns true only for the last
  // driver to reach this point. The last driver drains and closes the writer.
  // Wrap in try/catch because allPeersFinished throws if the task is
  // already terminating (e.g., due to an error on another pipeline).
  // In that case, the buffer's destructor will handle cleanup via abort().
  try {
    std::vector<velox::ContinuePromise> promises;
    std::vector<std::shared_ptr<velox::exec::Driver>> peers;
    velox::ContinueFuture peerFuture;
    auto* driverCtx = operatorCtx()->driverCtx();
    bool isLast = driverCtx->task->allPeersFinished(
        planNodeId(), driverCtx->driver, &peerFuture, promises, peers);

    if (isLast) {
      buffer_->noMoreData();
      for (const auto& [key, value] : buffer_->stats()) {
        addRuntimeStat(key, velox::RuntimeCounter(value));
      }
      for (auto& promise : promises) {
        promise.setValue();
      }
    }
  } catch (const velox::VeloxRuntimeError&) {
    // Task is terminating — abort the buffer if not already closed.
    buffer_->abort();
  }
  finished_ = true;
}

std::unique_ptr<Operator> MaterializedOutputTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto materializedOutputNode =
          std::dynamic_pointer_cast<const MaterializedOutputNode>(node)) {
    return std::make_unique<MaterializedOutput>(
        id, ctx, materializedOutputNode);
  }
  return nullptr;
}

} // namespace facebook::presto::operators
