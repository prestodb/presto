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
#include "presto_cpp/main/operators/ExchangeWrite.h"

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

folly::dynamic ExchangeWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["numPartitions"] = numPartitions_;
  obj["keys"] = ISerializable::serialize(keys_);
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  obj["outputType"] = outputType_->serialize();
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

core::PlanNodePtr ExchangeWriteNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto keys = ISerializable::deserialize<std::vector<core::ITypedExpr>>(
      obj["keys"], context);

  std::vector<core::TypedExprPtr> keyPtrs;
  keyPtrs.reserve(keys.size());
  for (auto& key : keys) {
    keyPtrs.push_back(std::shared_ptr<const core::ITypedExpr>(std::move(key)));
  }

  auto source = ISerializable::deserialize<std::vector<core::PlanNode>>(
      obj["sources"], context)[0];

  auto partitionFunctionSpec =
      ISerializable::deserialize<core::PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context);

  auto outputType = ISerializable::deserialize<RowType>(obj["outputType"]);

  // Buffer cannot be deserialized — it is set externally by the plan
  // converter.
  return std::make_shared<ExchangeWriteNode>(
      deserializePlanNodeId(obj),
      std::move(keyPtrs),
      static_cast<int>(obj["numPartitions"].asInt()),
      std::move(outputType),
      std::move(partitionFunctionSpec),
      std::move(source),
      std::shared_ptr<ExchangeOutputBuffer>{});
}

ExchangeWrite::ExchangeWrite(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const ExchangeWriteNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "ExchangeWrite"),
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

// Project input columns and lazy-load all children.
void ExchangeWrite::initializeInput(RowVectorPtr input) {
  if (outputType_->size() == 0) {
    output_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        nullptr,
        input->size(),
        std::vector<VectorPtr>{});
  } else if (outputChannels_.empty()) {
    output_ = std::move(input);
  } else {
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputChannels_.size());
    for (auto i : outputChannels_) {
      outputColumns.push_back(input->childAt(i));
    }
    output_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        nullptr,
        input->size(),
        std::move(outputColumns));
  }

  // Lazy load all input columns.
  for (auto i = 0; i < output_->childrenSize(); ++i) {
    output_->childAt(i)->loadedVector();
  }
}

// Assign partition IDs for all rows using the partition function.
void ExchangeWrite::computePartitions(
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

// Serialize all rows into the flat buffer using CompactRow. Handles both
// fixed-width (batch serialize) and variable-width (row-at-a-time) paths.
void ExchangeWrite::serializeRows(
    row::CompactRow& compactRow,
    int32_t numRows) {
  const auto startRow = rowCount_;

  // Pre-compute row sizes and total bytes needed.
  int64_t batchBytes = 0;
  rowSizes_.resize(startRow + numRows);
  if (fixedRowSize_.has_value()) {
    const auto fixedSize = fixedRowSize_.value();
    std::fill(rowSizes_.begin() + startRow, rowSizes_.end(), fixedSize);
    batchBytes = static_cast<int64_t>(numRows) * fixedSize;
  } else {
    for (vector_size_t i = 0; i < numRows; ++i) {
      const auto size = compactRow.rowSize(i);
      rowSizes_[startRow + i] = size;
      batchBytes += size;
    }
  }

  // Ensure flat buffer capacity (pool-tracked).
  const auto requiredSize = flatBufferSize_ + batchBytes;
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

  // Compute offsets and serialize.
  rowOffsets_.resize(startRow + numRows);
  rowPartitions_.resize(startRow + numRows);

  if (fixedRowSize_.has_value()) {
    // Batch serialize for fixed-width rows.
    std::vector<size_t> bufferOffsets(numRows);
    for (vector_size_t i = 0; i < numRows; ++i) {
      rowOffsets_[startRow + i] = flatBufferSize_;
      rowPartitions_[startRow + i] = partitions_[i];
      bufferOffsets[i] = static_cast<size_t>(flatBufferSize_);
      flatBufferSize_ += fixedRowSize_.value();
    }
    // Zero the buffer region for null-bits handling.
    std::memset(
        flatBuffer_->asMutable<char>() + rowOffsets_[startRow], 0, batchBytes);
    compactRow.serialize(
        0, numRows, bufferOffsets.data(), flatBuffer_->asMutable<char>());
  } else {
    // Row-at-a-time serialize for variable-width rows.
    for (vector_size_t i = 0; i < numRows; ++i) {
      const auto size = rowSizes_[startRow + i];
      rowOffsets_[startRow + i] = flatBufferSize_;
      rowPartitions_[startRow + i] = partitions_[i];
      // Zero for null-bits handling.
      std::memset(flatBuffer_->asMutable<char>() + flatBufferSize_, 0, size);
      compactRow.serialize(i, flatBuffer_->asMutable<char>() + flatBufferSize_);
      flatBufferSize_ += size;
    }
  }

  rowCount_ = startRow + numRows;
}

// Partition input, serialize into flat buffer, and flush if threshold reached.
void ExchangeWrite::addInput(RowVectorPtr input) {
  // Save a reference to the raw input before initializeInput() projects it.
  // The partition function's key channels are set up relative to inputType
  // (the plan node's input schema). We must partition on the raw input, not
  // the projected output, to ensure key channel indices resolve correctly.
  auto rawInput = input;
  initializeInput(std::move(input));

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

// Build a contiguous IOBuf with RowGroupHeader + TRowSize-framed CompactRow
// data for the given row indices belonging to one partition.
std::unique_ptr<folly::IOBuf> ExchangeWrite::buildRowGroup(
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

// Group accumulated rows by partition and enqueue per-partition pages
// to the ExchangeOutputBuffer.
void ExchangeWrite::flushBatch() {
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

RowVectorPtr ExchangeWrite::getOutput() {
  return nullptr;
}

void ExchangeWrite::noMoreInput() {
  Operator::noMoreInput();
  finalizeDriver();
}

BlockingReason ExchangeWrite::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    auto reason = blockingReason_;
    blockingReason_ = BlockingReason::kNotBlocked;
    return reason;
  }
  return BlockingReason::kNotBlocked;
}

bool ExchangeWrite::isFinished() {
  return finished_;
}

void ExchangeWrite::close() {
  finalizeDriver();
  Operator::close();
}

// Flush remaining data and coordinate with peer drivers. The last driver
// to arrive drains the buffer and closes the writer.
void ExchangeWrite::finalizeDriver() {
  if (driverFinalized_) {
    return;
  }
  driverFinalized_ = true;
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

std::unique_ptr<Operator> ExchangeWriteTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto exchangeWriteNode =
          std::dynamic_pointer_cast<const ExchangeWriteNode>(node)) {
    return std::make_unique<ExchangeWrite>(id, ctx, exchangeWriteNode);
  }
  return nullptr;
}

} // namespace facebook::presto::operators
