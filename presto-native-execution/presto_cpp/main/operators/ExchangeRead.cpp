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
#include "presto_cpp/main/operators/ExchangeRead.h"

#include <folly/lang/Bits.h>
#include <cstring>

#include "presto_cpp/main/operators/ShuffleExchangeSource.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/RowSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::operators {

namespace {
core::PlanNodeId deserializeExchangeReadNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}
} // namespace

folly::dynamic ExchangeReadNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

core::PlanNodePtr ExchangeReadNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<ExchangeReadNode>(
      deserializeExchangeReadNodeId(obj),
      ISerializable::deserialize<RowType>(obj["outputType"], context));
}

ExchangeRead::ExchangeRead(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const ExchangeReadNode>& exchangeReadNode,
    std::shared_ptr<ExchangeClient> exchangeClient)
    : Exchange(
          operatorId,
          ctx,
          std::make_shared<core::ExchangeNode>(
              exchangeReadNode->id(),
              exchangeReadNode->outputType(),
              "CompactRow"),
          exchangeClient,
          "ExchangeRead") {}

// Clear accumulated page and row state after all rows are consumed.
void ExchangeRead::resetOutputState() {
  currentPages_.clear();
  rows_.clear();
  nextRow_ = 0;
}

// Expand all current pages into row string_views by parsing the batched format.
uint64_t ExchangeRead::parseCurrentPages() {
  uint64_t rawInputBytes = 0;
  rows_.reserve(1024);
  for (const auto& page : currentPages_) {
    auto* batch = checkedPointerCast<ShuffleSerializedPage>(page.get());
    rawInputBytes += page->size();
    const int32_t driverId = operatorCtx()->driverCtx()->driverId;
    const auto& pageRowViews = batch->rows(driverId);
    for (const auto& value : pageRowViews) {
      if (value.empty()) {
        continue;
      }
      expandBatchedPage(value);
    }
  }
  ++numInputBatches_;
  return rawInputBytes;
}

// Compute output batch size and deserialize the next batch of CompactRow
// data into a RowVector.
RowVectorPtr ExchangeRead::deserializeNextBatch() {
  auto numOutputRows = kInitialOutputRows;
  if (estimatedRowSize_.has_value()) {
    numOutputRows = std::max(
        preferredOutputBatchBytes_ / estimatedRowSize_.value(),
        kInitialOutputRows);
  }
  numOutputRows = std::min<uint64_t>(numOutputRows, rows_.size() - nextRow_);

  RowVectorPtr resultRowVector;
  if (numOutputRows == rows_.size()) {
    resultRowVector = row::CompactRow::deserialize(rows_, outputType_, pool());
  } else {
    std::vector<std::string_view> outputRows(
        rows_.begin() + nextRow_, rows_.begin() + nextRow_ + numOutputRows);
    resultRowVector =
        row::CompactRow::deserialize(outputRows, outputType_, pool());
  }

  nextRow_ += numOutputRows;
  totalRows_ += numOutputRows;
  estimatedRowSize_ = std::max(
      resultRowVector->estimateFlatSize() / numOutputRows,
      estimatedRowSize_.value_or(1L));
  return resultRowVector;
}

RowVectorPtr ExchangeRead::getOutput() {
  if (currentPages_.empty()) {
    return nullptr;
  }

  SCOPE_EXIT {
    if (nextRow_ == rows_.size()) {
      resetOutputState();
    }
  };

  uint64_t rawInputBytes{0};
  if (rows_.empty()) {
    VELOX_CHECK_EQ(nextRow_, 0);
    rawInputBytes = parseCurrentPages();
  }

  if (rows_.empty()) {
    return nullptr;
  }

  result_ = deserializeNextBatch();
  recordInputStats(rawInputBytes);
  return result_;
}

void ExchangeRead::close() {
  Exchange::close();
  if (numInputBatches_ != 0) {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        "exchangeReadInputBatches", RuntimeCounter(numInputBatches_));
    lockedStats->addRuntimeStat(
        "exchangeReadTotalRows", RuntimeCounter(totalRows_));
  }
  LOG(INFO) << "[ExchangeRead] close: totalRows=" << totalRows_
            << " inputBatches=" << numInputBatches_
            << " planNodeId=" << planNodeId();
}

// Parses RowGroupHeader + TRowSize-framed CompactRow data written by
// ExchangeWrite. This is a lightweight alternative to VectorStreamGroup
// deserialization — it operates directly on the framed byte layout without
// going through the serde abstraction, since ExchangeWrite always produces
// CompactRow data with a fixed framing format (RowGroupHeader followed by
// big-endian TRowSize + row bytes).
void ExchangeRead::expandBatchedPage(std::string_view pageData) {
  const size_t kPageHeaderSize = serializer::detail::RowGroupHeader::size();

  const char* ptr = pageData.data();
  size_t remaining = pageData.size();

  // Iterate over one or more RowGroupHeaders in the buffer.
  while (remaining > 0) {
    int32_t uncompressedSize;
    std::memcpy(&uncompressedSize, ptr, sizeof(int32_t));
    ptr += kPageHeaderSize;
    remaining -= kPageHeaderSize;

    VELOX_CHECK_GE(
        remaining,
        static_cast<size_t>(uncompressedSize),
        "Page data truncated: expected {} bytes, got {}",
        uncompressedSize,
        remaining);

    // Parse TRowSize-framed rows within this RowGroup.
    size_t pageRemaining = uncompressedSize;
    while (pageRemaining > 0) {
      VELOX_CHECK_GE(pageRemaining, sizeof(uint32_t), "Truncated TRowSize");
      uint32_t rowSize;
      std::memcpy(&rowSize, ptr, sizeof(uint32_t));
      rowSize = folly::Endian::big(rowSize);
      ptr += sizeof(uint32_t);
      pageRemaining -= sizeof(uint32_t);

      VELOX_CHECK_GE(pageRemaining, rowSize, "Truncated row data");
      rows_.emplace_back(ptr, rowSize);
      ptr += rowSize;
      pageRemaining -= rowSize;
    }

    remaining -= uncompressedSize;
  }
}

std::unique_ptr<Operator> ExchangeReadTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node,
    std::shared_ptr<ExchangeClient> exchangeClient) {
  if (auto exchangeReadNode =
          std::dynamic_pointer_cast<const ExchangeReadNode>(node)) {
    return std::make_unique<ExchangeRead>(
        id, ctx, exchangeReadNode, exchangeClient);
  }
  return nullptr;
}

} // namespace facebook::presto::operators
