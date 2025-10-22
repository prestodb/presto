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
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleExchangeSource.h"
#include "velox/common/Casts.h"
#include "velox/exec/Exchange.h"
#include "velox/row/CompactRow.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

namespace {
class ShuffleRead : public Exchange {
 public:
  ShuffleRead(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const ShuffleReadNode>& shuffleReadNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : Exchange(
            operatorId,
            ctx,
            std::make_shared<core::ExchangeNode>(
                shuffleReadNode->id(),
                shuffleReadNode->outputType(),
                VectorSerde::Kind::kCompactRow),
            exchangeClient,
            "ShuffleRead") {}

  RowVectorPtr getOutput() override;

 protected:
  VectorSerde* getSerde() override {
    VELOX_UNSUPPORTED("ShuffleReadOperator doesn't use serde");
  }

 private:
  size_t nextRow_{0};
  // Reusable buffers.
  std::vector<std::string_view> rows_;
};

RowVectorPtr ShuffleRead::getOutput() {
  if (currentPages_.empty()) {
    return nullptr;
  }

  SCOPE_EXIT {
    if (nextRow_ == rows_.size()) {
      currentPages_.clear();
      rows_.clear();
      nextRow_ = 0;
    }
  };

  uint64_t rawInputBytes{0};
  if (rows_.empty()) {
    VELOX_CHECK_EQ(nextRow_, 0);
    size_t numRows{0};
    for (const auto& page : currentPages_) {
      rawInputBytes += page->size();
      numRows += page->numRows().value();
    }
    rows_.reserve(numRows);
    for (const auto& page : currentPages_) {
      auto* batch = checked_pointer_cast<ShuffleRowBatch>(page.get());
      const auto& rows = batch->rows();
      for (const auto& row : rows) {
        rows_.emplace_back(row);
      }
    }
  }
  VELOX_CHECK_LE(nextRow_, rows_.size());
  if (rows_.empty()) {
    return nullptr;
  }

  auto numOutputRows = kInitialOutputRows;
  if (estimatedRowSize_.has_value()) {
    numOutputRows = std::max(
        (preferredOutputBatchBytes_ / estimatedRowSize_.value()),
        kInitialOutputRows);
  }
  numOutputRows = std::min<uint64_t>(numOutputRows, rows_.size() - nextRow_);

  // Create a view of the rows to deserialize from nextRow_ to nextRow_ +
  // numOutputRows.
  if (numOutputRows == rows_.size()) {
    result_ = row::CompactRow::deserialize(rows_, outputType_, pool());
  } else {
    std::vector<std::string_view> outputRows(
        rows_.begin() + nextRow_, rows_.begin() + nextRow_ + numOutputRows);
    result_ = row::CompactRow::deserialize(outputRows, outputType_, pool());
  }
  nextRow_ += numOutputRows;
  estimatedRowSize_ = std::max(
      result_->estimateFlatSize() / numOutputRows,
      estimatedRowSize_.value_or(1L));
  recordInputStats(rawInputBytes);
  return result_;
}
} // namespace

folly::dynamic ShuffleReadNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

velox::core::PlanNodePtr ShuffleReadNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<ShuffleReadNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<RowType>(obj["outputType"], context));
}

std::unique_ptr<Operator> ShuffleReadTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node,
    std::shared_ptr<ExchangeClient> exchangeClient) {
  if (auto shuffleReadNode =
          std::dynamic_pointer_cast<const ShuffleReadNode>(node)) {
    return std::make_unique<ShuffleRead>(
        id, ctx, shuffleReadNode, exchangeClient);
  }
  return nullptr;
}
} // namespace facebook::presto::operators
