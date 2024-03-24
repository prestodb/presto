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
#include "velox/exec/OrderBy.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

namespace {
CompareFlags fromSortOrderToCompareFlags(const core::SortOrder& sortOrder) {
  return {
      sortOrder.isNullsFirst(),
      sortOrder.isAscending(),
      false,
      CompareFlags::NullHandlingMode::kNullAsValue};
}
} // namespace

OrderBy::OrderBy(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "OrderBy",
          orderByNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt) {
  maxOutputRows_ = outputBatchRows(std::nullopt);
  VELOX_CHECK(pool()->trackUsage());
  std::vector<column_index_t> sortColumnIndices;
  std::vector<CompareFlags> sortCompareFlags;
  sortColumnIndices.reserve(orderByNode->sortingKeys().size());
  sortCompareFlags.reserve(orderByNode->sortingKeys().size());
  for (int i = 0; i < orderByNode->sortingKeys().size(); ++i) {
    const auto channel =
        exprToChannel(orderByNode->sortingKeys()[i].get(), outputType_);
    VELOX_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortColumnIndices.push_back(channel);
    sortCompareFlags.push_back(
        fromSortOrderToCompareFlags(orderByNode->sortingOrders()[i]));
  }
  sortBuffer_ = std::make_unique<SortBuffer>(
      outputType_,
      sortColumnIndices,
      sortCompareFlags,
      pool(),
      &nonReclaimableSection_,
      spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr,
      &spillStats_);
}

void OrderBy::addInput(RowVectorPtr input) {
  sortBuffer_->addInput(input);
}

void OrderBy::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  VELOX_CHECK(!nonReclaimableSection_);

  // TODO: support fine-grain disk spilling based on 'targetBytes' after
  // having row container memory compaction support later.
  sortBuffer_->spill();

  // Release the minimum reserved memory.
  pool()->release();
}

void OrderBy::noMoreInput() {
  Operator::noMoreInput();
  sortBuffer_->noMoreInput();
  maxOutputRows_ = outputBatchRows(sortBuffer_->estimateOutputRowSize());
}

RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  RowVectorPtr output = sortBuffer_->getOutput(maxOutputRows_);
  finished_ = (output == nullptr);
  return output;
}

void OrderBy::close() {
  Operator::close();
  sortBuffer_.reset();
}
} // namespace facebook::velox::exec
