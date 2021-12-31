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
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

OrderBy::OrderBy(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "OrderBy"),
      data_(std::make_unique<RowContainer>(
          outputType_->as<TypeKind::ROW>().children(),
          operatorCtx_->mappedMemory())) {
  auto type = orderByNode->outputType();
  auto numKeys = orderByNode->sortingKeys().size();
  for (int i = 0; i < numKeys; ++i) {
    auto channel = exprToChannel(orderByNode->sortingKeys()[i].get(), type);
    VELOX_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant grouping keys");
    keyInfo_.emplace_back(channel, orderByNode->sortingOrders()[i]);
  }
}

void OrderBy::addInput(RowVectorPtr input) {
  SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }
  for (size_t col = 0; col < input->childrenSize(); ++col) {
    DecodedVector decoded(*input->childAt(col), allRows);
    for (int i = 0; i < input->size(); ++i) {
      data_->store(decoded, i, rows[i], col);
    }
  }

  numRows_ += allRows.size();
}

void OrderBy::finish() {
  Operator::finish();

  // No data.
  if (numRows_ == 0) {
    finished_ = true;
    return;
  }

  // Sort the pointers to the rows in RowContainer (data_) instead of sorting
  // the rows.
  returningRows_.resize(numRows_);
  RowContainerIterator iter;
  data_->listRows(&iter, numRows_, returningRows_.data());

  std::sort(
      returningRows_.begin(),
      returningRows_.end(),
      [this](const char* leftRow, const char* rightRow) {
        for (auto& [channelIndex, sortOrder] : keyInfo_) {
          if (auto result = data_->compare(
                  leftRow,
                  rightRow,
                  channelIndex,
                  {sortOrder.isNullsFirst(), sortOrder.isAscending(), false})) {
            return result < 0;
          }
        }
        return false; // lhs == rhs.
      });
}

RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !isFinishing_ || returningRows_.size() == numRowsReturned_) {
    return nullptr;
  }

  size_t numRows = data_->estimatedNumRowsPerBatch(kBatchSizeInBytes);
  int32_t numRowsToReturn =
      std::min(numRows, returningRows_.size() - numRowsReturned_);

  VELOX_CHECK_GT(numRowsToReturn, 0);

  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numRowsToReturn, operatorCtx_->pool()));

  for (int i = 0; i < outputType_->size(); ++i) {
    data_->extractColumn(
        returningRows_.data() + numRowsReturned_,
        numRowsToReturn,
        i,
        result->childAt(i));
  }

  numRowsReturned_ += numRowsToReturn;

  finished_ = (numRowsReturned_ == returningRows_.size());

  return result;
}
} // namespace facebook::velox::exec
