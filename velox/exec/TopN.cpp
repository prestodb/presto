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
#include "velox/exec/TopN.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {
TopN::TopN(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TopNNode>& topNNode)
    : Operator(
          driverCtx,
          topNNode->outputType(),
          operatorId,
          topNNode->id(),
          "TopN"),
      count_(topNNode->count()),
      data_(std::make_unique<RowContainer>(
          outputType_->children(),
          operatorCtx_->mappedMemory())),
      comparator_(
          outputType_,
          topNNode->sortingKeys(),
          topNNode->sortingOrders(),
          data_.get()),
      topRows_(comparator_),
      decodedVectors_(outputType_->children().size()) {}

TopN::Comparator::Comparator(
    const RowTypePtr& type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    RowContainer* rowContainer)
    : rowContainer_(rowContainer) {
  auto numKeys = sortingKeys.size();
  for (int i = 0; i < numKeys; ++i) {
    auto channel = exprToChannel(sortingKeys[i].get(), type);
    VELOX_CHECK(
        channel != kConstantChannel,
        "TopN doesn't allow constant comparison keys");
    keyInfo_.push_back(std::make_pair(channel, sortingOrders[i]));
  }
}

void TopN::addInput(RowVectorPtr input) {
  SelectivityVector allRows(input->size());

  // TODO Decode keys first, then decode the rest only for passing positions
  for (int col = 0; col < input->childrenSize(); ++col) {
    decodedVectors_[col].decode(*input->childAt(col), allRows);
  }

  for (int row = 0; row < input->size(); ++row) {
    char* newRow = nullptr;
    if (topRows_.size() < count_) {
      newRow = data_->newRow();
    } else {
      char* topRow = topRows_.top();

      if (comparator_(topRow, decodedVectors_, row)) {
        continue;
      }
      topRows_.pop();
      // Reuse the topRow's memory.
      newRow = data_->initializeRow(topRow, true /* reuse */);
    }

    for (int col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedVectors_[col], row, newRow, col);
    }

    topRows_.push(newRow);
  }
}

RowVectorPtr TopN::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  uint32_t numRowsToReturn =
      std::min(kMaxNumRowsToReturn, rows_.size() - numRowsReturned_);
  VELOX_CHECK(numRowsToReturn > 0);

  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numRowsToReturn, operatorCtx_->pool()));

  for (int i = 0; i < outputType_->size(); ++i) {
    data_->extractColumn(
        rows_.data() + numRowsReturned_,
        numRowsToReturn,
        i,
        result->childAt(i));
  }
  numRowsReturned_ += numRowsToReturn;
  finished_ = (numRowsReturned_ == rows_.size());
  return result;
}

void TopN::noMoreInput() {
  Operator::noMoreInput();
  if (topRows_.empty()) {
    finished_ = true;
    return;
  }
  rows_.resize(topRows_.size());
  for (int i = rows_.size(); i > 0; --i) {
    rows_[i - 1] = topRows_.top();
    topRows_.pop();
  }
}

bool TopN::isFinished() {
  return finished_;
}
} // namespace facebook::velox::exec
