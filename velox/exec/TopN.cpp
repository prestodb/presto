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
      data_(std::make_unique<RowContainer>(outputType_->children(), pool())),
      comparator_(
          outputType_,
          topNNode->sortingKeys(),
          topNNode->sortingOrders(),
          data_.get()),
      topRows_(comparator_),
      decodedVectors_(outputType_->children().size()) {}

void TopN::addInput(RowVectorPtr input) {
  // TODO Decode keys first, then decode the rest only for passing positions
  for (auto col = 0; col < input->childrenSize(); ++col) {
    decodedVectors_[col].decode(*input->childAt(col));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = nullptr;
    if (topRows_.size() < count_) {
      newRow = data_->newRow();
    } else {
      char* topRow = topRows_.top();

      if (!comparator_(decodedVectors_, row, topRow)) {
        continue;
      }
      topRows_.pop();
      // Reuse the topRow's memory.
      newRow = data_->initializeRow(topRow, true /* reuse */);
    }

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedVectors_[col], row, newRow, col);
    }

    topRows_.push(newRow);
  }
}

RowVectorPtr TopN::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  const auto numRowsToReturn = std::min<vector_size_t>(
      outputBatchSize_, rows_.size() - numRowsReturned_);
  VELOX_CHECK_GT(numRowsToReturn, 0);

  auto result = BaseVector::create<RowVector>(
      outputType_, numRowsToReturn, operatorCtx_->pool());

  for (auto i = 0; i < outputType_->size(); ++i) {
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
  for (auto i = rows_.size(); i > 0; --i) {
    rows_[i - 1] = topRows_.top();
    topRows_.pop();
  }

  outputBatchSize_ = outputBatchRows(data_->estimateRowSize());
}

bool TopN::isFinished() {
  return finished_;
}
} // namespace facebook::velox::exec
