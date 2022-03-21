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
#include "velox/exec/AssignUniqueId.h"

#include <algorithm>
#include <utility>

namespace facebook::velox::exec {

AssignUniqueId::AssignUniqueId(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::AssignUniqueIdNode>& planNode,
    int32_t uniqueTaskId,
    std::shared_ptr<std::atomic_int64_t> rowIdPool)
    : Operator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "AssignUniqueId"),
      rowIdPool_(std::move(rowIdPool)) {
  VELOX_CHECK_LT(uniqueTaskId, kTaskUniqueIdLimit)
  uniqueValueMask_ = ((int64_t)uniqueTaskId) << 40;

  const auto numColumns = planNode->outputType()->size();
  identityProjections_.reserve(numColumns - 1);
  for (ChannelIndex i = 0; i < numColumns - 1; ++i) {
    identityProjections_.emplace_back(i, i);
  }

  resultProjections_.emplace_back(0, numColumns - 1);
  results_.resize(1);

  rowIdCounter_ = 0;
  maxRowIdCounterValue_ = 0;
}

void AssignUniqueId::addInput(RowVectorPtr input) {
  auto numInput = input->size();
  VELOX_CHECK_NE(
      numInput, 0, "AssignUniqueId::addInput received empty set of rows")
  input_ = std::move(input);
}

RowVectorPtr AssignUniqueId::getOutput() {
  if (input_ == nullptr) {
    return nullptr;
  }
  generateIdColumn(input_->size());
  auto output = fillOutput(input_->size(), nullptr);
  input_ = nullptr;
  return output;
}

bool AssignUniqueId::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}

void AssignUniqueId::generateIdColumn(vector_size_t size) {
  // Try to re-use memory for the ID vector. This method populates results_[0],
  // then getOutput() std::move's it into last child of output_.
  VectorPtr result;
  if (output_ && output_.unique()) {
    BaseVector::prepareForReuse(output_->children().back(), size);
    result = output_->children().back();
  } else {
    result = BaseVector::create(BIGINT(), size, pool());
  }
  results_[0] = result;

  auto rawResults =
      result->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();

  vector_size_t start = 0;
  while (start < size) {
    if (rowIdCounter_ >= maxRowIdCounterValue_) {
      requestRowIds();
    }

    auto batchSize =
        std::min(maxRowIdCounterValue_ - rowIdCounter_ + 1, kRowIdsPerRequest);
    auto end = (int32_t)std::min((int64_t)size, start + batchSize);
    VELOX_CHECK_EQ((rowIdCounter_ + end - 1) & uniqueValueMask_, 0);
    std::iota(
        rawResults + start, rawResults + end, uniqueValueMask_ | rowIdCounter_);
    rowIdCounter_ += end;
    start = end;
  }
}

void AssignUniqueId::requestRowIds() {
  rowIdCounter_ = rowIdPool_->fetch_add(kRowIdsPerRequest);
  maxRowIdCounterValue_ =
      std::min(rowIdCounter_ + kRowIdsPerRequest, kMaxRowId);
}
} // namespace facebook::velox::exec
