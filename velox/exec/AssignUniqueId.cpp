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
  generateIdColumn();
  auto output = fillOutput(input_->size(), nullptr);
  input_ = nullptr;
  return output;
}

void AssignUniqueId::generateIdColumn() {
  auto numInput = input_->size();
  auto result = results_[0];
  if (!result || !BaseVector::isReusableFlatVector(result)) {
    result = BaseVector::create(BIGINT(), numInput, operatorCtx_->pool());
    results_[0] = result;
  } else {
    result->resize(numInput);
  }
  auto rawResults =
      result->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();

  vector_size_t start = 0;
  while (start < numInput) {
    if (rowIdCounter_ >= maxRowIdCounterValue_) {
      rowIdCounter_ = rowIdPool_->fetch_add(kRowIdsPerRequest);
      maxRowIdCounterValue_ =
          std::min({rowIdCounter_ + kRowIdsPerRequest, kMaxRowId});
    }

    auto end = std::min(numInput, (int32_t)(start + kRowIdsPerRequest));

    std::iota(
        rawResults + start, rawResults + end, uniqueValueMask_ | rowIdCounter_);
    rowIdCounter_ += end;
    start = end;
  }
}
} // namespace facebook::velox::exec
