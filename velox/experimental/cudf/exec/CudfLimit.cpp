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

#include "velox/experimental/cudf/exec/CudfLimit.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include <cudf/copying.hpp>

namespace facebook::velox::cudf_velox {

CudfLimit::CudfLimit(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::LimitNode>& limitNode)
    : Operator(
          driverCtx,
          limitNode->outputType(),
          operatorId,
          limitNode->id(),
          "CudfLimit"),
      NvtxHelper(
          nvtx3::rgb{112, 128, 144}, // Slate Gray
          operatorId,
          fmt::format("[{}]", limitNode->id())),
      remainingOffset_{limitNode->offset()},
      remainingLimit_{limitNode->count()} {
  isIdentityProjection_ = true;

  const auto numColumns = limitNode->outputType()->size();
  identityProjections_.reserve(numColumns);
  for (column_index_t i = 0; i < numColumns; ++i) {
    identityProjections_.emplace_back(i, i);
  }
}

bool CudfLimit::needsInput() const {
  return !finished_ && input_ == nullptr;
}

void CudfLimit::addInput(RowVectorPtr input) {
  VELOX_CHECK_NULL(input_);
  input_ = input;
}

RowVectorPtr CudfLimit::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (input_ == nullptr || (remainingOffset_ == 0 && remainingLimit_ == 0)) {
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (remainingOffset_ >= inputSize) {
    remainingOffset_ -= inputSize;
    input_ = nullptr;
    return nullptr;
  }

  auto cudfInput = std::dynamic_pointer_cast<cudf_velox::CudfVector>(input_);

  // This is the case where the offset lies in the middle of the current batch
  // we want to start outputting rows from the middle of the input.
  if (remainingOffset_ > 0) {
    // Return a subset of input_ rows.
    const auto outputSize =
        std::min(inputSize - remainingOffset_, remainingLimit_);

    auto slicedTable = cudf::slice(
        cudfInput->getTableView(),
        {static_cast<int>(remainingOffset_),
         static_cast<int>(remainingOffset_ + outputSize)},
        cudfInput->stream());

    auto materializedTable =
        std::make_unique<cudf::table>(slicedTable[0], cudfInput->stream());

    remainingOffset_ = 0;
    remainingLimit_ -= outputSize;
    if (remainingLimit_ == 0) {
      finished_ = true;
    }
    auto output = std::make_shared<cudf_velox::CudfVector>(
        input_->pool(),
        input_->type(),
        outputSize,
        std::move(materializedTable),
        cudfInput->stream());
    input_.reset();
    return output;
  }

  if (remainingLimit_ <= inputSize) {
    finished_ = true;
  }

  // This is the case where we want to output all rows from the input because
  // the range we want to output exceeds the input in both directions.
  if (remainingLimit_ >= inputSize) {
    remainingLimit_ -= inputSize;
    auto output = input_;
    input_.reset();
    return output;
  }

  // At this point, we have no offset but the limit is less than the input size.
  // We want to slice from the beginning but till the middle of the input.
  auto slicedTable = cudf::slice(
      cudfInput->getTableView(),
      {0, static_cast<int>(remainingLimit_)},
      cudfInput->stream());

  auto materializedTable =
      std::make_unique<cudf::table>(slicedTable[0], cudfInput->stream());

  auto output = std::make_shared<cudf_velox::CudfVector>(
      input_->pool(),
      input_->type(),
      remainingLimit_,
      std::move(materializedTable),
      cudfInput->stream());
  input_.reset();
  remainingLimit_ = 0;
  return output;
}

} // namespace facebook::velox::cudf_velox
