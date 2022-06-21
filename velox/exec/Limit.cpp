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
#include "velox/exec/Limit.h"

namespace facebook::velox::exec {
Limit::Limit(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::LimitNode>& limitNode)
    : Operator(
          driverCtx,
          limitNode->outputType(),
          operatorId,
          limitNode->id(),
          "Limit"),
      remainingOffset_{limitNode->offset()},
      remainingLimit_{limitNode->count()} {
  isIdentityProjection_ = true;

  const auto numColumns = limitNode->outputType()->size();
  identityProjections_.reserve(numColumns);
  for (column_index_t i = 0; i < numColumns; ++i) {
    identityProjections_.emplace_back(i, i);
  }
}

bool Limit::needsInput() const {
  return !finished_ && input_ == nullptr;
}

void Limit::addInput(RowVectorPtr input) {
  VELOX_CHECK(input_ == nullptr);
  input_ = input;
}

RowVectorPtr Limit::getOutput() {
  if (input_ == nullptr || (remainingOffset_ == 0 && remainingLimit_ == 0)) {
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (remainingOffset_ >= inputSize) {
    remainingOffset_ -= inputSize;
    input_ = nullptr;
    return nullptr;
  }

  if (remainingOffset_ > 0) {
    // Return a subset of input_ rows.
    auto outputSize = std::min(inputSize - remainingOffset_, remainingLimit_);

    BufferPtr indices = allocateIndices(outputSize, pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    std::iota(rawIndices, rawIndices + outputSize, remainingOffset_);

    auto output = fillOutput(outputSize, indices);
    remainingOffset_ = 0;
    remainingLimit_ -= outputSize;
    input_ = nullptr;
    if (remainingLimit_ == 0) {
      finished_ = true;
    }
    return output;
  }

  if (remainingLimit_ <= inputSize) {
    finished_ = true;
  }

  if (remainingLimit_ >= inputSize) {
    remainingLimit_ -= inputSize;
    auto output = input_;
    input_.reset();
    return output;
  }

  auto output = std::make_shared<RowVector>(
      input_->pool(),
      input_->type(),
      input_->nulls(),
      remainingLimit_,
      input_->children());
  input_.reset();
  remainingLimit_ = 0;
  return output;
}
} // namespace facebook::velox::exec
