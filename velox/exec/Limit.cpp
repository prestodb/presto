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
      remainingLimit_(limitNode->count()) {}

bool Limit::needsInput() const {
  return remainingLimit_ > 0 && input_ == nullptr;
}

void Limit::addInput(RowVectorPtr input) {
  VELOX_CHECK(input_ == nullptr);
  input_ = input;
}

RowVectorPtr Limit::getOutput() {
  if (input_ == nullptr || remainingLimit_ == 0) {
    return nullptr;
  }

  if (remainingLimit_ <= input_->size()) {
    isFinishing_ = true;
  }

  if (remainingLimit_ >= input_->size()) {
    remainingLimit_ -= input_->size();
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
