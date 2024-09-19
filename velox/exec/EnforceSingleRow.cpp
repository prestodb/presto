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
#include "velox/exec/EnforceSingleRow.h"

namespace facebook::velox::exec {

EnforceSingleRow::EnforceSingleRow(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::EnforceSingleRowNode>& planNode)
    : Operator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "EnforceSingleRow") {
  isIdentityProjection_ = true;
}

void EnforceSingleRow::addInput(RowVectorPtr input) {
  auto numInput = input->size();
  VELOX_CHECK_NE(
      numInput, 0, "EnforceSingleRow::addInput received empty set of rows");
  if (input_ == nullptr) {
    VELOX_USER_CHECK_EQ(
        numInput,
        1,
        "Expected single row of input. Received {} rows.",
        numInput);
    input_ = std::move(input);
  } else {
    VELOX_USER_FAIL(
        "Expected single row of input. Received {} extra rows.", numInput);
  }
}

RowVectorPtr EnforceSingleRow::getOutput() {
  if (!noMoreInput_) {
    return nullptr;
  }

  return std::move(input_);
}

void EnforceSingleRow::noMoreInput() {
  if (!noMoreInput_ && input_ == nullptr) {
    // We have not seen any data. Return a single row of all nulls.
    input_ = BaseVector::create<RowVector>(outputType_, 1, pool());
    for (auto& child : input_->children()) {
      child->resize(1);
      child->setNull(0, true);
    }
  }
  Operator::noMoreInput();
}

bool EnforceSingleRow::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}
} // namespace facebook::velox::exec
