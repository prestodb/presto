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
#include "velox/exec/Values.h"
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

Values::Values(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::ValuesNode> values)
    : SourceOperator(
          driverCtx,
          values->outputType(),
          operatorId,
          values->id(),
          "Values"),
      valueNodes_(std::move(values)),
      roundsLeft_(valueNodes_->repeatTimes()) {}

void Values::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(valueNodes_);
  VELOX_CHECK(values_.empty());
  // Drop empty vectors. Operator::getOutput is expected to return nullptr or a
  // non-empty vector.
  values_.reserve(valueNodes_->values().size());
  for (auto& vector : valueNodes_->values()) {
    if (vector->size() > 0) {
      if (valueNodes_->isParallelizable()) {
        // If this is parallelizable, copy the values to prevent Vectors from
        // being shared across threads.  Note that the contract in ValuesNode is
        // that this should only be enabled for testing.
        values_.emplace_back(std::static_pointer_cast<RowVector>(
            vector->copyPreserveEncodings()));
      } else {
        values_.emplace_back(vector);
      }
    }
  }
  // Drop the reference on the value nodes.
  valueNodes_ = nullptr;
}

RowVectorPtr Values::getOutput() {
  TestValue::adjust("facebook::velox::exec::Values::getOutput", this);
  if (current_ >= values_.size()) {
    if (roundsLeft_ > 0) {
      --roundsLeft_;
    }
    current_ = 0;
  }

  if (isFinished()) {
    return nullptr;
  }
  return values_[current_++];
}

void Values::close() {
  current_ = values_.size();
  roundsLeft_ = 0;
}

bool Values::isFinished() {
  return roundsLeft_ == 0;
}

} // namespace facebook::velox::exec
