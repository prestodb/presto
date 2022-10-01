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
          "Values") {
  // Drop empty vectors. Operator::getOutput is expected to return nullptr or a
  // non-empty vector.
  values_.reserve(values->values().size());
  for (auto& vector : values->values()) {
    if (vector->size() > 0) {
      values_.emplace_back(vector);
    }
  }
}

RowVectorPtr Values::getOutput() {
  TestValue::notify("facebook::velox::exec::Values::getOutput", &current_);
  if (current_ >= values_.size()) {
    return nullptr;
  }
  return values_[current_++];
}

void Values::close() {
  current_ = values_.size();
}

bool Values::isFinished() {
  return current_ >= values_.size();
}

} // namespace facebook::velox::exec
