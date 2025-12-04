/*
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

#pragma once

// #include "velox/core/Expressions.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

class TableFunctionResult {
 public:
  enum class TableFunctionState {
    kBlocked,
    kFinished,
    kProcessed,
  };

  TableFunctionResult(TableFunctionState state) : state_(state) {
    VELOX_CHECK(state == TableFunctionState::kFinished);
  }

  TableFunctionResult(bool usedInput, velox::RowVectorPtr result)
      : state_(TableFunctionState::kProcessed),
        usedInput_(usedInput),
        result_(std::move(result)) {}

  TableFunctionResult::TableFunctionState state() const {
    return state_;
  }

  bool usedInput() const {
    return usedInput_;
  }

  [[nodiscard]] velox::RowVectorPtr result() const {
    return result_;
  }

 private:
  TableFunctionState state_;

  bool usedInput_;
  velox::RowVectorPtr result_;
};

} // namespace facebook::presto::tvf
