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

#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

/// This class represents the result of processing input by
/// {@link TableFunctionDataProcessor} or {@link TableFunctionSplitProcessor}.
/// It can optionally include a portion of output data in the form of a
/// RowVectorPtr.
/// The returned RowVectorPtr should consist of:
/// -- proper columns produced by the table function
/// -- one column of type {@code BIGINT} for each table function's input table
/// having the pass-through property (see {@link
/// TableArgumentSpecification#isPassThroughColumns}), in order of the
/// corresponding argument specifications. Entries in these columns are the
/// indexes of input rows (from partition start) to be attached to output, or
/// null to indicate that a row of nulls should be attached instead of an input
/// row. The indexes are validated to be within the portion of the partition
/// provided to the function so far. Note: when the input is empty, the only
/// valid index value is null, because there are no input rows that could be
/// attached to output. In such case, for performance reasons, the validation of
/// indexes is skipped, and all pass-through columns are filled with nulls.
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

  TableFunctionResult(velox::ContinueFuture* future)
      : state_(TableFunctionState::kBlocked), future_(future) {}

  TableFunctionResult::TableFunctionState state() const {
    return state_;
  }

  bool usedInput() const {
    return usedInput_;
  }

  [[nodiscard]] velox::RowVectorPtr result() const {
    return result_;
  }

  [[nodiscard]] velox::ContinueFuture* future() const {
    return future_;
  }

 private:
  TableFunctionState state_;

  bool usedInput_;
  velox::RowVectorPtr result_;

  velox::ContinueFuture* future_;
};

} // namespace facebook::presto::tvf
