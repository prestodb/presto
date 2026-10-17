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

#include "velox/common/future/VeloxPromise.h"
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

  TableFunctionResult(TableFunctionState state)
      : state_(state),
        usedInput_(true),
        result_(nullptr),
        future_(velox::ContinueFuture::makeEmpty()) {
    VELOX_CHECK_EQ(state, TableFunctionState::kFinished);
  }

  TableFunctionResult(bool usedInput, velox::RowVectorPtr result)
      : state_(TableFunctionState::kProcessed),
        usedInput_(usedInput),
        result_(std::move(result)),
        future_(velox::ContinueFuture::makeEmpty()) {}

  /// Creates a kBlocked result. The function uses this to signal that it is
  /// waiting on an asynchronous dependency. The operator hands 'future' to the
  /// Driver, which parks the driver thread until the future is realized, and
  /// then calls apply() again.
  /// The result owns the future, so the function must not retain a reference to
  /// it after returning.
  explicit TableFunctionResult(velox::ContinueFuture future)
      : state_(TableFunctionState::kBlocked),
        usedInput_(false),
        result_(nullptr),
        future_(std::move(future)) {
    VELOX_CHECK(
        future_.valid(), "A kBlocked TableFunctionResult needs a valid future");
  }

  TableFunctionResult::TableFunctionState state() const {
    return state_;
  }

  bool usedInput() const {
    return usedInput_;
  }

  [[nodiscard]] velox::RowVectorPtr result() const {
    return result_;
  }

  /// Moves the future out of the result. Can be called only once and only on a
  /// kBlocked result.
  [[nodiscard]] velox::ContinueFuture takeFuture() {
    VELOX_CHECK(state_ == TableFunctionState::kBlocked);
    VELOX_CHECK(future_.valid(), "The future has already been taken");
    return std::move(future_);
  }

 private:
  TableFunctionState state_;

  bool usedInput_;
  velox::RowVectorPtr result_;

  velox::ContinueFuture future_;
};

} // namespace facebook::presto::tvf

template <>
struct fmt::formatter<
    facebook::presto::tvf::TableFunctionResult::TableFunctionState>
    : formatter<int> {
  auto format(
      facebook::presto::tvf::TableFunctionResult::TableFunctionState s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
