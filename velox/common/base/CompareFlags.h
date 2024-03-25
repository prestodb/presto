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
#include <fmt/core.h>
#include <optional>
#include <sstream>
#include <string>

#pragma once

namespace facebook::velox {

constexpr auto kIndeterminate = std::nullopt;

// Describes value collation in comparison.
struct CompareFlags {
  bool nullsFirst = true;

  bool ascending = true;

  // When true, comparison should return non-0 early when sizes mismatch.
  bool equalsOnly = false;

  enum class NullHandlingMode {

    /// The default null handling mode where nulls are treated as values such
    /// that:
    ///    - null == null is true,
    ///    - null == value is false.
    ///    - when equalsOnly=false null ordering is determined using the
    ///    nullsFirst flag.
    kNullAsValue,

    /// Presto semantics for handling nulls.
    /// It matches the behavior of ==, >, < functions and many other Presto
    /// functions such as array_remove and array_contains.
    ///
    /// Under this mode, result of comparison can be indeterminate.
    /// Such result is represented as std::nullopt and means that the
    /// function can not decide on the result of the comparison due to some
    /// existing nulls. Not every null results in indeterminate result.
    ///
    /// ## When equalsOnly=true:
    /// The compare can return kIndeterminate, or a value according to the
    /// following:
    ///   1. Primitive types and top level nulls:
    ///     - Comparing null with anything else is indeterminate, otherwise a
    ///       value result is returned.
    ///
    ///     - Comparing top level nulls in complex types is indeterminate.
    ///
    ///   2. Arrays:
    ///     - If the compared array sizes are different then result is false,
    ///       ex:[null] == [null, null] is false.
    ///
    ///      - If any two elements compare result is false, result is false
    ///        ex: [null, 1] = [null, 2] is false.
    ///
    ///      - If result is not false and any two elements compared result is
    ///        indeterminate, then result is indeterminate.
    ///        ex: [null, 1] = [null, 1] is indeterminate.
    ///
    ///      - If elements compare results are true, then result is true.
    ///        ex: [1, 1] = [1, 1] is true.
    ///
    ///   3. Rows: Follows the same logic as array, with fields being elements.
    ///     - If any two fields compare result is false, result is false
    ///       ex: (null, 1) = (null, 2) is false.
    ///
    ///     - If result is not false and any two fields compare result is
    ///       indeterminate, then the result is indeterminate.
    ///       ex: (null, 1) = (null, 1) is indeterminate.
    ///
    ///       - If all fields compare results are true, then result is true.
    ///       ex: (1, 1) = (1, 1) is indeterminate.
    ///
    ///   4. Maps:
    ///     - Keys are compared first, if keys are not equal values are not
    ///       checked even if they have nulls and result is false.
    ///
    ///    - If keys are the same for all maps, values are compared by applying
    ///      the array compare logic explained above observing each map as an
    ///      array of values.
    ///      ex: {1:null, 2:2} = {1:null, 2:3} is false.
    ///      ex: {1:null, 2:2} = {1:null, 2:2} is indeterminate.
    ///      ex: {1:1, 2:2} = {1:1, 2:2} is true.
    ///
    /// ## When equalsOnly=false:
    /// The compare either returns a value or throws a user error if a null is
    /// encountered before result is determined, as explained below:
    ///
    ///   1. Primitive types and top level nulls:
    ///     - Comparing null with anything else throws (note that functions like
    ///       >, < do not pass nulls to compare since they are default nulls
    ///       functions).
    ///
    ///     - Comparing top level nulls also throws.
    ///
    ///   2. Arrays:
    ///     - Only elements up to index min(rhs.size(), lhs.size()) are
    ///     compared.
    ///
    ///     - Elements are compared in order starting from index 0.
    ///
    ///     - If all elements in the range above are the same, then sizes are
    ///       compared.
    ///       ex: [1, null] > [1] -> result is true, and null is not read.
    ///
    ///     - If any two elements are different but not null, then result is
    ///       determined and remaining elements are not considered for
    ///       comparison.
    ///       ex: [1, null] > [2, null] -> result is false and nulls are not
    ///       compared.
    ///
    ///     - If a null element is encountered before result is determined then
    ///       the compare throws a user exception.
    ///       ex: [1, null, null] > [1, 2]-> throws.
    ///
    ///   3. Rows:
    ///     - Fields are compared in order starting from index 0.
    ///
    ///     - If any two fields are different but not null, then result is
    ///       determined and remaining elements are not considered for
    ///       comparison.
    ///       ex: (1, null) > (2, null) -> result is false and nulls not
    ///       compared.
    ///
    ///     - If a null element is encountered before result is determined then
    ///       the compare throws a user exception.
    ///       ex: (1, null) > (1, 2) -> throws.
    ///
    ///   4. Maps:
    ///     - This mode does not allow ordering maps.
    kNullAsIndeterminate
  };

  NullHandlingMode nullHandlingMode = NullHandlingMode::kNullAsValue;

  bool nullAsValue() const {
    return nullHandlingMode == CompareFlags::NullHandlingMode::kNullAsValue;
  }

  // Helper method to construct compare flags with equalsOnly = true, in that
  // case nullsFirst and ascending are not needed.
  static constexpr CompareFlags equality(NullHandlingMode nullHandlingMode) {
    return CompareFlags{
        .equalsOnly = true, .nullHandlingMode = nullHandlingMode};
  }

  static std::string nullHandlingModeToStr(NullHandlingMode mode) {
    switch (mode) {
      case CompareFlags::NullHandlingMode::kNullAsValue:
        return "NullAsValue";
      case CompareFlags::NullHandlingMode::kNullAsIndeterminate:
        return "NullAsIndeterminate";
      default:
        return fmt::format(
            "Unknown Null Handling mode {}", static_cast<int>(mode));
    }
  }

  std::string toString() const {
    return fmt::format(
        "[NullFirst[{}] Ascending[{}] EqualsOnly[{}] NullHandleMode[{}]]",
        nullsFirst,
        ascending,
        equalsOnly,
        nullHandlingModeToStr(nullHandlingMode));
  }
};

} // namespace facebook::velox
