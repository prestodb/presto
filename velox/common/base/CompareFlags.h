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
#include <sstream>
#include <string>

#pragma once

namespace facebook::velox {

// Describes value collation in comparison.
struct CompareFlags {
  enum class NullHandlingMode {
    // This is the default null handling mode, in this mode nulls are treated as
    // values such that:
    //  - null == null is true,
    //  - null == value is false.
    //  - when equalsOnly=false null ordering is determined using the nullsFirst
    // flag.
    kNullAsValue,
    kStopAtNull
  };

  bool nullsFirst = true;

  bool ascending = true;

  // When true, comparison should return non-0 early when sizes mismatch.
  bool equalsOnly = false;

  NullHandlingMode nullHandlingMode = NullHandlingMode::kNullAsValue;

  bool mayStopAtNull() const {
    return nullHandlingMode == CompareFlags::NullHandlingMode::kStopAtNull;
  }

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
      case CompareFlags::NullHandlingMode::kStopAtNull:
        return "StopAtNull";
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
