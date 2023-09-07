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
  // NoStop: The compare doesn't stop at null.
  // StopAtNull: The compare returns std::nullopt if null is encountered in rhs
  // or lhs.
  enum class NullHandlingMode { NoStop, StopAtNull };

  // This flag will be ignored if nullHandlingMode is true.
  bool nullsFirst = true;

  bool ascending = true;

  // When true, comparison should return non-0 early when sizes mismatch.
  bool equalsOnly = false;

  NullHandlingMode nullHandlingMode = NullHandlingMode::NoStop;

  bool mayStopAtNull() {
    return nullHandlingMode == CompareFlags::NullHandlingMode::StopAtNull;
  }

  static std::string nullHandlingModeToStr(NullHandlingMode mode) {
    switch (mode) {
      case CompareFlags::NullHandlingMode::NoStop:
        return "NoStop";
      case CompareFlags::NullHandlingMode::StopAtNull:
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
