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

#pragma once

#include <string>
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
#include "velox/type/UnscaledLongDecimal.h"
#include "velox/type/UnscaledShortDecimal.h"

namespace facebook::velox {

/// A static class that holds helper functions for DECIMAL type.
class DecimalUtil {
 public:
  static const int128_t kPowersOfTen[LongDecimalType::kMaxPrecision + 1];

  /// Helper function to convert a decimal value to string.
  template <typename T>
  static std::string toString(const T& value, const TypePtr& type);

  template <typename TInput, typename TOutput>
  inline static std::optional<TOutput> rescaleWithRoundUp(
      const TInput inputValue,
      const int fromPrecision,
      const int fromScale,
      const int toPrecision,
      const int toScale,
      const bool nullOnFailure) {
    int128_t rescaledValue = inputValue.unscaledValue();
    auto scaleDifference = toScale - fromScale;
    bool isOverflow = false;
    if (scaleDifference >= 0) {
      isOverflow = __builtin_mul_overflow(
          rescaledValue,
          DecimalUtil::kPowersOfTen[scaleDifference],
          &rescaledValue);
    } else {
      scaleDifference = -scaleDifference;
      const auto scalingFactor = DecimalUtil::kPowersOfTen[scaleDifference];
      rescaledValue /= scalingFactor;
      int128_t remainder = inputValue.unscaledValue() % scalingFactor;
      if (inputValue.unscaledValue() >= 0 && remainder >= scalingFactor / 2) {
        ++rescaledValue;
      } else if (remainder <= -scalingFactor / 2) {
        --rescaledValue;
      }
    }
    // Check overflow.
    if (rescaledValue < -DecimalUtil::kPowersOfTen[toPrecision] ||
        rescaledValue > DecimalUtil::kPowersOfTen[toPrecision] || isOverflow) {
      if (nullOnFailure) {
        return std::nullopt;
      }
      VELOX_USER_FAIL(
          "Cannot cast DECIMAL '{}' to DECIMAL({},{})",
          DecimalUtil::toString<TInput>(
              inputValue, DECIMAL(fromPrecision, fromScale)),
          toPrecision,
          toScale);
    }
    if constexpr (std::is_same_v<TOutput, UnscaledShortDecimal>) {
      return UnscaledShortDecimal(static_cast<int64_t>(rescaledValue));
    } else {
      return UnscaledLongDecimal(rescaledValue);
    }
  }
};
} // namespace facebook::velox
