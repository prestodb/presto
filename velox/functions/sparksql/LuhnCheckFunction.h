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

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

/// luhn_check(input) -> boolean
///
/// Checks if a given number string is a valid Luhn number.
template <typename T>
struct LuhnCheckFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<Varchar>& input) {
    // Empty string is not a valid Luhn number.
    if (input.empty()) {
      result = false;
      return;
    }

    int32_t checkSum = 0;
    bool isSecond = false;

    for (auto it = input.end(); it != input.begin();) {
      --it;
      if (!std::isdigit(*it)) {
        result = false;
        return;
      }

      const int digit = *it - '0';
      // Double the digit if it's the second digit in the sequence.
      const int doubled = isSecond ? digit * 2 : digit;
      // Add the two digits of the doubled number to the sum.
      checkSum = checkedPlus<int32_t>(checkSum, doubled % 10 + doubled / 10);
      // Toggle the isSecond flag for the next iteration.
      isSecond = !isSecond;
    }

    // Check if the final sum is divisible by 10.
    result = checkSum % 10 == 0;
  }
};

} // namespace facebook::velox::functions::sparksql
