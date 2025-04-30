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

/// Function that calculates the factorial of a non-negative integer.
///
/// Supports the factorial of a non-negative integers less than or equal to 20.
/// For example, 5! = 5 × 4 × 3 × 2 × 1 = 120.
///
/// This implementation pre-computes factorials for values 0 to 20 and returns
/// them from a lookup table for efficiency.
template <typename T>
struct FactorialFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int64_t kFactorials[21] = {
      1,
      1,
      2,
      6,
      24,
      120,
      720,
      5040,
      40320,
      362880,
      3628800,
      39916800,
      479001600,
      6227020800L,
      87178291200L,
      1307674368000L,
      20922789888000L,
      355687428096000L,
      6402373705728000L,
      121645100408832000L,
      2432902008176640000L};

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, int32_t input) {
    if (input < 0 || input > 20) {
      return false;
    }
    result = kFactorials[input];
    return true;
  }
};

} // namespace facebook::velox::functions::sparksql
