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

#include <functional>
#include <string>
#include "folly/Likely.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions {

template <typename T>
T checkedPlus(const T& a, const T& b) {
  T result;
  bool overflow = __builtin_add_overflow(a, b, &result);
  if (UNLIKELY(overflow)) {
    VELOX_ARITHMETIC_ERROR("integer overflow: {} + {}", a, b);
  }
  return result;
}

template <typename T>
T checkedMinus(const T& a, const T& b) {
  T result;
  bool overflow = __builtin_sub_overflow(a, b, &result);
  if (UNLIKELY(overflow)) {
    VELOX_ARITHMETIC_ERROR("integer overflow: {} - {}", a, b);
  }
  return result;
}

template <typename T>
T checkedMultiply(const T& a, const T& b) {
  T result;
  bool overflow = __builtin_mul_overflow(a, b, &result);
  if (UNLIKELY(overflow)) {
    VELOX_ARITHMETIC_ERROR("integer overflow: {} * {}", a, b);
  }
  return result;
}

template <typename T>
T checkedDivide(const T& a, const T& b) {
  if (b == 0) {
    VELOX_ARITHMETIC_ERROR("division by zero");
  }
  return a / b;
}

template <typename T>
T checkedModulus(const T& a, const T& b) {
  if (UNLIKELY(b == 0)) {
    VELOX_ARITHMETIC_ERROR("Cannot divide by 0");
  }
  return (a % b);
}

template <typename T>
T checkedNegate(const T& a) {
  if (UNLIKELY(a == std::numeric_limits<T>::min())) {
    VELOX_ARITHMETIC_ERROR("Cannot negate minimum value");
  }
  return std::negate<std::remove_cv_t<T>>()(a);
}

} // namespace facebook::velox::functions
