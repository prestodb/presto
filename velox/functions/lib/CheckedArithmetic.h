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
#include <limits>
#include "CheckedArithmeticImpl.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions {

template <typename T>
struct CheckedPlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = checkedPlus(a, b);
  }
};

template <typename T>
struct CheckedMinusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = checkedMinus(a, b);
  }
};

template <typename T>
struct CheckedMultiplyFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = checkedMultiply(a, b);
  }
};

template <typename T>
struct CheckedDivideFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = checkedDivide(a, b);
  }
};

template <typename T>
struct CheckedModulusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = checkedModulus(a, b);
  }
};

template <typename T>
struct CheckedNegateFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a) {
    result = checkedNegate(a);
  }
};

} // namespace facebook::velox::functions
