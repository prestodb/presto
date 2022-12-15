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
#include "velox/functions/sparksql/Bitwise.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct BitwiseAndFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a & b;
  }
};

template <typename T>
struct BitwiseOrFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a | b;
  }
};

template <typename T>
struct ShiftLeftFunction {
  template <typename TInput1, typename TInput2>
  FOLLY_ALWAYS_INLINE void call(TInput1& result, TInput1 a, TInput2 b) {
    if constexpr (std::is_same_v<TInput1, int32_t>) {
      if (b < 0) {
        b = b % 32 + 32;
      }
      if (b >= 32) {
        b = b % 32;
      }
    }
    if constexpr (std::is_same_v<TInput1, int64_t>) {
      if (b < 0) {
        b = b % 64 + 64;
      }
      if (b >= 64) {
        b = b % 64;
      }
    }
    result = a << b;
  }
};

template <typename T>
struct ShiftRightFunction {
  template <typename TInput1, typename TInput2>
  FOLLY_ALWAYS_INLINE void call(TInput1& result, TInput1 a, TInput2 b) {
    if constexpr (std::is_same_v<TInput1, int32_t>) {
      if (b < 0) {
        b = b % 32 + 32;
      }
      if (b >= 32) {
        b = b % 32;
      }
    }
    if constexpr (std::is_same_v<TInput1, int64_t>) {
      if (b < 0) {
        b = b % 64 + 64;
      }
      if (b >= 64) {
        b = b % 64;
      }
    }
    result = a >> b;
  }
};

void registerBitwiseFunctions(const std::string& prefix) {
  registerBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
  registerBinaryIntegral<BitwiseOrFunction>({prefix + "bitwise_or"});

  registerFunction<ShiftLeftFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftleft"});
  registerFunction<ShiftLeftFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftleft"});

  registerFunction<ShiftRightFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftright"});
  registerFunction<ShiftRightFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftright"});
}

} // namespace facebook::velox::functions::sparksql
