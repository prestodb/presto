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
struct BitwiseXorFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a ^ b;
  }
};

template <typename T>
struct BitwiseNotFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a) {
    result = ~a;
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

template <typename T>
struct BitCountFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(int32_t& result, TInput num) {
    constexpr int kMaxBits = sizeof(TInput) * CHAR_BIT;
    auto value = static_cast<uint64_t>(num);
    result = bits::countBits(&value, 0, kMaxBits);
  }
};

template <typename T>
struct BitGetFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(int8_t& result, TInput num, int32_t pos) {
    constexpr int kMaxBits = sizeof(TInput) * CHAR_BIT;
    VELOX_USER_CHECK_GE(
        pos,
        0,
        "The value of 'pos' argument must be greater than or equal to zero.");
    VELOX_USER_CHECK_LT(
        pos,
        kMaxBits,
        "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");
    result = (num >> pos) & 1;
  }
};

} // namespace facebook::velox::functions::sparksql
