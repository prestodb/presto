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
namespace facebook::velox::functions {

template <typename T>
struct BitwiseAndFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput a, TInput b) {
    result = a & b;
    return true;
  }
};

template <typename T>
struct BitCountFunction {
  static constexpr int kMaxBits = std::numeric_limits<uint64_t>::digits;
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, int64_t num, int32_t bits) {
    VELOX_USER_CHECK(
        bits >= 2 && bits <= kMaxBits,
        "Bits specified in bit_count must be between 2 and 64, got {}",
        bits)
    // Check if input "num" falls within the limits of max and min that
    // can be represented with "bits".
    const uint64_t lowBitsMask = 1L << (bits - 1);
    const int64_t upperBound = lowBitsMask - 1;
    VELOX_USER_CHECK(
        num >= ~upperBound && num <= upperBound,
        "Number must be representable with the bits specified."
        " {} can not be represented with {} bits",
        num,
        bits);
    result = bits::countBits(reinterpret_cast<uint64_t*>(&num), 0, bits);
    return true;
  }
};

template <typename T>
struct BitwiseNotFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput a) {
    result = ~a;
    return true;
  }
};

template <typename T>
struct BitwiseOrFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput a, TInput b) {
    result = a | b;
    return true;
  }
};

template <typename T>
struct BitwiseXorFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput a, TInput b) {
    result = a ^ b;
    return true;
  }
};

template <typename T>
struct BitwiseArithmeticShiftRightFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE
#if defined(__clang__)
      __attribute__((no_sanitize("integer")))
#endif
      bool
      call(int64_t& result, TInput number, TInput shift) {
    VELOX_USER_CHECK_GE(shift, 0, "Shift must be positive")
    result = number >> shift;
    return true;
  }
};

namespace {
template <typename T, int MAX_SHIFT>
#if defined(__clang__)
__attribute__((no_sanitize("integer")))
#endif
FOLLY_ALWAYS_INLINE bool
bitwiseLeftShift(int64_t& result, T number, T shift) {
  if ((uint32_t)shift >= MAX_SHIFT) {
    result = 0;
  } else {
    result = (number << shift);
  }
  return true;
}
} // namespace

template <typename T>
struct BitwiseLeftShiftFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput number, TInput shift) {
    if constexpr (std::is_same_v<TInput, int8_t>) {
      return bitwiseLeftShift<TInput, 8>(result, number, shift);
    } else if constexpr (std::is_same_v<TInput, int16_t>) {
      return bitwiseLeftShift<TInput, 16>(result, number, shift);
    } else if constexpr (std::is_same_v<TInput, int32_t>) {
      return bitwiseLeftShift<TInput, 32>(result, number, shift);
    } else {
      return bitwiseLeftShift<TInput, 64>(result, number, shift);
    }
  }
};

namespace {
template <typename T, typename UT, int MAX_SHIFT>
FOLLY_ALWAYS_INLINE bool bitwiseRightShift(int64_t& result, T number, T shift) {
  if ((uint32_t)shift >= MAX_SHIFT) {
    result = 0;
  } else {
    result = ((UT)number >> shift);
  }
  return true;
}
} // namespace

template <typename T>
struct BitwiseRightShiftFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput number, TInput shift) {
    if constexpr (std::is_same_v<TInput, int8_t>) {
      return bitwiseRightShift<int8_t, uint8_t, 8>(result, number, shift);
    } else if constexpr (std::is_same_v<TInput, int16_t>) {
      return bitwiseRightShift<int16_t, uint16_t, 16>(result, number, shift);
    } else if constexpr (std::is_same_v<TInput, int32_t>) {
      return bitwiseRightShift<int32_t, uint32_t, 32>(result, number, shift);
    } else {
      return bitwiseRightShift<int64_t, uint64_t, 64>(result, number, shift);
    }
  }
};

namespace {
template <typename T, uint64_t MASK, uint64_t SIGNED_BIT>
FOLLY_ALWAYS_INLINE int64_t preserveSign(T number) {
  if ((number & SIGNED_BIT) != 0) {
    return (number | ~MASK);
  } else {
    return (number & MASK);
  }
}
} // namespace

template <typename T>
struct BitwiseRightShiftArithmeticFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, TInput number, TInput shift) {
    if ((uint64_t)shift >= 64) {
      if (number >= 0) {
        result = 0L;
      } else {
        result = -1L;
      }
      return true;
    }

    // clang-format off
    if constexpr (std::is_same_v<TInput, int8_t>) {
      result = preserveSign<int8_t, 0b11111111L, 0b10000000L>(number) >> shift;
    } else if constexpr (std::is_same_v<TInput, int16_t>) {
      result = preserveSign<int16_t, 0b1111111111111111L, 0b1000000000000000L>(number)
                >> shift;
    } else if constexpr (std::is_same_v<TInput, int32_t>) {
      result = preserveSign<int32_t, 0x00000000ffffffffL, 0x000000000080000000L>(number)
                >> shift;
    } else {
      result = number >> shift;
    }
    // clang-format on

    return true;
  }
};

template <typename T>
struct BitwiseLogicalShiftRightFunction {
  FOLLY_ALWAYS_INLINE bool
#if defined(__clang__)
      __attribute__((no_sanitize("integer")))
#endif
      call(int64_t& result, int64_t number, int64_t shift, int64_t bits) {
    // Presto defines this only for bigint, thus we will define this only for
    // int64_t.
    if (bits == 64) {
      result = number >> shift;
      return true;
    }

    VELOX_USER_CHECK(
        !(bits <= 1 || bits > 64), "Bits must be between 2 and 64");
    VELOX_USER_CHECK_GT(shift, 0, "Shift must be positive");

    result = (number & ((1LL << bits) - 1)) >> shift;
    return true;
  }
};

template <typename T>
struct BitwiseShiftLeftFunction {
  FOLLY_ALWAYS_INLINE bool
#if defined(__clang__)
      __attribute__((no_sanitize("integer")))
#endif
      call(int64_t& result, int64_t number, int64_t shift, int64_t bits) {
    // Presto defines this only for bigint, thus we will define this only for
    // int64_t.
    if (bits == 64) {
      result = number >> shift;
      return true;
    }

    VELOX_USER_CHECK(
        !(bits <= 1 || bits > 64), "Bits must be between 2 and 64");
    VELOX_USER_CHECK_GT(shift, 0, "Shift must be positive");

    if (shift > 64) {
      result = 0;
    } else {
      result = (number << shift & ((1LL << bits) - 1));
    }
    return true;
  }
};

} // namespace facebook::velox::functions
