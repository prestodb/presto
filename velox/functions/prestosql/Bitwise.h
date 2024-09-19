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
        bits);
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
  // Only support bigint inputs.
  FOLLY_ALWAYS_INLINE void
  call(int64_t& result, int64_t number, int64_t shift) {
    VELOX_USER_CHECK_GE(shift, 0, "Shift must be non-negative");
    if (shift >= 63) {
      if (number >= 0) {
        result = 0;
      } else {
        result = -1;
      }
    } else {
      result = number >> shift;
    }
  }
};

template <typename TExec>
struct BitwiseLeftShiftFunction {
  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, T number, int32_t shift) {
    static constexpr uint32_t kMaxShift = sizeof(T) * 8;

    // Return zero if 'shift' is negative or exceeds number of bits in T.
    if ((uint32_t)shift >= kMaxShift) {
      result = 0;
    } else {
      result = (number << shift);
    }
  }
};

template <typename TExec>
struct BitwiseRightShiftFunction {
  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, T number, int32_t shift) {
    static constexpr uint32_t kMaxShift = sizeof(T) * 8;

    // Return zero if 'shift' is negative or exceeds number of bits in T.
    if ((uint32_t)shift >= kMaxShift) {
      result = 0;
    } else {
      result = ((std::make_unsigned_t<T>)number) >> shift;
    }
  }
};

namespace detail {
template <typename T, uint64_t MASK, uint64_t SIGNED_BIT>
FOLLY_ALWAYS_INLINE int64_t preserveSign(T number) {
  if ((number & SIGNED_BIT) != 0) {
    return (number | ~MASK);
  } else {
    return (number & MASK);
  }
}
} // namespace detail

template <typename TExec>
struct BitwiseRightShiftArithmeticFunction {
  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, T number, int32_t shift) {
    if ((uint32_t)shift >= 64) {
      if (number >= 0) {
        result = 0L;
      } else {
        result = -1L;
      }
      return;
    }

    // clang-format off
    if constexpr (std::is_same_v<T, int8_t>) {
      result = detail::preserveSign<int8_t, 0b11111111L, 0b10000000L>(number) >> shift;
    } else if constexpr (std::is_same_v<T, int16_t>) {
      result = detail::preserveSign<int16_t, 0b1111111111111111L, 0b1000000000000000L>(number)
                >> shift;
    } else if constexpr (std::is_same_v<T, int32_t>) {
      result = detail::preserveSign<int32_t, 0x00000000ffffffffL, 0x000000000080000000L>(number)
                >> shift;
    } else {
      result = number >> shift;
    }
    // clang-format on
  }
};

template <typename T>
struct BitwiseLogicalShiftRightFunction {
  FOLLY_ALWAYS_INLINE void
#if defined(__clang__)
      __attribute__((no_sanitize("integer")))
#endif
      call(int64_t& result, int64_t number, int64_t shift, int64_t bits) {
    // Presto defines this only for bigint, thus we will define this only for
    // int64_t.
    if (bits == 64) {
      if (number < 0) {
        // >> operator may perform an arithmetic shift right for signed
        // integers, depending on the compiler, which gives wrong result when
        // the input is negative. To ensure a logical shift, we cast it to
        // uint64_t.
        uint64_t unsignedNumber = static_cast<uint64_t>(number) >> shift;
        result = static_cast<int64_t>(unsignedNumber);
        return;
      }
      result = number >> shift;
      return;
    }

    VELOX_USER_CHECK(
        !(bits <= 1 || bits > 64), "Bits must be between 2 and 64");
    VELOX_USER_CHECK_GE(shift, 0, "Shift must be non-negative");

    result = (number & ((1LL << bits) - 1)) >> shift;
    return;
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
      result = number << shift;
      return true;
    }

    VELOX_USER_CHECK(
        !(bits <= 1 || bits > 64), "Bits must be between 2 and 64");
    VELOX_USER_CHECK_GE(shift, 0, "Shift must be non-negative");

    if (shift > 64) {
      result = 0;
    } else {
      result = (number << shift & ((1LL << bits) - 1));
    }
    return true;
  }
};

} // namespace facebook::velox::functions
