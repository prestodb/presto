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
VELOX_UDF_BEGIN(bitwise_and)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a, T b) {
  result = a & b;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_not)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a) {
  result = ~a;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_or)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a, T b) {
  result = a | b;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_xor)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T a, T b) {
  result = a ^ b;
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(bitwise_arithmetic_shift_right)
FOLLY_ALWAYS_INLINE
#if defined(__clang__)
    __attribute__((no_sanitize("integer")))
#endif
    bool call(int64_t& result, T number, T shift) {
  VELOX_USER_CHECK_GE(shift, 0, "Shift must be positive")
  result = number >> shift;
  return true;
}
VELOX_UDF_END();

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
VELOX_UDF_BEGIN(bitwise_left_shift)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T number, T shift) {
  if constexpr (std::is_same<T, int8_t>::value) {
    return bitwiseLeftShift<T, 8>(result, number, shift);
  } else if constexpr (std::is_same<T, int16_t>::value) {
    return bitwiseLeftShift<T, 16>(result, number, shift);
  } else if constexpr (std::is_same<T, int32_t>::value) {
    return bitwiseLeftShift<T, 32>(result, number, shift);
  } else {
    return bitwiseLeftShift<T, 64>(result, number, shift);
  }
}
VELOX_UDF_END();

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
VELOX_UDF_BEGIN(bitwise_right_shift)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T number, T shift) {
  if constexpr (std::is_same<T, int8_t>::value) {
    return bitwiseRightShift<int8_t, uint8_t, 8>(result, number, shift);
  } else if constexpr (std::is_same<T, int16_t>::value) {
    return bitwiseRightShift<int16_t, uint16_t, 16>(result, number, shift);
  } else if constexpr (std::is_same<T, int32_t>::value) {
    return bitwiseRightShift<int32_t, uint32_t, 32>(result, number, shift);
  } else {
    return bitwiseRightShift<int64_t, uint64_t, 64>(result, number, shift);
  }
}
VELOX_UDF_END();

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
VELOX_UDF_BEGIN(bitwise_right_shift_arithmetic)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, T number, T shift) {
  if ((uint32_t)shift >= 64) {
    if (number >= 0) {
      result = 0L;
    } else {
      result = -1L;
    }
    return true;
  }

  // clang-format off
  if constexpr (std::is_same<T, int8_t>::value) {
    result = preserveSign<int8_t, 0b11111111L, 0b10000000L>(number) >> shift;
  } else if constexpr (std::is_same<T, int16_t>::value) {
    result = preserveSign<int16_t, 0b1111111111111111L, 0b1000000000000000L>(number)
              >> shift;
  } else if constexpr (std::is_same<T, int32_t>::value) {
    result = preserveSign<int32_t, 0x00000000ffffffffL, 0x000000000080000000L>(number)
              >> shift;
  } else {
    result = number >> shift;
  }
  // clang-format on

  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(bitwise_logical_shift_right)
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

  VELOX_USER_CHECK(!(bits <= 1 || bits > 64), "Bits must be between 2 and 64");
  VELOX_USER_CHECK_GT(shift, 0, "Shift must be positive");

  result = (number & ((1LL << bits) - 1)) >> shift;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(bitwise_shift_left)
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

  VELOX_USER_CHECK(!(bits <= 1 || bits > 64), "Bits must be between 2 and 64");
  VELOX_USER_CHECK_GT(shift, 0, "Shift must be positive");

  if (shift > 64) {
    result = 0;
  } else {
    result = (number << shift & ((1LL << bits) - 1));
  }

  return true;
}
VELOX_UDF_END();

} // namespace facebook::velox::functions
