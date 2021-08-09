/*
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

// Declares constants for null flag polarity.

#include "velox/common/base/BitUtil.h"

namespace facebook::velox::bits {

// Indicates a null value in a nulls bitmap.
constexpr bool kNull = false;
constexpr bool kNotNull = !kNull;

// Use for initialization with memset.
constexpr char kNullByte = 0;
constexpr char kNotNullByte = 0xff;

// Null flags are generally uint64_t for efficient bit counting.
constexpr uint64_t kNull64 = 0UL;
constexpr uint64_t kNotNull64 = (~0UL);

inline bool isBitNull(const uint64_t* bits, int32_t index) {
  return isBitSet(bits, index) == kNull;
}

inline void setNull(uint64_t* bits, int32_t index) {
  clearBit(bits, index);
}

inline void clearNull(uint64_t* bits, int32_t index) {
  setBit(bits, index);
}

inline void setNull(uint64_t* bits, int32_t index, bool isNull) {
  setBit(bits, index, !isNull);
}

inline uint64_t
countNonNulls(const uint64_t* nulls, int32_t begin, int32_t end) {
  return countBits(nulls, begin, end);
}

inline uint64_t countNulls(const uint64_t* nulls, int32_t begin, int32_t end) {
  return (end - begin) - countNonNulls(nulls, begin, end);
}

} // namespace facebook::velox::bits
