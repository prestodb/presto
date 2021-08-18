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

namespace facebook::velox::dwrf {

template <typename T = int64_t>
class NextVisitorWithNulls {
 public:
  static constexpr bool dense = true;

  NextVisitorWithNulls(uint64_t numValues, T* values)
      : numValues_(numValues), values_(values) {}

  bool allowNulls() {
    return false;
  }

  int32_t start() {
    return 0;
  }

  // Tests for a null value and processes it. If the value is not
  // null, returns 0 and has no effect. If the value is null, advances
  // to the next non-null value in 'rows_'. Returns the number of
  // non-nulls to skip.  If there is no next non-null, sets 'atEnd' .
  FOLLY_ALWAYS_INLINE int32_t
  checkAndSkipNulls(const uint64_t* nulls, int32_t& current, bool& atEnd) {
    uint32_t nullIndex = position_ >> 6;
    uint64_t nullWord = nulls[nullIndex];
    if (!nullWord) {
      return 0;
    }
    uint8_t nullBit = position_ & 63;
    if ((nullWord & (1UL << nullBit)) == 0) {
      return 0;
    }
    // We have a null. We find the next non-null.
    if (++position_ >= numValues_) {
      atEnd = true;
      return 0;
    }
    auto rowOfNullWord = (position_ - 1) - nullBit;
    if (nullBit == 63) {
      nullBit = 0;
      rowOfNullWord += 64;
      nullWord = nulls[++nullIndex];
    } else {
      ++nullBit;
      // set all the bits below the position to null.
      nullWord |= bits::lowMask(nullBit);
    }
    for (;;) {
      volatile int8_t nextNonNull = __builtin_ctzll(~nullWord);
      if (rowOfNullWord + nextNonNull >= numValues_) {
        // Nulls all the way to the end.
        atEnd = true;
        return 0;
      }
      if (nextNonNull < 64) {
        VELOX_CHECK(position_ <= rowOfNullWord + nextNonNull);
        position_ = rowOfNullWord + nextNonNull;
        current = position_;
        return 0;
      }
      rowOfNullWord += 64;
      nullWord = nulls[++nullIndex];
    }
  }

  int32_t processNull(bool& atEnd) {
    ++position_;
    if (position_ == numValues_) {
      atEnd = true;
    }

    return 0;
  }

  int32_t process(int64_t value, bool& atEnd) {
    values_[position_++] = value;
    if (position_ == numValues_) {
      atEnd = true;
    }
    return 0;
  }

 private:
  uint64_t position_ = 0;
  const uint64_t numValues_;
  T* const values_;
};

template <typename T = int64_t>
class NextVisitorNoNulls {
 public:
  static constexpr bool dense = true;

  NextVisitorNoNulls(uint64_t numValues, T* values)
      : numValues_(numValues), values_(values) {}

  bool allowNulls() {
    return false;
  }

  int32_t start() {
    return 0;
  }

  int32_t
  checkAndSkipNulls(const uint64_t* nulls, int32_t& position, bool& atEnd) {
    return 0;
  }

  int32_t processNull(bool& atEnd) {
    ++position_;
    if (position_ == numValues_) {
      atEnd = true;
    }
    return 0;
  }

  int32_t process(int64_t value, bool& atEnd) {
    values_[position_++] = value;
    if (position_ == numValues_) {
      atEnd = true;
    }
    return 0;
  }

 private:
  uint64_t position_ = 0;
  const uint64_t numValues_;
  T* const values_;
};

} // namespace facebook::velox::dwrf
